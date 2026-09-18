# End-to-End Write Path Architecture

This document details the complete end-to-end write path architecture in NATS JetStream. It is structured into two distinct layers: **Layer 1** provides a conceptual, protocol-level overview of the architectural design principles and execution flow without code specifics; **Layer 2** provides a deep dive into the Go runtime implementation, file locations, goroutine boundaries, and function chains.

---

## 1. Layer 1: Architectural Flow & Working Principles (High-Level View)

The NATS JetStream write path connects client TCP publishers to distributed Raft replicas and stored stream files through a 4-phase pipeline.

```text
[ Client TCP Socket ]
          |
          v
+-------------------------------------------------------------+
| PHASE 1: Network Ingress & Protocol Routing                 |
| - Read NATS protocol frame (PUB subject reply length)       |
| - Resolve subject mapping via internal account sublist      |
+------------------------------+------------------------------+
                               |
                               v
+-------------------------------------------------------------+
| PHASE 2: Stream Queueing & Event Loop Batching              |
| - Buffer incoming message into stream inbound queue         |
| - Wake stream event loop & drain pending batch in 1 slice   |
+------------------------------+------------------------------+
                               |
                               v
+-------------------------------------------------------------+
| PHASE 3: Raft Consensus & Replication Barrier               |
| - Leader verifies stream state, limits, & seals             |
| - Propose entry to Leader Write-Ahead Log (WAL)             |
| - Broadcast AppendEntries to Followers over cluster mesh    |
| - Majority Quorum Reached (R/2 + 1) -> Entry COMMITTED      |
+------------------------------+------------------------------+
                               |
                               v
+-------------------------------------------------------------+
| PHASE 4: Storage Engine Persistence & Egress                |
| - Apply committed log entry to local Stream Storage         |
| - Write block segment, update index & deduplication state   |
| - Send PubAck JSON response to client (Leader Only)         |
| - Signal active consumer workers for real-time delivery     |
+-------------------------------------------------------------+
```

### 1.1 Core Architectural Design Principles

1. **Decoupled Network Ingress & Storage Processing**: Network connection goroutines do not write directly to disk or wait synchronously for Raft consensus under lock. They buffer messages into an inbound stream queue and return immediately to service TCP sockets.
2. **Single-Goroutine Event Loop & Automatic Batching**: Each stream runs a single event loop that drains all accumulated messages from the queue in a single slice. Under heavy load, this automatically converts individual publishes into bulk batch proposals, drastically reducing lock contention and I/O operations.
3. **Strict Post-Consensus State Mutation**: Storage writes (`FileStore` / `MemStore`) occur **only AFTER majority quorum consensus ($Q = \lfloor R/2 \rfloor + 1$) is achieved**. Uncommitted Raft log entries are never written into primary stream data files.
4. **Leader-Only Egress & Follower Silence**: Only the active Raft Leader constructs and flushes `PubAck` responses to client publishers. Follower nodes replicate the WAL and execute state machine writes silently.
5. **Decoupled Consumer Signals**: Stream persistence signals consumer delivery workers asynchronously, waking pull/push consumers without delaying publisher acknowledgements.

---

## 2. Layer 2: Go Runtime & Code Implementation Mechanics (Detailed View)

This section maps the 4-phase write pipeline directly to `nats-server` Go source files, goroutine boundaries, structures, and function calls.

```text
+---------------------------------------------------------------------------------------+
| Phase 1: Ingress (Client Goroutine -> server/client.go)                               |
| c.readLoop() -> c.parse() -> acc.sl.Match() -> processInboundJetStreamMsg()           |
+-------------------------------------------+-------------------------------------------+
                                            |
                                            v
+---------------------------------------------------------------------------------------+
| Phase 2: Queueing (Stream Goroutine -> server/stream.go)                              |
| queueInbound() -> mset.msgs.push() -> internalLoop() -> msgs.pop()                    |
+-------------------------------------------+-------------------------------------------+
                                            |
                                            v
+---------------------------------------------------------------------------------------+
| Phase 3: Consensus (Raft Engine -> server/jetstream_cluster.go & server/raft.go)      |
| processClusteredInboundMsg() -> node.Propose() -> AppendEntries -> Quorum Commit      |
+-------------------------------------------+-------------------------------------------+
                                            |
                                            v
+---------------------------------------------------------------------------------------+
| Phase 4: Storage & Egress (Apply Loop -> server/filestore.go & server/stream.go)      |
| applyStreamEntries() -> fs.StoreRawMsg() -> mset.outq.push() -> mset.sch signal       |
+---------------------------------------------------------------------------------------+
```

### 2.1 Phase 1: Network Ingress & Protocol Parsing

- **Primary Source File**: `server/client.go`
- **Execution Context**: Client Connection Read Loop Goroutine (`c.readLoop()`)

1. **TCP Frame Read**: `c.readLoop()` reads raw NATS protocol bytes from the client TCP connection (`net.Conn`).
2. **Protocol Parsing**: `c.parse()` parses protocol tokens (`PUB <subject> [reply-to] <bytes>\r\n<payload>\r\n`).
3. **Subject Match**: NATS account sublist router (`acc.sl.Match()`) matches the published subject against the stream's internal system subscription.
4. **Inbound Callback**: Invokes the stream's inbound message callback `processInboundJetStreamMsg()`.

### 2.2 Phase 2: Stream Event Loop & Inbound Queueing

- **Primary Source File**: `server/stream.go`
- **Execution Context**: Handed off from Client Goroutine to Stream Dedicated Goroutine

1. **Message Envelope Packaging**: `processInboundJetStreamMsg()` constructs a `jsPubMsg` struct containing message payload, headers, subject, arrival timestamp, and client reply inbox.
2. **Queue Push**: `queueInbound()` calls `mset.msgs.push(jsMsg)` (`server/ipqueue.go`). If the queue was empty, a signal (`struct{}{}`) is pushed to `msgs.ch`.
3. **Event Loop Wake**: The stream's single dedicated event loop (`internalLoop()`) unblocks at `case <-msgs.ch:`.
4. **Batch Slice Drain**: `internalLoop()` calls `ims := msgs.pop()`, draining **all pending messages from the queue in a single slice** for bulk processing.

### 2.3 Phase 3: Raft Proposal & Quorum Consensus

- **Primary Source Files**: `server/jetstream_cluster.go` & `server/raft.go`
- **Execution Context**: Stream `internalLoop` Goroutine + Raft Node Goroutines

1. **Leader Verification & Guard Checks**: `processClusteredInboundMsg()` verifies `mset.isLeader() == true` and validates stream limits (`MaxBytes`, `MaxMsgSize`, sealed status).
2. **Opcode Serialization**: Prepends the 1-byte opcode (`streamMsgOp`) and serializes message payload and headers into an `esm` binary buffer.
3. **Raft Proposal**: Calls `node.Propose(term, esm)` (`server/jetstream_batching.go`), appending the entry to the Leader's local Raft WAL (`n.wal`) as uncommitted.
4. **Replication Broadcast**: The Leader broadcasts `AppendEntries` RPCs containing log entry $I$ over `$SYS.RAFT.<group_id>.A` to all follower replicas.
5. **Follower WAL Append**: Followers append entry $I$ to their local `n.wal` as uncommitted and reply with ACKs over `$SYS.RAFT.<group_id>.AR`.
6. **Quorum Commit**: When ACKs reach Majority Quorum ($Q = \lfloor R/2 \rfloor + 1$), the Leader advances its commit index (`commitIndex = I`). Entry $I$ is now **COMMITTED**.

### 2.4 Phase 4: Storage Engine Persistence & Egress

- **Primary Source Files**: `server/filestore.go` & `server/stream.go`
- **Execution Context**: Raft Apply Loop Goroutine (`applyStreamEntries`) on ALL quorum nodes

1. **Apply Channel Consumption**: The Raft engine notifies `applyStreamEntries()` -> `applyStreamMsgOp()` -> `processJetStreamMsg()`.
2. **Storage Engine Write**: Calls `fs.StoreRawMsg()` (`server/filestore.go`):
   - **Block Segment Write**: Appends binary record `[magic | sequence | timestamp | subject_len | hdr_len | payload_len | payload]` to active block file (e.g., `1.blk`).
   - **Sparse Index Update**: Updates in-memory index mapping sequence to file offset and updates subject tree state (`psim`).
   - **Deduplication Map**: Updates `mset.ddmap` for `Nats-Msg-Id` duplicate tracking.
3. **Outbound PubAck (Leader Only)**: Only the Raft Leader constructs `PubAck` JSON `{"stream":"ORDERS","seq":101}` and pushes it to `mset.outq` (`ipQueue`), flushing over TCP back to the client socket.
4. **Consumer Notification Signal**: The stream pushes a signal to `mset.sch` / `mset.sigq` (`server/stream.go`), waking JetStream consumer delivery workers to deliver the message to subscribers.

---

## 3. Summary Execution Matrix

| Pipeline Phase | Architectural Boundary | Core Go Source Files | Key Types & Functions | Primary I/O Primitive |
| :--- | :--- | :--- | :--- | :--- |
| **Phase 1: Ingress** | Network -> Account Router | `server/client.go` | `c.readLoop()`, `c.parse()`, `acc.sl.Match()` | TCP Socket (`net.Conn`) |
| **Phase 2: Queueing** | Connection -> Stream Loop | `server/stream.go` | `queueInbound()`, `mset.msgs`, `internalLoop()`, `msgs.pop()` | Lock-free Queue (`ipQueue`) |
| **Phase 3: Consensus** | Stream -> Raft Cluster | `server/jetstream_cluster.go`, `server/raft.go` | `processClusteredInboundMsg()`, `node.Propose()`, `AppendEntries` | Raft WAL (`n.wal`) & System Subjects (`$SYS.RAFT`) |
| **Phase 4: Storage & Egress** | Cluster -> Storage / Consumer | `server/filestore.go`, `server/stream.go` | `applyStreamEntries()`, `fs.StoreRawMsg()`, `mset.outq`, `mset.sch` | Block Files (`1.blk`) & Client TCP Socket |