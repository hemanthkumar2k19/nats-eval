# Cluster Operations & Unified Execution Pipeline

Every cluster operation in NATS JetStream -- whether publishing a message, deleting a message, purging a stream, updating consumer state, or stepping down leadership -- executes through a unified multi-stage Raft pipeline. This document details the 2-tier opcode architecture, the 5-stage operation lifecycle, operation-specific deep dives, atomic batch publishing, stream leadership stepdown flows, and Go runtime loop internals.

---

## 1. Architectural Foundation: The 2-Tier Raft System

NATS JetStream separates management metadata from stream data using two distinct layers of Raft groups:

```text
                  +-------------------------------------------+
                  |    Meta Raft Group ($JS.META)             |
                  |  (Stream/Consumer Creation, Placement,    |
                  |   Cluster Topology, Scale Up/Down)        |
                  +---------------------+---------------------+
                                        | Spawns & Manages
             +--------------------------+--------------------------+
             |                                                     |
             v                                                     v
+--------------------------+                             +--------------------------+
| Stream Raft Group (R=3)  |                             | Stream Raft Group (R=3)  |
|  "ORDERS"                |                             |  "EVENTS"                |
|  Operations:             |                             |  Operations:             |
|  - Publish / Batch Msg   |                             |  - Publish / Batch Msg   |
|  - Delete / Purge Msg    |                             |  - Delete / Purge Msg    |
|  - Ack / Deliver State   |                             |  - Ack / Deliver State   |
+--------------------------+                             +--------------------------+
```

1. **Meta Group (`$JS.META`)**: Controls stream placement, stream creation and deletion (`assignStreamOp`, `removeStreamOp`), and cluster topology.
2. **Stream / Consumer Raft Group (`mset.node`)**: Each replicated stream ($R > 1$) has its own independent Raft group managing data operations on that stream.

---

## 2. 2-Tier Opcode Architecture

NATS operates with two distinct layers of opcodes to cleanly separate Raft cluster protocol mechanics from JetStream stream/consumer state machine operations:

```text
+------------------------------------------------------------------------+
| Layer 1: Low-Level Raft Entry Types (EntryType in raft.go)             |
| (Controls the Raft Cluster Protocol itself)                            |
|  - EntryNormal     : Holds application data (payload below)            |
|  - EntrySnapshot   : Signals a state snapshot compacting the log       |
|  - EntryPeerState  : Peer metadata / term updates                      |
|  - EntryCatchup    : Sent to lagging followers to bring up to date     |
|  - EntryAddPeer    : Node added to cluster                             |
|  - EntryRemovePeer : Node removed from cluster                         |
+-----------------------------------v------------------------------------+
                                    | Carried inside EntryNormal.Data
                                    v
+------------------------------------------------------------------------+
| Layer 2: JetStream Application Ops (entryOp in jetstream_cluster.go)   |
| (Controls Stream and Consumer State Machine)                           |
|  - streamMsgOp / compressedStreamMsgOp                                 |
|  - batchMsgOp / batchCommitMsgOp                                       |
|  - purgeStreamOp / deleteMsgOp / deleteRangeOp                         |
|  - updateDeliveredOp / updateAcksOp / updateSkipOp                     |
|  - assignStreamOp / removeStreamOp / updateStreamOp                   |
+------------------------------------------------------------------------+
```

### 2.1 Catalog of Stream-Cluster Operations (`entryOp`)

All operations executed within a stream Raft group are identified by a 1-byte opcode (`entryOp`):

| Category | `entryOp` Byte Identifier | Operational Meaning |
| :--- | :--- | :--- |
| **Stream Data** | `streamMsgOp` / `compressedStreamMsgOp` | Normal single-message publish |
| **Batch Data** | `batchMsgOp` / `batchCommitMsgOp` | Atomic batch publishing |
| **Stream Management** | `purgeStreamOp` | Purge stream messages (all or subject-filtered) |
| **Message Erasure** | `deleteMsgOp` / `deleteRangeOp` | Delete single message sequence or sequence range |
| **Consumer State** | `updateDeliveredOp` / `updateAcksOp` / `updateSkipOp` | Update consumer message delivery or ACK tracking |
| **Stream Lifecycle** | `assignStreamOp` / `removeStreamOp` / `updateStreamOp` | Meta group commands for stream creation/deletion |
| **Consumer Lifecycle** | `assignConsumerOp` / `removeConsumerOp` | Create / destroy consumer inside stream group |

---

## 3. The Unified 5-Stage Operation Pipeline

Every stream-cluster operation follows the exact same 5-stage Raft lifecycle from initial client request to cluster-wide state machine application:

```text
[ Client / System Request ]
         |
         v
+-------------------------------------------------------------+
| STAGE 1: Leader Verification                                |
| - Node checks: Am I Raft Leader? (isLeader())              |
| - Pre-checks: Sealed? Limits exceeded?                      |
+------------------------------+------------------------------+
                               |
                               v
+-------------------------------------------------------------+
| STAGE 2: Proposal & Serialization                           |
| - Encode Payload: [entryOp byte] + [Operation Data]         |
| - Submit to Raft WAL: node.Propose(term, entryBytes)       |
+------------------------------+------------------------------+
                               |
                               v
+-------------------------------------------------------------+
| STAGE 3: Consensus & Replication                            |
| - Leader sends Raft AppendEntries RPC to Followers          |
| - Followers append to local Raft WAL and acknowledge        |
| - Majority Quorum Reached (R/2 + 1) -> Entry COMMITTED      |
+------------------------------+------------------------------+
                               |
                               v
+-------------------------------------------------------------+
| STAGE 4: State Machine Apply (ALL QUORUM NODES)             |
| - Raft triggers applyStreamEntries() on Leader & Followers  |
| - Read entryOp -> Execute operation on local State Machine  |
|   (e.g., FileStore.WriteMsg / FileStore.RemoveMsg)          |
+------------------------------+------------------------------+
                               |
                               v
+-------------------------------------------------------------+
| STAGE 5: Client Response (LEADER ONLY)                      |
| - Leader checks: canRespond (reply subject present?)        |
| - Formats response & flushes to Client via mset.outq        |
+-------------------------------------------------------------+
```

---

## 4. Operation-Specific Execution Deep Dives

### 4.1 Normal Publishing (`streamMsgOp` / `compressedStreamMsgOp`)

- **Behavior**: Each message published by a client is handled as an independent transaction.
- **Flow**: Client sends Msg A -> Proposed to Raft (`node.Propose`) -> Replicated to Quorum -> Committed -> Written to Stream Store (`FileStore`/`MemStore`) -> `PubAck` sent.
- **Characteristics**: Every message incurs its own Raft consensus proposal and storage sync cycle. If a client publishes 100 individual messages, it triggers 100 distinct Raft commits.

#### Step-by-Step Publish Sequence

```text
[Client]                [Node A (Leader)]             [Node B (Follower)]          [Node C (Follower)]
   |                            |                             |                            |
   | 1. Publish Msg             |                             |                            |
   +--------------------------->|                             |                            |
   |                            | 2. Propose()                |                            |
   |                            |    Assign Index 101, Term 2 |                            |
   |                            |    Write to WAL (Uncommitted)|                           |
   |                            |                             |                            |
   |                            | 3. AppendEntries RPC (101)  |                            |
   |                            +---------------------------->|                            |
   |                            +--------------------------------------------------------->|
   |                            |                             |                            |
   |                            |                             | 4. Write 101 to WAL        |
   |                            |                             |    Send ACK                |
   |                            |<----------------------------+                            |
   |                            |                             |                            |
   |                            | 5. MAJORITY REACHED!        |                            |
   |                            |    (Node A + Node B = 2/3)  |                            |
   |                            |    Index 101 is COMMITTED   |                            |
   |                            |                             |                            |
   |                            | 6. Apply to Stream Storage  | 7. Apply to Stream Storage |
   |                            |    FileStore.WriteMsg(seq)  |    FileStore.WriteMsg(seq) |
   |                            |                             |                            |
   | 8. Send PubAck Response    |                             |                            |
   |<---------------------------+                             |                            |
```

1. **Client Publish Request**: Client sends a message payload to subject `order.created`.
2. **Leader Propose**: Node A verifies leadership (`isLeader()`), wraps message into `streamMsgOp`, calls `node.Propose(term, payload)`, and writes log entry #101 to its local WAL as uncommitted.
3. **Replication**: Node A broadcasts `AppendEntries` RPC containing log entry #101 to Node B and Node C over `$JSC.SYNC.*`.
4. **Follower WAL Write**: Node B receives entry #101, verifies Term 2, appends entry #101 to its local WAL, and sends an ACK to Node A. (Node C is slightly slower on network I/O).
5. **Quorum Commitment**: Node A receives Node B's ACK. With 2 out of 3 nodes (Node A + Node B), Majority Quorum is achieved. Node A advances `commitIndex` to 101. Log entry #101 is COMMITTED.
6. **State Machine Apply**: Raft triggers `applyStreamEntries()` on Node A and Node B. Both decode `streamMsgOp` and invoke `mset.store.StoreRawMsg()`, writing the message to local stream storage and assigning sequence 101.
7. **Client Response**: Node A (Leader) constructs `PubAck` JSON (`{"stream":"ORDERS","seq":101}`) and writes it back to the client socket.

### 4.2 Atomic Batch Publishing (`batchMsgOp` & `batchCommitMsgOp`)

- **Behavior**: A client sends a group of messages under a single batch transaction identifier (`batchId`).
- **Key Mechanics**:
  - **All-Or-Nothing Atomicity**: The entire batch is applied as a single atomic unit. If the server crashes or the batch is interrupted mid-stream, the incomplete batch state is rolled back via `rejectBatchState()`.
  - **Consumer Read Isolation**: Consumers cannot see or fetch any message within an active batch until the full batch commit frame (`batchCommitMsgOp`) has been committed and applied (`mset.isolateMu`).
  - **High Throughput**: Multiple messages share a single Raft commitment and storage write cycle, dramatically reducing per-message IOPS and network overhead.

### 4.3 Message Erasure (`deleteMsgOp` & `deleteRangeOp`)

- **Behavior**: Deletes a single message sequence (`deleteMsgOp`) or a range of sequence numbers (`deleteRangeOp`).
- **Execution**: The leader proposes the delete operation. Upon commit, all nodes execute `mset.store.RemoveMsg(seq)` or `RemoveRange(start, end)`, updating the sequence set (`dmap`) and message/byte count metrics.

### 4.4 Stream Purge (`purgeStreamOp`)

- **Behavior**: Clears all active messages from a stream (or messages matching a specific subject filter).
- **Execution**: The leader proposes `purgeStreamOp`. Upon commit, all nodes invoke `mset.store.Purge()` or `PurgeSubject()`, advancing `FirstSeq` to `LastSeq + 1` and updating subject tree indices (`psim`).

### 4.5 Consumer State Tracking (`updateDeliveredOp`, `updateAcksOp`, `updateSkipOp`)

- **Behavior**: Replicates consumer state changes across the Raft group so consumer progress survives leader failures.
- **Execution**: When a consumer acknowledges a message, the leader proposes `updateAcksOp`. Upon commit, all nodes update the consumer's ACK floor, pending ACK map, and redelivery counts in local storage (`o.dat` or memory consumer store).

---

## 5. Stream Leadership Stepdown Flow

When an administrator or automated system requests a stream leadership transfer (e.g., via `nats stream cluster stepdown ORDERS`), the stepdown request undergoes strict validation before triggering Raft leader transfer.

### 5.1 API Request Handling (`jsStreamLeaderStepDownRequest`)

```text
[ API Request: $JS.API.STREAM.LEADER.STEPDOWN.<stream_name> ]
                            |
                            v
+-----------------------------------------------------------+
| 1. Guard Checks & Account Resolution                      |
|    - Verify client != nil && JetStreamEnabled             |
|    - Resolve target account from subject token            |
+----------------------------+------------------------------+
                            |
                            v
+-----------------------------------------------------------+
| 2. Cluster & Meta Raft Validation                         |
|    - Verify JetStream is clustered (!JetStreamIsClustered) |
|    - Verify Meta Raft has active leader                   |
+----------------------------+------------------------------+
                            |
                            v
+-----------------------------------------------------------+
| 3. Stream Leader Gate                                     |
|    - Is THIS node the active Stream Leader (mset.isLeader)?|
|    - If NO -> Exit silently (only active leader proceeds) |
|    - If YES -> Resolve local Stream Instance              |
+----------------------------+------------------------------+
                            |
                            v
+-----------------------------------------------------------+
| 4. Preferred Placement & StepDown Execution               |
|    - Parse preferred target node if specified in payload  |
|    - Invoke raft.StepDown()                               |
+-----------------------------------------------------------+
```

### 5.2 Internal Raft StepDown Execution (`raft.StepDown`)

1. **Leader Verification**: Under lock, verify the node is still Leader (`n.State() == Leader`).
2. **Preferred Peer Check**: If a preferred target node is specified in the request, verify that the peer is online and healthy (recent heartbeat ACK < 3s).
3. **Fallback Peer Selection**: If the preferred peer is unhealthy or omitted, select the first available healthy follower node.
4. **Leader Transfer Entry**: Broadcast a special `EntryLeaderTransfer` log entry containing the target peer ID directly via `sendAppendEntry()`.
5. **Demote Local State**: Invoke `n.stepdown(noLeader)`, demoting the current node to Follower and resetting election timers.
6. **Target Peer Fast-Track**: The target peer receives `EntryLeaderTransfer` and immediately triggers `CampaignImmediately()` (10ms timer), skipping standard election waits and becoming the new Stream Raft Leader within milliseconds.

---

## 6. Go Runtime Internal Loop Architecture

Inside `nats-server`, each stream runs a single dedicated internal loop goroutine (`internalLoop()`) that processes all stream I/O serially:

```text
+-------------------------------------------------------------------+
| internalLoop() Goroutine (Dedicated per Stream)                   |
|                                                                   |
|  Create internal stream client & inbound message channel          |
|                                                                   |
|  for {                                                            |
|      select {                                                     |
|      case msg := <-inboundChan:                                   |
|          Process inbound message (Publish / API request)          |
|          Drain channel queue for batch processing                 |
|      case <-signalChan:                                           |
|          Handle shutdown / leader stepdown signals                |
|      }                                                            |
|  }                                                                |
+-------------------------------------------------------------------+
```

### 6.1 Design Rationale

- **Single-Threaded Safety**: Operating stream I/O within a single dedicated goroutine eliminates complex lock contention on stream data structures during message writes.
- **Channel Draining**: Under heavy publish load, `internalLoop()` drains all pending messages from `inboundChan` in a single iteration, enabling automatic batching for storage writes and Raft proposals.