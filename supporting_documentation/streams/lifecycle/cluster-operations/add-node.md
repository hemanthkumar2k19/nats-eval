# Adding New Nodes & Stream Replica Scaling

Adding new nodes or scaling up stream replicas in NATS JetStream allows clusters to expand fault tolerance ($R > 1$) and automatically recover from node failures. This document details the trigger pathways, high-level conceptual flow, Go runtime mechanics, and operational performance impact during node addition across both **`FileStore` (Disk)** and **`MemStore` (Memory)** storage modes.

---

## 1. Triggers & Operator Actions

Stream replica scale-up or peer addition can be initiated through three distinct pathways:

| Trigger Pathway | Initiator | Required Operator Action | Operational Outcome |
| :--- | :--- | :--- | :--- |
| **Manual Scale-Up** | Client / Operator | Run `nats stream update <stream> --replicas=N` or issue API update | Increases stream replication factor and selects optimal candidate nodes across cluster |
| **Manual Peer Addition** | Operator | Run `nats stream cluster peer add <stream> <node_id>` | Directs replica allocation to a specific target node |
| **Auto Self-Healing** | Meta Leader (`$JS.META`) | **Zero operator action required** | Meta Leader automatically detects node death, allocates replacement node, and restores replica count |

---

## 2. Layer 1: High-Level Conceptual Flow & Working Principles

```mermaid
sequenceDiagram
    autonumber
    actor Op as Operator / Client
    participant Meta as "Meta Leader ($JS.META)"
    participant Target as Target Node
    participant Leader as Stream Leader
    participant Quorum as Stream Raft Quorum

    Op->>Meta: 1. Trigger Scale-Up / Peer Addition API
    Note over Meta: Checks quotas & ranks nodes via selectPeerGroup()
    Meta->>Meta: Propose updated streamAssignment to WAL
    Meta->>Target: 2. Broadcast committed assignment
    Note over Target: Local Init: Creates storage (FileStore directory / MemStore RAM) & spawns Follower
    Target->>Leader: Join stream Raft bus
    Leader->>Leader: 3. Detect target peer & call ProposeAddPeer()
    Leader->>Quorum: Replicate EntryAddPeer entry
    Leader->>Target: 4. Stream S2 Compressed Snapshot & WAL Catch-up
    Note over Target: Catch-up Sync: Leader reads storage & streams S2 chunks to Target
    Target-->>Leader: Catchup complete (appliedIndex == commitIndex)
    Leader->>Leader: 5. Recalculate Quorum (recalcQuorum)
    Leader->>Quorum: Promote Target to full voting member (Q expanded)
```

### 2.1 Working Principles

- **Non-Blocking Catch-up**: The catching-up node receives stream snapshot data in the background while the active Stream Leader continues processing live client publishes without interruption.
- **Quorum Isolation (`catchupPeers`)**: Target nodes starting with empty storage (`pindex = 0`) are placed into an isolated catch-up pool and strictly excluded from quorum voting. This prevents lagging nodes from delaying client publishes or reducing write availability.
- **S2 Snapshot Compression**: Snapshot data is compressed using S2 compression before transmission over internal system subjects (`$SYS.RAFT.<group_id>.A`).
- **Storage-Specific Catch-up Behavior**:
  - **`FileStore` (Disk)**: Target node streams snapshot chunks directly to disk block files (`1.blk`) via streaming buffers. Memory footprint remains low and constant; performance is bound by disk write throughput.
  - **`MemStore` (Memory)**: Target node streams snapshot chunks into RAM memory structures. Target node experiences a rapid Go heap memory allocation spike; performance is bound by network throughput and RAM bus speed.
- **Dynamic Quorum Expansion**: Quorum size ($Q = \lfloor R/2 \rfloor + 1$) expands only after data synchronization completes (`appliedIndex == commitIndex`), promoting the node to a full voting member.

---

## 3. Layer 2: Go Runtime Implementation Mechanics

### 3.1 Step 1: Ingress & Meta Assignment

- **Primary Source File**: `server/jetstream_cluster.go`
- **Function**: `s.jsClusteredStreamUpdateRequestLocked()`
- **Goroutine Context**: API Handler Goroutine

1. Detects scale-up condition (`newCfg.Replicas > len(currentPeers)`).
2. Verifies account storage quotas via `js.jsClusteredStreamLimitsCheck()`.
3. Invokes `cc.selectPeerGroup(newCfg.Replicas, ...)` to rank candidate nodes based on available disk/memory, liveness, and placement anti-affinity rules (`uniqueTag`).
4. Wraps target nodes in `rg.withDesired(newRaftGroup)`.
5. Calls `meta.Propose(encodeUpdateStreamAssignment(sa))` to commit the assignment into the `$JS.META` Raft log.

### 3.2 Step 2: Target Node Processing & Local Init

- **Primary Source File**: `server/jetstream_cluster.go`
- **Function**: `js.processStreamAssignment()`
- **Goroutine Context**: Target Server `$JS.META` Apply Loop Goroutine

1. Target node receives the committed `streamAssignment` frame from `$JS.META`.
2. Invokes `js.createStream()` -> `mset.store.Store()`:
   - **FileStore Mode**: Instantiates `fileStore` (`server/filestore.go`), creating active storage directories on disk.
   - **MemStore Mode**: Instantiates `memStore` (`server/memstore.go`), allocating in-memory message data structures.
3. Spawns `mset.node` (Raft node) via `s.createRaftNode()` in **Follower State**.
4. Subscribes to internal stream Raft subjects (`$SYS.RAFT.<group_id>.A` / `$JSC.SYNC.*`).

### 3.3 Step 3: Stream Leader Peer Addition (`ProposeAddPeer`)

- **Primary Source Files**: `server/jetstream_cluster.go` & `server/raft.go`
- **Function**: `runStreamMigration()` -> `s.selectPeerToAdd()` -> `n.ProposeAddPeer(add)`
- **Goroutine Context**: Stream Leader `internalLoop()` / Migration Worker

1. Stream Leader detects the target node listening on Raft system subjects.
2. Calls `n.ProposeAddPeer(add)` (`server/raft.go`).
3. Appends an `EntryAddPeer` log entry to its local WAL (`n.wal`) and replicates to current quorum nodes.
4. Once committed, all group members update their internal `n.peers[newNodeID]` membership map.

### 3.4 Step 4: Snapshot Stream Sync & Non-Blocking Catch-up

- **Primary Source Files**: `server/jetstream_cluster.go` & `server/raft.go`
- **Function**: `calculateSyncRequest()` -> `n.SendSnapshot()` -> `InstallSnapshot()`
- **Goroutine Context**: Stream Leader Sync Worker Goroutine -> Target Node Catchup Worker

1. Because the target node starts with `pindex == 0`, the Leader calculates sync bounds via `calculateSyncRequest()`.
2. Leader streams compressed snapshot blocks over `$SYS.RAFT.<group_id>.A`.
3. Target node calls `InstallSnapshot()`:
   - **FileStore Mode**: Writes incoming data chunks directly to disk block files (`1.blk`) and updates sparse disk index tables.
   - **MemStore Mode**: Appends incoming message byte arrays directly into Go slice structures in RAM.
4. Target node is tracked in `mset.catchupPeers()` and **excluded from quorum voting**.

### 3.5 Step 5: Quorum Expansion (`recalcQuorum`)

- **Primary Source File**: `server/raft.go`
- **Function**: `n.recalcQuorum()`
- **Goroutine Context**: Stream Leader Raft Loop

1. Target node's `appliedIndex` catches up to Leader's `commitIndex`.
2. Leader removes target node from `catchupPeers` list.
3. Leader calls `n.recalcQuorum()`, expanding `n.qn = n.csz/2 + 1` (e.g., $Q = 1 \rightarrow 2$).
4. Target node becomes a full voting member of the stream cluster.

---

## 4. Operational & Performance Impact During Operation

| Operational Dimension | `FileStore` (Disk Storage Mode) | `MemStore` (Memory Storage Mode) |
| :--- | :--- | :--- |
| **Client Publish Latency** | **Zero Impact**; target node is placed in `catchupPeers` and excluded from quorum voting; client writes continue to be committed by active quorum without delay | **Zero Impact**; target node is placed in `catchupPeers` and excluded from quorum voting; client writes continue to be committed by active quorum without delay |
| **Network Bandwidth** | **Moderate to High**; Leader streams compressed S2 snapshot chunks over `$SYS.RAFT.<group_id>.A` to target node | **Extremely High**; Leader streams full in-memory message history over network; bounded by network interface card (NIC) throughput |
| **Leader Resource Impact** | **Moderate Disk & CPU**; Leader reads block files (`1.blk`) sequentially from disk and compresses chunks using S2 | **Moderate CPU & RAM**; Leader reads message slices directly from RAM and compresses chunks using S2 |
| **Target Node Resource Impact**| **High Disk I/O**; Target node writes incoming snapshot chunks directly to disk block files (`1.blk`) | **High Memory (RAM) Spike**; Target node allocates Go heap memory rapidly to store incoming stream history |
| **Primary System Bottleneck**| **Disk I/O Write Bandwidth & IOPS** | **Network Throughput & Memory Allocation (GC)** |
| **Memory Headroom** | **Minimal**; Direct-to-disk streaming prevents heap allocation spikes on target node | **High Demand**; Must have sufficient unreserved RAM to hold 100% of replicated stream content |
| **Post-Restart Recovery** | **Fast / Incremental**; Target node reads local disk block files (`1.blk`) and fetches only missing delta messages | **Slow / Full Sync**; Restart wipes RAM state; node must re-download 100% of stream history from Leader |