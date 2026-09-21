# Stream Balancing & Peer Migration Operations

Stream balancing and peer migration in NATS JetStream allow operators or cluster self-healing mechanisms to rebalance stream replicas across cluster nodes, evacuate streams from degrading hardware, or adjust stream placement according to tag rules. This document details the trigger pathways, high-level conceptual flow, Go runtime mechanics, and operational performance impact during stream balancing and migration across both **`FileStore` (Disk)** and **`MemStore` (Memory)** storage modes.

---

## 1. Triggers & Operator Actions

Stream balancing or migration can be initiated through three distinct pathways:

### 1.1 Trigger Comparison & Required Operator Actions

| Trigger Pathway | Initiator | Required Operator Action | Operational Outcome |
| :--- | :--- | :--- | :--- |
| **Manual Migration** | Operator / CLI | Run `nats stream cluster peer migrate` or `nats stream cluster peer balance` | Reallocates stream replicas to target node set or rebalances replicas across cluster |
| **Placement Tag Update** | Operator | Update `Placement.Tags` in stream configuration | Re-homes stream replicas onto cluster nodes matching updated tags |
| **Auto Self-Healing** | Meta Leader (`$JS.META`) | **Zero operator action required** | Automatically detects node evacuation/drain or imbalance and migrates stream replicas |

---

## 2. Layer 1: High-Level Conceptual Flow & Working Principles

```mermaid
sequenceDiagram
    autonumber
    actor Op as Operator / Client
    participant Meta as "Meta Leader ($JS.META)"
    participant Leader as Active Stream Leader
    participant Target as Target Candidate Peer(s)
    participant OldNode as Evicted Old Peer(s)

    Op->>Meta: 1. Trigger Stream Migration / Rebalance
    Note over Meta: Calculates target peer set (Desired.Move = true)
    Meta->>Meta: Propose updated streamAssignment to WAL
    Meta->>Leader: Broadcast committed assignment update
    Leader->>Leader: 2. Install Raft Snapshot & call extendPeerSet()
    Leader->>Target: ProposeAddPeer(targetNodeID)
    Note over Target: Local Init: Creates storage (FileStore directory / MemStore RAM) & spawns Follower
    Leader->>Target: 3. Stream S2 Compressed Snapshot & WAL Catch-up
    Note over Target: Catch-up Sync: Leader reads storage & streams S2 chunks to Target
    Target-->>Leader: Catchup complete (catchups == 0)
    Leader->>Leader: 4. Execute StepDown(preferred) if Leader is being evacuated
    Leader->>Target: Transfer leadership to caught-up target
    Leader->>OldNode: 5. ProposeRemovePeer(oldNodeID)
    Note over OldNode: Eviction & Cleanup: Stops raftNode & purges storage (os.RemoveAll / Go GC)
```

### 2.1 Working Principles

- **Phased Overlap Expansion ($A \rightarrow A \cup B \rightarrow B$)**: The stream peer set temporarily expands from existing peers to the combined peer set while target nodes catch up, ensuring the stream is never under-replicated.
- **Strict Quorum Protection during Migration**: Old nodes continue serving quorum votes while target nodes download snapshots in the background. Old nodes are only evicted after target nodes are 100% caught up.
- **Single Inflight Reconfiguration Guard (`osa.moveInFlight()`)**: NATS strictly forbids overlapping move or scale operations per stream to prevent cluster state drift.
- **Storage-Specific Migration Behavior**:
  - **`FileStore` (Disk)**: Target node streams snapshot chunks directly to disk block files (`1.blk`). Memory footprint remains constant; performance is bound by disk I/O write throughput. Upon eviction, old nodes purge disk block files using `os.RemoveAll`.
  - **`MemStore` (Memory)**: Target node streams snapshot chunks into RAM memory structures. Target node experiences a rapid Go heap memory allocation spike; performance is bound by network throughput and RAM bus speed. Upon eviction, RAM memory is reclaimed via Go Garbage Collection (`debug.FreeOSMemory()`).
- **Zero-Downtime Leadership Transition**: If the active Stream Leader itself is being migrated, it executes `StepDown(preferred)` to hand over leadership to a caught-up target replica *before* evicting old nodes.

---

## 3. Layer 2: Go Runtime Implementation Mechanics

### 3.1 Step 1: Ingress & Meta Desired State Proposal

- **Primary Source File**: `server/jetstream_cluster.go`
- **Function**: `s.jsClusteredStreamUpdateRequestLocked()`
- **Goroutine Context**: API Handler Goroutine

1. Detects `isMoveRequest == true`.
2. Calls `cc.selectPeerGroup()` to select new candidate target nodes based on disk/memory availability, liveness, and placement tags.
3. Wraps target peer set into `rg = osa.Group.withDesired(rg)` with `rg.Desired.Move = true`.
4. Calls `meta.Propose(encodeUpdateStreamAssignment(sa))` to commit the assignment into the `$JS.META` Raft log.

### 3.2 Step 2: Stream Migration Runner & Snapshot Installation (`js.runStreamMigration`)

- **Primary Source File**: `server/jetstream_cluster.go`
- **Function**: `js.runStreamMigration()`
- **Goroutine Context**: Stream Leader Worker Goroutine

1. Stream Leader reads `sa.Group.desiredSnapshot(leaderTerm)`.
2. Verifies `n.NeedSnapshot()`. Flushes pending writes (`mset.flushAllPending()`) and installs snapshot via `n.InstallSnapshot(mset.stateSnapshot(), true)`.
3. Calls `s.extendPeerSet(n, ...)` -> `n.ProposeAddPeer(add)` to add target candidate nodes to the Raft group:
   - **FileStore Mode**: Instantiates `fileStore` (`server/filestore.go`), creating active storage directories on disk.
   - **MemStore Mode**: Instantiates `memStore` (`server/memstore.go`), allocating in-memory message data structures.

### 3.3 Step 3: Background Catch-up & Quorum Guard (`mset.catchupPeers`)

- **Primary Source File**: `server/jetstream_cluster.go`
- **Function**: `mset.catchupPeers()`
- **Goroutine Context**: Sync Worker Goroutines

1. Target node is tracked in `catchups := mset.catchupPeers()`.
2. Leader streams compressed S2 snapshot blocks over `$SYS.RAFT.<group_id>.A`:
   - **FileStore Mode**: Writes incoming data chunks directly to disk block files (`1.blk`) and updates sparse disk index tables.
   - **MemStore Mode**: Appends incoming message byte arrays directly into Go slice structures in RAM.
3. `runStreamMigration()` delays peer eviction while `len(catchups) > 0`.

### 3.4 Step 4: StepDown & Old Peer Eviction (`s.removeEvictedPeers`)

- **Primary Source File**: `server/jetstream_cluster.go`
- **Function**: `s.removeEvictedPeers()`
- **Goroutine Context**: Stream Leader Worker Goroutine

1. Once `catchups` is empty, `removeEvictedPeers()` identifies old nodes no longer in `desiredPeers`.
2. If the Leader itself is being evicted, it calls `n.StepDown(preferred)` to hand over leadership first.
3. Calls `n.ProposeRemovePeer(remove)` to evict old nodes one by one.
4. Evicted node receives notification, stops `raftNode`, and purges its storage directory:
   - **FileStore Mode**: Deletes block files (`1.blk`) and purges directory from disk via `mset.store.Delete()`.
   - **MemStore Mode**: Purges in-memory byte slice structures and releases RAM to Go Garbage Collection.

---

## 4. Operational & Performance Impact During Operation

| Operational Dimension | `FileStore` (Disk Storage Mode) | `MemStore` (Memory Storage Mode) |
| :--- | :--- | :--- |
| **Client Publish Availability** | **Zero Impact**; active quorum processes ACKs while target nodes catch up in background | **Zero Impact**; active quorum processes ACKs while target nodes catch up in background |
| **Network Bandwidth** | **Moderate to High**; Leader streams compressed S2 snapshot chunks over `$SYS.RAFT.<group_id>.A` to target node | **Extremely High**; Leader streams full in-memory message history over network; bounded by network interface card (NIC) throughput |
| **Leader Resource Impact** | **Moderate Disk & CPU**; Leader reads block files (`1.blk`) sequentially from disk and compresses chunks using S2 | **Moderate CPU & RAM**; Leader reads message slices directly from RAM and compresses chunks using S2 |
| **Target Node Resource Impact**| **High Disk I/O**; Target node writes incoming snapshot chunks directly to disk block files (`1.blk`) | **High Memory (RAM) Spike**; Target node allocates Go heap memory rapidly to store incoming stream history |
| **Primary System Bottleneck**| **Disk I/O Write Bandwidth & IOPS** | **Network Throughput & Memory Allocation (GC)** |
| **Old Node Eviction & Cleanup**| **OS Disk Space Reclaimed**; Filesystem block files (`1.blk`) deleted via `os.RemoveAll()` | **RAM Reclaimed via Go GC**; In-memory data structures released to Go Garbage Collector |
| **Transitional Space Overhead**| **Temporary Disk Double-Allocation**; Stream storage footprint is doubled across old and target disks ($A \cup B$) until eviction | **Temporary RAM Double-Allocation**; Stream memory footprint is doubled across old and target RAM ($A \cup B$) until eviction |