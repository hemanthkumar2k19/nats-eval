# Cluster Operations Overview & Administrative Controls

NATS JetStream provides operational controls, safety mechanisms, and administrative interventions to manage stream durability, maintain Raft consensus health, recover from disasters, and execute cluster maintenance. This document covers storage sync policies (`SyncAlways`), emergency quorum overrides (`RescueQuorum`), dynamic membership management (`EvictPeers` / `ProposeAddPeer` / `ProposeRemovePeer`), stream balancing & migration (`runStreamMigration`), apply channel controls (`PauseApply`), non-voting observer mode (`SetObserver`), and automated reconciliation (`meta.reconcile`).

---

## 1. Overview

In a distributed NATS JetStream cluster, operations span three distinct administrative domains:

1. **Storage Durability Controls**: Regulating operating system disk flushing (`fsync`) vs. Raft network replication performance (`FileStore` vs. `MemStore`).
2. **Consensus & Emergency Interventions**: Overriding Raft quorum rules (`RescueQuorum`), scaling stream replicas (`ProposeAddPeer`), evicting peers (`ProposeRemovePeer`), or migrating stream placement (`runStreamMigration`).
3. **Operational State Controls**: Pausing state machine execution (`PauseApply`), running read-only shadow replicas (`SetObserver`), and automated self-healing reconciliation (`meta.reconcile`).

---

## 2. Storage Durability & Sync Controls (`SyncAlways`)

### 2.1 What Is `SyncAlways`

`SyncAlways` is a storage durability setting in `FileStore` that forces the operating system to issue a synchronous disk flush (`fsync()` / `O_SYNC`) to physical storage on **every single message write**.

### 2.2 Why Use `SyncAlways`

Operating systems buffer file writes in volatile RAM (the kernel page cache) to optimize write performance. If a server experiences a sudden hardware power loss or kernel panic before RAM flushes to disk, unflushed data in RAM is lost. `SyncAlways` guarantees single-node power-loss durability by forcing data to non-volatile disk before acknowledging writes.

### 2.3 Standalone vs. Clustered Behavior

- **Standalone Streams (`Replicas = 1`)**: Used when running a single-node stream that cannot afford message loss during an unplanned power outage.
- **Clustered Streams (`Replicas > 1`)**: In clustered streams, NATS **automatically relaxes `SyncAlways` to `SyncOnFlush`** (`filestore.go`). Because Raft replication across independent physical machines already guarantees hardware fault durability via majority quorum, per-message `fsync()` disk I/O latency penalties are eliminated.

### 2.4 Code-Level Mechanics

1. **Configuration**: Enabled globally at server startup (`opts.SyncAlways = true`) or via storage options.
2. **File Open Flags**: When enabled without `SyncOnFlush`, `fileStore` opens underlying block files with OS synchronous flags or issues explicit `fsync()` calls.
3. **Clustered Auto-Tuning**:
   ```go
   // server/filestore.go
   if syncOnFlush {
       fs.syncOnFlush.Store(true)
       fs.syncAlways.Store(false) // Relaxes per-message fsync in favor of Raft network quorum
   }
   ```

---

## 3. Administrative Consensus Interventions & Cluster Operations

### 3.1 Emergency Quorum Override (`RescueQuorum`)

#### What
`RescueQuorum` is an administrative disaster-recovery mechanism in NATS Raft that **manually and temporarily lowers the required quorum size (`n.qn`)** of a stuck Raft group so surviving nodes can elect a leader and resume operations.

#### Why
Under standard Raft rules, a 3-node cluster requires 2 nodes for quorum ($Q = \lfloor 3/2 \rfloor + 1 = 2$). If a catastrophic event permanently destroys 2 out of 3 servers (e.g., a datacenter failure), the 1 remaining node can never reach a 2-node majority. The cluster becomes permanently deadlocked, unable to elect a leader or process writes. `RescueQuorum` breaks this deadlock safely.

#### When to Use
- **Catastrophic Peer Loss Only**: Use ONLY when nodes are permanently destroyed and will not return.
- **Cluster Deadlock**: Use when no leader exists (`n.leader == noLeader`) and automatic elections are stuck.

#### Code-Level Execution & Safety Assertions

1. **Invocation**: The operator executes an admin rescue command via CLI/API, which calls `RescueQuorum(qn)` on the surviving node.
2. **Safety Assertions**:
   - **Rejects if Leader Exists**: `if n.leader != noLeader { return errRescueLeaderKnown }` (Prevents split-brain scenarios).
   - **Rejects if Log Empty**: `if n.pindex == 0 { return errRescueEmptyLog }` (Prevents an empty node from wiping data of nodes that held entries).
3. **Rescue Lifecycle**:
   ```go
   // server/raft.go
   n.rescue = time.AfterFunc(rescueQuorumTimeout, ...) // Set temporary rescue timer
   n.qn = qn                                           // Lower quorum (e.g., 2 -> 1)
   n.resetElect(randCampaignTimeout())                 // Force immediate election
   ```
4. **Recovery Completion**:
   - The surviving node wins the election with 1 vote ($Q = 1$).
   - It becomes Leader and proposes removing the dead peers (`ProposeRemovePeer`).
   - Once dead peers are removed, `recalcQuorum()` recalculates natural quorum ($1/2 + 1 = 1$), permanently clearing the emergency rescue state.

---

### 3.2 Manual Peer Eviction & Scale-Down (`EvictPeers` & `ProposeRemovePeer`)

#### What
An internal administrative API allowing operators or cluster controllers to evict peer nodes out of a stream's Raft group (`raft.go`).

#### Why
If a server node is retired, scaled down ($R=3 \rightarrow R=1$), or permanently lost, its peer ID must be removed from the Raft membership list so it no longer counts against majority quorum calculations ($Q = \lfloor N/2 \rfloor + 1$).

#### Code-Level Flow
1. Operator or Meta-Controller issues `EvictPeers([]string{deadNodeID})` or updates `Replicas`.
2. **Graceful Leader Stepdown Guard**: If the node to be evicted is the active Leader, it calls `n.StepDown(preferred)` to hand over leadership to a non-evicted follower **first**.
3. Leader packages a membership change entry `ProposeRemovePeer(peerID)`.
4. Once committed by remaining peers, all nodes delete the peer from `n.peers` and execute `n.recalcQuorum()`, shrinking quorum size dynamically.
5. **Storage Cleanup**: Evicted node stops its `raftNode` (`n.Stop()`) and purges its storage backend (`FileStore` deletes disk block files via `os.RemoveAll()`; `MemStore` releases RAM to Go Garbage Collection).

---

### 3.3 Peer Scale-Up & Node Addition (`ProposeAddPeer`)

#### What
The operational process of adding a new candidate node to an existing stream Raft group to expand replication ($R=1 \rightarrow R=3$) or replace a failed replica (`jetstream_cluster.go`).

#### Why
Enables clusters to expand stream fault tolerance live without stopping client publishers or taking the stream offline.

#### Code-Level Flow
1. Operator triggers scale-up or Meta Leader runs `cc.selectPeerGroup()` to rank candidates based on disk/memory availability, liveness, and placement anti-affinity rules (`uniqueTag`).
2. Target node receives assignment from `$JS.META`, creates local storage (`FileStore` / `MemStore`), and spawns a `raftNode` in Follower state (`pindex = 0`).
3. Stream Leader calls `n.ProposeAddPeer(newNodeID)` to propose an `EntryAddPeer` log entry to the Raft WAL.
4. **Non-Blocking Catchup (`catchupPeers`)**: Target node starts empty and is placed in `catchupPeers`. It receives compressed S2 snapshot chunks over `$SYS.RAFT` in the background and is **strictly excluded from quorum voting** while catching up.
5. **Quorum Expansion**: Once target node's `appliedIndex == commitIndex`, Leader removes it from `catchupPeers` and calls `n.recalcQuorum()`, expanding quorum size ($Q = \lfloor R/2 \rfloor + 1$).

---

### 3.4 Stream Balancing & Migration Operations (`runStreamMigration`)

#### What
The operational workflow for re-homing stream replicas from an existing peer set $A$ to a target peer set $B$ across cluster nodes (`jetstream_cluster.go`).

#### Why
Used to rebalance stream assets across cluster servers, evacuate streams from degrading hardware, or adjust stream placement according to updated tag rules.

#### Code-Level Flow
1. **Desired State Proposal**: Meta Leader proposes an updated `streamAssignment` with `Group.Desired.Move = true` into the `$JS.META` Raft log.
2. **Phased Overlap Expansion ($A \rightarrow A \cup B \rightarrow B$)**: Stream Leader calls `extendPeerSet()` -> `n.ProposeAddPeer()` to add target candidate nodes. Peer set temporarily expands to $A \cup B$.
3. **Background Sync**: Target nodes download S2 compressed snapshot blocks in the background while old nodes continue serving quorum votes for client publishes.
4. **Leadership Handover**: If the active Leader itself is being migrated, it executes `StepDown(preferred)` to hand over leadership to a caught-up target replica *before* evicting old nodes.
5. **Old Peer Eviction**: Once target nodes are caught up (`catchups == 0`), Leader calls `removeEvictedPeers()` -> `n.ProposeRemovePeer()` to evict old nodes one by one and shrink the peer set to $B$.
6. **Single Inflight Guard (`osa.moveInFlight()`)**: NATS strictly forbids overlapping move or scale operations per stream to prevent state drift.

---

### 3.5 State Machine Apply Pause/Resume (`PauseApply` & `ResumeApply`)

#### What
An internal safety lock that temporarily suspends committed entry processing onto the stream state machine (`FileStore`).

#### Why
When a follower falls far behind (e.g., after a network partition or server restart), receiving thousands of missing log entries all at once could cause state machine corruption or heavy lock contention if applied prematurely.

#### Code-Level Flow
1. Sets `n.paused = true` and freezes `n.hcommit = n.commit`.
2. Disables external apply channel `n.apply` pushes.
3. Sets election timer to `observerModeInterval` so the lagging follower **cannot accidentally campaign for leadership** while catching up.
4. Once the follower's WAL catch-up completes, `ResumeApply()` unpauses the channel.

---

### 3.6 Non-Voting Observer Mode (`SetObserver`)

#### What
Converts a Raft node into a passive, read-only "Observer" (`raft.go`).

#### Why
Used when operators want a node to replicate stream data for local read scaling, analytics, or backup without giving it voting rights that could impact cluster quorum or leader elections.

#### Code-Level Flow
1. `n.observer = true` is set on the node.
2. The node receives `AppendEntries` over `$SYS.RAFT.<group_id>.A` and writes to its local `FileStore` / `MemStore`.
3. **Quorum Exclusion**: `n.observer` nodes are strictly excluded from `recalcQuorum()` and cannot cast votes or become Leader.

---

### 3.7 Meta Leader Automated Reconciliation Loop (`meta.reconcile`)

#### What
An autonomous background reconciliation loop running on the Meta Leader (`$JS.META`) that continuously audits all stream Raft groups in the cluster.

#### Why
Guarantees that if a node crashes or disk fails, the cluster automatically repairs its own consistency state without human intervention.

#### Code-Level Flow
1. The Meta Leader monitors node heartbeats across the NATS cluster.
2. If a node hosting an R=3 stream replica dies for longer than `peerUnbusyTimeout`, the Meta Leader automatically allocates a new peer on a healthy server.
3. It proposes an `assignStreamOp` to add the new peer and a `removeStreamOp` to evict the dead peer, restoring stream health autonomously.

---

## 4. Summary Matrix of Cluster Controls & Operations

| Control Mechanism | Code Method / Location | Domain | Primary Operational Intent |
| :--- | :--- | :--- | :--- |
| **Disk Sync Policy** | `opts.SyncAlways` (`filestore.go`) | Storage Durability | Single-node power-loss protection; auto-relaxed in $R > 1$ clusters |
| **Emergency Quorum** | `RescueQuorum(qn)` (`raft.go`) | Consensus Recovery | Lower quorum size to recover deadlocked cluster after catastrophic peer loss |
| **Peer Scale-Down / Remove Node** | `EvictPeers()` / `ProposeRemovePeer()` (`raft.go`) | Membership Management | Remove dead/decommissioned nodes and dynamically shrink cluster quorum |
| **Peer Scale-Up / Add Node** | `ProposeAddPeer()` (`raft.go`, `jetstream_cluster.go`) | Replica Expansion | Scale up stream replicas or allocate replacement nodes with non-blocking catchup |
| **Stream Balancing & Migration** | `runStreamMigration()` (`jetstream_cluster.go`) | Asset Rebalancing | Re-home stream replicas onto new nodes with zero downtime and $A \cup B$ overlap |
| **Apply Suspension** | `PauseApply()` / `ResumeApply()` (`raft.go`) | State Protection | Prevent state corruption during large follower catch-up syncs |
| **Read-Only Shadowing** | `SetObserver(true)` (`raft.go`) | Read Scaling | Replicate data for local reads without impacting Raft quorum voting |
| **Auto Self-Healing** | `meta.reconcile` loop (`jetstream_cluster.go`) | Autonomous Repair | Reallocate stream replicas automatically when a node dies |
| **Leadership Handover** | `StepDown(preferred)` (`raft.go`) | Cluster Maintenance | Zero-downtime rolling maintenance and leadership migration |