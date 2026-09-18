# Cluster Operations Overview & Administrative Controls

NATS JetStream provides operational controls, safety mechanisms, and administrative interventions to manage stream durability, maintain Raft consensus health, recover from disasters, and execute cluster maintenance. This document covers storage sync policies (`SyncAlways`), emergency quorum overrides (`RescueQuorum`), dynamic membership management (`EvictPeers`), apply channel controls (`PauseApply`), non-voting observer mode (`SetObserver`), and automated reconciliation (`meta.reconcile`).

---

## 1. Overview

In a distributed NATS JetStream cluster, operations span three distinct administrative domains:

1. **Storage Durability Controls**: Regulating operating system disk flushing (`fsync`) vs. Raft network replication performance.
2. **Consensus & Emergency Interventions**: Overriding Raft quorum rules (`RescueQuorum`) or evicting dead peers (`EvictPeers`) during catastrophic hardware failures.
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

## 3. Administrative Consensus Interventions

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

### 3.2 Manual Peer Eviction & Dynamic Resizing (`EvictPeers` & `ProposeRemovePeer`)

#### What
An internal API allowing operators to dynamically strip dead or decommissioned peer nodes out of a stream's Raft group (`raft.go`).

#### Why
If a server node is permanently retired or scaled down, its peer ID must be removed from the Raft membership list so it no longer counts against majority quorum calculations ($Q = \lfloor N/2 \rfloor + 1$).

#### Code-Level Flow
1. Operator or Meta-Controller issues `EvictPeers([]string{deadNodeID})`.
2. Leader packages a special membership entry `ProposeRemovePeer(peerID)`.
3. Once committed by remaining peers, all nodes delete the peer from `n.peers` and execute `recalcQuorum()`, shrinking quorum size dynamically.

---

### 3.3 State Machine Apply Pause/Resume (`PauseApply` & `ResumeApply`)

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

### 3.4 Non-Voting Observer Mode (`SetObserver`)

#### What
Converts a Raft node into a passive, read-only "Observer" (`raft.go`).

#### Why
Used when operators want a node to replicate stream data for local read scaling, analytics, or backup without giving it voting rights that could impact cluster quorum or leader elections.

#### Code-Level Flow
1. `n.observer = true` is set on the node.
2. The node receives `AppendEntries` over `$SYS.RAFT.<group_id>.A` and writes to its local `FileStore`.
3. **Quorum Exclusion**: `n.observer` nodes are strictly excluded from `recalcQuorum()` and cannot cast votes or become Leader.

---

### 3.5 Meta Leader Automated Reconciliation Loop (`meta.reconcile`)

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
| **Peer Eviction** | `EvictPeers()` (`raft.go`) | Membership Management | Remove dead nodes and dynamically shrink cluster quorum |
| **Peer Scale-Up / Add Node** | `ProposeAddPeer()` (`raft.go`) | Replica Expansion | Scale up stream replicas or allocate replacement nodes with non-blocking catchup |
| **Apply Suspension** | `PauseApply()` / `ResumeApply()` (`raft.go`) | State Protection | Prevent state corruption during large follower catch-up syncs |
| **Read-Only Shadowing** | `SetObserver(true)` (`raft.go`) | Read Scaling | Replicate data for local reads without impacting Raft quorum voting |
| **Auto Self-Healing** | `meta.reconcile` loop (`jetstream_cluster.go`) | Autonomous Repair | Reallocate stream replicas automatically when a node dies |
| **Leadership Handover** | `StepDown(preferred)` (`raft.go`) | Cluster Maintenance | Zero-downtime rolling maintenance and leadership migration |
