# Stream Consistency & Raft Foundations

JetStream uses single-leader Raft consensus to guarantee strong consistency across replicated streams ($R > 1$). This document details the architectural foundations of NATS JetStream consistency, including stream boundaries, the 2-tier Raft system, internal transport over NATS system subjects, replica placement algorithms, Raft index state management, WAL log structures, quorum mechanics, leader elections, and Go runtime entities.

---

## 1. Overview & Stream Boundaries

### 1.1 What Is Consistency in NATS JetStream

In a distributed NATS cluster, stream replicas ($R > 1$) must maintain identical message sequence ordering and consumer state across multiple server nodes. NATS JetStream implements a strong consistency model based on the Raft consensus algorithm:

- **Single-Leader Model**: For each replicated stream, exactly one server node acts as the Raft Leader. All state mutations (publishes, deletes, purges, consumer ACKs) are processed through the leader.
- **Log Equivalence**: Replicas maintain identical Raft Write-Ahead Logs (WAL). Log entry order determines state machine execution order across all cluster nodes.
- **Quorum Commitment**: State updates are committed only after a majority quorum ($Q = \lfloor R/2 \rfloor + 1$) of nodes successfully append the log entry to their local storage.

### 1.2 Core Stream Architectural Properties

- **Uniform Storage Constraint**: All replicas of a stream must share the exact same storage backend type (`storage: "file"` or `storage: "memory"`). Mixed-storage replication (e.g. 2 file nodes and 1 memory node for the same stream) is not permitted.
- **S2 Standard Compression**: S2 (high-performance Snappy extension) is the official standard compression algorithm used throughout NATS:
  - JetStream message payload and header compression (`compression: "s2"`)
  - Raft snapshot chunk transfers over the wire
  - Stream backup and restore archives (`.tar.s2`)
  - Internal cluster route and leafnode wire compression
  - Inflight batch payload compression
- **Account Tenancy & Scope**: Accounts act as cluster-wide tenant namespaces. Every stream is owned exclusively by a single parent Account. Cross-account access is achieved via NATS Service/Stream Exports and Imports, or Stream Mirrors and Sources, while ownership remains strictly bound to the parent account.

### 1.3 In-Memory Stream State Pointers

Every active stream replica maintains six core state pointers in memory to track stream state and index boundaries:

| Pointer / Field | Description |
| :--- | :--- |
| `FirstSeq` / `FirstTime` | Sequence number and nanosecond timestamp of the oldest active message in the stream |
| `LastSeq` / `LastTime` | Sequence number and nanosecond timestamp of the newest (most recently published) message |
| `Msgs` | Total count of currently active (non-purged, non-deleted) messages in the stream |
| `Bytes` | Total aggregate byte size of active message payloads and headers in the stream |
| `NumDeleted` / `dmap` | AVL tree / set tracking sequence numbers of deleted or purged messages between `FirstSeq` and `LastSeq` |
| `Subjects` (`psim`) | In-memory radix subject tree tracking `FirstSeq`, `LastSeq`, and message count per subject |

---

## 2. Decoupled Transport over NATS System Subjects (`syncSubjForStream`)

In traditional distributed stores (such as Etcd or Consul), Raft consensus nodes establish dedicated peer-to-peer TCP sockets or gRPC channels. NATS JetStream uses NATS itself as the high-performance transport bus for Raft consensus messages.

### 2.1 Virtual Bus Allocation

When a stream Raft group is created, `syncSubjForStream()` generates a unique internal NATS subject prefix:

```text
$JSC.SYNC.<unique_inbox>.<node_id>
```

This subject acts as a dedicated private virtual bus for all Raft nodes assigned to host that stream.

```text
  +---------------------------------------+
  |    Stream Raft Leader (Node-A)        |
  +-------------------+-------------------+
                      |
                      | Publish AppendEntries / Heartbeat
                      |
                      +---------------------------------------+
                      |                                       |
                      v                                       v
         Subject: $JSC.SYNC.v9x7K2.Node-B        Subject: $JSC.SYNC.v9x7K2.Node-C
                      |                                       |
                      v                                       v
  +-------------------+-------------------+   +---------------+-------------------+
  |   Stream Replica (Node-B)             |   |   Stream Replica (Node-C)             |
  |   Subscribed on:                      |   |   Subscribed on:                      |
  |   $JSC.SYNC.v9x7K2.Node-B             |   |   $JSC.SYNC.v9x7K2.Node-C             |
  +-------------------+-------------------+   +---------------+-------------------+
                      |                                       |
                      +-------------------+-------------------+
                                          |
                                          | Follower ACKs
                                          v
                              Subject: $JSC.R.v9x7K2
```

### 2.2 Replication Protocol over System Subjects

1. **Subscription Setup**: All nodes assigned to host the stream subscribe to internal subjects matching `$JSC.SYNC.<unique_inbox>.*`.
2. **Raft Frame Packaging**: When a user publishes a message, the Stream Leader packs the Raft entry (`AppendEntries`, `VoteRequest`, `InstallSnapshot`) into a NATS binary payload.
3. **Direct System Publish**: The leader publishes the Raft RPC over NATS directly to `$JSC.SYNC.<unique_inbox>.<peer_id>`.
4. **Follower ACKs**: Followers process the Raft frame, write the entry to their local WAL, and publish a Raft response back to the leader over system reply subject `$JSC.R.<unique_inbox>`.
5. **Quorum Commitment**: Once a majority quorum ($Q = \lfloor R/2 \rfloor + 1$) ACKs over `$JSC.R`, the Stream Leader commits the entry and responds to the client.

### 2.3 Architectural Benefits

1. **Zero Additional Open Ports**: Raft replication flows through existing NATS cluster route mesh connections. No extra firewall rules or network ports are required.
2. **Location Transparency**: A Raft peer can be relocated or reassigned to any server in the cluster topology without updating network configurations -- it simply subscribes to its assigned system subject.
3. **Security & Isolation**: `$JSC` system subjects are scoped to the internal NATS System Account (`$SYS`) and are completely inaccessible to standard client connections.

---

## 3. The 2-Tier Raft System

NATS JetStream separates control plane cluster management from data plane stream processing using two distinct layers of Raft groups:

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

### 3.1 Meta Raft Group ($JS.META) - Control Plane

- **Scope**: Single cluster-wide Raft group formed by all JetStream-enabled servers in the cluster.
- **Leadership**: Managed by the Meta Raft Leader (`cc.meta`).
- **Responsibilities**:
  - Processes management API requests (`$JS.API.STREAM.CREATE.*`, `STREAM.DELETE`, etc.).
  - Tracks account storage limits and memory/file resource quotas across all cluster nodes.
  - Executes replica placement algorithms (`cc.selectPeerGroup`) to select nodes for new streams.
  - Commits stream assignments (`streamAssignment`) to the `$JS.META` Raft log so all nodes maintain cluster state consensus.

### 3.2 Stream Raft Groups - Data Plane

- **Scope**: Independent, dedicated Raft group spawned per stream configured with $R > 1$ (e.g., `num_replicas: 3`).
- **Leadership**: Managed by the elected Stream Leader (`mset.node`) assigned to host that specific stream.
- **Responsibilities**:
  - Replicates published messages, atomic batches, message purges, deletes, and consumer state updates.
  - Operates independently from other stream Raft groups, isolating data plane traffic per stream.

---

## 4. Replica Allocation & Node Selection Pipeline (`cc.selectPeerGroup`)

When a stream is created or scaled in a cluster, the Meta Raft Leader invokes `js.createGroupForStream` to select the physical server nodes that will host the stream's Raft group. The decision algorithm (`cc.selectPeerGroup`) executes a 2-stage pipeline:

```text
Candidates (meta.Peers) -> Random Shuffle (rand.Shuffle)
                                  |
                                  v
                       Hard Filtering Phase
     (Discard: Offline, Tag Mismatch, Insufficient Disk/RAM,
      MaxHAAssets Limit, Duplicate Unique Tag / Same Rack)
                                  |
                                  v
                        Candidate Pool (wn)
                                  |
                                  v
                    Weighted Scoring & Sorting Phase
     (Sort by: Online Status -> HA Asset Count (ha) ->
      Available Storage (avail) -> Total Stream Count (ns))
                                  |
                                  v
                    Pick Top R Nodes for Raft Group
```

### 4.1 Stage 1: Candidate Shuffling & Hard Filtering Rules

The candidate list is randomized (`rand.Shuffle`) to prevent tie-breaking bias toward the first registered cluster server. Candidate nodes are evaluated against 6 hard placement rules:

1. **Liveness & Target Cluster**: Node must belong to the target cluster (`ni.cluster == cluster`) and be active and online (`ni.selectable()`).
2. **Explicit Exclusion Tags**: Nodes bearing the `!jetstream` tag (`jsExcludePlacement`) are skipped.
3. **User Placement Tags (`Placement.Tags`)**:
   - Mandatory tags (e.g., `cloud:aws`): Node must possess the tag.
   - Excluded tags (e.g., `!rack:us-east-1c`): Node carrying the tag is discarded.
4. **Storage Capacity Constraints**: Node must have sufficient unreserved storage space for `cfg.MaxBytes`. If `MaxBytes` exceeds available disk or memory, the node is skipped.
5. **HA Assets Cap**: If `JetStreamLimits.MaxHAAssets` is set, nodes exceeding this count are discarded.
6. **Fault Domain & Anti-Affinity Enforcement (`JetStreamUniqueTag`)**: Configured via `uniqueTagPrefix` (e.g., `"rack:"` or `"zone:"`). Function `checkUniqueTag` guarantees that **no two replicas of the same stream share the same unique tag value** (e.g., two replicas cannot land on `rack:A`).

### 4.2 Stage 2: Weighted Scoring & Load Balancing

To prevent hot-spotting specific servers, NATS calculates real-time resource density for remaining candidates:

- **`peerStreams` (`ns`)**: Total number of streams assigned to the node.
- **`peerHA` (`ha`)**: Total number of high-availability ($R > 1$) streams/consumers assigned to the node.

#### Sorting Priority Order

1. **Online Status**: Online servers strictly preferred over offline servers.
2. **HA Asset Density (`ha` ascending)**: `slices.SortStableFunc` sorts candidate nodes by HA asset count. Nodes hosting fewer replicated streams are prioritized.
3. **Available Storage (`avail` descending)**: Nodes with the most available free disk/memory space are ranked higher.
4. **Total Stream Count (`ns` ascending)**: Tie-breaker prioritizing nodes hosting fewer total streams.

### 4.3 Multi-Cluster Fallback Retry

If placement fails on the primary cluster due to `JSInsufficientResourcesErr`, `processStreamAssignmentResults` automatically inspects `ci.Alternates` and attempts to retry `createGroupForStream` on alternate clusters within the account's placement configuration.

---

## 5. Core Raft Concepts & Index Tracking

Every Raft node maintains three sequence indexes to track consensus progress:

| Index Symbol | Field Name | Description |
| :--- | :--- | :--- |
| `lastIndex` | `n.last` | Sequence number of the latest entry appended to the node's local Raft WAL (`n.wal`) |
| `commitIndex` | `n.commit` | Highest entry sequence replicated to a Majority Quorum ($Q = \lfloor R/2 \rfloor + 1$) of nodes |
| `appliedIndex` | `n.applied` | Highest Raft entry sequence that has been executed into the Stream Store (`memStore` or `fileStore`) |

### Index Invariants

In a healthy operational stream replica, the sequence indexes maintain the following strict ordering invariant:

```text
appliedIndex <= commitIndex <= lastIndex
```

- **`lastIndex` advance**: Occurs as soon as the node receives and writes an `AppendEntries` payload to its local WAL (`n.wal`).
- **`commitIndex` advance**: Occurs on the leader when quorum ACKs are tallied, and on followers when notified of `commitIndex` updates by the leader.
- **`appliedIndex` advance**: Occurs asynchronously as the state machine (`applyStreamEntries`) processes committed log entries and applies them to local stream storage.

---

## 6. Raft Write-Ahead Log (WAL) & Transport Mechanics

### 6.1 Log Frame Structure

The Raft Write-Ahead Log is an append-only sequential file on disk (`n.wal`) or memory structure (`memStore`). Each log entry contains four fields:

```text
+----------------+----------------+------------------+-----------------------------+
| Index (uint64) | Term (uint64)  | Type (EntryType) | Data (Binary Payload)       |
+----------------+----------------+------------------+-----------------------------+
```

- **Index**: Monotonically increasing sequence number within the Raft group.
- **Term**: Consensus epoch during which the entry was proposed by a leader.
- **Type**: 1-byte protocol entry type (`EntryNormal`, `EntrySnapshot`, `EntryPeerState`, `EntryCatchup`, `EntryAddPeer`, `EntryRemovePeer`).
- **Data**: Binary payload. For `EntryNormal`, this contains the JetStream application header (`entryOp`) followed by operation data.

### 6.2 Physical Storage & Resilience

- **Disk Streams (`storage: "file"`)**: Log entries are appended to disk (`n.wal`). On node crash, uncommitted entries beyond `commitIndex` can be truncated, but committed entries remain durable on disk.
- **Memory Streams (`storage: "memory"`)**: Log entries are stored in RAM (`memStore`). On restart, the local WAL is lost, and the node relies on snapshot catchup from active Raft peers.

### 6.3 Network Messaging Protocols

Raft consensus protocol frames are exchanged over dedicated system subjects:

| RPC Type | Subject Pattern | Purpose |
| :--- | :--- | :--- |
| `AppendEntries` | `$SYS.RAFT.<group_id>.A` | Log entry replication & heartbeat signals |
| `VoteRequest` | `$SYS.RAFT.<group_id>.V` | Leader election vote solicitation |
| `Replies` | `$SYS.RAFT.<group_id>.AR` / `.VR` | Response frames for replication & voting |

---

## 7. Quorum & Consensus Calculations

### 7.1 Quorum Formula

Raft requires a majority quorum to elect leaders and commit log entries:

$$Q = \lfloor R/2 \rfloor + 1$$

| Replicas ($R$) | Quorum ($Q$) | Maximum Tolerated Node Failures ($F$) |
| :---: | :---: | :---: |
| 1 | 1 | 0 |
| 3 | 2 | 1 |
| 5 | 3 | 2 |

### 7.2 Fault Tolerance Limits

To tolerate $F$ node failures, a stream must be configured with at least $R = 2F + 1$ replicas. If node failures reduce active replicas below quorum $Q$, the stream enters a read-only or unavailable state, rejecting write operations with `503 No Leader Available`.

---

## 8. Leader Election & Heartbeat Dynamics

### 8.1 Campaign Execution

When a node initiates a leadership election (transition to Candidate state):

1. **Term Increment**: Increments its current consensus term (`term++`).
2. **Self Vote**: Votes for itself (`vote = candidateID`) and persists the vote to disk/memory.
3. **Vote Request Broadcast**: Broadcasts `voteRequest(term, pterm, pindex, candidateID)` to all Raft group peers.
4. **Peer Voting Logic**: A peer grants its vote if:
   - Candidate term $\ge$ peer current term.
   - Peer has not voted for another candidate in this term.
   - Candidate log is at least as up-to-date as the peer's log (`lastTerm` and `lastIndex`).
5. **Election Victory**: If votes $\ge Q$, the candidate transitions to Leader (`switchToLeader()`) and broadcasts `sendPeerState` to announce leadership and synchronize peer topology.

### 8.2 Heartbeats & Liveness

- **Heartbeat Interval**: The active leader periodically (every 1 second) broadcasts `sendHeartbeat()` (an empty `AppendEntries` frame).
- **Follower Election Timer**: Followers reset their randomized election timer (1.5s - 3.0s) upon receiving any valid `AppendEntries` or heartbeat frame.
- **Quorum Verification**: The leader periodically checks `lostQuorumLocked()`. If a majority of peers fail to ACK heartbeats, the leader voluntarily steps down (`stepdown`) to prevent split-brain scenarios.

### 8.3 Re-Election Pathways

#### Voluntary Stepdown (Graceful / `CampaignImmediately`)

- The existing leader picks the healthiest, most up-to-date follower node.
- The leader sends an `EntryLeaderTransfer(peerID)` log entry directly to the cluster and steps down.
- The nominated peer receives the entry, skips the election timer wait, and invokes `CampaignImmediately()` (10ms timer) to become the new leader fast.

#### Unplanned Node Failure (Crash / Partition)

- Leader crashes or is partitioned; heartbeats stop.
- Followers' election timers count down independently.
- The follower whose election timer expires first transitions to Candidate and calls `Campaign()`.
- Upon receiving majority votes, it becomes the new Raft Leader and resumes message replication.

---

## 9. Go Runtime Core Entities

Inside the `nats-server` codebase, stream consistency and cluster orchestration are managed by four core Go structures:

```text
s (*Server) -- Node Level Daemon
  |
  +-- acc (*Account) -- Multi-tenant Boundary
  |     |
  |     +-- jsa (*jsAccount) -- Account Storage Quotas & Stream Lookup
  |
  +-- js (*jetStream) -- Node-Local JetStream Engine
        |
        +-- cc (*jetStreamCluster) -- Cluster Controller
              |
              +-- cc.meta (*raftNode) -- Participant in $JS.META Group
```

### 9.1 Entity Descriptions

1. **`s (*Server)`**: Represents the single `nats-server` process daemon. Owns TCP client listeners, server options, route mesh sockets, and the `s.js` pointer.
2. **`acc (*Account)`**: Represents a multi-tenant account boundary (e.g., `$SYS`, `PAYMENTS`). Holds authorization rules, user limits, and local stream lookups.
3. **`js (*jetStream)`**: The local JetStream subsystem controller running on the server node. Manages storage engines and contains the cluster controller pointer (`js.cluster`).
4. **`cc (*jetStreamCluster)`**: The cluster controller present on every JetStream-enabled server node. Holds `cc.meta` (this node's local participant instance in the Meta Raft Group `$JS.META`).

### 9.2 Checking Leader Status

To determine if the local server node is the active Meta Raft Leader, NATS executes:

```go
if cc.isLeader() {
    // This server node is the Meta Raft Leader (Control Plane Leader)
}
```

To determine if the local node is the active Stream Raft Leader for a specific stream:

```go
if mset.isLeader() {
    // This server node is the Stream Raft Leader for this stream
}
```
