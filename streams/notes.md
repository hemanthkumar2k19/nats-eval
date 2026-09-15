# Stream

# Questions
- What, How, Why
- What happens internally when creating a stream?
- How nodes for replicas are choosen?
- Cluster Operations on Stream - Using nats stream are automatic or manual
- Node loss and other scenrios
- Additonal New Node



## Internals

# RAFT 

1. Meta Raft Group ($JS.META) — The Control Plane
What it is: A single cluster-wide Raft group formed by all JetStream servers in the cluster.
The Meta Raft Leader: The ONE node elected as the leader of this $JS.META group.
Role (Control Plane):
Acts as the "Brain / Orchestrator" of the cluster.
Processes all management API requests ($JS.API.STREAM.CREATE.*, STREAM.DELETE, etc.).
Tracks account storage quotas across all nodes.
Decides which nodes in the cluster will store replicas for a new stream.
Writes streamAssignment entries to the $JS.META Raft log so all nodes agree on stream placements.

2. Stream Raft Groups — The Data Plane
What it is: Individual, independent Raft groups created per stream whenever a stream is created with $R > 1$ (e.g. Replicas: 3).
The Stream Raft Leader: The node elected as the Raft leader for that specific stream's message storage.
Role (Data Plane):
Replicates actual published messages and consumer ACKs between the 3 nodes assigned to host that stream.
Each $R > 1$ stream has its own separate Stream Raft group and its own Stream Leader.

This is one of the most brilliant architectural designs in NATS JetStream: NATS uses NATS itself as the transport layer for Raft consensus!



1. What is syncSubject?
In traditional distributed systems (like Etcd or Consul), Raft nodes open separate TCP ports or gRPC channels to talk to each other.

In NATS JetStream:

Calling 

syncSubjForStream()
 generates a unique internal NATS subject: $JSC.SYNC.<unique_inbox>.<node-id>
This subject acts as a dedicated private virtual bus for all Raft nodes assigned to this stream.

2. How Replication Works over syncSubject
```text
  [ Stream Raft Leader ] (Node-A)
            │
            ├────── Publish AppendEntries / Heartbeat ──────┐
            │       Subject: $JSC.SYNC.v9x7K2.Node-B        │
            │       Subject: $JSC.SYNC.v9x7K2.Node-C        │
            ▼                                               ▼
  [ Stream Replica ] (Node-B)                     [ Stream Replica ] (Node-C)
  Subscribed on: $JSC.SYNC.v9x7K2.Node-B          Subscribed on: $JSC.SYNC.v9x7K2.Node-C
```

Subscription Setup: When nodes Node-A, Node-B, and Node-C are assigned to host stream EVENTS, all 3 nodes subscribe to internal subjects under $JSC.SYNC.<unique_inbox>.*.
Raft Messages over NATS: When a user publishes a message to stream EVENTS:
The Stream Raft Leader receives the message.
The Leader packs the Raft entry (AppendEntries / VoteRequest / InstallSnapshot) into a NATS binary payload.
It publishes the Raft RPC over NATS directly to $JSC.SYNC.<unique_inbox>.<peer_id>.
Follower ACKs: Followers receive the Raft message on their system subscription, write the entry to their local stream store, and publish a Raft response back to $JSC.R.<unique_inbox>.
Commit & Client Ack: Once a quorum of followers (2 out of 3) ACK the Raft message over $JSC.R, the Stream Leader commits the message and responds to the client user.

3. Why is this design so powerful?
Zero Extra Open Ports: Raft replication flows through existing NATS cluster connections. You don't need to open extra firewall ports per stream or per node.
Location Transparency: A Raft peer can move or be re-assigned to any server in the cluster without changing network configurations—it just subscribes to the stream's $JSC.SYNC subject.
Security & Isolation: $JSC subjects are internal System Account subjects, completely inaccessible to standard user connections.


### 1. Overview & Key Entry Points

When creating a stream in a cluster, the Meta Raft Leader invokes `js.createGroupForStream` to select the physical peer nodes that will host the stream's Raft group (`raftGroup`).

The core decision-making algorithm is implemented in `cc.selectPeerGroup`. It executes a **2-stage process**:
1. **Hard Filtering**: Discards ineligible cluster nodes based on status, storage, placement tags, and fault domains.
2. **Weighted Scoring & Sorting**: Ranks remaining valid nodes to avoid hotspots and balance load.

---

### 2. Replica Allocation & Node Selection Pipeline

```mermaid
flowchart TD
    A["Cluster Candidates (meta.Peers)"] --> B["Random Shuffle (rand.Shuffle)"]
    B --> C{"Hard Filtering Phase"}
    C -->|Offline / Non-selectable| D[Discard]
    C -->|Cluster Mismatch| D
    C -->|Tagged '!jetstream'| D
    C -->|Placement Tag Mismatch| D
    C -->|Insufficient Memory/Disk| D
    C -->|Exceeds MaxHAAssets Limit| D
    C -->|Duplicate Unique Tag (Same Rack/Zone)| D
    
    C -->|Passed Filters| E["Candidate Pool (wn)"]
    E --> F{"Sorting & Hotspot Avoidance Phase"}
    F -->|1. Online status| G[Sort]
    F -->|2. HA Asset Density (ha ascending)| G
    F -->|3. Available Storage (avail descending)| G
    F -->|4. Total Stream Count (ns ascending)| G
    G --> H["Pick top R Nodes for Raft Group"]
```

#### Step-by-Step Flow:
1. **Target Cluster Resolution**:
   - Checks `cfg.Placement.Cluster`. If not explicitly set, defaults to `ci.Cluster` (the cluster of the originating client/request) and appends `ci.Alternates`.
2. **Candidate Shuffling**:
   - Peer list is initially randomized (`rand.Shuffle`) so tie-breaking doesn't always favor the first registered server in cluster topology.

---

### 3. Replica Placement Rules (Hard Constraints)

During the filtering loop, candidate nodes are evaluated against 6 placement rules:

1. **Liveness & Target Cluster**:
   - Node must belong to the target cluster (`ni.cluster == cluster`).
   - Node must be online and active (`ni.selectable()`).
2. **Explicit Placement Exclusions**:
   - Nodes carrying the `!jetstream` tag (`jsExcludePlacement`) are skipped.
3. **User Placement Tags (`Placement.Tags`)**:
   - **Mandatory Tags**: If `tag` is specified (e.g., `cloud:aws`), the node **must** have it.
   - **Excluded Tags**: If `!tag` is specified (e.g., `!rack:us-east-1c`), any node with that tag is discarded.
4. **Storage Space Constraints**:
   - Computes real-time available storage (`cfg.MaxMemory` or `cfg.MaxStore` minus reserved/used space).
   - If `cfg.MaxBytes` is defined on the stream and exceeds available space on a node, the node is discarded.
5. **HA Assets Cap**:
   - If `JetStreamLimits.MaxHAAssets` is set, nodes exceeding this count are discarded.
6. **Fault Domain & Anti-Affinity Enforcement (`JetStreamUniqueTag`)**:
   - Configured via `uniqueTagPrefix` (e.g., `"rack:"` or `"zone:"`).
   - Function `checkUniqueTag` ensures **no two replicas of the same stream share the same unique tag value** (e.g., node 1 on `rack:A` and node 2 on `rack:A` cannot be in the same Raft group).

---

### 4. Hotspot Avoidance & Load Balancing (Scoring)

To prevent hot-spotting specific servers, NATS calculates real-time resource density for every node in the cluster before placing a stream:
- **`peerStreams`**: Total number of streams assigned to the node.
- **`peerHA`**: Total number of high-availability (Replicas > 1) streams/consumers assigned to the node.

#### Multi-Tiered Sorting Engine:
1. **Primary Sort (Storage & Stream Count)**:
   - Prefer **Online** servers over Offline servers.
   - Rank by **Available Storage** (`avail` descending: nodes with the most free disk/memory space first).
   - Tie-breaker: Rank by **Total Stream Count** (`ns` ascending: nodes hosting fewer total streams first).
2. **Secondary Stable Sort (HA Asset Density)**:
   - For replicated streams ($R > 1$), `slices.SortStableFunc` sorts candidate nodes by **HA Asset Count** (`ha` ascending).
   - Because it is a stable sort, among nodes with equal HA asset density, it preserves the primary order (free storage & lower stream count).

---

### 5. Multi-Cluster Fallback Retry

If placement fails on the primary cluster due to `JSInsufficientResourcesErr`, [`processStreamAssignmentResults`] automatically inspects `ci.Alternates` and attempts to retry `createGroupForStream` on alternate clusters within the account's placement configuration.


## Go Code Internals
1. s (*Server) — Node Level
What it is: The single nats-server process/daemon running on this local machine or container.
Scope: Node-Local. Every server in the cluster has its own s instance.
Holds: TCP client connections, server configuration options, route mesh sockets, and the pointer to s.js.

2. acc (*Account) — Logical Tenant Level
What it is: Represents a multi-tenant Account boundary (e.g., $G, $SYS, or custom accounts like PAYMENTS).
Scope: Logical Tenant.
Holds: Account authorization rules, user credentials, and acc.js (jsa / jsAccount) for tracking local stream lookups and storage quotas for this account on this node.

3. js (*jetStream) — Local JetStream Engine
What it is: The JetStream Subsystem Controller running on this server node.
Scope: Node-Local Engine (with cluster awareness). Every node running JetStream has its own js (s.js) struct.
Holds: Local storage engine references, background queues, account JetStream structures, and a pointer to the cluster controller js.cluster (cc).

4. cc (*jetStreamCluster) — Cluster Controller & Meta Raft Participant
Is cc the Leader Node?
NO! Every JetStream node in the cluster has a cc object.
cc (js.cluster) is the Cluster Controller struct present on every clustered node.
Inside cc, it holds cc.meta — which is this node's local participant instance in the Meta Raft Group ($JS.META).
To check if this node's cc is currently the elected Leader, NATS calls:
go
cc.isLeader()  // or s.JetStreamIsLeader()
If cc.isLeader() == true $\rightarrow$ This server node is the Meta Raft Leader!
If cc.isLeader() == false $\rightarrow$ This server node is a Meta Raft Follower.

