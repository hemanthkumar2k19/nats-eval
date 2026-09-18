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
- 3 indexes are tracked
    - lastIndex: Sequence number of the latest entry appended to the node's Raft WAL.
    - commitIndex: Highest entry sequence replicated to a Quorum ($R/2 + 1$) of nodes.
    - appliedIndex (n.applied): Highest Raft entry sequence that has actually been executed/applied into the Stream Store (memStore or fileStore).
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



## Stream Cluster Operations

### Leadership Stepdown
- Subject: $JS.API.STREAM.LEADER.STEPDOWN.<stream_name>
- Handler: jsStreamLeaderStepDownRequest
- Guard Check: Return if client == nil or !JetStreamEnabled
- Request Info: Extract ClientInfo and Resolve Target Account
- Stream Name: Extract 6th token from subject string
- Cluster Check: If !JetStreamIsClustered -> Return NewJSClusterRequiredError()
- Meta Leaderless: Get JetStream Cluster state; if Meta Raft is leaderless -> Return NewJSClusterNotAvailError()
- Stream Assignment: Meta Leader checks if Stream Assignment exists in cluster registry; if Meta Leader & missing -> Return NewJSStreamNotFoundError() (Followers exit silently)
- API Level: If client API level is incompatible -> Return NewJSRequiredApiLevelError()
- Account JetStream: If JetStream disabled for account & not a LeafNode -> Return NewJSNotEnabledForAccountError()
- Stream Group Leaderless: If Stream Raft Group has lost quorum/leader -> Return NewJSClusterNotAvailError()
- Stream Leader Gate: If THIS node is NOT the active Stream Raft Leader -> Exit silently (only active Stream Leader proceeds)
- Stream Lookup: Resolve local Stream Instance from account; if lookup fails -> Return NewJSStreamNotFoundError()
- Inactive Stream Guard: If local Stream Instance or Stream Raft Node is inactive/nil -> Return Success = true
- Preferred Placement: If JSON body present -> Unmarshal request & resolve preferred target node (Placement.Preferred)
- Execute StepDown: Invoke Raft StepDown -> Appends EntryLeaderTransfer log entry to initiate leadership transfer

### Raft StepDown Internal Process (raft.StepDown)
- Leader Verification: Under lock, verify current node is still Leader (`n.State() == Leader`)
- Preferred Peer Check: If preferred target specified, verify peer is online & healthy (`!offline` & recent heartbeat ACK < 3s)
- Fallback Peer Selection: If preferred peer is absent or unhealthy, pick first available healthy follower node
- Leader Transfer Entry: Broadcast special `EntryLeaderTransfer` log entry containing target peer ID directly via `sendAppendEntry()`
- Demote Local State: Invoke `n.stepdown(noLeader)` -> demotes current node to Follower and resets election timers
- Target Peer Fast-Track: Target peer receives `EntryLeaderTransfer` -> triggers `CampaignImmediately()` to become new Raft Leader instantly

- Final Response: If Raft error -> Return NewJSRaftGeneralError(), else -> Return Success = true


## Understanding Raft Mechanism

### Communication b/w Nodes
- Asynchronous NATS System Messages over internal subjects
- Append Entries Subject: $SYS.RAFT.<group_id>.A
- Vote Request Subject: $SYS.RAFT.<group_id>.V
- Replies sent to designated reply subjects ($SYS.RAFT.<group_id>.AR / .VR)

### WAL Log
- Append only on disk
- Sequential index data: {index, term, Type, Data(Binary)}
- Physically each node maintains its own local WAL file on disk (`n.wal`)
- Logical content consistency is enforced across peers via Quorum commit

### Quorum
- Floor[n/2] + 1 (e.g., 2 of 3, 3 of 5)

### Flow
Step 1: Client Publish Request
Client sends a message payload to subject ORDERS.created.
Step 2: Leader Receives & Proposes (Propose)
Node A verifies it is the Leader.
It wraps the message into a streamMsgOp payload.
It calls node.Propose(term, payload).
Node A writes the record to its local Raft WAL at Index 101, Term 2 as Uncommitted.
Step 3: Replication (AppendEntries)
Node A broadcasts an AppendEntries RPC containing Log Entry #101 to Node B and Node C.
Step 4: Follower WAL Write
Node B receives the entry, verifies Term 2 is valid, appends Log Entry #101 to its local Raft WAL, and sends back an ACK to Node A.
Node C is slightly slower on network IO.
Step 5: Quorum Commitment (Commit)
Node A receives the ACK from Node B.
Since 2 out of 3 nodes (Node A + Node B) now have Log Entry #101 in their WAL, Majority Quorum is achieved.
Node A marks Log Entry #101 as COMMITTED.
Step 6 & 7: State Machine Apply across Servers
Raft triggers the apply loop (

applyStreamEntries
).
Node A and Node B decode streamMsgOp and invoke mset.store.StoreRawMsg(), appending the message to their local FileStore stream storage and assigning sequence number 101.
(When Node C's ACK eventually arrives or catchup triggers, Node C will also commit and write to its local FileStore).
Step 8: Client PubAck
Node A (Leader) constructs the PubAck JSON ({"stream":"ORDERS","seq":101}) and writes it back to the Client TCP socket.
```bash
[Client]                [Node A (Leader)]             [Node B (Follower)]          [Node C (Follower)]
   │                            │                             │                            │
   │ 1. Publish Msg             │                             │                            │
   ├───────────────────────────►│                             │                            │
   │                            │ 2. Propose()                │                            │
   │                            │    Assign Index 101, Term 2 │                            │
   │                            │    Write to WAL (Uncommitted)│                           │
   │                            │                             │                            │
   │                            │ 3. AppendEntries RPC (101)  │                            │
   │                            ├────────────────────────────►│                            │
   │                            ├─────────────────────────────────────────────────────────►│
   │                            │                             │                            │
   │                            │                             │ 4. Write 101 to WAL        │
   │                            │                             │    Send ACK                │
   │                            │◄────────────────────────────┤                            │
   │                            │                             │                            │
   │                            │ 5. MAJORITY REACHED!        │                            │
   │                            │    (Node A + Node B = 2/3)  │                            │
   │                            │    Index 101 is COMMITTED   │                            │
   │                            │                             │                            │
   │                            │ 6. Apply to Stream Storage  │ 7. Apply to Stream Storage │
   │                            │    FileStore.WriteMsg(seq)  │    FileStore.WriteMsg(seq) │
   │                            │                             │                            │
   │ 8. Send PubAck Response    │                             │                            │
   │◄───────────────────────────┤                             │                            │
```

### Entry and Commit
1. Proposal (Leader):
   - Leader receives request -> wraps in `Entry{Type, Data}` with next index (`pindex + 1`)
   - Leader writes `Entry` to its local WAL (`n.wal.StoreMsg`)
   - Leader broadcasts `appendEntry` frame to all followers
2. Replication (Followers):
   - Follower receives `appendEntry` -> verifies log consistency (`pterm` & `pindex`)
   - Follower appends entry to its local WAL -> sends `appendEntryResponse` ACK to leader
3. Commit (Leader):
   - Leader tallies ACKs asynchronously
   - Once entry is replicated on Quorum (majority) -> leader advances `commit` index (`n.commit = index`)
4. Apply (Leader & Followers):
   - Both leader and followers push committed entries (`commit > applied`) to internal `n.apply` queue
   - Upper layers (`jetStreamCluster` or `stream`) consume `n.apply` to update state machines

### Campaign
- Will be normal or immediate (`xferCampaign`)
1. Transition to Candidate:
   - Increment term (`term++`)
   - Vote for itself (`vote = id`, persisted to disk)
   - Change State to Candidate
   - Update leader state to none
2. Requesting Votes:
   - Broadcast `voteRequest(term, pterm, pindex, candidateID)` to peers
3. Peer Voting Logic:
   - Candidate term >= peer's current term
   - Peer did not vote for another candidate in this term
   - Candidate log is at least as up-to-date as peer log (`lastTerm` & `lastIndex`)
4. Winning:
   - Tally votes as they arrive asynchronously
   - Votes >= Quorum -> switch to Leader (`switchToLeader()`)
   - Broadcast `sendPeerState` to announce leadership and synchronize peer topology

### HeartBeat
- Every 1s leader sends `sendHeartbeat()` (empty `appendEntry`)
- When a follower responds, leader updates peer timestamp (`ps.ts = time.Now()`)
- Leader periodically checks `lostQuorumLocked()`
- If majority is not active/responsive, leader steps down (self-demotes)

### Recovery
- 

### Re Election
- Followers reset election timer on receiving any `appendEntry` (heartbeat, data, or peer state) from leader
- When leader fails, follower election timer expires -> triggers `Campaign`

#### Case 1: Voluntary Stepdown (Graceful / `CampaignImmediately`)
- Leader checks authority -> picks healthiest up-to-date peer
- Leader sends `EntryLeaderTransfer(peerID)` directly to cluster -> steps down
- Nominated peer receives entry -> skips election wait -> triggers `CampaignImmediately` (10ms timer)
- Nominated peer increments term, votes for self, requests votes, wins quorum -> broadcasts `sendPeerState`

#### Case 2: Node Unavailable (Unplanned Crash / Network Partition)
- Leader crashes -> heartbeats stop
- Followers' randomized election timers (1.5s–3.0s) count down independently
- Follower with shortest timer expires first -> wakes up and calls `Campaign`
- Candidate increments term (`term++`), votes for self, broadcasts `voteRequest(term, pterm, pindex, candidateID)`
- Peers grant vote IF: candidate term >= peer term AND candidate log is at least as up-to-date
- Candidate collects votes >= Quorum -> switches to Leader -> broadcasts `sendPeerState`