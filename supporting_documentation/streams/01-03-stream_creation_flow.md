# Stream Creation Flow

A client creates a stream by publishing a request to `$JS.API.STREAM.CREATE.<stream_name>`. In a clustered deployment, only the Meta Raft Leader processes it -- validates the configuration, selects replica nodes, and proposes a stream assignment to the Meta Raft log. Once the proposal commits, all assigned nodes instantiate the stream locally, and the newly elected Stream Leader replies to the client. In a standalone deployment, the receiving server validates and creates the stream synchronously.

---

## Client-Side Configuration

The client influences stream creation through `StreamConfig`. The server autonomously selects the final nodes and leader, but the client can declaratively constrain replica placement and request leadership transfer post-creation.

### Replica Placement

The client constrains node selection through `StreamConfig.Placement`. The client cannot pass explicit node IDs -- the server always makes the final selection from the constrained candidate pool using its scoring engine.

| Placement Control | Config Field | Effect | CLI Example |
| ----------------- | ------------ | ------ | ----------- |
| Target cluster | `Placement.Cluster` | Restricts selection exclusively to nodes in the specified cluster | `nats stream add ORDERS --cluster aws-us-east-1` |
| Mandatory tags | `Placement.Tags` (e.g., `cloud:aws`, `disk:nvme`) | Node must have all specified tags to be eligible | `nats stream add ORDERS --tag cloud:aws --tag disk:nvme` |
| Exclusion tags | `Placement.Tags` with `!` prefix (e.g., `!rack:us-east-1c`) | Node is discarded if it carries the excluded tag | `nats stream add ORDERS --tag '!rack:us-east-1c'` |
| Replica count | `Replicas` | Dictates the Raft group size (R) | `nats stream add ORDERS --replicas 3` |

Within the pool of nodes satisfying these constraints, the server's selection engine autonomously picks the top R nodes using available storage, HA asset density, stream counts, and fault domain unique tags.

### Leadership Control

**At creation**: The client cannot specify or force an initial Stream Leader. The server randomly selects an online peer from the Raft group to initiate the Raft campaign, and Raft consensus elects the leader autonomously.

**Post-creation**: The client can request leadership transfer to a specific node using the Leader Step-Down API:

- Subject: `$JS.API.STREAM.LEADER.STEPDOWN.<stream_name>`
- Payload: `{ "placement": { "preferred": "<node_name>" } }`
- The current Stream Leader evaluates the preferred placement and invokes a graceful Raft leadership transfer to the specified node.

### Client Control Summary

| Decision | Client Control | Mechanism |
| -------- | :------------: | --------- |
| Target cluster | Yes | `Placement.Cluster` |
| Node tag requirements / exclusions | Yes | `Placement.Tags` |
| Exact replica node IDs | No | Server-side selection engine |
| Initial Stream Leader | No | Server-side random selection + Raft election |
| Leader transfer post-creation | Yes | `$JS.API.STREAM.LEADER.STEPDOWN` with preferred placement |

---

## Creation Flow

### 1. Request Validation

The receiving server validates the request before any cluster or storage logic executes.

| Check | Detail |
| ----- | ------ |
| JetStream and account enablement | Server-level and account-level JetStream must be enabled |
| Account resolution | Resolve target account and verify API version compatibility |
| Stream config parsing | Name validation (stream names become directory names on disk, so path separators are rejected) |
| Feature constraints | Sealed streams cannot be created directly; MirrorDirect requires a MirrorStream source |

If any validation fails, the server returns an appropriate error or silently drops the request (the client SDK retries through the route mesh).

### 2. Meta Raft Leadership Gate

In a clustered deployment, only the Meta Raft Leader processes stream creation. If the receiving node is not the leader, the request is silently dropped. The client SDK retries, and the route mesh delivers the retry to the actual leader. If the cluster has no leader (leaderless state), the server returns an error immediately.

### 3. Execution Fork

#### Standalone Path

The server checks account limits, verifies no subject overlap with existing streams, creates the stream instance (storage engine, background loops), and replies synchronously to the client.

#### Clustered Path

The Meta Raft Leader performs additional cluster-wide checks and orchestrates a distributed creation.

##### 3a. Cluster-Wide Validation

| Check | Detail |
| ----- | ------ |
| Config normalization | Full stream config normalization and validation |
| Idempotent retry detection | If an identical stream assignment already exists or is inflight, the existing state is reused rather than rejected |
| Subject overlap | A subject can belong to only one stream across the entire cluster |
| Account limits | MaxStreams, MaxMemory, MaxStore aggregated across nodes |

##### 3b. Meta Raft Proposal

The Meta Leader builds a stream assignment containing:

- Raft group (selected replica nodes)
- Sync subject (`$JSC.SYNC.<unique_inbox>`)
- Stream config
- Client reply inbox
- Timestamp

It proposes this assignment to the Meta Raft log for cluster-wide replication and tracks the inflight proposal to handle concurrent creates correctly. At this point, the Meta Leader's synchronous work is complete. The client reply happens asynchronously.

##### 3c. Post-Commit (Asynchronous)

Once the Meta Raft log entry commits across the cluster:

1. **All cluster members** process the stream assignment from the committed log entry and update their internal state
2. **Assigned replica nodes** instantiate the stream locally -- initializing the storage engine (file or memory) and starting the Stream Raft Group
3. **The elected Stream Leader** sends the reply to the client

This is the key architectural difference from standalone: the client's reply comes from the Stream Leader after Raft commit and local instantiation, not from the node that received the original request.

---

## Stream Raft Group Internals

Stream creation spans two distinct Raft layers that NATS maintains for all JetStream operations.

### Control Plane -- Meta Raft Group (`$JS.META`)

A single cluster-wide Raft group formed by all JetStream servers. The Meta Raft Leader acts as the cluster orchestrator: processes all management API requests (create, delete, update), tracks account storage quotas across nodes, decides which nodes will host stream replicas, and writes `streamAssignment` entries to the Meta Raft log so all nodes agree on placements.

### Data Plane -- Stream Raft Groups

Each stream with replicas > 1 gets its own independent Raft group with its own elected leader. Stream Raft Groups replicate published messages and consumer ACKs between the assigned replica nodes. The Stream Leader is distinct from the Meta Raft Leader -- a node can be the Meta Leader and a follower for a particular stream, or vice versa.

### Replication Transport

NATS uses NATS itself as the Raft transport layer.

| Component | Subject Pattern | Purpose |
| --------- | --------------- | ------- |
| Sync (leader to followers) | `$JSC.SYNC.<unique_inbox>` | AppendEntries, VoteRequest, InstallSnapshot |
| Reply (followers to leader) | `$JSC.R.<unique_inbox>` | Raft RPC responses |

This design requires zero additional ports, provides location transparency for peer reassignment, and isolates Raft traffic within the System Account (`$JSC` subjects are inaccessible to user connections).

### Replica Selection Pipeline

During clustered stream creation, the Meta Raft Leader decides which nodes host the stream's replicas. This is handled by a two-stage pipeline: discard ineligible nodes through hard constraints, then rank remaining candidates to balance load. The Meta Leader starts with all peers known to the Meta Raft Group as candidates.

#### 1. Target Cluster Resolution

If `Placement.Cluster` is specified in the stream config, only nodes in that cluster are considered. If not set, defaults to the cluster of the originating client, with configured alternate clusters available as fallback.

#### 2. Candidate Randomization

The candidate list is shuffled randomly before filtering. This prevents tie-breaking from always favoring the same nodes based on registration order in the cluster topology.

#### 3. Hard Filtering

Each candidate is evaluated against placement constraints (see Placement Rules below). Nodes that fail any constraint are discarded.

#### 4. Scoring and Sorting

Remaining candidates are ranked by a multi-tiered sort to distribute load (see Hotspot Avoidance below).

#### 5. Selection

The top R nodes (where R is the configured replica count) are selected to form the Stream Raft Group.

#### 6. Initial Leader Selection

For multi-replica streams, a preferred initial Stream Leader is selected randomly from online healthy nodes in the group. For R=1, the sole node is used.

#### 7. Sync Subject Assignment

A unique internal subject (`$JSC.SYNC.<unique_inbox>`) is generated for the stream's Raft replication transport.

#### 8. Multi-Cluster Fallback

If placement fails on the primary cluster due to insufficient resources, the system automatically retries on alternate clusters within the account's placement configuration.

### Placement Rules

During the filtering phase, candidate nodes are evaluated against six placement constraints. A node that fails any constraint is discarded.

| Rule | Constraint | What It Prevents |
| ---: | ---------- | ---------------- |
| 1 | **Liveness and Target Cluster** -- Node must belong to the target cluster, be online, and be active | Placing replicas on unreachable or wrong-cluster nodes |
| 2 | **JetStream Exclusion** -- Nodes tagged with `!jetstream` are skipped | Placing replicas on nodes explicitly excluded from JetStream workloads |
| 3 | **User Placement Tags** -- Mandatory tags (e.g., `cloud:aws`) must be present; exclusion tags (e.g., `!rack:us-east-1c`) cause the node to be discarded | Misplacement relative to operator-defined infrastructure topology |
| 4 | **Storage Capacity** -- Real-time available storage (memory or disk minus reserved/used) must accommodate the stream's `MaxBytes` if configured | Placing replicas on nodes that cannot store the stream's data |
| 5 | **HA Assets Cap** -- If `MaxHAAssets` limit is configured, nodes already at the limit are discarded | Over-concentrating HA workloads on specific nodes |
| 6 | **Fault Domain Anti-Affinity** -- Configured via `JetStreamUniqueTag` (e.g., `rack:` or `zone:` prefix). No two replicas of the same stream may share the same unique tag value | Co-locating replicas in the same failure domain (same rack, same zone) |

### Hotspot Avoidance

After filtering, NATS ranks the remaining candidates using real-time resource density metrics to prevent hot-spotting.

| Metric | Description |
| ------ | ----------- |
| `peerStreams` | Total number of streams currently assigned to the node |
| `peerHA` | Total number of high-availability (R > 1) streams and consumers assigned to the node |

Candidates are sorted through a two-pass ranking.

#### Primary Sort (determines base order)

| Priority | Criterion | Direction | Effect |
| -------: | --------- | --------- | ------ |
| 1 | Online status | Online first | Avoids placing on nodes that are currently offline |
| 2 | Available storage | Descending | Nodes with the most free disk/memory space rank higher |
| 3 | Total stream count | Ascending | Nodes hosting fewer streams rank higher (tie-breaker) |

#### Secondary Stable Sort (applied on top for R > 1 streams)

| Criterion | Direction | Effect |
| --------- | --------- | ------ |
| HA asset count | Ascending | Nodes with fewer HA workloads rank higher |

Because the secondary sort is stable, nodes with equal HA density preserve their primary order (most free storage, fewest total streams). The top R nodes from this ranked list form the stream's Raft group.

---

## Key Architectural Points

| Aspect | Detail |
| ------ | ------ |
| API subject | `$JS.API.STREAM.CREATE.<stream_name>` -- broadcast cluster-wide |
| Processing node | Meta Raft Leader only (clustered) or any JetStream node (standalone) |
| Non-leader handling | Silent drop; client SDK retries through route mesh |
| Subject ownership | A subject can belong to exactly one stream; enforced cluster-wide |
| Idempotent creates | Identical config retry reuses existing assignment state |
| Replica selection | Two-stage pipeline: hard filtering (6 constraints) then multi-tiered scoring |
| Hotspot avoidance | Sorted by online status, available storage, stream count, and HA density |
| Fault domain isolation | `JetStreamUniqueTag` prevents co-locating replicas in the same rack/zone |
| Multi-cluster fallback | Automatic retry on alternate clusters if primary has insufficient resources |
| Raft transport | Internal NATS subjects (`$JSC.SYNC`, `$JSC.R`) -- no extra ports |
| Standalone reply | Synchronous from receiving server |
| Clustered reply | Asynchronous from elected Stream Leader after Meta Raft commit |

---

## Diagram

<!-- Detailed sequence diagram to be added -->
