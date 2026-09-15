# Stream Creation Flow

## Overview

A client creates a stream by publishing a request to `$JS.API.STREAM.CREATE.<stream_name>`. The request is broadcast cluster-wide. In a clustered deployment, only the Meta Raft Leader processes it. The leader validates the configuration, selects replica nodes, and proposes a stream assignment to the Meta Raft log. Once the proposal commits, all assigned nodes instantiate the stream locally, and the newly elected Stream Leader replies to the client.

In a standalone deployment, the receiving server validates and creates the stream synchronously.

---

## Control Plane vs Data Plane

Stream creation spans two distinct Raft layers that NATS maintains for all JetStream operations.

**Meta Raft Group (`$JS.META`) -- Control Plane**

A single cluster-wide Raft group formed by all JetStream servers. The elected Meta Raft Leader acts as the cluster orchestrator: it processes all management API requests (create, delete, update), tracks account storage quotas across nodes, decides which nodes will host stream replicas, and writes `streamAssignment` entries to the Meta Raft log so all nodes agree on placements.

**Stream Raft Groups -- Data Plane**

Each stream with replicas > 1 gets its own independent Raft group with its own elected leader. Stream Raft Groups replicate published messages and consumer ACKs between the assigned replica nodes. The Stream Leader is distinct from the Meta Raft Leader -- a node can be the Meta Leader and a follower for a particular stream, or vice versa.

**Replication Transport**

NATS uses NATS itself as the Raft transport layer. Each Stream Raft Group is assigned a unique internal sync subject (`$JSC.SYNC.<unique_inbox>`). Raft RPCs (AppendEntries, VoteRequest, InstallSnapshot) are published as NATS messages on these internal subjects. Followers respond on `$JSC.R.<unique_inbox>`. This design requires zero additional ports, provides location transparency for peer reassignment, and isolates Raft traffic within the System Account (`$JSC` subjects are inaccessible to user connections).

---

## Creation Flow

### 1. Request Validation

The receiving server validates the request before any cluster or storage logic executes. Validation covers:

- JetStream and account enablement
- Account resolution and API version compatibility
- Stream config parsing and name validation (stream names become directory names on disk, so path separators are rejected)
- Feature constraint enforcement (sealed streams cannot be created directly; MirrorDirect requires a MirrorStream source)

If any validation fails, the server returns an appropriate error or silently drops the request (the client SDK retries through the route mesh).

### 2. Meta Raft Leadership Gate

In a clustered deployment, only the Meta Raft Leader processes stream creation. If the receiving node is not the leader, the request is silently dropped. The client SDK retries, and the route mesh delivers the retry to the actual leader. If the cluster has no leader (leaderless state), the server returns an error immediately.

### 3. Execution Fork

#### Standalone

The server checks account limits, verifies no subject overlap with existing streams, creates the stream instance (storage engine, background loops), and replies synchronously to the client.

#### Clustered

The Meta Raft Leader performs additional cluster-wide checks and orchestrates a distributed creation:

**Cluster-Wide Validation**

- Full stream config normalization and validation
- Idempotent retry detection: if an identical stream assignment already exists or is inflight, the existing state is reused rather than rejected
- Cluster-wide subject overlap check: a subject can belong to only one stream across the entire cluster
- Cluster-wide account limits check (MaxStreams, MaxMemory, MaxStore aggregated across nodes)

**Replica Placement and Raft Group Creation**

The Meta Leader selects which nodes will host the stream replicas:

- Determines replica count from config
- Evaluates placement rules (tags, cluster constraints)
- Picks target peer nodes, considering node health and distribution
- Creates the Stream Raft Group for this stream
- Selects a preferred initial Stream Leader (random among online healthy nodes for multi-replica; the sole node for R=1)
- Generates the stream's sync subject (`$JSC.SYNC.<unique_inbox>`) for Raft replication transport

**Meta Raft Proposal**

The Meta Leader builds a stream assignment containing the Raft group, sync subject, stream config, client reply inbox, and timestamp. It proposes this assignment to the Meta Raft log for cluster-wide replication and tracks the inflight proposal to handle concurrent creates correctly.

At this point, the Meta Leader's synchronous work is complete. The client reply happens asynchronously.

**Post-Commit (Asynchronous)**

Once the Meta Raft log entry commits across the cluster:

1. **All cluster members** process the stream assignment from the committed log entry and update their internal state
2. **Assigned replica nodes** instantiate the stream locally -- initializing the storage engine (file or memory) and starting the Stream Raft Group
3. **The elected Stream Leader** sends the reply to the client

This is the key architectural difference from standalone: the client's reply comes from the Stream Leader after Raft commit and local instantiation, not from the node that received the original request.

---

## Key Architectural Points

| Aspect | Detail |
| ------ | ------ |
| API subject | `$JS.API.STREAM.CREATE.<stream_name>` -- broadcast cluster-wide |
| Processing node | Meta Raft Leader only (clustered) or any JetStream node (standalone) |
| Non-leader handling | Silent drop; client SDK retries through route mesh |
| Subject ownership | A subject can belong to exactly one stream; enforced cluster-wide |
| Idempotent creates | Identical config retry reuses existing assignment state |
| Replica selection | Meta Leader picks nodes based on replica count, placement rules, and node health |
| Raft transport | Internal NATS subjects (`$JSC.SYNC`, `$JSC.R`) -- no extra ports |
| Standalone reply | Synchronous from receiving server |
| Clustered reply | Asynchronous from elected Stream Leader after Meta Raft commit |

---

## Diagram

<!-- Detailed sequence diagram to be added -->
