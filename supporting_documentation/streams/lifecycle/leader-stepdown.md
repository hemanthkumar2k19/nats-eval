# Stream Cluster Operations

## Leadership Stepdown

A client or operator requests leadership transfer for a specific stream by publishing to `$JS.API.STREAM.LEADER.STEPDOWN.<stream_name>`. The current Stream Leader validates the request, resolves the target peer, and delegates to the Raft layer to execute the transfer. The operation is only meaningful in clustered deployments with replicas > 1.

Optional request payload:

```json
{ "placement": { "preferred": "<server_name>" } }
```

If no preferred target is specified, the Raft layer selects an available healthy follower automatically.

---

### Stepdown Request Flow

The handler (`jsStreamLeaderStepDownRequest`) processes the request through a series of guard checks before executing the transfer.

#### 1. Pre-Condition Guards

These checks run on every node that receives the request. Each failed check returns immediately with the corresponding error.

| Guard | Condition | Error |
| ----- | --------- | ----- |
| JetStream enabled | Client is nil or JetStream is disabled on this server | Request silently dropped |
| Clustering required | Server is not part of a JetStream cluster | `JSClusterRequiredError` |
| Meta Leader available | Meta Raft Group is in a leaderless state | `JSClusterNotAvailError` |
| Stream exists | Stream assignment is missing from Meta Leader's cluster registry | `JSStreamNotFoundError` (Meta Leader returns error; followers exit silently) |
| API compatibility | Client API level is incompatible with this operation | `JSRequiredApiLevelError` |
| Account JetStream | JetStream is disabled for the requesting account (and not a LeafNode) | `JSNotEnabledForAccountError` |
| Stream group quorum | Stream Raft Group has lost quorum or has no leader | `JSClusterNotAvailError` |

#### 2. Stream Leader Gate

Only the active Stream Raft Leader proceeds past this point. If the receiving node is not the Stream Leader, it exits silently. The client SDK retries through the route mesh until the request reaches the actual Stream Leader.

#### 3. Stream Resolution

The Stream Leader resolves the local stream instance from the account. If the stream or its Raft node is inactive or nil, the server returns `Success = true` (no transfer needed for an inactive stream).

#### 4. Preferred Target Resolution

If the request body contains a `placement.preferred` field, the handler resolves the target node name to a Raft peer ID. If no preferred target is specified, the Raft layer selects one during execution (see below).

#### 5. Raft StepDown Execution

The handler invokes `raft.StepDown(preferred)` on the Stream Raft Group. If the Raft layer returns an error, the handler responds with `JSRaftGeneralError`. Otherwise, it returns `Success = true`.

---

### Raft Leadership Transfer Internal Process

Once `raft.StepDown(preferred)` is invoked, the Raft layer executes the transfer internally.

#### 1. Leader Verification

Under lock, the Raft node verifies it is still the Leader. If the node has already been demoted (e.g., by a concurrent stepdown or quorum loss), the call returns an error.

#### 2. Target Peer Selection

- **Preferred peer specified**: Verify the peer is online and healthy (not marked `offline`, last heartbeat ACK received within 3 seconds). If unhealthy, fall through to automatic selection.
- **Automatic selection**: Pick the first available healthy follower from the peer list.

If no healthy peer is available, the stepdown cannot proceed.

#### 3. Leadership Transfer Broadcast

The current leader broadcasts a special `EntryLeaderTransfer` log entry containing the target peer ID via `sendAppendEntry()`. This is a direct Raft log entry, not a regular append -- it signals the transfer intent to all group members.

#### 4. Local Demotion

The current leader invokes `stepdown(noLeader)`, which:

- Demotes local state from Leader to Follower
- Resets election timers
- Stops heartbeat broadcasts

The old leader is now a follower and will not initiate new elections immediately.

#### 5. Target Peer Campaign

The target peer receives the `EntryLeaderTransfer` entry and calls `CampaignImmediately()`, bypassing the normal election timeout. This fast-tracks the target into a Raft election that it wins (assuming it has an up-to-date log), making it the new Stream Leader.

---

### Key Behaviors

| Aspect | Detail |
| ------ | ------ |
| API subject | `$JS.API.STREAM.LEADER.STEPDOWN.<stream_name>` |
| Who processes | Only the current Stream Raft Leader |
| Non-leader handling | Silent exit; client SDK retries through route mesh |
| Preferred target | Optional; Raft falls back to any healthy follower if preferred is unavailable |
| Health check | Target peer must not be offline and must have ACKed a heartbeat within 3 seconds |
| Transfer mechanism | `EntryLeaderTransfer` Raft log entry + `CampaignImmediately()` on target peer |
| Availability during transfer | Brief window with no Stream Leader until target peer wins election |
| Failure response | `JSRaftGeneralError` if Raft layer cannot execute the transfer |
