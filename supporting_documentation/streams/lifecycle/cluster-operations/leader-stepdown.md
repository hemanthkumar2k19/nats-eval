# Stream Leader Step-Down

A client or operator requests leadership transfer for a specific stream by publishing an API request to `$JS.API.STREAM.LEADER.STEPDOWN.<stream_name>`. The current Stream Leader validates the request, resolves the target peer, and delegates to the Raft consensus layer to execute a fast-track leadership transfer. This document details operational impact, automatic vs. manual triggers, API guard checks, and internal Raft transfer mechanics.

---

## 1. Overview & Primary Use Cases

Stream leadership stepdown allows operators to migrate stream leadership gracefully without triggering unplanned election timeouts:

- **Rolling Server Maintenance**: Moving leadership off a node before restarting it for OS/kernel updates, eliminating client error windows.
- **Cluster Load Balancing**: Rebalancing CPU and disk I/O load across cluster nodes when a single node becomes a hotspot.
- **Hardware Degradation**: Moving leadership away from a server experiencing disk latency degradation or network instability before failure occurs.

---

## 2. Operational Impact Comparison

When a Stream Leader steps down, the operational impact depends on whether the transfer is graceful (voluntary) or unplanned (crash or partition):

| Operational Dimension | Graceful Transfer (~10ms - 50ms) | Unplanned Failure (1.5s - 3.0s) |
| :--- | :--- | :--- |
| **Client Publishes** | Briefly buffered; succeed on immediate SDK retry without returning errors | Return `503 No Stream Leader Available` or time out until a new leader is elected; SDKs retry automatically |
| **Message Storage (WAL)** | Stepping-down leader flushes uncommitted messages to disk before converting to follower | Uncommitted messages flushed if process stops gracefully; lost if node crashes abruptly |
| **Consumer Delivery** | Momentary pause while new leader initializes active consumers | Brief delivery pause until election completes |
| **Duplicate Delivery Risk** | Messages sent by old leader but unacknowledged are re-delivered by new leader | Same re-delivery risk |
| **Mirrors & Sources** | Internal fetch loop pauses briefly and auto-reconnects to new leader | Auto-reconnects after election window |

---

## 3. Automatic Step-Down Triggers

NATS automatically invokes `mset.node.StepDown()` on a Stream Leader under these operational conditions:

| Trigger Scenario | Internal Handler | Operational Action & Result |
| :--- | :--- | :--- |
| **Disk Write Error** | `handleStorageError` | Leader fails to write to disk (disk full, filesystem corruption, or I/O failure); steps down so a healthy replica with a working disk takes over |
| **Raft Quorum Loss** | `lostQuorumLocked` | Leader loses contact with a majority of stream peers (e.g., 2 of 3 unreachable); demotes to follower to prevent split-brain writes |
| **Stream Migration / Evacuation** | `peerStreamMove` / `peerEvacuate` | Operator initiates stream migration; NATS issues graceful `StepDown` to hand off leadership to the target replica |
| **Server Shutdown** | `s.shutdown()` | Server hosting the leader shuts down; `StepDown` is executed on all stream Raft nodes on that server |
| **Higher Term Received** | Raft Protocol | Leader receives a message with a higher consensus term; automatically demotes to follower |

---

## 4. Stepdown Request Flow & Guard Checks

When a client or operator issues a stepdown request (`$JS.API.STREAM.LEADER.STEPDOWN.<stream_name>`), `jsStreamLeaderStepDownRequest` processes the request through five stages:

```text
[ API Request: $JS.API.STREAM.LEADER.STEPDOWN.<stream_name> ]
                            |
                            v
+-----------------------------------------------------------+
| 1. Pre-Condition Guard Checks                             |
|    - Verify client != nil && JetStreamEnabled             |
|    - Resolve target account from subject token            |
|    - Verify JetStream is clustered                        |
|    - Verify Meta Raft Leader is online                    |
|    - Verify stream assignment exists                      |
|    - Verify API level compatibility                       |
|    - Verify Stream Raft Group has quorum                  |
+----------------------------+------------------------------+
                            |
                            v
+-----------------------------------------------------------+
| 2. Stream Leader Gate                                     |
|    - Is THIS node the active Stream Leader (mset.isLeader)?|
|    - If NO -> Exit silently (only active leader proceeds) |
|    - If YES -> Resolve local Stream Instance              |
+----------------------------+------------------------------+
                            |
                            v
+-----------------------------------------------------------+
| 3. Stream Resolution                                      |
|    - Resolve local stream instance from account           |
|    - If inactive/nil -> Return Success = true             |
+----------------------------+------------------------------+
                            |
                            v
+-----------------------------------------------------------+
| 4. Preferred Target Resolution                            |
|    - Parse preferred target node if specified in payload  |
+----------------------------+------------------------------+
                            |
                            v
+-----------------------------------------------------------+
| 5. Raft StepDown Execution                                |
|    - Invoke raft.StepDown(preferred) on Stream Raft Node  |
+-----------------------------------------------------------+
```

### 4.1 Pre-Condition Guards

The handler executes 7 guard checks upon receiving a stepdown request:

| Guard Check | Condition | Error Response |
| :--- | :--- | :--- |
| **JetStream Enabled** | Client is nil or JetStream is disabled on this server | Request silently dropped |
| **Clustering Required** | Server is not part of a JetStream cluster | `JSClusterRequiredError` |
| **Meta Leader Available** | Meta Raft Group (`$JS.META`) is leaderless | `JSClusterNotAvailError` |
| **Stream Registry Check** | Stream assignment is missing from cluster registry | `JSStreamNotFoundError` (Meta Leader returns error; followers exit silently) |
| **API Level Check** | Client API level is incompatible | `JSRequiredApiLevelError` |
| **Account JetStream Check** | JetStream is disabled for the account | `JSNotEnabledForAccountError` |
| **Stream Quorum Check** | Stream Raft Group has lost quorum | `JSClusterNotAvailError` |

### 4.2 Stream Leader Gate

Only the active Stream Raft Leader proceeds past this point. If a receiving follower node gets the request, it exits silently. Official NATS client SDKs automatically retry through the route mesh until the request reaches the active Stream Leader.

---

## 5. Raft Leadership Transfer Internal Process

Once `raft.StepDown(preferred)` is invoked, the Raft layer executes the transfer internally:

```text
[ raft.StepDown(preferred) ]
             |
             v
1. Verify Current Node is Leader (n.State() == Leader)
             |
             v
2. Select Target Peer (Preferred Peer if Healthy, else First Available Follower)
             |
             v
3. Broadcast EntryLeaderTransfer Log Entry via sendAppendEntry()
             |
             v
4. Demote Local Node to Follower (stepdown(noLeader)) & Reset Election Timers
             |
             v
5. Target Peer Receives Entry -> Triggers CampaignImmediately() (10ms Timer) -> Wins Leadership
```

### 5.1 Step-by-Step Internal Protocol

1. **Leader Verification**: Under lock, the Raft node verifies it is still the Leader (`n.State() == Leader`).
2. **Target Peer Selection**:
   - **Preferred Peer Specified**: Verifies the peer is online and healthy (not marked `offline`, last heartbeat ACK received within 3 seconds). If unhealthy, falls back to automatic selection.
   - **Automatic Selection**: Picks the first available healthy follower from the peer list.
3. **Leadership Transfer Entry**: The leader broadcasts a special `EntryLeaderTransfer` log entry containing the target peer ID directly via `sendAppendEntry()`.
4. **Local Demotion**: The leader invokes `stepdown(noLeader)`, demoting local state to Follower, resetting election timers, and stopping heartbeat broadcasts.
5. **Target Peer Fast-Track Campaign**: The target peer receives `EntryLeaderTransfer` and immediately calls `CampaignImmediately()` (10ms timer), skipping standard election waits and fast-tracking its election victory to become the new Stream Raft Leader.

---

## 6. Operational Workflows & CLI

### 6.1 Basic Step-Down

To trigger a graceful leadership transfer to any healthy follower:

```bash
nats stream cluster step-down EVENTS
```

### 6.2 Targeted Step-Down

To request leadership transfer to a specific target node:

```bash
nats stream cluster step-down EVENTS --peer nats-2
```
