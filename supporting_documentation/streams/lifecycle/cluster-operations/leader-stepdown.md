# Stream Leader Step-Down Operations

A client or operator requests leadership transfer for a specific stream by publishing an API request to `$JS.API.STREAM.LEADER.STEPDOWN.<stream_name>`. The current Stream Leader validates the request, resolves the target peer, and delegates to the Raft consensus layer to execute a fast-track leadership transfer. This document details trigger pathways, high-level conceptual flow, Go runtime mechanics, and operational performance impact during leader stepdown.

---

## 1. Triggers & Operator Actions

Stream leadership stepdown can be initiated through manual client/CLI API commands or automated server engine conditions:

```mermaid
flowchart TD
    subgraph Triggers["Trigger Pathways"]
        T1["1. Manual Client API / Admin CLI Request\nCLI: nats stream cluster step-down ORDERS [--peer nats-2]\nSubject: $JS.API.STREAM.LEADER.STEPDOWN.<stream>"]
        T2["2. Automated Server Storage / Quorum Failure\nTriggers: Storage Write Error (handleStorageError) or Raft Quorum Loss (lostQuorumLocked)"]
        T3["3. Automated Stream Evacuation / Server Shutdown\nTriggers: Stream Move/Evacuation (peerEvacuate) or Node Shutdown (s.shutdown())"]
    end

    T1 --> Process["Leader Step-Down Process"]
    T2 --> Process
    T3 --> Process
```

### 1.1 Trigger Comparison & Required Operator Actions

| Trigger Pathway | Initiator | Required Operator Action | Operational Outcome |
| :--- | :--- | :--- | :--- |
| **Manual Step-Down** | Operator / CLI | Run `nats stream cluster step-down <stream> [--peer target]` | Initiates fast-track leadership transfer to target or optimal follower |
| **Storage / Quorum Failure** | Server Engine | **Zero operator action required** | Automatically demotes faulty leader on disk error or quorum loss to protect stream consistency |
| **Stream Move / Evacuation** | Operator / System | Run stream evacuation command or trigger node shutdown | Automatically executes `StepDown` before node shutdown to prevent election delays |

---

## 2. Layer 1: High-Level Conceptual Flow & Working Principles

```mermaid
flowchart TD
    Trigger["Step-Down Trigger Event\n(API Request / Storage Error / Server Shutdown)"] --> Step1

    subgraph Step1["1. Pre-Condition Guard Checks"]
        S1["- Verify JetStream enabled, clustered, & Meta Leader online\n- Verify stream registry assignment and quorum liveness\n- Reject invalid API requests (JSClusterRequiredError)"]
    end

    Step1 -- "Guards Passed" --> Step2

    subgraph Step2["2. Stream Leader Gate & Target Resolution"]
        S2["- Verify THIS node is active leader (mset.isLeader)\n- Non-leader followers exit silently\n- Resolve preferred target peer if specified in payload"]
    end

    Step2 --> Step3

    subgraph Step3["3. Raft Leadership Transfer Proposal"]
        S3["- Invoke raft.StepDown(preferred)\n- Select target peer (preferred or best follower)\n- Broadcast EntryLeaderTransfer log entry to target"]
    end

    Step3 --> Step4

    subgraph Step4["4. Local Leader Demotion"]
        S4["- Demote local node to Follower state (stepdown(noLeader))\n- Reset election timers and stop heartbeat broadcasts"]
    end

    Step4 --> Step5

    subgraph Step5["5. Fast-Track Target Campaign & Victory"]
        S5["- Target peer receives EntryLeaderTransfer entry\n- Triggers CampaignImmediately() (10ms timer)\n- Target wins election and assumes Stream Leader role"]
    end
```

### 2.1 Working Principles

- **Graceful Fast-Track Transfer**: Unlike unplanned node crashes that rely on full election timeouts (1.5s - 3.0s), stepdown uses `EntryLeaderTransfer` and `CampaignImmediately()` (10ms timer) to complete leadership transfer in ~10ms - 50ms.
- **Leader Gate & Follower Silence**: Stepdown requests sent to follower nodes are silently dropped. Official client SDKs automatically route requests across the NATS mesh to the active leader.
- **Preferred Peer Override**: If a specific target node is specified (`--peer nats-2`), the leader verifies that peer's health (heartbeat ACK within 3s). If healthy, leadership is transferred directly to the requested node.
- **Automatic Fallback on Disk / Quorum Loss**: If a node suffers disk write errors or loses connection to quorum, it demotes immediately to follower state to prevent stale split-brain writes.

---

## 3. Layer 2: Go Runtime Implementation Mechanics

### 3.1 Step 1: Pre-Condition Guards & Ingress

- **Primary Source File**: `server/jetstream_cluster.go`
- **Function**: `jsStreamLeaderStepDownRequest()`
- **Goroutine Context**: API Handler Goroutine

1. Verifies JetStream is enabled and server is clustered (`JSClusterRequiredError`).
2. Checks `$JS.META` Meta Leader availability (`JSClusterNotAvailError`).
3. Resolves target account and verifies stream assignment exists.
4. Checks stream Raft group quorum liveness (`JSClusterNotAvailError`).

### 3.2 Step 2: Stream Leader Gate & Target Parsing

- **Primary Source File**: `server/jetstream_cluster.go`
- **Function**: `jsStreamLeaderStepDownRequest()`
- **Goroutine Context**: API Handler Goroutine

1. Checks `mset.isLeader()`. If false, exits silently (followers ignore request).
2. Resolves local stream instance `mset`.
3. Parses optional `TargetPeer` parameter from JSON payload if specified.

### 3.3 Step 3: Raft Leadership Transfer (`raft.StepDown`)

- **Primary Source File**: `server/raft.go`
- **Function**: `n.StepDown(preferred)`
- **Goroutine Context**: Stream Leader Raft Loop

1. Under lock, verifies `n.State() == Leader`.
2. Resolves target peer: uses `preferred` if online and ACKed within 3s, else selects first healthy follower.
3. Broadcasts `EntryLeaderTransfer` log entry via `sendAppendEntry()`.

### 3.4 Step 4: Local Leader Demotion

- **Primary Source File**: `server/raft.go`
- **Function**: `n.stepdown(noLeader)`
- **Goroutine Context**: Stream Leader Raft Loop

1. Changes local node state to `Follower`.
2. Resets election timers.
3. Stops broadcasting leader heartbeats (`sendHeartbeat()`).

### 3.5 Step 5: Fast-Track Target Election (`CampaignImmediately`)

- **Primary Source File**: `server/raft.go`
- **Function**: `n.processLeaderTransfer()` -> `n.CampaignImmediately()`
- **Goroutine Context**: Target Peer Raft Loop

1. Target peer receives `EntryLeaderTransfer` entry matching its peer ID.
2. Invokes `n.CampaignImmediately()` setting a 10ms fast election timer.
3. Fast-tracks election, sends Vote Requests (`EntryRequestVote`), collects quorum ACKs, and becomes the new Stream Leader.

---

## 4. Operational & Performance Impact During Operation

| Operational Dimension | Graceful StepDown (~10ms - 50ms) | Unplanned Failure (1.5s - 3.0s) | Detailed Behavioral Characteristics |
| :--- | :--- | :--- | :--- |
| **Client Publishes** | Briefly buffered (~10ms) | Error / Timeout (1.5s - 3s) | During graceful stepdown, client publishes buffer briefly in SDK memory and succeed immediately without client-facing errors |
| **Message Storage (WAL)** | Flushed to disk before transfer | Flushed if clean stop; uncommitted lost if crash | Stepping-down leader flushes pending WAL entries before demoting to follower |
| **Consumer Delivery** | Momentary pause (~10ms) | Delivery pause until election | Consumer delivery pauses briefly during handover and resumes immediately under new leader |
| **Duplicate Delivery Risk** | Low / Minimal | Low / Moderate | Any unacknowledged inflight messages are re-delivered by new leader; consumer deduplication handles repeats |
| **Mirrors & Sources** | Auto-reconnect (~10ms) | Auto-reconnect after election | Internal fetch loops pause momentarily and reconnect to new leader subject endpoint |
