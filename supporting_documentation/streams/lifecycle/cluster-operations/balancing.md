# Stream Balancing & Peer Migration Operations

Stream balancing and peer migration in NATS JetStream allows operators or cluster self-healing mechanisms to rebalance stream replicas across cluster nodes, evacuate streams from degrading hardware, or adjust stream placement according to tag rules. This document details the trigger pathways, high-level conceptual flow, Go runtime mechanics, and operational performance impact during stream balancing and migration.

---

## 1. Triggers & Operator Actions

Stream balancing or migration can be initiated through three distinct pathways:

```mermaid
flowchart TD
    subgraph Triggers["Trigger Pathways"]
        T1["1. Manual Client / Admin Migration API\nCLI: nats stream cluster peer migrate ORDERS --peers=nats-2,nats-3,nats-4\nCLI: nats stream cluster peer balance"]
        T2["2. Placement Tag Update\nTrigger: Operator updates Placement.Tags in stream config\nExample: Changing tag requirement from rack:A to rack:B"]
        T3["3. Automated Meta-Leader Rebalancing\nTrigger: Meta Leader ($JS.META) detects peer set drift or node evacuation\nAction: cc.reconcile() automatically triggers background stream migration"]
    end

    T1 --> Process["Stream Peer Migration Process"]
    T2 --> Process
    T3 --> Process
```

### 1.1 Trigger Comparison & Required Operator Actions

| Trigger Pathway | Initiator | Required Operator Action | Operational Outcome |
| :--- | :--- | :--- | :--- |
| **Manual Migration** | Operator / CLI | Run `nats stream cluster peer migrate` or `nats stream cluster peer balance` | Reallocates stream replicas to target node set or rebalances replicas across cluster |
| **Placement Tag Update** | Operator | Update `Placement.Tags` in stream configuration | Re-homes stream replicas onto cluster nodes matching updated tags |
| **Auto Self-Healing** | Meta Leader (`$JS.META`) | **Zero operator action required** | Automatically detects node evacuation/drain or imbalance and migrates stream replicas |

---

## 2. Layer 1: High-Level Conceptual Flow & Working Principles

```mermaid
flowchart TD
    Trigger["Trigger Event\n(Migrate CLI / Placement Update / Auto Rebalance)"] --> Step1

    subgraph Step1["1. Desired State Definition ($JS.META)"]
        S1["- Meta Leader calculates target peer set (Desired)\n- Sets Group.Desired.Move = true\n- Commits updated streamAssignment to $JS.META log"]
    end

    Step1 -- "Broadcast via $JS.META log" --> Step2

    subgraph Step2["2. Leader Snapshot & Peer Extension"]
        S2["- Stream Leader flushes pending state & creates Raft Snapshot\n- Calls extendPeerSet() to add target node to Raft group\n- Target node initializes local FileStore & joins bus"]
    end

    Step2 --> Step3

    subgraph Step3["3. Background Catch-up Phase"]
        S3["- Target node starts empty (pindex = 0)\n- Leader streams compressed Snapshot / WAL entries to target node\n- Target node populates local FileStore in background\n- Target node EXCLUDED from quorum voting & old peer set remains active"]
    end

    Step3 -- "Target node fully caught up (catchups == 0)" --> Step4

    subgraph Step4["4. Leadership Handover (If Leader is Migrating)"]
        S4["- Stream Leader checks if it is being evacuated\n- If yes, executes StepDown(preferred) to a target peer\n- Preferred target node becomes new Stream Leader fast"]
    end

    Step4 --> Step5

    subgraph Step5["5. Old Peer Eviction & Cleanup"]
        S5["- Active Leader calls ProposeRemovePeer(oldNodeID)\n- Old node removed from Raft peers & quorum recalculated\n- Old node shuts down raftNode & deletes local storage from disk"]
    end
```

### 2.1 Working Principles

- **Phased Overlap Expansion**: The stream peer set temporarily expands from existing peers to the combined peer set while target nodes catch up, ensuring the stream is never under-replicated.
- **Strict Quorum Protection during Migration**: Old nodes continue serving quorum votes while target nodes download snapshots in the background. Old nodes are only evicted after target nodes are 100% caught up.
- **Single Inflight Reconfiguration Guard (`osa.moveInFlight()`)**: NATS strictly forbids overlapping move or scale operations per stream to prevent cluster state drift.
- **Zero-Downtime Leadership Transition**: If the active Stream Leader itself is being migrated, it executes `StepDown(preferred)` to hand over leadership to a caught-up target replica *before* evicting old nodes.

---

## 3. Layer 2: Go Runtime Implementation Mechanics

### 3.1 Step 1: Ingress & Meta Desired State Proposal

- **Primary Source File**: `server/jetstream_cluster.go`
- **Function**: `s.jsClusteredStreamUpdateRequestLocked()`
- **Goroutine Context**: API Handler Goroutine

1. Detects `isMoveRequest == true`.
2. Calls `cc.selectPeerGroup()` to select new candidate target nodes based on disk/memory availability, liveness, and placement tags.
3. Wraps target peer set into `rg = osa.Group.withDesired(rg)` with `rg.Desired.Move = true`.
4. Calls `meta.Propose(encodeUpdateStreamAssignment(sa))` to commit the assignment into the `$JS.META` Raft log.

### 3.2 Step 2: Stream Migration Runner & Snapshot Installation (`js.runStreamMigration`)

- **Primary Source File**: `server/jetstream_cluster.go`
- **Function**: `js.runStreamMigration()`
- **Goroutine Context**: Stream Leader Worker Goroutine

1. Stream Leader reads `sa.Group.desiredSnapshot(leaderTerm)`.
2. Verifies `n.NeedSnapshot()`. Flushes pending writes (`mset.flushAllPending()`) and installs snapshot via `n.InstallSnapshot(mset.stateSnapshot(), true)`.
3. Calls `s.extendPeerSet(n, ...)` -> `n.ProposeAddPeer(add)` to add target candidate nodes to the Raft group.

### 3.3 Step 3: Background Catch-up & Quorum Guard (`mset.catchupPeers`)

- **Primary Source File**: `server/jetstream_cluster.go`
- **Function**: `mset.catchupPeers()`
- **Goroutine Context**: Sync Worker Goroutines

1. Target node is tracked in `catchups := mset.catchupPeers()`.
2. Leader streams compressed S2 snapshot blocks over `$SYS.RAFT.<group_id>.A`.
3. `runStreamMigration()` delays peer eviction while `len(catchups) > 0`.

### 3.4 Step 4: StepDown & Old Peer Eviction (`s.removeEvictedPeers`)

- **Primary Source File**: `server/jetstream_cluster.go`
- **Function**: `s.removeEvictedPeers()`
- **Goroutine Context**: Stream Leader Worker Goroutine

1. Once `catchups` is empty, `removeEvictedPeers()` identifies old nodes no longer in `desiredPeers`.
2. If the Leader itself is being evicted, it calls `n.StepDown(preferred)` to hand over leadership first.
3. Calls `n.ProposeRemovePeer(remove)` to evict old nodes one by one.
4. Evicted node receives notification, stops `raftNode`, and purges its local `FileStore` directory from disk (`mset.store.Delete()`).

---

## 4. Operational & Performance Impact During Operation

| Operational Dimension | Impact Level | Detailed Behavioral Characteristics |
| :--- | :--- | :--- |
| **Client Publish Availability** | Zero Downtime | Client publishes continue normally; active quorum processes ACKs while target nodes catch up |
| **Network Bandwidth** | Temporary Spike | Snapshot data transfer to target nodes generates temporary network traffic on system subjects (`$SYS.RAFT`) |
| **Disk & Memory Overhead** | Temporary Allocation | Stream data is briefly duplicated across old and target nodes during migration until old nodes are evicted |
| **Leadership Transition** | Zero Message Loss | If the leader node is being migrated, it executes `StepDown(preferred)` before eviction, preventing publisher errors |
| **Reconfiguration Safety** | Single Guard | `moveInFlight()` prevents concurrent move or scale operations per stream to prevent state drift |