# Stream Leader Step-Down -- Operational Impact and Triggers

Covers the operational impact of stream leader step-down and the conditions (automatic and manual) that trigger it.

---

## Operational Impact During Step-Down

When a Stream Leader steps down, the impact depends on whether the transfer is graceful (voluntary) or unplanned (crash/partition).

| Area | Graceful (~10ms-50ms) | Unplanned (1.5s-3.0s) |
| ---- | --------------------- | ---------------------- |
| **Client Publishes** | Briefly buffered; succeed on immediate SDK retry without error | `503 No Stream Leader Available` or timeout until new leader elected; SDKs retry automatically |
| **Message Storage (WAL)** | Stepping-down leader flushes uncommitted messages to disk before converting to follower | Same flush behavior if node is still running; lost if node crashed |
| **Consumer Delivery** | Momentary pause while new leader initializes active consumers | Same pause, longer duration |
| **Duplicate Delivery Risk** | Messages sent by old leader but not yet ACKed are re-delivered by new leader | Same risk |
| **Mirrors / Sources** | Fetch-loop pauses briefly and auto-reconnects to new leader | Same behavior, longer reconnect window |

---

## Automatic Step-Down Triggers

NATS automatically calls `mset.node.StepDown()` on a Stream Leader in these scenarios:

| Trigger | Internal Handler | What Happens |
| ------- | ---------------- | ------------ |
| Disk write error | `handleStorageError` | Leader fails to write to disk (full, corruption, I/O error); steps down so a healthy replica with working disk takes over |
| Raft quorum loss | `lostQuorumLocked` | Leader loses contact with majority of stream peers (e.g., 2 of 3 unreachable); demotes to follower to prevent split-brain writes |
| Stream migration / node evacuation | `peerStreamMove` / `peerEvacuate` | Operator initiates migration; NATS issues graceful StepDown to hand off leadership to target replica |
| Server shutdown / restart | `s.shutdown()` | Server hosting the leader shuts down; StepDown executed on all stream Raft nodes on that server |
| Higher Raft term detected | Raft protocol | Leader receives a message with a higher term; automatically demotes to follower |

---

## Manual Step-Down Triggers

Manual step-down is used when the operator wants a graceful (~10ms) transition instead of waiting for an unplanned election (1.5s-3.0s).

| Enterprise Scenario | Primary Goal | Operational Benefit |
| ------------------- | ------------ | ------------------- |
| Rolling server reboot | Gracefully move leadership before restart | Eliminates 1.5s-3s client publish errors (~10ms transition) |
| Cluster load balancing | Balance CPU/disk load across nodes | Prevents single-node bottlenecking |
| Disk/network degradation | Shift leadership away from slow hardware | Maintains low publish latencies |

**CLI:**

```
nats stream cluster step-down EVENTS
nats stream cluster step-down EVENTS --peer nats-2
```
