# Consumer Update

This document details the operational lifecycle of updating existing NATS JetStream consumers, detailing immutable vs editable configuration fields, server validation behavior, and CLI update workflows.

## Overview

In NATS JetStream, existing consumers can be dynamically reconfigured at runtime without deleting the consumer or resetting sequence delivery state. The JetStream server evaluates consumer update requests by checking field mutability rules:
- **Immutable Fields**: Fundamental parameters defining consumer delivery type, starting position, ack contract, or storage backing cannot be altered after creation.
- **Editable Fields**: Operational parameters such as rate limits, redelivery backoffs, subject filters, and threshold timers can be modified live on an active consumer.

## Immutable vs Editable Configuration Parameters

### Immutable Fields

Attempting to update any of the following fields on an active consumer returns a server error:

| Field | Description | Server Error Reason |
| :--- | :--- | :--- |
| **`DeliverPolicy`** | Starting delivery position in stream history | `deliver policy can not be updated` |
| **`OptStartSeq`** | Starting sequence number for `by_start_sequence` | `start sequence can not be updated` |
| **`OptStartTime`** | Starting timestamp for `by_start_time` | `start time can not be updated` |
| **`AckPolicy`** | Message acknowledgement contract (`explicit`, `all`, `none`) | `ack policy can not be updated` |
| **`ReplayPolicy`** | Historical replay rate (`instant`, `original`) | `replay policy can not be updated` |
| **`Heartbeat` (`idle_heartbeat`)** | Idle heartbeat interval for push consumers | `heart beats can not be updated` |
| **`FlowControl`** | Flow control enabled status for push consumers | `flow control can not be updated` |
| **`MaxWaiting`** | Maximum waiting pull requests buffer size | `max waiting can not be updated` |
| **`MemoryStorage`** | State storage backend (memory vs file) | `storage type can not be updated` |
| **`Direct` / `Sourcing`** | Direct access and sourcing flags | `direct can not be updated` / `sourcing can not be updated` |
| **Push / Pull Mode Switch** | Switching between Push and Pull delivery models | `can not update pull consumer to push based` / `can not update push consumer to pull based` |

### Editable Field

The JetStream server dynamically reconfigures its internal runtime state when the following fields are updated:

| Field | Server Runtime Action on Update |
| :--- | :--- |
| **`FilterSubject` / `FilterSubjects`** | Re-applies subject filter routing trees (`o.subjf`). |
| **`DeliverSubject`** | Updates target push subject (must remain a push consumer). |
| **`AckWait`** | Resets internal ack wait timer (`o.resetPtmr`). |
| **`BackOff`** | Updates retry backoff array. |
| **`MaxDeliver`** | Updates maximum delivery count limit (`o.maxdc`). |
| **`MaxAckPending`** | Adjusts pending ACK buffer capacity (`o.maxp`) and resizes pending maps. |
| **`RateLimit` (`rate_limit_bps`)** | Re-initializes token bucket rate limiter (`o.rlimit`). |
| **`SampleFrequency`** | Adjusts monitoring sampling rate (`o.sfreq`). |
| **`InactiveThreshold`** | Restarts or updates inactive cleanup timer (`o.dtmr`). |
| **`Description` / `Metadata`** | Updates consumer description and key-value metadata annotations. |
| **`PauseUntil`** | Suspends or updates delivery pause deadline. |
| **`PriorityGroups` / `PriorityPolicy`** | Reconfigures consumer group priority assignment. |

## Hands-On CLI Demonstrations

### 1. Updating Editable Consumer Fields

1. Setting up:
Create a pull consumer `orders-processor` on stream `EVENTS`:
```bash
nats consumer add EVENTS orders-processor --filter=order.created --ack=explicit --max-deliver=5
```

2. Simulating the Behaviour:
Update editable parameters live (subject filters, max ack pending, and description):
```bash
nats consumer edit EVENTS orders-processor --filter="order.created,order.updated" --max-pending=2000 --description="Updated orders worker"
```

3. Checking the behaviour and working:
Inspect consumer configuration to verify updated fields:
```bash
nats consumer info EVENTS orders-processor
```

### 2. Attempting Invalid Immutable Field Modification

1. Setting up:
Existing consumer `orders-processor`.

2. Simulating the Behaviour:
Attempt to change immutable fields (e.g. changing delivery policy or switching to push mode):
```bash
nats consumer edit EVENTS orders-processor --deliver=new
```

3. Checking the behaviour and working:
Observe the server validation error rejecting the edit:
```text
nats: error: consumer update failed: deliver policy can not be updated
```