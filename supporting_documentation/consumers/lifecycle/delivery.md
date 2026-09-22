# Delivery Semantics

This document details NATS JetStream consumer delivery semantics, including delivery policies, acknowledgement models, redelivery backoff mechanisms, push consumer flow control, and hands-on CLI demonstrations.

## Overview

JetStream consumer delivery governs how stored stream messages are read by or pushed to subscriber applications. It encompasses:
- **Delivery Policy**: Where in the stream history the consumer begins reading.
- **Acknowledgement Policy**: How message receipt and processing are confirmed.
- **Redelivery & BackOff**: How unacknowledged or failed message processing is retried.
- **Push Flow Control**: How NATS manages backpressure and byte-window tracking for push consumers.

## Delivery Policies and Acknowledgement Models

### Delivery Policies

Consider a stream with sequence bounds `[FirstSeq, LastSeq]`:

- **`all` (Default)**: Delivers all stored messages `[FirstSeq, LastSeq]` from past to present, then waits for live incoming messages.
- **`last`**: Delivers the single newest stored message `[LastSeq, LastSeq]` before continuing with live messages.
- **`new`**: Ignores existing stream history and delivers only new messages published after consumer creation.
- **`by_start_sequence`**: Begins delivery from a specified sequence number `[opt_start_seq, LastSeq]`.
- **`by_start_time`**: Begins delivery from the first message stored at or after a specified RFC3339 timestamp `[opt_start_time, LastSeq]`.
- **`last_per_subject`**: Delivers the latest message for every distinct subject in the stream.

### Acknowledgement Policies

- **`explicit` (Default)**: Subscriber must explicitly send an ACK (`+ACK`) for every delivered message. Unacknowledged messages are retried after `AckWait` or `BackOff`.
- **`all`**: Acknowledging sequence N automatically acknowledges all preceding messages from `FirstSeq` up to N.
- **`none`**: NATS treats messages as acknowledged immediately upon delivery (at-most-once delivery).

### Replay Policy (`replay_policy`)

In NATS JetStream, `replay_policy` dictates the pacing and rate at which historical stream messages are delivered to a consumer during catch-up or replay scenarios.

Defined in `server/consumer.go`:

```go
type ReplayPolicy int

const (
    // ReplayInstant will replay messages as fast as possible.
    ReplayInstant ReplayPolicy = iota // Default ("instant")
    
    // ReplayOriginal will maintain the same timing as the messages were received.
    ReplayOriginal                    // ("original")
)
```

#### 1. `instant` (`ReplayInstant`) - Default
- **Behavior**: Pushes historical messages as fast as possible based on available system and network throughput.
- **How it works**: The server loops through stored messages and delivers them sequentially without inserting delays between messages.
- **Primary Use Cases**:
  - Standard application startup (hydrating in-memory state or cache).
  - Batch processing and ETL pipelines.
  - Rapid catch-up after consumer downtime.

#### 2. `original` (`ReplayOriginal`)
- **Behavior**: Replays historical messages maintaining the exact time deltas that occurred when they were originally published into the stream.
- **How it works**: During replay, the server compares the timestamp of each message (`pmsg.ts`) with the previous message (`lts`). If `delay = time.Duration(pmsg.ts - lts)` exceeds 1ms, the server pauses execution for that exact duration before sending the next message.
- Once the consumer reaches the end of historical messages (`o.sseq > lseq`), replay mode finishes and live incoming messages are delivered instantly.
- **Primary Use Cases**:
  - **Algorithmic Backtesting**: Simulating financial trade feeds or stock ticks at true real-time pace.
  - **IoT and Telemetry Simulations**: Re-enacting vehicle/sensor telemetry trails as if they were happening live.
  - **Debugging and Incident Analysis**: Reproducing complex time-dependent race conditions or event timing issues.

#### Replay Policy Summary

| Feature | `instant` | `original` |
| :--- | :--- | :--- |
| **JSON String** | `"instant"` | `"original"` |
| **Default?** | **Yes** | No |
| **Delivery Speed** | Maximum network/CPU speed | Matched to historical time intervals |
| **CLI Flag** | `--replay=instant` | `--replay=original` |


## Redelivery and BackOff Mechanics

### 1. `AckWait` vs `BackOff`

By default, when a message delivery times out without receiving an ACK, NATS server waits a fixed duration specified by `AckWait` (e.g., `30s`) before every redelivery attempt.

The **`BackOff`** option replaces fixed `AckWait` retries with an explicit sequence of custom duration intervals:

```json
{
  "ack_wait": 30000000000,
  "backoff": [
    1000000000,
    5000000000,
    15000000000,
    60000000000
  ]
}
```

### 2. BackOff Execution Lifecycle

1. **Attempt 1 (First Retry)**: Server waits `BackOff[0]`.
2. **Attempt 2 (Second Retry)**: Server waits `BackOff[1]`.
3. **Attempt N**: Server waits `BackOff[N-1]`.
4. **Beyond the list**: If delivery attempt count exceeds `len(BackOff)`, the server remains at the **last element** (`BackOff[len-1]`) for all subsequent retries until `max_deliver` is reached.

> [!NOTE]
> When `BackOff` is configured, `BackOff[0]` automatically overrides the default `AckWait`.

### 3. NATS CLI `--backoff` Options

When using `nats consumer add` or `nats consumer edit`:

| Option | Description | Generated Config |
| :--- | :--- | :--- |
| **`none`** | Clears backoff sequence; reverts to fixed `AckWait`. | `"backoff": []` |
| **`linear`** | Generates a linearly increasing retry interval (1s, 2s, 3s, 4s, 5s...). | `"backoff": ["1s", "2s", "3s", "4s", "5s"]` |
| **Custom List** | Comma-separated custom duration list. | `"backoff": ["1s", "5s", "15s", "1m"]` |

## Push Consumer Flow Control

Standard push consumers receive messages asynchronously to a designated `deliver_subject`. To prevent fast streams from overflowing slow subscribers, NATS uses **Flow Control**:

1. **Window Tracking**: The server tracks outstanding unacknowledged bytes and message counts against `max_ack_pending` or 50% byte buffer limits.
2. **Control Signal Ping**: When the unacked threshold is reached, the server sends a Flow Control request ping to the subscriber client.
3. **Client Response**: The client SDK automatically responds with headers indicating highest received sequences:
   - `Nats-Last-Stream-Sequence` (`JSLastStreamSeq`)
   - `Nats-Last-Consumer-Sequence` (`JSLastConsumerSeq`)
4. **Bulk ACK Execution**: Upon receiving the response, the server calls `processAckMsg()`, executing a bulk ACK up to the reported sequence number (similar to `AckAll`) to advance the consumer ACK floor and resume push delivery.

## Hands-On CLI Demonstrations

### 1. Consumer Delivery Policy & BackOff Creation

1. Setting up:
Create a consumer configuration `delivery-consumer.json` with `deliver_policy` set to `all` and custom `backoff`:
```json
{
  "stream_name": "EVENTS",
  "name": "orders-backoff-worker",
  "deliver_policy": "all",
  "ack_policy": "explicit",
  "ack_wait": 30000000000,
  "max_deliver": 5,
  "backoff": [
    1000000000,
    5000000000,
    15000000000
  ],
  "filter_subject": "order.*"
}
```

2. Simulating the Behaviour:
Add the consumer using NATS CLI:
```bash
nats consumer add EVENTS orders-backoff-worker --config=delivery-consumer.json
```

3. Checking the behaviour and working:
Verify consumer configuration and delivery settings:
```bash
nats consumer info EVENTS orders-backoff-worker
```

### 2. Modifying BackOff via CLI

1. Setting up:
Existing consumer `orders-backoff-worker`.

2. Simulating the Behaviour:
Update consumer backoff using CLI generator options:
```bash
# Apply a linear backoff policy
nats consumer edit EVENTS orders-backoff-worker --backoff=linear

# Apply a custom backoff sequence
nats consumer edit EVENTS orders-backoff-worker --backoff=1s,10s,1m

# Revert to standard AckWait by clearing backoff
nats consumer edit EVENTS orders-backoff-worker --backoff=none
```

3. Checking the behaviour and working:
Inspect updated consumer info to confirm backoff sequence:
```bash
nats consumer info EVENTS orders-backoff-worker
```