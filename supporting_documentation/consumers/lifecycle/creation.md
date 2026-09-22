# Consumer Creation

This document details the operational lifecycle of NATS JetStream consumers, focusing on consumer creation, configuration, sequence delivery state management, and CLI demonstrations.

## Overview

Consumer creation establishes a JetStream view or subscription contract over messages stored within a parent stream.

## Configuration

Consumer configuration options define delivery behavior, replay rates, acknowledgement contracts, push/pull mechanics, flow control, and durable storage parameters.

For the official latest JetStream Consumer Create API documentation, refer to:
https://docs.nats.io/reference/jetstream/api/consumer/create

### Complete Consumer Creation JSON Reference

Below is a comprehensive JSON payload (`consumer-full.json`) demonstrating **all available consumer configuration parameters**, including common, pull-specific, push-specific, backoff, and advanced options:

```json
{
  "stream_name": "EVENTS",

  "// 1. Identity & Metadata": "------------------------------------------------",
  "name": "orders-consumer-v1",
  "durable_name": "orders-consumer-v1",
  "description": "Comprehensive reference payload containing every JetStream consumer option",
  "metadata": {
    "environment": "production",
    "team": "data-platform",
    "owner": "platform-engineering"
  },

  "// 2. Delivery Starting Position": "-----------------------------------------",
  "deliver_policy": "all",
  "opt_start_seq": 1,
  "opt_start_time": "2026-01-01T00:00:00Z",

  "// 3. Subject Filtering": "-------------------------------------------------",
  "filter_subject": "order.created",
  "filter_subjects": [
    "order.created",
    "order.updated",
    "order.completed"
  ],

  "// 4. Acknowledgement & Redelivery": "------------------------------------",
  "ack_policy": "explicit",
  "ack_wait": 30000000000,
  "max_deliver": 5,
  "backoff": [
    1000000000,
    5000000000,
    15000000000
  ],
  "max_ack_pending": 1000,

  "// 5. Replay Pacing & Rate Control": "------------------------------------",
  "replay_policy": "instant",
  "rate_limit_bps": 1000000,
  "sample_freq": "100%",

  "// 6. Push Consumer Options": "-------------------------------------------",
  "deliver_subject": "orders.deliver.push",
  "deliver_group": "orders-worker-pool",
  "flow_control": true,
  "idle_heartbeat": 5000000000,

  "// 7. Pull Consumer Options": "-------------------------------------------",
  "max_waiting": 512,
  "max_batch": 100,
  "max_bytes": 1048576,
  "max_expires": 30000000000,

  "// 8. Lifecycle & Inactivity": "------------------------------------------",
  "inactive_threshold": 86400000000000,
  "pause_until": "2026-12-31T23:59:59Z",

  "// 9. Payload & Storage Behavior": "------------------------------------",
  "headers_only": false,
  "num_replicas": 3,
  "mem_storage": false,
  "direct": false,
  "sourcing": false,

  "// 10. Consumer Priority & Groups": "-----------------------------------",
  "priority_policy": "none",
  "priority_groups": [
    "group-a",
    "group-b"
  ],
  "priority_timeout": 10000000000
}
```

## Sequence Handling

Consumer state tracking maintains two sequence pointers:
- **Stream Sequence (`s`)**: The absolute log sequence number of the message in the parent stream.
- **Consumer Sequence (`c`)**: The sequential delivery sequence number assigned specifically to this consumer.

### Sequence Pointer Concepts
- **Delivered Pointer (`s_del`, `c_del`)**: The highest sequence delivered to the subscriber.
- **Ack Floor (`s_ack`, `c_ack`)**: The sequence up to which all prior messages have been explicitly acknowledged.

### Number Line View

Initial State (Stream contains sequences 1 to 10):
```bash
Stream Sequence:   1  2  3  4  5  6  7  8  9  10
Consumer Sequence: 1  2  3  4  5  6  7  8  9  10
                   s_ack       s_del
                   c_ack       c_del
```

1. Delivery Phase: Consumer receives messages up to stream sequence 5.
   - Stream Delivered (`s_del`): 5
   - Consumer Delivered (`c_del`): 5
   - Stream Acked (`s_ack`): 1
   - Consumer Acked (`c_ack`): 1

2. Explicit Acknowledgement Phase: Consumer acknowledges sequence 4.
   - Stream Acked (`s_ack`): 4
   - Consumer Acked (`c_ack`): 4
   - Pending Messages: Sequence 5 remains unacknowledged until processed or until AckWait timeout triggers redelivery.

## Hands-On CLI Demonstrations

### Creating a Consumer

1. Setting up:
Prepare `consumer-full.json` with the consumer configuration.

2. Simulating the Behaviour:
Create a consumer using the NATS CLI:
```bash
nats consumer add EVENTS orders-processor --config=consumer-full.json
```

3. Checking the behaviour and working:
Verify consumer details, sequence status, and pending message counts:
```bash
nats consumer info EVENTS orders-processor
```

### Interactive Pull Consumer Usage

1. Setting up:
Ensure test messages exist in the `EVENTS` stream on `order.placed`.

2. Simulating the Behaviour:
Fetch a batch of messages using the pull consumer:
```bash
nats consumer next EVENTS orders-processor --count=5
```

3. Checking the behaviour and working:
Inspect updated consumer delivery pointers and unacknowledged counts:
```bash
nats consumer info EVENTS orders-processor
```