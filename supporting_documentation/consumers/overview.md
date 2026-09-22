# Consumer Overview

This document provides a comprehensive beginner-friendly overview of NATS JetStream Consumers, detailing what consumers are, how they fit into the JetStream architecture, key operational concepts, and delivery semantics.

## What is a JetStream Consumer?

In NATS Core, messages published to a subject are broadcast live to active subscribers. If a subscriber is offline or slow, messages are dropped.

A **JetStream Consumer** is a stateful abstraction built on top of a JetStream Stream. It acts as a managed view or subscription interface over stored messages, tracking:
- Which messages have been delivered to subscribers.
- Which messages have been acknowledged.
- Which messages need to be redelivered due to timeouts or processing failures.

Streams store messages; Consumers govern how messages are read and acknowledged.

## Core Consumer Concepts

### 1. Durable vs Ephemeral Consumers
- **Durable Consumers**: State and delivery progress are saved on the JetStream cluster. If the client disconnects or restarts, the consumer resumes from its last acknowledged position. Identified by `name` with `inactive_threshold` unset or 0 (note: `durable_name` is deprecated in favor of `name`).
- **Ephemeral Consumers**: Temporary consumers tied to an active subscriber session. When the client disconnects or an `inactive_threshold` expires, the consumer is automatically removed.

### 2. Pull vs Push Delivery Models
- **Pull Consumers (Recommended for Microservices)**:
  - Subscribers explicitly request batches of messages from NATS (`nats consumer next`).
  - Provides application-driven flow control, preventing fast streams from overwhelming slow workers.
  - Ideal for competing worker pools and scalable microservice architectures.
- **Push Consumers**:
  - NATS proactively pushes messages to a designated client `deliver_subject`.
  - Simple for event notification callbacks, but requires flow control (`flow_control`, `idle_heartbeat`) to manage backpressure.

### 3. Subject Filtering
- By default, a consumer receives all messages stored in the parent stream.
- Using `filter_subject` or `filter_subjects`, a consumer can subscribe to a subset of subjects matching wildcard patterns (e.g. `order.created`, `order.*`).

### 4. Acknowledgement Policies (Ack Policy)
- **Explicit (`explicit`, Default)**: Each message must be explicitly acknowledged by the subscriber. Unacknowledged messages are redelivered after `ack_wait`.
- **All (`all`)**: Acknowledging sequence N automatically acknowledges all preceding messages up to N.
- **None (`none`)**: Messages are treated as acknowledged immediately upon delivery by NATS (at-most-once delivery).

### 5. Delivery Policies (Deliver Policy)
Controls where in the stream history the consumer begins reading messages:
- **All (`all`)**: Start from the very first message stored in the stream.
- **Last (`last`)**: Start from the most recently published message in the stream.
- **New (`new`)**: Receive only new messages published after the consumer is created.
- **By Start Sequence (`by_start_sequence`)**: Start from a specific stream sequence number.
- **By Start Time (`by_start_time`)**: Start from a specific timestamp.
- **Last Per Subject (`last_per_subject`)**: Start from the latest message for each subject in the stream.

### 6. Replay Policies
Controls the rate at which historical messages are replayed:
- **Instant (`instant`)**: Historical messages are delivered as fast as possible.
- **Original (`original`)**: Historical messages are replayed matching the original time gaps between publications.

### 7. Redelivery and Fault Tolerance
- **AckWait (`ack_wait`)**: Maximum time NATS waits for an ACK before assuming processing failed and triggering redelivery.
- **MaxDeliver (`max_deliver`)**: Maximum number of delivery attempts per message before NATS stops redelivering.
- **MaxAckPending (`max_ack_pending`)**: Maximum number of unacknowledged inflight messages allowed for the consumer.

## Stream vs Consumer Summary

| Feature | Stream | Consumer |
| :--- | :--- | :--- |
| **Primary Role** | Message Storage & WAL Log | Delivery & Ack Tracking |
| **State** | Holds message payloads & indexes | Holds delivery pointers & ACK state |
| **Lifetime** | Long-lived persistent store | Durable (persistent) or Ephemeral |
| **Multiplicity** | 1 per topic/domain | Multiple consumers per stream |
