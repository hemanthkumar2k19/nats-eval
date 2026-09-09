# NATS JetStream Streams — Technical Reference & Architecture Considerations

## 1. Purpose

This document provides the technical reference for evaluating and reviewing **NATS JetStream Streams** as a durable messaging and event-storage capability.

It covers:

* Stream fundamentals and architecture
* Stream lifecycle and configuration
* Subject organization
* Stream sequence management
* Retention and cleanup
* Storage and persistence
* Replication and clustering
* Backup and restore
* Observability and capacity management
* Stream partitioning and separation criteria
* NATS CLI configuration coverage

This document is intended to support the architectural review and the associated CLI demonstration.

---

# 2. JetStream Overview

NATS provides two messaging models:

### NATS Core

NATS Core provides lightweight, real-time messaging where messages are generally delivered to active subscribers and are not inherently persisted.

### JetStream

JetStream adds persistence and state management capabilities on top of NATS.

JetStream provides capabilities including:

* Durable message storage
* Message replay
* Retention policies
* Stream sequencing
* Replication
* Consumer state
* Message management
* Snapshot and restore

The central persistence construct in JetStream is the **Stream**.

---

# 3. Stream Concept

A JetStream Stream captures messages published to configured NATS subjects.

Conceptually:

```text
Publisher
    |
    v
NATS Subject
    |
    | Subject matching
    v
+----------------------+
| JetStream Stream     |
|----------------------|
| Stored Messages      |
| Stream Sequences     |
| Retention Policy     |
| Storage              |
| Limits               |
| Replication          |
+----------------------+
```

A Stream therefore represents a **message persistence and lifecycle boundary**.

The Stream configuration determines:

* Which messages are captured
* Where messages are stored
* How long they are retained
* How much data can be retained
* How messages are discarded
* Whether the Stream is replicated
* Which administrative operations are permitted

---

## 4. Stream Configuration Reference

The following table consolidates the Stream configuration fields relevant to the current NATS JetStream review. It describes the supported values, their behavioral impact, and defaults.

| Configuration                | Value / Option       | Description                                                                                                                                                 | Default |
| ---------------------------- | -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- | :-----: |
| **Name**                     | String               | Unique identifier of the Stream. Used for all Stream administration and management operations.                                                              |    —    |
| **Description**              | String               | Human-readable description of the Stream's purpose. Does not affect message processing.                                                                     |  Empty  |
| **Subjects**                 | Subject list         | Defines the NATS subjects captured by the Stream. Multiple subjects and wildcard patterns can be configured.                                                |  Empty  |
| **Subjects**                 | `*`                  | Matches exactly one subject token. Example: `order.*` matches `order.created` and `order.placed`.                                                           |         |
| **Subjects**                 | `>`                  | Matches one or more subject tokens at the end of a subject. Example: `order.>` matches `order.customer.created`.                                            |         |
| **Retention**                | `limits`             | Retains messages according to configured limits such as `max_msgs`, `max_bytes`, and `max_age`. General-purpose retention model.                            |    ✓    |
| **Retention**                | `interest`           | Message retention is influenced by consumer interest.                                                                                                       |         |
| **Retention**                | `workqueue`          | Messages follow work-queue retention semantics.                                                                                                             |         |
| **Max Consumers**            | `-1`                 | No configured limit on the number of consumers.                                                                                                             |    ✓    |
| **Max Consumers**            | Positive integer     | Maximum number of consumers that can be associated with the Stream.                                                                                         |         |
| **Max Messages per Subject** | `-1`                 | No per-subject message-count limit.                                                                                                                         |    ✓    |
| **Max Messages per Subject** | Positive integer     | Maximum messages retained for each concrete subject. For `order.*`, the limit applies independently to `order.created`, `order.placed`, etc.                |         |
| **Max Messages**             | `-1`                 | No total message-count limit.                                                                                                                               |    ✓    |
| **Max Messages**             | Positive integer     | Maximum number of messages retained in the Stream.                                                                                                          |         |
| **Max Bytes**                | `-1`                 | No total byte limit.                                                                                                                                        |    ✓    |
| **Max Bytes**                | Positive integer     | Maximum amount of message data retained by the Stream.                                                                                                      |         |
| **Max Age**                  | `0`                  | No age-based expiration.                                                                                                                                    |    ✓    |
| **Max Age**                  | Duration             | Maximum age for retained messages. Messages exceeding the configured age become eligible for removal.                                                       |         |
| **Max Message Size**         | `-1`                 | No Stream-specific maximum message size.                                                                                                                    |    ✓    |
| **Max Message Size**         | Positive integer     | Maximum size of an individual message accepted by the Stream.                                                                                               |         |
| **Storage**                  | `file`               | Persists Stream messages to disk. Messages survive a NATS server restart when the underlying storage is persistent.                                         |    ✓    |
| **Storage**                  | `memory`             | Stores Stream messages in memory. Messages do not survive a NATS server restart.                                                                            |         |
| **Num Replicas**             | `1`                  | Single Stream replica. No Stream-level replica redundancy.                                                                                                  |    ✓    |
| **Num Replicas**             | `3`                  | Maintains three Stream replicas across suitable JetStream servers. Provides tolerance for individual replica/server failure while quorum remains available. |         |
| **Num Replicas**             | `5`                  | Maintains five Stream replicas across suitable JetStream servers. Provides higher failure tolerance at additional resource cost.                            |         |
| **Discard**                  | `old`                | When applicable limits are reached, older messages are discarded to allow newer messages to be retained.                                                    |    ✓    |
| **Discard**                  | `new`                | New messages are rejected/discarded when the configured limits prevent additional storage.                                                                  |         |
| **Duplicate Window**         | `0`                  | Uses the server's default duplicate-detection behavior.                                                                                                     |    ✓    |
| **Duplicate Window**         | Duration             | Defines how long JetStream tracks message identifiers for duplicate detection.                                                                              |         |
| **Sealed**                   | `false`              | Stream remains operational and can continue to accept messages according to its configuration.                                                              |    ✓    |
| **Sealed**                   | `true`               | Seals the Stream and prevents further message storage. This is a lifecycle/terminal operation rather than a normal configuration change.                    |         |
| **Deny Delete**              | `false`              | Stream can be deleted through normal administrative operations.                                                                                             |    ✓    |
| **Deny Delete**              | `true`               | Prevents deletion of the Stream through normal administrative operations.                                                                                   |         |
| **Deny Purge**               | `false`              | Messages can be removed using Stream purge operations.                                                                                                      |    ✓    |
| **Deny Purge**               | `true`               | Prevents purge operations against the Stream.                                                                                                               |         |
| **Allow Rollup Headers**     | `false`              | Rollup functionality is disabled.                                                                                                                           |    ✓    |
| **Allow Rollup Headers**     | `true`               | Enables the Stream to process supported rollup headers.                                                                                                     |         |
| **Allow Direct**             | `false`              | Direct Stream message retrieval is disabled.                                                                                                                |    ✓    |
| **Allow Direct**             | `true`               | Enables direct retrieval of Stream messages through the direct Stream API path.                                                                             |         |
| **Mirror Direct**            | `false`              | Direct access behavior for mirrored Streams is disabled.                                                                                                    |    ✓    |
| **Mirror Direct**            | `true`               | Enables direct access behavior for mirrored Streams. Relevant when using Stream mirrors.                                                                    |         |
| **Consumer Limits**          | `{}`                 | No Stream-level consumer limits explicitly configured.                                                                                                      |    ✓    |
| **Consumer Limits**          | Configuration object | Defines Stream-level constraints/defaults for consumer configuration. Detailed consumer behavior is outside this review.                                    |         |

### Configuration Scope

The fields above can be grouped conceptually into five architectural decisions:

| Decision                                           | Configuration                                                                           |
| -------------------------------------------------- | --------------------------------------------------------------------------------------- |
| **What enters the Stream?**                        | `subjects`                                                                              |
| **How much / how long is retained?**               | `retention`, `max_msgs`, `max_msgs_per_subject`, `max_bytes`, `max_age`, `max_msg_size` |
| **Where is it stored?**                            | `storage`                                                                               |
| **How much failure tolerance is required?**        | `num_replicas`                                                                          |
| **How is the Stream administratively controlled?** | `discard`, `duplicate_window`, `sealed`, `deny_delete`, `deny_purge`, capability flags  |

**Architecture note:** `storage` and `num_replicas` should be treated differently from ordinary operational limits. Storage determines the underlying persistence model, while replication depends on the JetStream cluster topology. Both therefore have stronger lifecycle/topology constraints than properties such as `max_msgs` or `max_age`.

---

# 5. Subjects and Stream Scope

Subjects define the messages captured by a Stream.

For example:

```text
order.*
```

captures:

```text
order.created
order.placed
order.cancelled
```

A Stream can also capture multiple subject patterns:

```text
order.*
payment.*
```

This allows multiple event types to share the same persistence boundary.

## Subject Wildcards

NATS supports:

* `*` — matches one subject token
* `>` — matches one or more subject tokens at the end of a subject

Example:

```text
order.*
```

matches:

```text
order.created
order.placed
```

but not:

```text
order.customer.created
```

A broader pattern:

```text
order.>
```

can match hierarchical subjects such as:

```text
order.created
order.customer.created
order.customer.address.updated
```

Subject design should therefore be deliberate because the Stream's subject configuration determines its message scope.

---

# 6. Stream Lifecycle

The Stream lifecycle can be viewed as:

```text
Create
  |
  v
Configure
  |
  v
Capture Messages
  |
  v
Sequence Assignment
  |
  v
Retention / Cleanup
  |
  v
Configuration Updates
  |
  v
Backup / Recovery
  |
  v
Retirement / Deletion
```

A Stream is a long-lived configuration and persistence object. Individual messages have their own lifecycle within that Stream.

---

# 7. Creating a Stream

A Stream is created by supplying a Stream configuration.

Important creation decisions include:

### Subject Scope

Which messages should the Stream capture?

### Retention

How should messages be retained?

### Storage

Should messages be kept in memory or persisted to file?

### Limits

How much data should the Stream retain?

### Replication

How many replicas are required?

### Discard Behavior

What should happen when configured limits are reached?

These decisions establish the initial operational behavior of the Stream.

---

# 8. Stream Updates

Stream configuration is not entirely immutable.

Operational policies can be updated for supported fields, including configuration such as:

* Subject definitions
* Retention
* Message limits
* Maximum age
* Maximum bytes
* Maximum message size
* Discard policy
* Duplicate detection configuration
* Supported Stream capabilities

However, some configuration is tied to the underlying storage or cluster topology.

## Storage

The storage mode:

```text
memory
file
```

should be considered a Stream creation/storage decision rather than a normal runtime policy update.

## Replication

The replica count:

```text
1
3
5
```

is dependent on the JetStream cluster topology and available servers.

Therefore, Stream updates should be categorized into:

```text
Operational Policies
        |
        +-- Generally updateable


Storage / Topology
        |
        +-- More constrained
```

---

# 9. Stream Storage

JetStream provides:

```text
Memory Storage
File Storage
```

## 9.1 Memory Storage

Memory storage maintains Stream data in memory.

Characteristics:

* Low-latency access
* No persistent Stream data across server restart
* Requires sufficient memory for retained data
* Suitable where persistence is not required

Conceptually:

```text
Stream
   |
   v
Memory
```

## 9.2 File Storage

File storage persists Stream data to disk.

Characteristics:

* Durable across NATS server restart
* Requires persistent disk
* Requires capacity planning
* Suitable for durable event storage

Conceptually:

```text
Stream
   |
   v
Persistent Storage
```

For containerized deployments, the JetStream storage directory must be backed by persistent storage if data must survive container recreation.

---

# 10. Storage Capacity

File-backed Stream capacity depends primarily on:

```text
Message Rate
×
Average Message Size
×
Retention Duration
```

The resulting estimate should also account for:

* Replication
* Operational overhead
* Growth
* Recovery requirements
* Storage safety margin

A Stream's configured `max_bytes` can be used to impose an explicit upper bound on retained message data.

---

# 11. Retention Policies

JetStream supports three primary retention policies:

```text
limits
interest
workqueue
```

## 11.1 Limits Policy

Messages are retained according to configured limits such as:

* Maximum messages
* Maximum bytes
* Maximum age

This is the general-purpose retention model.

## 11.2 Interest Policy

Retention is associated with consumer interest.

The message lifecycle therefore depends on consumer state.

## 11.3 Work Queue Policy

Messages are managed according to work-queue semantics.

Interest and Work Queue policies should be evaluated together with the intended consumer model.

---

# 12. Message Limits

JetStream provides multiple mechanisms for controlling Stream growth.

### Maximum Messages

```text
max_msgs
```

Controls the total number of messages retained.

### Maximum Messages Per Subject

```text
max_msgs_per_subject
```

Controls the number of messages retained for each concrete subject.

For a Stream capturing:

```text
order.*
```

the limit applies independently to:

```text
order.created
order.placed
order.cancelled
```

It does not mean one combined limit for the wildcard `order.*`.

### Maximum Bytes

```text
max_bytes
```

Controls the total amount of retained message data.

### Maximum Age

```text
max_age
```

Controls how long messages can remain based on age.

### Maximum Message Size

```text
max_msg_size
```

restricts the size of an individual message.

---

# 13. Discard Policy

When a Stream reaches applicable limits, its discard policy determines how additional messages are handled.

Supported policies include:

```text
old
new
```

## Discard Old

Older retained messages are removed to make room for new messages.

## Discard New

New messages can be rejected/discarded when the Stream cannot accept additional data under the configured limits.

The choice depends on whether the application prioritizes retaining the newest events or preserving the existing retained dataset.

---

# 14. Stream Sequence

Every message stored in a Stream receives a Stream Sequence.

Example:

```text
Sequence    Message
1           Order-1
2           Order-2
3           Order-3
4           Order-4
```

The Stream exposes boundaries:

```text
First Sequence → oldest retained message
Last Sequence  → newest retained message
```

## Sequence Advancement

When a new message is stored:

```text
Last Sequence
      ↓
moves forward
```

When old messages are removed:

```text
First Sequence
      ↓
moves forward
```

Deleting a message does not cause remaining messages to be renumbered.

Example:

```text
1  2  3  4  5
```

Delete sequence `3`:

```text
1  2     4  5
```

A later message receives the next sequence:

```text
1  2     4  5  6
```

Sequence values therefore provide a historical position within the Stream.

---

# 15. Message Deletion

JetStream supports deletion of individual Stream messages.

A message can be identified using its Stream Sequence.

This is different from purging the Stream because individual deletion targets a specific stored message while preserving other Stream data.

Sequence gaps resulting from deletion are not automatically filled.

---

# 16. Stream Purge

Purge provides bulk message cleanup.

The NATS CLI supports sequence-based and count-based purge operations.

### Purge by Sequence

The `--seq` option establishes a sequence boundary.

For:

```text
--seq=6
```

messages before sequence 6 are purged.

```text
[1][2][3][4][5] [6][7][8]
----------------  --------
     Purged          Kept
```

### Keep Latest Messages

The `--keep` option keeps a specified number of newest messages.

For:

```text
--keep=3
```

the latest three messages remain.

This is a one-time administrative cleanup operation.

It should not be confused with:

```text
max_msgs
```

which establishes an ongoing Stream retention limit.

---

# 17. Duplicate Detection

JetStream supports duplicate detection through a duplicate window.

Publishers can associate a message identifier with a message.

Conceptually:

```text
Message ID = event-123

First publish
     ↓
Accepted

Duplicate publish within window
     ↓
Identified as duplicate
```

The duplicate window determines how long the Stream maintains the information required to identify duplicate publishes.

---

# 18. Stream Replication

JetStream can replicate Stream state across multiple NATS servers.

Example:

```text
                 EVENTS
                    |
        +-----------+-----------+
        |           |           |
      NATS-1      NATS-2      NATS-3
      Replica     Replica     Replica
```

The Stream replica count is configured using:

```text
num_replicas
```

Common replication configurations include:

```text
1
3
5
```

subject to the available JetStream cluster topology.

Replication provides resilience against server failure.

---

# 19. NATS Cluster and JetStream Replication

A **NATS cluster** and a **replicated Stream** are related but different concepts.

### NATS Cluster

Defines the participating NATS servers.

```text
NATS-1
NATS-2
NATS-3
```

### Stream Replication

Defines how a particular Stream's state is replicated.

```text
EVENTS
 ├── Replica 1
 ├── Replica 2
 └── Replica 3
```

A three-node NATS cluster does not imply that every Stream is configured with three replicas.

Replication is a Stream-level decision.

---

# 20. Consensus and Quorum

Replicated JetStream state requires coordination between replicas.

A replicated Stream uses a consensus mechanism to maintain consistent state.

For three replicas:

```text
NATS-1
NATS-2
NATS-3
```

a majority/quorum is required for normal replicated operation.

This provides resilience against individual server failure.

Replication should therefore be evaluated against:

* Required availability
* Failure tolerance
* Storage overhead
* Network overhead
* Cluster size
* Operational complexity

---

# 21. Replication vs Backup

Replication and backup solve different problems.

| Capability   | Primary Purpose               |
| ------------ | ----------------------------- |
| File Storage | Persistence                   |
| Replication  | Availability / server failure |
| Backup       | Recovery / disaster recovery  |

For example:

```text
Server Failure
      ↓
Replication


Data Corruption / Disaster
      ↓
Backup / Restore
```

Replication should not be considered a substitute for an independent backup strategy.

---

# 22. Backup and Restore

JetStream supports Stream snapshot and restore capabilities.

Conceptually:

```text
Stream
   |
   | Snapshot
   v
Backup Artifact
   |
   | Restore
   v
Restored Stream
```

Backup/restore can support scenarios such as:

* Disaster recovery
* Data recovery
* Environment migration
* Operational recovery

The backup lifecycle should be managed independently from the operational Stream replication topology.

---

# 23. Object Storage and S3

Object storage can be incorporated into a backup architecture where supported by the selected NATS backup mechanism and operational tooling.

A conceptual architecture is:

```text
JetStream
    |
 Snapshot / Backup
    |
    v
Object Storage
    |
    +-- S3
    +-- S3-compatible storage
    +-- Other supported object storage
```

Object storage should be considered a **backup/recovery target**, not automatically as a replacement for JetStream's normal operational file storage.

The exact S3/object-storage workflow should be validated against the deployed NATS Server version and selected backup implementation.

---

# 24. Stream Observability

Stream observability should provide visibility into:

### Stream Health

* Stream state
* Operational errors
* Availability

### Resource Consumption

* Stored messages
* Stored bytes
* Storage utilization

### Message State

* First Sequence
* Last Sequence
* Message age
* Message growth rate

### Replication

* Replica state
* Leader state
* Quorum
* Replica availability

### Infrastructure

* CPU
* Memory
* Disk capacity
* Disk I/O
* Network utilization

---

# 25. Capacity Monitoring

Capacity should be evaluated at two levels.

## Stream-Level Capacity

Configured constraints:

```text
max_msgs
max_bytes
max_age
max_msg_size
```

## Infrastructure-Level Capacity

Underlying resources:

```text
CPU
Memory
Disk
Disk I/O
Network
```

A Stream approaching `max_msgs` does not necessarily mean infrastructure is overloaded. It may simply indicate that the configured retention policy is being enforced.

Conversely, increasing Stream limits without evaluating available disk capacity can create infrastructure risk.

---

# 26. Overload Detection

Potential indicators include:

* Rapid storage growth
* Increasing message rate
* Disk capacity approaching threshold
* High disk I/O
* CPU or memory pressure
* Replication overhead
* Increasing number of Streams
* Sustained increase in retained data

Monitoring should distinguish between:

```text
Expected retention behavior
```

and:

```text
Infrastructure capacity exhaustion
```

---

# 27. Adding a Subject vs Creating a Stream

The decision should be based on **shared lifecycle and operational requirements**, rather than simply event naming.

## Add a Subject to Existing Stream

Consider adding a subject when the new event shares:

* Retention policy
* Storage type
* Replication requirements
* Backup policy
* Lifecycle
* Ownership
* Capacity characteristics

Example:

```text
order.created
order.placed
order.cancelled
```

can share:

```text
EVENTS
  └── order.*
```

when their operational requirements are aligned.

---

# 28. Creating a Separate Stream

A separate Stream should be considered when there is a meaningful difference in one or more operational dimensions.

### Retention

```text
Orders       → 30 days
Audit events → 1 year
```

### Storage

```text
Critical events  → File
Transient events → Memory
```

### Replication

```text
Critical events → 3 replicas
Other events    → 1 replica
```

### Backup and Recovery

Different recovery objectives can justify separate Streams.

### Ownership

Different operational owners can justify independent Stream boundaries.

### Capacity

Very high-volume subjects may warrant isolation from lower-volume event categories.

The core principle is:

> **A Stream should group subjects that share the same durability, lifecycle and operational requirements.**

---

# 29. Stream Design Decision Matrix

| Requirement                       | Same Stream | Separate Stream |
| --------------------------------- | ----------- | --------------- |
| Same retention                    | ✓           |                 |
| Same storage                      | ✓           |                 |
| Same replication                  | ✓           |                 |
| Same backup policy                | ✓           |                 |
| Same lifecycle                    | ✓           |                 |
| Same ownership                    | ✓           |                 |
| Significantly different volume    |             | ✓               |
| Different retention               |             | ✓               |
| Different storage                 |             | ✓               |
| Different replication             |             | ✓               |
| Different recovery requirements   |             | ✓               |
| Independent operational ownership |             | ✓               |

This should be treated as a design guideline rather than a strict technical rule.

---

# 30. CLI Configuration Coverage

The CLI should be used to validate the actual capabilities available in the deployed environment.

Relevant commands include:

```text
nats stream --help
nats stream add --help
nats stream edit --help
nats stream update --help
nats stream info --help
nats stream get --help
nats stream msg --help
nats stream purge --help
```

The configuration review should capture:

* Supported fields
* Allowed values
* Defaults
* Update behavior
* CLI-specific limitations
* Server-version dependencies

---

# 31. Server and CLI Version Consideration

The demonstration environment currently uses:

```text
NATS Server : v2.14.6
NATS CLI    : v0.4.0
```

The two components have independent versioning.

Therefore:

```text
Official NATS Documentation
          |
          v
NATS Server API / Capabilities
          |
          v
Installed NATS CLI
          |
          v
Actual CLI Demonstration
```

The deployed NATS Server determines the available JetStream server capabilities.

The CLI determines how those capabilities can be administered from the command line.

Where differences exist, the deployed environment should be treated as the source of truth for the demonstration.

---

# 32. Configuration Values Used for Review

The current demonstration Stream uses:

```json
{
  "name": "EVENTS",
  "description": "Stream for storing orders related events",
  "subjects": [
    "order.*"
  ],
  "retention": "interest",
  "max_consumers": -1,
  "max_msgs_per_subject": -1,
  "max_msgs": -1,
  "max_bytes": -1,
  "max_age": 0,
  "max_msg_size": -1,
  "storage": "memory",
  "discard": "old",
  "num_replicas": 1,
  "duplicate_window": 120000000000,
  "sealed": false,
  "deny_delete": false,
  "deny_purge": false,
  "allow_rollup_hdrs": false,
  "allow_direct": true,
  "mirror_direct": false
}
```

This configuration is intended for demonstrating Stream concepts and should not be interpreted as a production sizing recommendation.

---

# 33. Architectural Summary

JetStream Streams provide a combination of:

```text
                 JetStream Stream
                       |
       +---------------+---------------+
       |               |               |
   Subject Scope    Persistence     Retention
       |               |               |
   order.*         File/Memory     Limits/Age
       |                               |
       +---------------+---------------+
                       |
                   Sequences
                       |
                 Message Lifecycle
                       |
                 +-----+-----+
                 |           |
             Replication   Backup
                 |           |
              Cluster     Recovery
```

The Stream should be treated as a **durability and operational boundary**, not simply as a container for subjects.

The primary architecture decisions are:

1. Which subjects belong together
2. How long messages need to be retained
3. Where messages need to be stored
4. How much data can be retained
5. What failure tolerance is required
6. What recovery mechanism is required
7. How Stream health and capacity will be monitored
8. Whether subjects require independent operational boundaries

---

# 34. Key Architecture Principles

### Principle 1 — Subject scope should follow lifecycle

Subjects sharing the same lifecycle and operational requirements can generally share a Stream.

### Principle 2 — Storage is a durability decision

Memory and File storage have fundamentally different persistence characteristics.

### Principle 3 — Retention is a Stream policy

Message cleanup can be automatic through limits or explicitly triggered through administrative operations.

### Principle 4 — Sequence numbers are Stream state

Stream sequences provide ordered positions for messages and continue advancing even when individual messages are deleted.

### Principle 5 — Replication is not backup

Replication protects against server failure; backup provides an independent recovery mechanism.

### Principle 6 — Cluster and Stream replication are different

A NATS cluster provides the server topology. Stream configuration determines the replication of a particular Stream.

### Principle 7 — Observability should drive Stream decisions

Metrics should be used to determine whether to:

* Adjust Stream limits
* Add subjects
* Isolate workloads into new Streams
* Scale infrastructure

### Principle 8 — Stream boundaries should be intentional

Separate Streams should be introduced when durability, retention, storage, replication, recovery, ownership or capacity requirements materially differ.

---

# 35. Scope Boundary

This document focuses on **JetStream Streams**.

Consumer-specific behavior is intentionally outside the detailed scope of this review, including:

* Durable consumer configuration
* Delivery policies
* Consumer acknowledgements
* Redelivery
* Consumer state management

These topics should be covered separately when reviewing the consumer side of JetStream.
