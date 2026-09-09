# NATS Stream

## 1. Overview

A **JetStream Stream** is a durable message store that captures messages published to one or more NATS subjects.

A Stream provides:

* Subject-based message capture
* Durable message storage
* Stream sequence numbers
* Retention and cleanup policies
* Message limits
* Replication across NATS servers
* Administrative operations such as message retrieval, deletion and purge
* Backup and restore capabilities

### Mental Model

```text
Publisher
    |
    | order.placed
    v
NATS Subject
    |
    | subject matches Stream
    v
EVENTS Stream
    |
    +-- Sequence 1
    +-- Sequence 2
    +-- Sequence 3
    |
    +-- Retention
    +-- Storage
    +-- Replication
```

---

# 2. Stream Creation

A Stream is created by defining:

* Stream identity
* Subjects captured by the Stream
* Retention policy
* Storage type
* Message limits
* Replication configuration
* Stream capabilities

### Subject Matching

`order.*` matches individual subjects such as:

```text
order.placed
order.created
order.cancelled
```

It does not mean that `max_msgs_per_subject` applies to `order.*` as one subject.

For example:

```text
order.placed       → independent limit
order.created      → independent limit
order.cancelled    → independent limit
```

### Live Demo

**Terminal:** Create `EVENTS` Stream using the configuration.

**Show:** Stream information after creation.

---

# 3. Editing an Existing Stream

A Stream configuration can be updated after creation.

Typical operational properties that can be changed include:

* Subjects
* Retention policy
* Maximum messages
* Maximum bytes
* Maximum message age
* Maximum message size
* Discard policy
* Duplicate detection window
* Stream capability flags

Some properties are constrained or effectively creation/topology decisions.

### Important examples

```text
Storage
memory → file
```

is not a normal Stream configuration update.

Similarly, changing:

```text
Replicas
1 → 3
```

requires an appropriate clustered JetStream environment.

### Live Demo

**Terminal:**

1. Show current Stream configuration.
2. Update selected limits/policies.
3. Show Stream information again.
4. Demonstrate that the Stream remains the same Stream.

### Takeaway

> Stream configuration can be adjusted operationally, while storage and topology-related properties have stronger lifecycle constraints.

---

# 4. Stream Storage

JetStream supports two primary Stream storage types:

| Storage    | Behavior                       |
| ---------- | ------------------------------ |
| **Memory** | Messages are held in memory    |
| **File**   | Messages are persisted to disk |

### Memory Storage

```text
Publisher
    ↓
EVENTS
    ↓
Memory
```

Fast access, but Stream messages do not survive a NATS server restart.

### File Storage

```text
Publisher
    ↓
EVENTS
    ↓
Disk
```

Messages survive a NATS server restart when the JetStream storage directory is persistent.

### Live Demo

**Terminal:**

1. Create/use a memory-backed Stream.
2. Publish messages.
3. Verify messages exist.
4. Restart NATS.
5. Verify memory-backed messages are lost.
6. Create/use a file-backed Stream.
7. Publish messages.
8. Restart NATS.
9. Verify messages remain.

### Operational Consideration

For production durable event streams, **file storage is normally the relevant choice**.

Memory storage is useful when persistence is not required and fast in-memory operation is preferred.

---

# 5. Stream Retention

Retention determines **when messages are eligible to leave the Stream**.

JetStream provides:

### Limits Policy

Messages are retained according to configured limits such as:

* Maximum messages
* Maximum bytes
* Maximum age

Example:

```text
max_msgs = 10000
```

Once the limit is reached, older messages may be discarded according to the discard policy.

### Interest Policy

Messages are retained based on consumer interest.

### Work Queue Policy

Messages are retained according to work-queue semantics.

> Interest and Work Queue retention are closely related to consumer behavior and are therefore covered only at a conceptual level in this review.

### Live Demo

Demonstrate a small `max_msgs` value:

```text
max_msgs = 5
```

Publish more than five messages and observe older messages being removed.

---

# 6. Stream Sequences

Every message stored in a Stream receives a **Stream Sequence**.

Example:

```text
Sequence    Message
1           Order-1
2           Order-2
3           Order-3
4           Order-4
5           Order-5
```

The Stream maintains boundaries:

```text
First Sequence → oldest retained message
Last Sequence  → newest retained message
```

### Sequence Behavior

When a new message is published:

```text
Last Sequence moves forward
```

When old messages are removed:

```text
First Sequence moves forward
```

Deleted sequences are **not reused**.

Example:

```text
1  2  3  4  5
```

Delete sequence `3`:

```text
1  2     4  5
```

Publish another message:

```text
1  2     4  5  6
```

Sequence `3` remains a historical gap.

### Live Demo

**Terminal:**

1. Publish multiple messages.
2. Inspect Stream sequences.
3. Retrieve a message using its sequence.
4. Delete one message by sequence.
5. Publish another message.
6. Observe that the sequence continues forward.

---

# 7. Stream Cleanup

JetStream provides both automatic and explicit cleanup mechanisms.

### Individual Message Delete

A specific Stream message can be deleted using its sequence.

```text
Sequence 5
     ↓
Delete message
```

### Stream Purge

A purge removes multiple messages according to the selected criteria.

Example:

```text
--seq=6
```

means:

> Purge messages before sequence 6.

So:

```text
1  2  3  4  5 | 6  7  8
---------------   --------
    Purged           Kept
```

### Keep Latest Messages

```text
--keep=3
```

retains the latest three messages and removes older messages.

This is a **one-time cleanup operation**, unlike `max_msgs`, which is an ongoing Stream limit.

### Live Demo

**Terminal:**

Demonstrate:

1. Delete one sequence.
2. Purge using `--seq`.
3. Purge using `--keep`.
4. Verify Stream sequence boundaries.

---

# 8. Replicated Streams

A Stream can be replicated across multiple NATS servers.

Example:

```text
              EVENTS
                 |
       +---------+---------+
       |         |         |
    NATS-1    NATS-2    NATS-3
    Replica   Replica   Replica
```

With:

```text
num_replicas = 3
```

JetStream maintains replicated Stream state across the cluster.

### Why Replication?

Replication provides:

* Fault tolerance
* Server failure resilience
* Durable replicated state
* Continued operation when a replica/server fails

### Important Distinction

**NATS Cluster**

```text
NATS-1
NATS-2
NATS-3
```

is the server topology.

**Stream Replication**

```text
EVENTS
 ├── replica
 ├── replica
 └── replica
```

is the replicated JetStream state.

A NATS cluster does not automatically mean every Stream has three replicas.

### Live Demo

**Terminal:**

1. Start a 3-node NATS cluster.
2. Create `EVENTS` with three replicas.
3. Show Stream information.
4. Identify the replicas.
5. Stop/fail the active server.
6. Show the Stream continuing through the remaining cluster.

---

# 9. Backup and Restore

JetStream supports Stream backup and restore through Stream snapshots.

Conceptually:

```text
EVENTS Stream
      |
      | Snapshot
      v
Backup Artifact
      |
      | Restore
      v
EVENTS Stream
```

Backup/restore is different from replication.

### Replication

Protects against:

```text
Server failure
```

### Backup

Protects against scenarios such as:

```text
Data recovery
Disaster recovery
Migration
Long-term backup
```

Object storage such as S3-compatible storage can be incorporated into an operational backup strategy, but this should be validated against the exact JetStream version and supported backup/restore mechanism used by the platform.

---

# 10. When Should We Create a Separate Stream?

A Stream should represent a meaningful **durability and lifecycle boundary**.

Consider a separate Stream when there is a difference in:

### Retention

```text
Orders → 30 days
Audit events → 1 year
```

### Storage

```text
High-value events → File
Temporary events → Memory
```

### Replication

```text
Critical events → 3 replicas
Non-critical events → 1 replica
```

### Lifecycle

If two event groups have different cleanup, backup, recovery or operational requirements, separate Streams may be appropriate.

### Operational Isolation

Separate Streams can provide clearer boundaries for:

* Capacity
* Monitoring
* Recovery
* Administration
* Retention
* Ownership

### Avoid unnecessary Streams

Do not create one Stream for every individual subject without a lifecycle reason.

For example:

```text
order.created
order.placed
order.cancelled
```

can be captured by:

```text
EVENTS
  └── order.*
```

when they share the same storage, retention, replication and operational requirements.

---

# 11. Stream Observability

Stream monitoring should answer four questions:

### Health

```text
Is the Stream healthy?
```

### Resource Consumption

```text
How much storage is being consumed?
How many messages are stored?
```

### Replication

```text
Are all replicas healthy?
Is quorum available?
```

### Capacity

```text
Are configured limits approaching?
```

Important signals include:

* Message count
* Storage bytes
* First/last sequence
* Message age
* Stream state
* Replica state
* Server resource utilization
* JetStream storage utilization

---

# 12. Capacity-Based Decisions

Observability should drive architectural decisions.

```text
                 Stream Metrics
                      |
        +-------------+-------------+
        |             |             |
     Healthy       Capacity       Overload
        |             |             |
        |             |             |
      Keep        Review limits   Investigate
                    |
              +-----+------+
              |            |
        Add subject    New Stream
```

### Add a Subject

Consider adding a subject when:

* The new event has the same lifecycle.
* Same retention policy.
* Same storage requirement.
* Same replication requirement.
* Same ownership/operational boundary.

### Create a New Stream

Consider a new Stream when the event requires:

* Different retention
* Different storage
* Different replication
* Different backup/recovery
* Different operational ownership
* Independent capacity management

### Add Infrastructure

Infrastructure scaling should be considered when:

* Server CPU/memory is consistently constrained.
* JetStream storage is approaching capacity.
* Replication requirements increase resource consumption.
* Message throughput exceeds the current infrastructure envelope.

---

# 13. CLI Demonstration Plan

The review uses CLI-focused demonstrations rather than a UI.

| Demo          | CLI Action                             | What the audience should observe |
| ------------- | -------------------------------------- | -------------------------------- |
| Create Stream | Create `EVENTS`                        | Stream configuration             |
| Inspect       | `nats stream info`                     | Current state                    |
| Edit          | Update configuration                   | Configuration changes            |
| Subjects      | Publish matching/non-matching subjects | Subject filtering                |
| Storage       | Memory vs File                         | Persistence behavior             |
| Sequences     | Publish/get/delete messages            | Sequence behavior                |
| Purge         | `--seq`, `--keep`                      | Bulk cleanup                     |
| Retention     | Small limits                           | Automatic cleanup                |
| Replication   | 3-node cluster                         | Replicated Stream                |
| Failure       | Stop a server                          | Failover behavior                |
| Backup        | Snapshot                               | Recovery mechanism               |

---

# 14. Review Scope

### Covered

* Stream creation
* Stream configuration
* Stream updates
* Subject management
* Storage
* Retention
* Stream sequences
* Message deletion
* Stream purge
* Replication
* Cluster behavior
* Backup and restore
* Stream observability
* Capacity considerations
* Criteria for separate Streams

### Deferred

**Consumers and delivery semantics** are outside this review.

Consumer concepts such as:

* Durable consumers
* Acknowledgements
* Delivery policies
* Redelivery
* Consumer state

will be covered separately.

---

# 15. Key Takeaways

A JetStream Stream should be viewed as a **durable, policy-controlled event storage boundary**.

The major architectural decisions are:

```text
Stream
 ├── Subjects
 ├── Retention
 ├── Storage
 ├── Limits
 ├── Sequence
 ├── Cleanup
 ├── Replication
 └── Operational boundary
```

The fundamental design question is not:

> "Should every subject have its own Stream?"

It is:

> **"Which subjects share the same lifecycle, durability, retention, replication and operational requirements?"**

Subjects that share those requirements can typically belong to the same Stream. Different lifecycle or operational requirements are strong reasons to create separate Streams.
