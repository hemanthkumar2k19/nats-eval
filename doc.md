# NATS JetStream Streams — Technical Reference

## 1. Purpose

This document provides a technical reference for understanding and evaluating **NATS JetStream Streams**, with a focus on their concepts, internal behaviour, configuration, persistence, replication, and architectural characteristics.

It covers:

* NATS and JetStream fundamentals
* Stream architecture and internal concepts
* Stream configuration
* Subject organisation and subject matching
* Stream lifecycle
* Message storage and persistence
* Retention and message limits
* Stream sequences and message state
* Message deletion and purge behaviour
* Replication, clustering, and consensus
* Backup and restore
* Stream observability and resource management
* Stream organisation and separation criteria
* NATS CLI and Stream configuration coverage

The purpose of this document is to establish the **NATS concepts and internal mechanisms** required to reason about Stream behaviour and make appropriate architectural decisions.

This document is a **technical reference**. CLI demonstration procedures and demo-specific steps are maintained separately.

---

# 2. NATS and JetStream Fundamentals

## 2.1 NATS

NATS is a lightweight messaging system based around subjects.

A publisher sends a message to a subject, and subscribers receive messages based on subject subscriptions.

For example:

```text
order.created
order.placed
order.cancelled
payment.completed
payment.failed
```

Core NATS provides real-time messaging, but messages are generally transient. A message is delivered to active subscribers and is not inherently stored as durable event history.

JetStream extends NATS with **persistence and message storage**.

---

## 2.2 JetStream

JetStream is the persistence and streaming subsystem of NATS.

It introduces durable message storage, allowing messages to be retained independently of whether a subscriber was connected at the time of publication.

Conceptually:

```text
                  NATS
                   │
        ┌──────────┴──────────┐
        │                     │
   Core NATS              JetStream
  Real-time messaging     Persistence
                              │
                           Streams
                              │
                         Stored Messages
```

JetStream therefore provides the storage layer required for use cases such as:

* Event history
* Durable messaging
* Work queues
* Replayable events
* Event-driven workflows
* Integration event storage

NATS documentation describes JetStream as the durable messaging capability built into NATS.

---

# 3. JetStream Architecture

A useful conceptual model is:

```text
Publisher
    │
    │ publish(subject, message)
    ▼
NATS Server
    │
    │ subject matching
    ▼
JetStream Stream
    │
    ├── Message Storage
    ├── Stream Sequence
    ├── Retention
    ├── Limits
    └── Replication
```

A Stream is therefore not simply a topic.

A subject determines **where messages are published**.

A Stream determines **which messages are captured and how those messages are stored and managed**.

---

## 3.1 Stream as a Storage Boundary

A Stream defines a durable storage boundary around one or more NATS subjects.

For example:

```text
Stream: EVENTS

Subjects:
    order.*
```

The Stream captures messages published to:

```text
order.created
order.placed
order.cancelled
```

while messages published to unrelated subjects are not part of the Stream.

A Stream therefore combines:

* Subject scope
* Storage
* Retention
* Message limits
* Sequence management
* Replication configuration
* Lifecycle controls

---

# 4. Stream Concepts

A JetStream Stream has several fundamental dimensions.

| Dimension          | Determines                                       |
| ------------------ | ------------------------------------------------ |
| Subjects           | Which messages belong to the Stream              |
| Storage            | Where messages are persisted                     |
| Retention          | When messages cease to be retained               |
| Limits             | Maximum messages, bytes, age, etc.               |
| Sequence           | Position of messages within the Stream           |
| Replicas           | Number of copies maintained                      |
| Discard policy     | Behaviour when configured limits are reached     |
| Lifecycle controls | Whether administrative deletion/purge is allowed |

These dimensions should be considered independently.

For example:

```text
Subject scope
      +
Retention policy
      +
Storage type
      +
Replication
      +
Capacity limits
      =
Stream behaviour
```

---

# 5. Stream Configuration Reference

The following represents the primary Stream configuration concepts relevant to architectural evaluation.

| Configuration            | Value / Option       | Description                                         | Default |
| ------------------------ | -------------------- | --------------------------------------------------- | ------- |
| Name                     | String               | Unique Stream identifier                            | —       |
| Description              | String               | Human-readable Stream description                   | Empty   |
| Subjects                 | Subject list         | Subjects captured by the Stream                     | —       |
| Subjects                 | `*`                  | Matches exactly one subject token                   |         |
| Subjects                 | `>`                  | Matches one or more trailing subject tokens         |         |
| Retention                | `limits`             | Retention governed by configured limits             | ✓       |
| Retention                | `interest`           | Messages retained based on consumer interest        |         |
| Retention                | `workqueue`          | Work-queue-oriented retention semantics             |         |
| Max Consumers            | `-1`                 | No configured consumer limit                        | ✓       |
| Max Consumers            | Positive integer     | Maximum number of consumers                         |         |
| Max Messages per Subject | `-1`                 | No per-subject message limit                        | ✓       |
| Max Messages per Subject | Positive integer     | Maximum messages retained for each concrete subject |         |
| Max Messages             | `-1`                 | No total message-count limit                        | ✓       |
| Max Messages             | Positive integer     | Maximum messages retained by the Stream             |         |
| Max Bytes                | `-1`                 | No total byte limit                                 | ✓       |
| Max Bytes                | Positive integer     | Maximum message data retained                       |         |
| Max Age                  | `0`                  | No age-based expiration                             | ✓       |
| Max Age                  | Duration             | Maximum age of retained messages                    |         |
| Max Message Size         | `-1`                 | No Stream-specific message-size limit               | ✓       |
| Max Message Size         | Positive integer     | Maximum size of an individual message               |         |
| Storage                  | `file`               | File-backed persistent storage                      | ✓       |
| Storage                  | `memory`             | In-memory storage                                   |         |
| Num Replicas             | `1`                  | Single Stream replica                               | ✓       |
| Num Replicas             | `3`                  | Three Stream replicas                               |         |
| Num Replicas             | `5`                  | Five Stream replicas                                |         |
| Discard                  | `old`                | Discard older messages when limits are reached      | ✓       |
| Discard                  | `new`                | Reject/discard new messages when limits are reached |         |
| Duplicate Window         | `0`                  | Default duplicate-detection behaviour               | ✓       |
| Duplicate Window         | Duration             | Time window for duplicate detection                 |         |
| Sealed                   | `false`              | Stream remains mutable                              | ✓       |
| Sealed                   | `true`               | Prevents further modification to Stream data        |         |
| Deny Delete              | `false`              | Message deletion permitted                          | ✓       |
| Deny Delete              | `true`               | Prevents message deletion                           |         |
| Deny Purge               | `false`              | Stream purge permitted                              | ✓       |
| Deny Purge               | `true`               | Prevents Stream purge                               |         |
| Allow Rollup Headers     | `false`              | Rollup headers not permitted                        | ✓       |
| Allow Rollup Headers     | `true`               | Allows rollup operations                            |         |
| Allow Direct             | `false`              | Direct message access disabled                      | ✓       |
| Allow Direct             | `true`               | Allows direct message access                        |         |
| Mirror Direct            | `false`              | Direct mirror access disabled                       | ✓       |
| Mirror Direct            | `true`               | Allows direct access for mirror scenarios           |         |
| Consumer Limits          | Configuration object | Stream-level limits applicable to consumers         | `{}`    |

NATS also exposes additional Stream configuration capabilities in newer server/API versions, including subject transforms, placement constraints, sources, mirrors, republish configuration, compression, metadata, message TTL-related options, and other advanced features. The exact set available depends on the NATS Server/API version in use.

---

# 6. Subjects and Subject Matching

Subjects define the message namespace used by NATS.

A subject consists of tokens separated by `.`.

For example:

```text
order.created
order.payment.completed
customer.profile.updated
```

Two wildcard tokens are fundamental:

### `*`

Matches exactly one token.

```text
order.*
```

matches:

```text
order.created
order.placed
order.cancelled
```

but does not match:

```text
order.payment.completed
```

### `>`

Matches one or more trailing tokens.

```text
order.>
```

can match:

```text
order.created
order.payment.completed
order.shipping.address.updated
```

---

## 6.1 Subject Scope

Subjects determine the boundary of the Stream's captured message set.

For example:

```text
Stream: ORDER_EVENTS

Subjects:
    order.*
```

means the Stream captures messages matching that subject pattern.

The Stream does not create a separate storage object for each wildcard expression.

The wildcard defines the **matching rule**.

---

## 6.2 Per-Subject Limits

`max_msgs_per_subject` applies to each **concrete subject**, not to the wildcard expression itself.

For:

```text
Subjects:
    order.*
```

the following are independent concrete subjects:

```text
order.created
order.placed
order.cancelled
```

Therefore:

```text
max_msgs_per_subject = 1000
```

means that each matching concrete subject can retain up to the configured limit.

It does **not** mean that all `order.*` messages share a single 1000-message bucket.

---

# 7. Stream Lifecycle

A Stream progresses through a lifecycle consisting broadly of:

```text
Create
  │
  ▼
Active
  │
  ├── Update
  │
  ├── Retain / Expire
  │
  ├── Delete Messages
  │
  ├── Purge
  │
  └── Seal
        │
        ▼
     Terminal State
```

## 7.1 Creation

At creation time, the Stream establishes:

* Name
* Subject scope
* Retention model
* Storage model
* Capacity limits
* Replication configuration
* Administrative controls

These choices establish the Stream's fundamental behaviour.

---

## 7.2 Update

A Stream can be updated to modify supported configuration properties.

Typical operationally adjustable properties include:

* Description
* Subjects
* Retention configuration
* Message limits
* Age limits
* Discard policy
* Duplicate window
* Administrative permissions

Some properties are constrained by the underlying storage or cluster topology.

For example, changing the conceptual storage model from memory-backed storage to file-backed storage is not equivalent to changing a simple metadata field.

Similarly, increasing replication requires suitable JetStream servers capable of hosting the additional replicas.

---

## 7.3 Sealing

A Stream can be sealed to prevent further changes to the stored message set.

Sealing should be treated as a lifecycle operation rather than a normal configuration toggle.

A sealed Stream is therefore appropriate for scenarios where the retained event history is intended to become immutable.

---

# 8. Message Storage and Persistence

JetStream supports two primary storage modes:

```text
File
Memory
```

## 8.1 File Storage

File-backed storage persists Stream messages to the server's filesystem.

Conceptually:

```text
Publisher
    │
    ▼
JetStream
    │
    ▼
Disk
```

Advantages:

* Durable across server process restart
* Suitable for persistent event storage
* Supports larger datasets than memory-only storage
* Appropriate for production durable messaging

Actual durability also depends on the underlying filesystem, disk, container volume, and infrastructure configuration.

---

## 8.2 Memory Storage

Memory-backed Streams retain messages in server memory.

```text
Publisher
    │
    ▼
JetStream
    │
    ▼
Memory
```

Advantages:

* Lower storage overhead
* Fast access
* Useful for transient or high-speed workloads

Trade-off:

* Stored data is not durable across server restart in the same way as file-backed storage.

Memory storage should therefore be selected when the loss of retained Stream data following server failure is acceptable.

---

# 9. Retention Policies

Retention determines when JetStream is allowed to remove messages.

The major retention modes are:

```text
Limits
Interest
Work Queue
```

## 9.1 Limits Retention

`limits` is the general-purpose retention model.

Messages remain available until one or more configured limits causes them to be removed.

Relevant limits include:

* Maximum messages
* Maximum messages per subject
* Maximum bytes
* Maximum age

Conceptually:

```text
Message
   │
   ├── max age exceeded ──────► Remove
   │
   ├── max messages exceeded ─► Remove
   │
   └── max bytes exceeded ────► Remove
```

---

## 9.2 Interest Retention

Interest retention relates message retention to consumer interest.

The retention decision therefore depends on whether there are consumers with an applicable interest in the message.

This model is useful when the Stream represents messages that only need to exist while required consumers still have interest in them.

---

## 9.3 Work Queue Retention

Work-queue retention is designed around queue-style processing.

The conceptual model is:

```text
Message
   │
   ▼
Available for processing
   │
   ▼
Consumed
   │
   ▼
No longer required
```

It is therefore different from an event-history Stream whose primary purpose is replay.

---

# 10. Message Limits and Cleanup

Retention limits provide the mechanisms through which Stream storage is bounded.

### Maximum messages

```text
max_msgs
```

defines the maximum number of messages retained by the Stream.

### Maximum messages per subject

```text
max_msgs_per_subject
```

defines the maximum retained messages for each concrete subject.

### Maximum bytes

```text
max_bytes
```

limits total retained message data.

### Maximum age

```text
max_age
```

removes messages once they exceed the configured age.

### Maximum message size

```text
max_msg_size
```

restricts the size of individual messages accepted into the Stream.

These limits can operate together.

For example:

```text
max_msgs = 1,000,000
max_bytes = 10 GB
max_age = 7 days
```

means retention is bounded by all configured constraints rather than by only one of them.

---

# 11. Discard Behaviour

When a Stream reaches a configured limit, the `discard` policy determines which side of the boundary is affected.

## `discard: old`

Older messages are removed to make room for newer messages.

Conceptually:

```text
Oldest ─────────────────── Newest
   X                         +
   │                         │
 removed                    added
```

This is appropriate for bounded event history where the newest events are considered more valuable.

## `discard: new`

New messages are rejected/discarded when the configured limit prevents additional storage.

Conceptually:

```text
Existing messages → retained

New message
     │
     ▼
Limit reached
     │
     ▼
Rejected
```

This is appropriate when preserving the existing dataset is more important than accepting new messages.

---

# 12. Stream Sequences

JetStream assigns a monotonically increasing **Stream Sequence** to stored messages.

Conceptually:

```text
Sequence    Subject
--------    ----------------
1           order.created
2           order.placed
3           order.cancelled
4           order.created
5           order.placed
```

The Stream sequence provides an ordered position within the Stream.

It is independent of the application-level event identifier.

For example:

```text
Stream Sequence = 42
Order ID        = ORD-10021
```

These represent different concepts.

---

## 12.1 First and Last Sequence

A Stream maintains a current sequence range.

Conceptually:

```text
FirstSeq ------------------------ LastSeq
   101                              250
```

`FirstSeq` represents the oldest currently retained sequence.

`LastSeq` represents the newest currently retained sequence.

As old messages expire:

```text
FirstSeq
   │
   ▼
moves forward
```

As new messages arrive:

```text
LastSeq
   │
   ▼
moves forward
```

---

## 12.2 Sequence Gaps

Deleting a message does not renumber subsequent messages.

For example:

```text
1  2  3  4  5
```

Delete sequence `3`:

```text
1  2     4  5
```

The next published message receives:

```text
6
```

The Stream therefore preserves sequence identity rather than compacting the sequence after deletion.

---

## 12.3 Initial Sequence

The initial Stream sequence and the current Stream `FirstSeq` are different concepts.

The initial sequence controls the starting sequence numbering when the Stream is established.

The current `FirstSeq` represents the oldest sequence currently retained after normal Stream operation.

---

# 13. Message Deletion and Purging

JetStream supports removal of individual messages as well as bulk removal.

## 13.1 Individual Message Deletion

An individual message can be removed using its Stream sequence.

For example:

```text
1  2  3  4  5
      X
```

After deletion:

```text
1  2     4  5
```

The sequence is not reused.

---

## 13.2 Purge

A purge removes multiple messages from a Stream.

Purging can be scoped according to available purge criteria such as:

* Sequence boundary
* Subject
* Number of messages to retain

For example, conceptually:

```text
1  2  3  4  5  6  7

Purge before 6

1  2  3  4  5     X

Remaining:
6  7
```

Purging is an administrative operation and is distinct from automatic retention.

---

## 13.3 Retention vs Purge

These should not be treated as the same mechanism.

| Mechanism   | Purpose                                 |
| ----------- | --------------------------------------- |
| `max_msgs`  | Continuous automatic retention boundary |
| `max_bytes` | Continuous storage boundary             |
| `max_age`   | Continuous age-based expiration         |
| Delete      | Explicit removal of selected messages   |
| Purge       | Explicit bulk removal                   |

Retention is part of the Stream's ongoing lifecycle.

Purge is an administrative action.

---

# 14. Duplicate Detection

JetStream supports duplicate detection using the message identifier supplied through the NATS message headers.

A publisher can associate a unique message identifier with a message.

JetStream can then use the configured duplicate window to identify repeated publication of the same message.

Conceptually:

```text
Publisher
   │
   ├── Message ID = MSG-100
   │
   ▼
JetStream
   │
   ├── First MSG-100 → Stored
   │
   └── Duplicate MSG-100 within window
                    │
                    ▼
                 Detected
```

The duplicate window therefore provides protection against certain publisher retry scenarios.

It should not be confused with application-level exactly-once processing.

---

# 15. Replication

JetStream Streams can be replicated across multiple NATS servers.

The primary configuration concept is:

```text
num_replicas
```

Typical values include:

```text
1
3
5
```

Conceptually:

```text
                 Stream
                   │
          ┌────────┼────────┐
          ▼        ▼        ▼
       Replica 1 Replica 2 Replica 3
          │        │        │
       NATS-1   NATS-2   NATS-3
```

Replication provides resilience against server failure.

---

## 15.1 Single Replica

With:

```text
num_replicas = 1
```

there is only one Stream replica.

A server failure can therefore make the Stream unavailable.

---

## 15.2 Three Replicas

With:

```text
num_replicas = 3
```

the Stream can be distributed across three suitable JetStream servers.

This provides tolerance for a server failure while maintaining quorum.

---

## 15.3 Five Replicas

Five replicas provide greater failure tolerance but also increase:

* Storage consumption
* Network traffic
* Replication overhead
* Resource requirements

Replication should therefore be selected according to availability requirements rather than simply maximised.

---

# 16. Consensus and Quorum

JetStream replication is associated with a consensus-based replication model.

For a three-replica group:

```text
Replica 1
Replica 2
Replica 3
```

a majority requires:

```text
2 / 3
```

replicas.

Therefore:

```text
3 replicas
    │
    ├── 1 failure → quorum maintained
    │
    └── 2 failures → quorum lost
```

This is the architectural reason a three-replica Stream provides resilience against a single server failure but not against two simultaneous replica failures.

Replication therefore has two dimensions:

1. **Data redundancy**
2. **Availability through quorum**

A replica count should consequently be evaluated together with cluster topology.

---

# 17. NATS Cluster vs Stream Replication

These concepts should not be conflated.

## NATS Cluster

A NATS cluster is a group of NATS servers that communicate through server-to-server routes.

```text
NATS Server
     │
     ├──── NATS Server
     │
     └──── NATS Server
```

The cluster provides the server topology required for distributed NATS operation.

## JetStream Replication

JetStream uses multiple servers to maintain replicated Stream state.

```text
NATS Cluster
     │
     └── JetStream
           │
           └── Stream
                ├── Replica
                ├── Replica
                └── Replica
```

Therefore:

> A NATS cluster provides the server topology; JetStream replication determines how a particular Stream is replicated across that topology.

---

# 18. Storage vs Replication vs Backup

These three mechanisms solve different problems.

| Mechanism      | Primary Purpose                     |
| -------------- | ----------------------------------- |
| File storage   | Persistence on the local server     |
| Memory storage | Fast, non-durable storage           |
| Replication    | Availability and failure tolerance  |
| Backup         | Recovery from data loss or disaster |

For example:

```text
File Storage
     │
     ▼
Persistent local data

Replication
     │
     ▼
Multiple NATS servers

Backup
     │
     ▼
Independent recovery copy
```

Replication should therefore **not** be treated as a replacement for backup.

If data is intentionally deleted or purged, replication will generally replicate the resulting state rather than provide an independent historical copy.

---

# 19. Backup and Restore

JetStream supports snapshot-based backup and restore mechanisms.

The conceptual model is:

```text
JetStream Stream
       │
       ▼
   Snapshot
       │
       ▼
Backup Storage
       │
       ▼
     Restore
       │
       ▼
JetStream Stream
```

Backup is intended to provide an independent recovery mechanism.

---

## 19.1 Object Storage

For larger deployments, object storage can be used as an external durability layer for backup workflows.

Examples include:

* Amazon S3
* Google Cloud Storage
* S3-compatible object stores

This introduces a separation between:

```text
Operational storage
        │
        ▼
JetStream local storage

and

Recovery storage
        │
        ▼
Object storage
```

Object storage should therefore be considered a **backup/recovery mechanism**, not simply another Stream replica.

---

# 20. Stream Observability

A Stream should be monitored across several dimensions.

## 20.1 Message State

Important indicators include:

* Number of stored messages
* First sequence
* Last sequence
* Number of subjects
* Number of consumers
* Message age
* Stream state

These provide visibility into the logical state of the Stream.

---

## 20.2 Storage Consumption

Storage-related metrics should include:

* Bytes stored
* Storage growth rate
* Disk utilisation
* Memory utilisation for memory-backed Streams
* Storage capacity remaining

A Stream may be logically healthy while the underlying storage infrastructure approaches capacity.

---

## 20.3 Replication Health

For replicated Streams, monitor:

* Replica count
* Replica availability
* Leader state
* Replication lag/state
* Quorum availability
* Server health

The important architectural question is not merely:

> "Are there three replicas configured?"

but:

> "Are the required replicas healthy and is quorum currently available?"

---

# 21. Capacity Management

Capacity should be evaluated at multiple levels.

```text
Stream
  │
  ├── Message count
  ├── Message size
  ├── Byte limit
  ├── Age
  └── Subject distribution
        │
        ▼
NATS Server
  │
  ├── CPU
  ├── Memory
  ├── Disk
  └── Network
        │
        ▼
Infrastructure
```

A Stream limit and infrastructure capacity are therefore separate concerns.

For example:

```text
max_bytes = 100 GB
```

does not imply that the server should necessarily have exactly 100 GB of available disk.

Operational overhead, other Streams, replication, filesystem requirements, and infrastructure headroom must also be considered.

---

# 22. Stream Organisation and Separation

A key architectural decision is determining when multiple subjects should belong to the same Stream versus separate Streams.

## 22.1 Multiple Subjects in One Stream

Multiple subjects can share a Stream when they have compatible:

* Retention requirements
* Storage requirements
* Replication requirements
* Lifecycle
* Operational ownership
* Capacity characteristics

Example:

```text
ORDER_EVENTS

order.created
order.placed
order.cancelled
```

These may naturally represent one event domain.

---

## 22.2 Separate Streams

Separate Streams become appropriate when the underlying requirements differ materially.

Examples:

### Different retention

```text
ORDER_EVENTS
    30 days

AUDIT_EVENTS
    7 years
```

These should generally not share the same Stream.

### Different storage characteristics

```text
Operational events → high-volume file storage

Transient events → memory storage
```

### Different replication requirements

```text
Normal events → 1 replica

Critical financial events → 3 replicas
```

### Different operational ownership

If two message domains have different lifecycle or operational ownership, separate Streams can provide clearer administrative boundaries.

---

# 23. Stream Separation Decision Criteria

The following decision model can be used when determining Stream boundaries.

| Criterion                         | Same Stream | Separate Stream |
| --------------------------------- | ----------- | --------------- |
| Same retention                    | ✓           |                 |
| Different retention               |             | ✓               |
| Same storage requirement          | ✓           |                 |
| Different storage requirement     |             | ✓               |
| Same replication requirement      | ✓           |                 |
| Different replication requirement |             | ✓               |
| Same lifecycle                    | ✓           |                 |
| Independent lifecycle             |             | ✓               |
| Same operational ownership        | ✓           |                 |
| Different ownership               |             | ✓               |
| Similar capacity characteristics  | ✓           |                 |
| Significantly different capacity  |             | ✓               |
| Same event domain                 | ✓           |                 |
| Different event domains           |             | Often ✓         |

The objective is not to minimise the number of Streams.

The objective is to establish **coherent storage and lifecycle boundaries**.

---

# 24. Stream vs Subject

A subject and a Stream operate at different abstraction levels.

```text
Subject
  =
Messaging namespace

Stream
  =
Durable storage boundary
```

For example:

```text
order.created
order.placed
order.cancelled
```

are subjects.

A Stream such as:

```text
ORDER_EVENTS
```

can capture all three.

Therefore, adding a new subject does not necessarily require creating a new Stream.

A new Stream should be considered when the new subject has materially different storage, retention, replication, lifecycle, or operational requirements.

---

# 25. Stream Internals — Conceptual Message Path

A simplified message path is:

```text
Publisher
   │
   ▼
NATS subject
   │
   ▼
Subject matching
   │
   ▼
JetStream Stream
   │
   ├── Assign Stream Sequence
   │
   ├── Persist message
   │
   ├── Apply duplicate detection
   │
   ├── Apply retention constraints
   │
   └── Replicate if configured
           │
           ▼
      Stored Stream State
```

This is a conceptual model rather than an implementation-level representation of every internal operation.

The important architectural point is that a Stream maintains state beyond the transient NATS message delivery path.

---

# 26. Stream Data Lifecycle

A stored message can conceptually progress through:

```text
Published
    │
    ▼
Accepted by Stream
    │
    ▼
Assigned Stream Sequence
    │
    ▼
Stored
    │
    ├── Retained
    │
    ├── Replicated
    │
    ├── Deleted explicitly
    │
    ├── Purged
    │
    └── Expired by retention limits
            │
            ▼
         Removed
```

This lifecycle explains why JetStream can function as both:

* A durable messaging mechanism
* A bounded event store

depending on its configuration.

---

# 27. Administrative Protection

JetStream provides configuration controls to protect Stream data from administrative operations.

Important controls include:

### `deny_delete`

Prevents individual message deletion.

### `deny_purge`

Prevents bulk Stream purge operations.

### `sealed`

Provides a terminal lifecycle state for the Stream.

These controls are useful where accidental or unauthorised data removal is a concern.

They should be considered part of the Stream's governance model rather than merely configuration options.

---

# 28. Version and API Considerations

NATS Server and NATS CLI are independently versioned components.

For example, an environment may have:

```text
NATS Server
v2.x

NATS CLI
v0.x
```

The available CLI commands and exposed configuration options can therefore differ from the latest NATS documentation.

For architectural evaluation:

1. The NATS Server/API version determines actual server capabilities.
2. The NATS CLI version determines the CLI interface available to the operator.
3. The deployed environment should be treated as the source of truth for supported operational commands.
4. Official NATS documentation should be used as the authoritative reference for the corresponding server/API version.

This distinction is particularly important when reviewing newer Stream configuration fields.

---

# 29. CLI Configuration Coverage

The NATS CLI provides operational access to Stream lifecycle and management capabilities.

Relevant conceptual operations include:

| Operation          | Purpose                            |
| ------------------ | ---------------------------------- |
| Stream creation    | Establish a new Stream             |
| Stream update      | Modify supported configuration     |
| Stream information | Inspect Stream state               |
| Message inspection | Inspect individual stored messages |
| Message deletion   | Remove specific messages           |
| Stream purge       | Remove groups of messages          |
| Stream deletion    | Remove the Stream itself           |

The CLI is an operational interface over the NATS/JetStream API. It should therefore not be treated as the definition of JetStream's complete conceptual model.

The available options depend on the installed CLI version.

---

# 30. Architecture Considerations

The following principles should guide Stream design.

### 1. A Stream is a durable storage boundary

Do not treat a Stream as merely a topic container.

### 2. Subjects define message scope

Subject patterns determine which messages enter the Stream.

### 3. Retention defines lifecycle

A Stream's retention configuration determines how long messages remain available.

### 4. Storage and retention are independent

File storage does not determine how long messages are retained.

### 5. Replication is not backup

Replication provides operational resilience; backup provides independent recovery.

### 6. Replica count must match cluster topology

Configuring three replicas requires suitable JetStream servers capable of hosting those replicas.

### 7. Quorum matters

A replicated Stream requires a healthy quorum for distributed operation.

### 8. Stream boundaries should follow operational requirements

Subjects with materially different retention, storage, replication, lifecycle, or ownership requirements should generally be separated.

### 9. Limits should be intentional

Unlimited message count, bytes, or age should be an explicit architectural decision rather than an accidental default.

### 10. Observability must cover both logical and physical state

A Stream can be logically healthy while its underlying disk, memory, network, or replica resources approach failure conditions.

---

# 31. Conceptual Summary

The overall JetStream Stream model can be represented as:

```text
                         NATS
                          │
                     Subjects
                          │
                          ▼
                    ┌───────────┐
                    │  Stream   │
                    └─────┬─────┘
                          │
             ┌────────────┼────────────┐
             │            │            │
             ▼            ▼            ▼
         Retention     Storage      Sequences
             │            │            │
             │            │            │
             ▼            ▼            ▼
          Cleanup      Persistence   Ordering
                          │
                          ▼
                     Replication
                          │
                          ▼
                     NATS Cluster
                          │
                          ▼
                      Quorum
                          │
                          ▼
                 Operational Resilience

                  Independent of:

                     Backup
                        │
                        ▼
                 Object Storage
```

The central architectural model is:

> **Subjects define what enters a Stream; the Stream defines how those messages are stored, retained, sequenced, and replicated.**

This distinction is fundamental when evaluating NATS JetStream as a durable messaging and event-storage capability.

---

# 32. References

### NATS Documentation

[NATS Documentation](https://docs.nats.io/?utm_source=chatgpt.com)

Primary documentation portal covering NATS concepts, JetStream, server configuration, APIs, operations, and reference material.

### JetStream Concepts

[JetStream Documentation](https://docs.nats.io/nats-concepts/jetstream?utm_source=chatgpt.com)

Conceptual reference for JetStream, including Streams and durable messaging.

### Stream API — Create

[JetStream Stream Create API](https://docs.nats.io/reference/reference-protocols/jetstream-api/streams/stream-create?utm_source=chatgpt.com)

Reference for Stream creation and Stream configuration fields.

### Stream API — Update

[JetStream Stream Update API](https://docs.nats.io/reference/reference-protocols/jetstream-api/streams/stream-update?utm_source=chatgpt.com)

Reference for Stream configuration updates and update semantics.

### JetStream Clustering and Replication

[NATS JetStream Clustering](https://docs.nats.io/running-a-nats-service/configuration/clustering/jetstream_clustering?utm_source=chatgpt.com)

Reference for JetStream operation in clustered NATS environments and replicated Stream architecture.

### JetStream Persistence

[NATS JetStream Persistence](https://docs.nats.io/using-nats/jetstream/manage/storage?utm_source=chatgpt.com)

Reference for JetStream storage and persistence behaviour.

### JetStream Snapshots

[NATS JetStream Snapshots and Restore](https://docs.nats.io/using-nats/jetstream/manage/snapshots?utm_source=chatgpt.com)

Reference for Stream snapshot and restoration capabilities.

### NATS Monitoring

[NATS Monitoring](https://docs.nats.io/running-a-nats-service/configuration/monitoring?utm_source=chatgpt.com)

Reference for NATS server monitoring and operational endpoints.

### NATS CLI

[NATS CLI Repository](https://github.com/nats-io/natscli?utm_source=chatgpt.com)

Reference for the NATS command-line interface and its version-specific capabilities.
