# NATS JetStream Stream Creation and Design Guidelines

## 1. Purpose

This document defines the criteria for determining:

1. **Why a NATS JetStream Stream should be created**
2. **When a Stream should be created**
3. **Which subjects should share a Stream**
4. **When subjects should be separated into different Streams**
5. **What functional, resource, durability and operational considerations should influence Stream design**

The objective is to establish a consistent Platform Engineering approach to Stream creation that avoids both:

* unnecessary Stream proliferation, and
* excessive consolidation of unrelated workloads into a single Stream.

The recommendations in this document are based on NATS JetStream capabilities and documented behavior, with additional Platform Engineering guidelines derived from the implications of those capabilities.

---

# 2. Fundamental Concept: What Is a Stream?

A NATS Stream is the JetStream abstraction that stores messages published to a defined set of subjects.

### NATS Documentation Says

NATS describes the purpose of a Stream using the following example:

> **"Plain core NATS drops any of these messages the moment no service is listening. A stream saves them instead."**

NATS further describes a Stream as:

> **"a store that runs on the server and keeps every message on the subjects you choose."**

The documentation also highlights that the saved messages can be read again later.

### What This Means

The fundamental distinction is:

```text
Core NATS

Producer
    │
    ▼
Subject
    │
    ▼
Active Consumer

Transient message delivery
```

versus:

```text
JetStream

Producer
    │
    ▼
Subject
    │
    ▼
Stream
    │
    ├── Persistent message storage
    ├── Retention
    ├── Replay
    ├── Consumer recovery
    └── Replication / durability
```

Therefore, a Stream is not simply a logical grouping of subjects.

It is a **message storage and lifecycle boundary**.

### Platform Engineering Guideline

> **A Stream should be created when the message lifecycle requires persistent, replayable, retained, recoverable or otherwise managed JetStream semantics that are not required from transient Core NATS messaging.**

---

# 3. Why Create a Stream?

The first question should not be:

> "Which Stream should this subject belong to?"

The first question should be:

> **"Do we need a Stream at all?"**

## 3.1 Core NATS May Be Sufficient

If the application only requires transient message delivery, Core NATS may be sufficient.

For example:

```text
Service A
    │
    │ publish
    ▼
user.status.update
    │
    ▼
Service B
```

If Service B is unavailable and there is no requirement to recover the message later, there may be no need for JetStream.

NATS explicitly describes this distinction: Core NATS drops messages when no service is listening, whereas a Stream stores them.

### Typical Core NATS Use Cases

* Request/reply
* Transient commands
* Live notifications
* Ephemeral application signals
* Real-time updates where historical recovery is not required

---

## 3.2 JetStream Is Required When Message Persistence Matters

Consider:

```text
Producer
    │
    ▼
orders.created
    │
    ▼
ORDERS Stream
    │
    ├── Warehouse Consumer
    ├── Notification Consumer
    └── Analytics Consumer
```

If a consumer goes offline, the requirement may be:

> "The consumer must process the event when it becomes available again."

That is fundamentally different from transient messaging.

NATS's own `ORDERS` example uses multiple order-related subjects:

```text
orders.created
orders.shipped
orders.canceled
```

and captures them using:

```text
orders.>
```

within the `ORDERS` Stream.

### Platform Engineering Guideline

Create a Stream when the workload requires one or more of the following:

* Message persistence
* Historical replay
* Durable consumption
* Defined retention
* Consumer recovery
* High availability through replication
* Storage limits
* Stream-level operational management

---

# 4. When Should We Create a Stream?

The following questions provide the primary decision criteria.

| Question                                                    | If Yes                                                          | Decision                                 |
| ----------------------------------------------------------- | --------------------------------------------------------------- | ---------------------------------------- |
| Does the message need to survive beyond immediate delivery? | Consumer downtime must not cause message loss                   | **Create Stream**                        |
| Do consumers need historical replay?                        | Messages need to be read again later                            | **Create Stream**                        |
| Does the workload require a defined retention policy?       | Messages must remain for a specific lifecycle                   | **Create Stream**                        |
| Does the workload require durable consumer state?           | Consumer progress must survive failures                         | **Create Stream + appropriate Consumer** |
| Does the workload require replication/HA?                   | Stream must survive server failure                              | **Create replicated Stream**             |
| Does the workload require bounded storage?                  | MaxAge, MaxBytes or MaxMsgs are required                        | **Create Stream**                        |
| Does the workload require JetStream-specific management?    | Stream state, retention, storage or replication must be managed | **Create Stream**                        |

The fundamental rule is:

> **Create a Stream when the application or platform requires capabilities provided by JetStream's persistent message store.**

---

# 5. When Should We NOT Create a Stream?

Stream creation should not be automatic for every application or subject.

Avoid assuming:

```text
One Application = One Stream
```

or:

```text
One Subject = One Stream
```

unless the requirements justify those boundaries.

If the requirement is only:

> "Deliver this message to an active consumer."

then Core NATS may be sufficient.

If the requirement becomes:

> "The message must remain available until the consumer can process it."

then JetStream becomes relevant.

This distinction is directly supported by NATS's Core NATS vs Stream explanation.

---

# 6. Why Stream Boundaries Matter

Once JetStream is required, the next question is:

> **Which subjects should share the same Stream?**

This is important because several important behaviors are configured at the Stream level.

### NATS Documentation Says

The NATS Stream information output shows configuration including:

```text
Subjects
Replicas
Storage
Retention
Acknowledgments
Discard Policy
Maximum Messages
Maximum Bytes
Maximum Age
Maximum Message Size
Maximum Consumers
```

These are Stream-level configuration properties.

NATS also demonstrates capturing multiple related subjects using one Stream:

```text
ORDERS
subjects: orders.>
```

where subjects such as:

```text
orders.created
orders.shipped
orders.canceled
```

are all captured by the same Stream.

### What This Means

A Stream is the boundary at which multiple subjects inherit/share important message lifecycle and storage behavior.

Therefore:

> **Subjects should be grouped into the same Stream when they can reasonably share the same Stream-level policies and operational characteristics.**

Conversely:

> **Subjects should be separated when differences in Stream-level requirements create undesirable coupling.**

---

# 7. NATS Constraint: A Subject Belongs to One Stream

This is one of the most important constraints for Stream design.

### NATS Documentation Says

NATS explicitly states:

> **"Only one stream can keep a given subject."**

The documentation demonstrates that if an existing Stream captures:

```text
orders.>
```

another Stream cannot capture:

```text
orders.*
```

or:

```text
orders.created
```

because the subject filters overlap.

NATS rejects such overlapping Stream configurations.

### What This Means

Stream boundaries cannot be designed independently.

The platform must consider the complete subject namespace when defining Stream filters.

For example:

```text
ORDERS
subjects: orders.>
```

means that the following subjects are already owned by that Stream:

```text
orders.created
orders.shipped
orders.cancelled
orders.refunded
...
```

A second Stream cannot independently claim those subjects.

### Platform Engineering Guideline

> **Stream subject filters should be deliberately designed because a subject cannot arbitrarily belong to multiple independent Streams.**

When data needs to be copied or aggregated between Streams, NATS provides mechanisms such as **Mirrors and Sources** rather than overlapping subject ownership.

---

# 8. Retention as a Stream Boundary

Retention is one of the strongest reasons for separating Streams.

### NATS Documentation Says

NATS supports three Stream retention policies:

### Limits

Messages remain until configured Stream limits are reached, such as:

* Maximum messages
* Maximum bytes
* Maximum age

### Interest

Messages are retained while there are consumers interested in them.

### WorkQueue

Messages are removed once they have been successfully acknowledged by the consumer processing the work.

NATS documents these as different Stream retention models.

NATS also provides an example where:

```text
ORDERS
```

uses a Limits-style event history, while:

```text
FULFILLMENT
```

uses WorkQueue semantics for processing work.

### What This Means

These are not merely different consumer configurations.

They represent different **message lifecycle semantics**.

For example:

```text
orders.events

"Keep the event history so it can be replayed."
```

versus:

```text
fulfillment.jobs

"Process this work once and remove it after acknowledgement."
```

These are fundamentally different workloads.

### Platform Engineering Guideline

> **Subjects with materially different message lifecycle or retention semantics should normally be placed in separate Streams.**

---

# 9. Stream Storage as a Boundary

Storage type is also a Stream-level decision.

### NATS Documentation Says

NATS documents two Stream storage types:

* File
* Memory

The NATS documentation states that File storage writes messages to disk so they survive a server restart, while Memory storage keeps messages in RAM and is lost when the server restarts.

NATS also explicitly states:

> **"Storage type is a property of the whole stream, not of individual replicas."**

### What This Means

You cannot have:

```text
Stream
├── Subject A → File
└── Subject B → Memory
```

inside the same Stream.

The storage requirement applies to the Stream.

### Platform Engineering Guideline

> **Subjects requiring materially different storage durability characteristics should be evaluated for separate Stream boundaries.**

---

# 10. Replication and Durability

Replication is another major Stream-level characteristic.

### NATS Documentation Says

NATS describes:

```text
Replicas = number of copies of the Stream
```

and explains that:

> **"R=1 has no fault tolerance."**

The documentation further explains that R=3 allows the Stream to tolerate the loss of one server while continuing to serve reads and writes.

NATS also states:

> **"Replicas are a durability control."**

and explains that increasing replica count increases storage and write-related load.

### What This Means

Replication is not simply a deployment preference.

It represents a durability/availability requirement for the Stream.

For example:

```text
Business-critical events
        ↓
       R=3
```

versus:

```text
Reconstructable / low-value data
        ↓
       R=1
```

may represent materially different durability requirements.

### Platform Engineering Guideline

> **Subjects with materially different availability, durability or replication requirements should be evaluated for separate Stream boundaries.**

However, replica count should not be increased simply to increase throughput.

NATS explicitly notes that replicas do not scale writes; writes still go through the Stream leader.

---

# 11. Workload / Scale Characteristics

This is where Platform Engineering judgment becomes important.

Consider:

```text
orders.created       → 1,000 msg/sec
orders.cancelled     → 2 msg/sec
```

Different throughput does **not automatically** mean separate Streams.

If both subjects have:

```text
Same retention
Same storage
Same replication
Same placement
Same ownership
Same lifecycle
Same recovery requirements
```

they may still reasonably share a Stream.

However, NATS documents an important scaling characteristic:

> **To scale writes past one leader, split subjects across Streams.**

This gives us a concrete NATS-backed reason why workload/scale can sometimes become a Stream-boundary decision.

### Platform Engineering Guideline

> **Different workload characteristics alone should not automatically create separate Streams. Separate Streams should be considered when workload characteristics create a material resource-isolation or write-scaling requirement.**

This is a much stronger criterion than simply saying:

> "Different throughput = different Stream."

---

# 12. Placement and Cluster Boundaries

Stream placement determines where Stream replicas reside within the NATS deployment.

NATS documents placement controls as part of the clustering and replication model, including controls for which servers a Stream lands on and tag-based placement steering.

### What This Means

If two datasets have fundamentally different placement requirements, combining them into one Stream may not be appropriate.

For example:

```text
Stream A
Region: India

Stream B
Region: US
```

cannot simply be represented as one Stream if their placement requirements are fundamentally different.

### Platform Engineering Guideline

> **Subjects that must reside in different NATS clusters, regions or placement boundaries should be represented through separate Stream boundaries or an appropriate multi-Stream architecture.**

---

# 13. Backup and Disaster Recovery

Replication should not be confused with backup.

### NATS Documentation Says

NATS explicitly explains that replication keeps a Stream available when a node fails, but does not protect against logical mistakes such as:

* accidental purge
* bad migration
* application bugs
* incorrect deletion

NATS describes a snapshot as a **point-in-time copy** of a Stream. The snapshot contains the Stream configuration and state and can be restored to recreate the Stream.

### What This Means

There are different protection mechanisms:

```text
Replication
    ↓
High Availability / Node Failure

Snapshot / Backup
    ↓
Point-in-Time Recovery

Mirror / DR Architecture
    ↓
Cross-site / Disaster Recovery
```

Therefore, replication and backup requirements should be considered independently.

### Platform Engineering Guideline

> **Streams with materially different backup frequency, recovery objectives or DR requirements may require separate operational boundaries.**

---

# 14. Security / Tenancy Boundary

Security and tenancy are primarily **Platform Engineering criteria**, rather than a direct NATS rule saying "one tenant = one Stream."

The platform should therefore not claim that NATS mandates a Stream boundary for every tenant.

Instead, consider:

```text
Tenant A
    ↓
Security / ownership boundary

Tenant B
    ↓
Different security / ownership boundary
```

If combining subjects creates an undesirable administrative or operational coupling, separate Streams may be appropriate.

However:

> **Stream separation should complement, not replace, NATS security mechanisms such as accounts and permissions.**

### Platform Engineering Guideline

> **Where subjects belong to materially different trust, tenancy or administrative boundaries, evaluate separate Streams as an isolation mechanism.**

---

# 15. Lifecycle and Ownership

Similarly, ownership is a Platform Engineering consideration.

Consider:

```text
payments.events
    → Payments Team

audit.events
    → Security Team
```

If the two datasets have different:

* Owners
* Lifecycle
* Change processes
* Retention requirements
* Recovery requirements
* Operational responsibilities

then combining them may introduce unnecessary coupling.

### Platform Engineering Guideline

> **Independently owned or independently managed workloads should be evaluated for separate Stream boundaries when their lifecycle or operational requirements materially differ.**

---

# 16. Operational Management

A Stream is also an operational unit.

NATS exposes Stream-level information such as:

```text
Subjects
Replicas
Storage
Retention
Discard Policy
Limits
Messages
Bytes
Consumers
```

through Stream inspection.

Therefore, operational teams need to reason about Streams as manageable units.

### Platform Engineering Guideline

Subjects can share a Stream when they can reasonably be:

* Monitored together
* Governed together
* Backed up together
* Recovered together
* Capacity-managed together
* Owned operationally together

If these requirements differ materially, separate Stream boundaries should be considered.

---

# 17. Criteria for Creating Separate Streams

Based on the NATS capabilities above and the associated Platform Engineering implications, the following matrix can be used.

| Priority | Criterion                         | Same Stream                                               | Separate Streams                                                               |
| -------: | --------------------------------- | --------------------------------------------------------- | ------------------------------------------------------------------------------ |
|    **1** | **Retention / Message Lifecycle** | Subjects have compatible retention semantics              | Subjects require materially different retention or lifecycle semantics         |
|    **2** | **Storage**                       | Same storage durability requirement                       | File vs Memory or materially different storage requirements                    |
|    **3** | **Replication / Durability**      | Same availability and durability requirements             | Different replication/durability requirements                                  |
|    **4** | **Placement**                     | Same cluster/placement domain                             | Different cluster, region or placement requirements                            |
|    **5** | **Write Scaling / Workload**      | Workloads can share the same Stream leader                | Workload requires subject partitioning for write scaling or resource isolation |
|    **6** | **Backup / DR**                   | Same RPO/recovery requirements                            | Materially different backup or recovery requirements                           |
|    **7** | **Ordering / Processing Domain**  | Subjects belong to a compatible logical processing domain | Independent processing/order domains require isolation                         |
|    **8** | **Security / Tenancy**            | Same trust/access boundary                                | Different trust/tenant/admin boundaries                                        |
|    **9** | **Ownership / Lifecycle**         | Same owner and lifecycle                                  | Independently owned/managed lifecycle                                          |
|   **10** | **Operational Management**        | Can be monitored and recovered together                   | Requires independent operational treatment                                     |

---

# 18. Strong vs. Contextual Separation Criteria

Not every difference should automatically result in a new Stream.

## 18.1 Strong Separation Criteria

The following are generally strong reasons to separate:

### 1. Different retention/message lifecycle

Example:

```text
application.events → 7 days
audit.events       → 1 year
```

### 2. Different storage requirements

Example:

```text
critical.events → File
ephemeral.data  → Memory
```

### 3. Different replication/durability requirements

Example:

```text
business.events → R=3
reconstructable.data → R=1
```

### 4. Different placement

Example:

```text
Region A workload
Region B workload
```

### 5. Different write-scaling requirements

If a single Stream's leader becomes a write-scaling bottleneck, NATS documents splitting subjects across Streams as a scaling mechanism.

---

## 18.2 Contextual Separation Criteria

These require engineering judgment:

* Security / tenancy
* Ownership
* Lifecycle
* Backup / DR
* Operational management
* Ordering
* Consumer model
* Workload characteristics

The principle should be:

> **Separate only when the difference creates a material policy, resource, lifecycle or operational isolation requirement.**

---

# 19. Avoiding Stream Proliferation

NATS explicitly emphasizes that related subjects can be grouped under one Stream.

For example:

```text
ORDERS
subjects: orders.>
```

captures:

```text
orders.created
orders.shipped
orders.canceled
```

and potentially future subjects under the same namespace.

At the same time, Stream configuration introduces storage, retention, replication and operational responsibilities.

Therefore the platform should avoid both extremes.

## Too Few Streams

```text
Too few Streams
      ↓
Unwanted policy coupling
      ↓
Different workloads affect each other
```

## Too Many Streams

```text
Too many Streams
      ↓
More storage / replica overhead
      ↓
More configuration
      ↓
More monitoring
      ↓
More backup/recovery units
      ↓
More operational complexity
```

NATS specifically notes that replicas increase storage and write-related load, making unnecessary replication and Stream boundaries an actual resource consideration.

### Platform Engineering Guideline

> **Create the minimum practical number of Streams that satisfies the required message lifecycle, durability, isolation, scaling and operational requirements.**

---

# 20. Stream Limits Are Part of Creation

Stream creation should also include an explicit capacity and retention strategy.

### NATS Documentation Says

NATS's example defaults include:

```text
Maximum Messages: unlimited
Maximum Bytes:    unlimited
Maximum Age:      unlimited
```

The documentation explicitly warns that unlimited limits can allow the Stream to grow until the disk fills, potentially taking the server down. It states:

> **"a production stream needs at least one limit so old orders age out before the disk does."**

### Platform Engineering Guideline

Every production Stream should have an explicitly reviewed capacity/retention strategy, including appropriate consideration of:

* MaxAge
* MaxBytes
* MaxMsgs
* Maximum message size
* Discard policy

NATS documents these controls as part of shaping a Stream.

---

# 21. Practical Examples

## Example 1 — Same Stream

Subjects:

```text
orders.created
orders.updated
orders.cancelled
```

Requirements:

* Same retention
* Same storage
* Same replication
* Same cluster
* Same ownership
* Same lifecycle
* Same operational requirements

Recommended:

```text
ORDERS
├── orders.created
├── orders.updated
└── orders.cancelled
```

This follows the same grouping model demonstrated by NATS.

---

## Example 2 — Separate Streams Due to Retention

```text
application.events → 7 days
audit.events       → 1 year
```

Retention is a Stream-level characteristic.

Recommended:

```text
APPLICATION_EVENTS
└── application.events

AUDIT_EVENTS
└── audit.events
```

---

## Example 3 — Separate Streams Due to Message Lifecycle

```text
orders.events
```

requires:

```text
Retain event history
Replay events
```

while:

```text
fulfillment.jobs
```

requires:

```text
Process work
Acknowledge
Remove
```

These represent different retention semantics.

NATS itself demonstrates this distinction through the `ORDERS` and `FULFILLMENT` examples.

---

## Example 4 — Different Throughput Does Not Automatically Mean Separate Stream

```text
orders.created       → 1,000 msg/sec
orders.cancelled     → 2 msg/sec
```

Different traffic alone does not require separation.

However, if the aggregate workload requires write scaling beyond what a single Stream leader can provide, NATS documents splitting subjects across Streams as a scaling mechanism.

Therefore:

```text
Different throughput
        ≠
Automatic Stream separation
```

but:

```text
Material write-scaling requirement
        →
Consider Stream partitioning
```

---

## Example 5 — Replication Is Not Backup

Suppose:

```text
PAYMENTS
replicas = 3
```

This provides resilience against node failure.

It does **not** protect against:

```text
Accidental purge
Bad migration
Incorrect deletion
Application bug
```

NATS explicitly distinguishes replication from snapshot-based backup/recovery.

Therefore:

```text
Replication
    +
Backup
    +
DR
```

should be treated as separate architectural requirements.

---

# 22. Three-Level Stream Decision Model

The complete decision process is:

```text
                    Do we need JetStream?
                              │
                 ┌────────────┴────────────┐
                 │                         │
                NO                        YES
                 │                         │
            Core NATS                 Create Stream
                                           │
                                           ▼
                              Identify compatible subjects
                                           │
                                           ▼
                              Compare Stream-level policies
                                           │
                                           ▼
                              Evaluate isolation requirements
                                           │
                              ┌────────────┴────────────┐
                              │                         │
                         Compatible              Material difference
                              │                         │
                              ▼                         ▼
                         Same Stream             Separate Streams
                              │                         │
                              └────────────┬────────────┘
                                           │
                                           ▼
                               Validate resource,
                               scaling and operational cost
```

This produces three distinct architectural decisions:

### Level 1 — Do we need JetStream?

Core NATS vs Stream.

### Level 2 — What belongs in one Stream?

Group subjects with compatible Stream-level requirements.

### Level 3 — When should we split?

Separate subjects when there is a material policy, durability, scaling, lifecycle, security or operational boundary.

---

# 23. Platform Engineering Decision Rule

The platform should follow these steps.

### Step 1 — Determine Whether JetStream Is Required

Ask:

> Does the workload require persistence, replay, retention, durable consumption or other Stream-level behavior?

If **No**:

```text
Core NATS
```

If **Yes**:

```text
Proceed with Stream design
```

---

### Step 2 — Identify Compatible Subjects

Determine which subjects can share:

* Message lifecycle
* Retention
* Storage
* Replication
* Placement
* Recovery
* Operational management

---

### Step 3 — Identify Strong Boundaries

Consider separate Streams for material differences in:

* Retention
* Storage
* Replication/durability
* Placement
* Write scaling
* Message lifecycle

---

### Step 4 — Evaluate Contextual Boundaries

Evaluate:

* Security/tenancy
* Ownership
* Lifecycle
* Backup/DR
* Consumer model
* Ordering
* Operational management

---

### Step 5 — Validate Resource Cost

Before introducing another Stream, evaluate:

* Storage consumption
* Replica overhead
* Write traffic
* Stream configuration count
* Monitoring
* Backup/recovery
* Operational ownership

---

# 24. Final Stream Creation Principle

> **Create a Stream when the message lifecycle requires persistent, replayable, retained, recoverable or otherwise managed JetStream semantics.**
>
> **Group subjects into the same Stream when they can share compatible Stream-level policies and operational characteristics.**
>
> **Separate subjects into different Streams when differences in retention, storage, durability, placement, message lifecycle, write scaling, security, ownership, recovery or operational requirements create meaningful coupling.**
>
> **Do not create separate Streams merely because subjects are logically different, and do not combine subjects merely because they belong to the same application.**
>
> **The target is the minimum practical number of Streams that satisfies the required functional, durability, scaling, isolation and operational boundaries.**

---

# 25. Summary Decision Matrix

| Decision                                     | Guideline                                  | Basis                                         |
| -------------------------------------------- | ------------------------------------------ | --------------------------------------------- |
| Need persistence/replay?                     | Use JetStream Stream                       | **NATS Documentation**                        |
| Only transient delivery required?            | Core NATS may be sufficient                | **NATS Documentation**                        |
| Need retention?                              | Stream required                            | **NATS Documentation**                        |
| Different retention semantics?               | Consider separate Streams                  | **NATS capability → Platform guideline**      |
| Different storage durability?                | Consider separate Streams                  | **NATS capability → Platform guideline**      |
| Need replication/HA?                         | Configure Stream replicas                  | **NATS Documentation**                        |
| Different durability requirements?           | Consider separate Streams                  | **NATS capability → Platform guideline**      |
| Need bounded storage?                        | Configure Stream limits                    | **NATS Documentation**                        |
| Need write scaling beyond one Stream leader? | Consider splitting subjects across Streams | **NATS Documentation**                        |
| Different placement requirements?            | Separate placement/Stream boundary         | **NATS capability → Platform guideline**      |
| Different message lifecycle?                 | Prefer separate Streams                    | **NATS retention model → Platform guideline** |
| Different consumers only?                    | Usually does not require separate Stream   | **NATS consumer model**                       |
| Different throughput only?                   | Do not automatically split                 | **Platform Engineering guideline**            |
| Different security/tenancy?                  | Evaluate separate boundary                 | **Platform Engineering guideline**            |
| Different ownership/lifecycle?               | Evaluate separate boundary                 | **Platform Engineering guideline**            |
| Need HA?                                     | Replication                                | **NATS Documentation**                        |
| Need point-in-time recovery?                 | Snapshot/backup                            | **NATS Documentation**                        |
| Need DR?                                     | Mirror/DR architecture as appropriate      | **NATS Documentation**                        |
| Creating one Stream per subject?             | Avoid unless justified                     | **Platform Engineering guideline**            |
| Creating one Stream for everything?          | Avoid if it creates policy coupling        | **Platform Engineering guideline**            |

---

# 26. Key NATS Documentation References

The following official NATS documentation should be retained as the primary references for this guideline:

1. **Your First Stream** — Why Streams exist, Core NATS vs JetStream, subject capture, Stream configuration, subject ownership and Stream pitfalls.

2. **Retention Policies** — Limits, Interest and WorkQueue retention semantics.

3. **Shaping the Stream** — Stream limits, retention/capacity controls and discard behavior.

4. **Surviving Node Loss** — Replication, storage durability, fault tolerance, placement and the resource cost of replicas.

5. **Stream Backup and Restore** — Snapshots, point-in-time recovery and the distinction between replication and backup.

---

## Overall Decision Principle

The Stream creation decision should ultimately be viewed as:

```text
                    MESSAGE REQUIREMENT
                           │
                           ▼
                  Need JetStream?
                    /          \
                  NO            YES
                  │              │
                  ▼              ▼
             Core NATS       Create Stream
                                  │
                                  ▼
                       Group compatible subjects
                                  │
                                  ▼
                       Apply Stream-level policies
                                  │
                                  ▼
                      Evaluate material boundaries
                                  │
                    ┌─────────────┴─────────────┐
                    │                           │
              No material                 Material
               boundary                   boundary
                    │                           │
                    ▼                           ▼
              Same Stream                Separate Streams
                    │                           │
                    └─────────────┬─────────────┘
                                  │
                                  ▼
                       Validate capacity,
                       scaling and operations
```

### The core Platform Engineering principle is:

> **A Stream should be treated as a durable message-storage, lifecycle, durability and operational boundary—not merely as a grouping mechanism for subjects.**
>
> **Use NATS's Stream capabilities to determine the technical boundary, and use Platform Engineering considerations to determine the operational boundary.**
