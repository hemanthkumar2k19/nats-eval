# Stream Architecture & Developer Internals

In NATS JetStream, a Stream represents the fundamental unit of storage, retention, message sequence ordering, replication, placement, and security governance. This document covers developer-level stream internals, in-memory sequence state pointers, data structure representations, account tenancy boundaries, system-wide S2 compression standards, and storage interface abstractions.

---

## 1. Overview & Stream Scope

A Stream is the primary data storage abstraction in NATS JetStream. It binds to one or more NATS subject wildcards (e.g., `order.*`, `payment.*`), accepts incoming published messages, assigns monotonically increasing 64-bit sequence numbers, and persists data according to configured storage backends and retention policies.

### 1.1 Key Stream Architectural Boundaries

- **Unit of Storage & Retention**: Retention rules (`limits`, `interest`, `workqueue`), limits (`max_msgs`, `max_bytes`, `max_age`), storage engines (`file` vs `memory`), and discard policies (`discard: "old"` vs `discard: "new"`) are declared and enforced strictly per stream.
- **Unit of Ordering**: Message sequence numbers (`1, 2, 3, ...`) are assigned sequentially per stream by the active Stream Leader. Cross-subject ordering is preserved for all subjects bound to the same stream.
- **Unit of Replication & Placement**: Replication factor (`num_replicas`), cluster placement tags (`placement.tags`), and Raft consensus groups are managed at the stream level.

---

## 2. In-Memory Stream State Pointers & Data Structures

Every active stream replica maintains six core state pointers and indices in Go memory structures (`stream.go` / `store.go`) to track stream bounds, sequence progression, and message deletion:

```text
Stream In-Memory State
|-- FirstSeq / FirstTime   (uint64, int64)    Oldest active message sequence & timestamp
|-- LastSeq / LastTime     (uint64, int64)    Newest active message sequence & timestamp
|-- Msgs                   (uint64)           Total active message count
|-- Bytes                  (uint64)           Total aggregate byte size of active messages
|-- NumDeleted / dmap      (avl.SequenceSet)  AVL tree set tracking deleted/purged sequence gaps
|-- Subjects (psim)        (stree.SubjectTree) Subject tree tracking FirstSeq, LastSeq, & count per subject
```

### 2.1 Sequence Pointers (`FirstSeq` & `LastSeq`)

- **`FirstSeq` / `FirstTime`**: Sequence number and nanosecond timestamp of the oldest active message stored in the stream. Advances forward as messages age out, are purged, or are discarded by limit enforcement.
- **`LastSeq` / `LastTime`**: Sequence number and nanosecond timestamp of the newest (most recently published) message in the stream. Increments monotonically on every publish.

#### Sequence Pointer Movement Example

Initial State (10 active messages):
```text
Sequence Numbers: 1 2 3 4 5 6 7 8 9 10
                  |                  |
               FirstSeq           LastSeq (FirstSeq=1, LastSeq=10, Msgs=10)
```

After Purging All Messages:
```text
Sequence Numbers: 1 2 3 4 5 6 7 8 9 10 [11]
                                        |
                                  FirstSeq, LastSeq (FirstSeq=11, LastSeq=10, Msgs=0)
```

### 2.2 Aggregate Metrics (`Msgs` & `Bytes`)

- **`Msgs`**: Total count of active (non-purged, non-deleted) messages in the stream. Updated on every publish, delete, or purge.
- **`Bytes`**: Total aggregate byte size of active message payloads and headers. Used for `MaxBytes` limit calculations.

### 2.3 Message Deletion Index (`NumDeleted` / `dmap`)

When individual messages are deleted (`nats stream rm`) or range-purged between `FirstSeq` and `LastSeq`, NATS does not rewrite the log file segment immediately. Instead, it tracks sparse sequence gaps using an in-memory AVL tree set (`avl.SequenceSet`):

- **`NumDeleted`**: Total count of deleted sequences within active stream bounds.
- **`dmap`**: Memory-efficient range tracking (`SequenceSet`) storing sequence gap ranges without allocating heap objects per individual sequence.

### 2.4 Subject Index Tree (`Subjects` / `psim`)

To accelerate subject-filtered consumption and subject-filtered purges without scanning the entire Write-Ahead Log:

- **Structure**: Uses an in-memory radix subject tree (`stree.SubjectTree[SimpleState]`).
- **State per Subject**: Maintains `FirstSeq`, `LastSeq`, and message count (`msgs`) for every unique subject bound to the stream.

---

## 3. Account Tenancy & Namespace Boundaries

NATS uses Accounts as multi-tenant isolation boundaries across a cluster:

- **Cluster-Wide Namespace**: Accounts act as isolated namespaces across the entire NATS cluster mesh.
- **Single Parent Ownership**: Every Stream entity belongs exclusively to one parent Account.
- **Cross-Account Access**: Clients in secondary accounts can publish to or consume from a stream in a primary account using:
  - **NATS Service / Stream Exports and Imports**: Scoped subject mapping and permission authorization.
  - **Stream Mirrors & Sources**: Asynchronous cross-account data replication.
  - Ownership and administrative control remain strictly bound to the originating parent account.

---

## 4. System-Wide S2 Compression Standard

S2 (a high-performance extension of Snappy designed for high-throughput streaming data) is the official standard compression algorithm across the NATS codebase:

| Subsystem | Usage | Operational Detail |
| :--- | :--- | :--- |
| **Stream Storage** | Message Payload & Header Compression | Configured via `compression: "s2"` on file-backed streams; compresses log blocks before writing to disk |
| **Raft Consensus** | Snapshot Transfer Compression | Leaders compress large Raft snapshots using S2 before streaming to catching-up followers (`sendSnapshotToFollower`) |
| **Backups** | Stream Archive Compression | `nats stream backup` packages log segments into s2-compressed tarballs (`.tar.s2`) |
| **Cluster Mesh** | Wire & Leafnode Compression | Compresses inter-server route traffic and leafnode connections under high payload volume |
| **Batching** | Inflight Batch Compression | Compresses atomic batch publish frames (`compressedStreamMsgOp`) over the wire |

---

## 5. Stream Storage Engine Abstractions (`StreamStore`)

NATS decouples stream management logic from physical storage engines using Go interface abstractions:

```text
                  +-----------------------------------+
                  |      StreamStore Interface        |
                  |            (store.go)             |
                  +-----------------+-----------------+
                                    |
          +-------------------------+-------------------------+
          |                                                   |
          v                                                   v
+-------------------+                               +-------------------+
|     fileStore     |                               |     memStore      |
|  (filestore.go)   |                               |  (memstore.go)    |
| - WAL Log Blocks  |                               | - Go Heap Maps    |
| - Index DB (.idx) |                               | - Subject Tree    |
| - Consumer State  |                               | - AVL Delete Set  |
+-------------------+                               +-------------------+
```

- **`StreamStore` (`store.go`)**: Declares standard methods for message storage (`StoreMsg`), retrieval (`LoadMsg`), sequence purging (`Purge`), message erasure (`RemoveMsg`), and state inspection (`State`).
- **`fileStore` (`filestore.go`)**: Implements durable disk persistence using append-only log segments (`.blk`), index databases (`index.db`), and consumer state files (`o.dat`).
- **`memStore` (`memstore.go`)**: Implements volatile RAM storage using Go heap maps (`map[uint64]*StoreMsg`), in-memory subject trees, and AVL sequence sets.
