# File Storage (Persistent / Disk)

JetStream streams configured with `storage: "file"` persist messages, indices, consumer state, and consensus logs to disk. Data survives server restarts, host reboots, and process crashes. This document covers the on-disk layout, file formats, and operational details that SREs and developers need for troubleshooting and capacity planning.

---

## 1. Overview

### 1.1 What Is Stored on Disk

A file-backed stream writes four categories of data to disk:

| Category | Description |
| -------- | ----------- |
| Messages | Binary message records (payload, headers, subject, sequence, timestamp) written sequentially to block files |
| Stream indices | Subject state tree, sequence boundaries, block summaries, TTL wheels, and scheduling state |
| Consumer state | Delivered sequences, ACK floors, pending ACKs, and redelivery counts for each consumer |
| Consensus logs | Raft WAL entries, term/vote state, and snapshots (only for clustered streams with R > 1) |

### 1.2 Clustered Streams

For clustered streams (R > 1), each replica node stores a full copy of the stream data directory plus a separate Raft consensus directory. The Raft directory holds the Write-Ahead Log (WAL) used for consensus replication and is independent of the stream message store.

### 1.3 Storage Interface

NATS decouples stream execution logic from the storage backend using Go interfaces. The stream layer (`stream.go`) operates against a `StreamStore` interface (`store.go`), with `fileStore` (`filestore.go`) providing the persistent disk implementation. This means all stream operations (publish, consume, purge, delete) work identically regardless of storage backend -- only the durability and performance characteristics differ.

---

## 2. Configuration

The base directory for all JetStream data on disk is configured via `store_dir`:

```conf
jetstream {
    store_dir: "/data/jetstream"
}
```

All stream data, consumer state, and Raft consensus directories are created under this path. The NATS server process requires read/write permissions on this directory and all its contents.

---

## 3. Directory Structure

A file-backed stream creates two top-level directory trees under `store_dir`:

| Directory | Purpose | Present When |
| --------- | ------- | ------------ |
| `<store_dir>/<account>/streams/<stream_name>/` | Stream data: configuration, message blocks, indices, consumer state | Always |
| `<store_dir>/$SYS/_js_/<raft_group_name>/` | Raft consensus WAL and snapshots | Clustered streams (R > 1) only |

### 3.1 Stream Data Directory

```
<store_dir>/<account>/streams/<stream_name>/
|-- meta.inf
|-- meta.sum
|-- meta.key
|-- sources.db
|-- batches/
|-- msgs/
|   |-- 1.blk
|   |-- 1.idx
|   |-- 1.key
|   |-- 2.blk
|   |-- 2.idx
|   |-- index.db
|   |-- thw.db
|   |-- sched.db
|-- obs/
    |-- <consumer_name>/
        |-- meta.inf
        |-- meta.sum
        |-- meta.key
        |-- o.dat
```

### 3.2 Raft Cluster Directory (R > 1)

```
<store_dir>/$SYS/_js_/<raft_group_name>/
|-- meta.inf
|-- meta.sum
|-- names.dat / term.dat
|-- msgs/
|   |-- 1.blk
|   |-- 1.idx
|-- snapshots/
    |-- <term>.<applied_index>
```

---

## 4. File Formats

### 4.1 Stream Metadata Files

| File | Format | Contents |
| ---- | ------ | -------- |
| `meta.inf` | JSON | `FileStreamInfo` containing the full `StreamConfig` (retention policy, subjects, max bytes/msgs, max age, replicas, etc.) and the stream creation timestamp |
| `meta.sum` | Binary (8 bytes) | HighwayHash64 checksum of `meta.inf`, used to detect file corruption on restart |
| `meta.key` | Binary | Encrypted master key for the stream (present only when Encryption at Rest / `JetStreamKey` is enabled) |

### 4.2 Message Store Files (msgs/ Directory)

NATS breaks stream messages into sequential chunks called message blocks. Each block is typically 8MB (or 4MB depending on stream type and retention settings).

#### Block Message Files (`<N>.blk`)

Contains binary message records written sequentially. Each record has the following binary layout:

| Field | Size | Description |
| ----- | ---- | ----------- |
| Record Length | uint32 (4 bytes) | Total record size |
| Sequence Number | uint64 (8 bytes) | Stream sequence number |
| Timestamp | int64 (8 bytes) | Unix nanoseconds |
| Subject Length | uint16 (2 bytes) | Length of subject string |
| Subject | Variable | Subject bytes |
| Header Length | uint32 (4 bytes) | Length of headers (optional, present when headers exist) |
| Headers | Variable | Header bytes (optional) |
| Payload | Variable | Raw message payload bytes |
| Checksum | uint64 (8 bytes) | HighwayHash64 checksum of the record |

#### Block Index Files (`<N>.idx`)

Index header for fast lookup into the corresponding `.blk` file without scanning the entire block. Contains a magic header, message count, byte size, first and last sequence numbers, timestamp bounds, and a bitset (dmap) tracking deleted message sequence numbers within that block.

#### Block Key Files (`<N>.key`)

Per-block AEAD encryption keys (present only when Encryption at Rest is active).

#### Stream State Index (`index.db`)

A snapshot of the entire stream state flushed asynchronously to disk. Contains:

- Global sequence numbers (FirstSeq, LastSeq)
- Message counts and byte totals
- Tombstones for deleted messages
- Summary of all message blocks
- Subject State Tree (psim) mapping subjects to sequence boundaries

The Subject State Tree enables NATS to recover full stream state on startup without reading all `.blk` files, keeping startup times sub-second to a few seconds even for multi-gigabyte streams.

`index.db` is NOT an SQLite database. NATS uses `.db` as a file extension convention for binary state snapshot files. The format is a custom binary format: header starts with magic byte `11`, contains variable-length integer (varint) encoded data, and ends with an 8-byte HighwayHash64 checksum.

#### TTL and Scheduling Snapshots

| File | Contents |
| ---- | -------- |
| `thw.db` | Encoded state of the Time Hash Wheel tracking message expiration (`MaxAge`) |
| `sched.db` | Encoded state of scheduled message deliveries |

### 4.3 Stream Sources State (`sources.db`)

Located directly at `<store_dir>/<account>/streams/<stream_name>/sources.db`. Maintains replication state and sequence mappings for Stream Sourcing and Mirroring. Placed outside the `msgs/` directory so that a stream purge does not wipe source tracking state.

### 4.4 Consumer Files (obs/<consumer_name>/)

| File | Format | Contents |
| ---- | ------ | -------- |
| `meta.inf` | JSON | `FileConsumerInfo` containing the full consumer configuration |
| `meta.sum` | Binary (8 bytes) | HighwayHash64 checksum of `meta.inf` |
| `meta.key` | Binary | Encryption key for consumer metadata (present only when Encryption at Rest is enabled) |
| `o.dat` | Binary | Consumer state: delivered sequence (last delivered stream sequence and consumer sequence), ACK floor (highest sequence where all prior messages are acknowledged), pending ACKs (sequence numbers and timestamps of unacknowledged inflight messages), and redelivery counts per sequence number |

### 4.5 Raft Cluster Files (R > 1)

| File | Format | Contents |
| ---- | ------ | -------- |
| `meta.inf` | JSON | Raft Group configuration and node identity |
| `meta.sum` | Binary (8 bytes) | Checksum for `meta.inf` |
| `names.dat` / `term.dat` | Binary | Raft state: current term, voted-for candidate, peer node list |
| `msgs/<N>.blk` | Binary | Raft WAL log entry records (stream append/purge commands) |
| `msgs/<N>.idx` | Binary | Index for Raft WAL block |
| `snapshots/<term>.<applied_index>` | Binary | Raft state snapshots used to truncate older WAL entries |

---

## 5. Transient and Recovery Files

During normal operation, NATS creates temporary files for atomic writes and compaction:

| Pattern | Purpose |
| ------- | ------- |
| `.tmp` files (e.g., `meta.inf.tmp`, `index.db.tmp`, `1.blk.tmp`) | Created during atomic file writes or block compaction, then atomically renamed to the final filename |
| `__msgs__` and `__new_msgs__` directories | Temporary directories created during full stream purge or block compaction operations |

These files are safe to ignore during normal operation. Their presence after a crash indicates an incomplete atomic write -- NATS recovers automatically on restart by detecting the incomplete state and falling back to the last consistent version.

---

## 6. Node Recovery

### 6.1 Startup Sequence

When a node hosting a file-backed stream replica restarts:

1. **Local metadata load**: Reads `meta.inf` and loads `index.db` to restore the Subject State Tree and block summaries. Because `index.db` holds pre-indexed state, NATS does not scan all `.blk` files on disk. Local boot takes sub-seconds to a few seconds even for multi-gigabyte streams.
2. **Raft group re-join**: Initializes the Raft node instance and sends heartbeats to rejoin the stream's Raft group.
3. **Catchup request**: Sends an append entry response to the Raft Leader requesting catchup from the last known applied index.
4. **Leader catchup**: The leader sends missing entries via one of two paths:
   - **WAL streaming**: If the leader still has the missing Raft log entries in its WAL, it streams the delta entries. This is fast (sub-second to seconds) with minimal CPU and disk I/O impact.
   - **Snapshot install**: If the leader has already compacted those entries from its WAL, it generates and transfers a full stream snapshot (S2-compressed). Duration is proportional to stream size. NATS uses disk I/O semaphores and concurrent catchup limits to prevent overwhelming the leader.
5. **Active replication**: Once caught up, the follower resumes live message replication.

### 6.2 Traffic Impact During Recovery

| Component | Status During Follower Recovery | Impact on Live Operations |
| --------- | ------------------------------- | ------------------------- |
| Stream writes (publishers) | Active, serviced by Leader + quorum | Zero impact |
| Stream reads / consumer ACKs | Active, serviced by Leader | Zero impact |
| Quorum availability | Requires R/2 + 1 nodes online (e.g., 2 out of 3) | Healthy |
| Restarting node role | Re-joins as follower, catching up in background | Does not block Leader |

When the Raft Leader itself restarts:

- **Graceful shutdown**: The leader issues a Raft stepdown leadership transfer to an active follower before stopping. A new leader is elected in approximately 10-50ms.
- **Unplanned crash**: Followers detect missing leader heartbeats after the Raft election timeout (approximately 1-2 seconds) and elect a new leader. Client publishes during the brief election window return `503 No Leader Available`. Official NATS client SDKs automatically retry until the new leader is established.

---

## 7. Troubleshooting

### 7.1 Creation Failures

| Symptom | Likely Cause | Resolution |
| ------- | ------------ | ---------- |
| Stream creation fails with permission error | NATS process lacks write permissions on `store_dir` | Ensure the NATS process user has read/write access to the configured `store_dir` and all subdirectories |
| Stream creation fails with storage exceeded | Account or server `max_store` limit reached | Check account and server-level JetStream storage limits with `nats account info` and `nats server info` |
| Stream creation fails with disk space error | Filesystem is full or near capacity | Free disk space or move `store_dir` to a volume with sufficient capacity |

### 7.2 Publish Failures

| Symptom | Likely Cause | Resolution |
| ------- | ------------ | ---------- |
| Publish returns `max bytes exceeded` | Stream `MaxBytes` limit reached | Adjust `MaxBytes`, change discard policy, or archive/purge old messages |
| Publish returns `storage resources exceeded` | Server or account storage quota exhausted | Review storage allocation across streams with `nats stream ls -v` |
| Publish latency spikes | Disk I/O contention, slow storage, or snapshot generation in progress | Use SSD/NVMe storage, check disk utilization, review concurrent catchup activity |

### 7.3 File Integrity

| Symptom | Likely Cause | Resolution |
| ------- | ------------ | ---------- |
| `meta.sum` checksum mismatch on startup | Corrupted `meta.inf` from incomplete write or disk error | NATS logs a warning; restore from backup or let the node recover from cluster peers |
| `.tmp` files present after restart | Incomplete atomic write from a previous crash | NATS handles this automatically; no manual intervention needed |
| `index.db` corruption | Disk error or incomplete flush | NATS rebuilds state from `.blk` files on startup (slower recovery) |
