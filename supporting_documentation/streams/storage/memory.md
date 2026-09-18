# Memory Storage (Volatile / RAM)

JetStream streams configured with `storage: "memory"` store messages, indices, consumer state, and scheduling data entirely in Go heap memory (RAM). Data is volatile -- it is lost on server restart or process crash. In clustered deployments (R > 1), a restarted node automatically recovers its full state from surviving Raft peers. This document covers the in-memory data layout, clustered behavior, memory protection, recovery mechanics, and operational details.

---

## 1. Overview

### 1.1 What Is Stored in Memory

A memory-backed stream holds all data in Go heap structures:

| Category | Description |
| -------- | ----------- |
| Messages | Go map keyed by 64-bit sequence number, values are pointers to `StoreMsg` structs containing subject, headers, payload, and timestamp |
| Stream indices | In-memory subject state tree (radix/subject tree) for instant subject boundary resolution, plus AVL tree set tracking deleted sequences |
| Consumer state | Delivered sequences, ACK floors, pending ACK maps, and redelivery counts per consumer -- all in Go memory structures |
| TTL and scheduling | Time Hash Wheel for `MaxAge` expirations and scheduled message delivery state |

### 1.2 Clustered Streams

For clustered memory streams (R > 1), the Raft consensus engine also uses a `memStore` as its Write-Ahead Log (WAL). Raft log entries remain in memory and are not written to `.blk` files on disk. If a node running a memory-backed stream restarts, its memory state is lost and it automatically recovers its full state by catching up from surviving Raft peers in the cluster.

### 1.3 Storage Interface

NATS decouples stream execution logic from the storage backend using Go interfaces. The stream layer (`stream.go`) operates against a `StreamStore` interface (`store.go`), with `memStore` (`memstore.go`) providing the volatile in-memory implementation. This means all stream operations (publish, consume, purge, delete) work identically regardless of storage backend -- only the durability and performance characteristics differ.

---

## 2. Configuration

Memory storage is selected at stream creation time via `storage: "memory"` in the stream configuration. There is no separate directory configuration -- all data lives in the NATS server process heap.

Memory limits are enforced at three levels:

| Level | Config | Default | Effect |
| ----- | ------ | ------- | ------ |
| Stream | `max_bytes` in stream config | Unlimited (`-1`) | When reached, applies discard policy (evict oldest or reject new) |
| Account | `max_memory` in account config | Unlimited | Publishes return `ErrMemoryResourcesExceeded` when exceeded |
| Server | `jetstream.max_memory` in server config | 75% of total system RAM | Publishes rejected when aggregate JetStream memory usage exceeds this limit |

---

## 3. Data Structure Layout

A `memStore` instance holds the following in-memory structures:

```
memStore (In-Memory Heap Object)
|-- msgs: map[uint64]*StoreMsg              Primary Message Map (Seq -> StoreMsg)
|-- fss: *stree.SubjectTree[SimpleState]    In-Memory Subject State Tree
|-- dmap: avl.SequenceSet                   AVL tree set tracking deleted sequences
|-- ttls: *thw.HashWheel                    Time Hash Wheel for MaxAge TTL expirations
|-- scheduling: *MsgScheduling              Scheduled message delivery wheel
|-- sources: map[string]*StreamSourceState  In-Memory mirror/source sequence tracking
|-- consumers: []ConsumerStore              Slice of in-memory Consumer Stores
```

### 3.1 Messages Map

Go map where the key is the 64-bit sequence number. The value is a pointer to a `StoreMsg` containing the subject string, header slice, payload byte slice, and nanosecond timestamp.

### 3.2 Subject Index

Uses an in-memory radix/subject tree (`stree.SubjectTree`) to instantly resolve subject boundaries (`FirstSeq`, `LastSeq`, message count per subject) without scanning the message map.

### 3.3 Deleted Sequences

Uses an AVL tree set (`avl.SequenceSet`) to track deleted sequence numbers efficiently in memory. Maintains sparse ranges without allocating entries for every sequence.

### 3.4 Consumer State

Each consumer (`consumerMemStore`) stores its delivered sequences, ACK floor, pending ACK maps, and redelivery counts in Go memory structures. There are no `o.dat` files or disk persistence under the stream directory.

---

## 4. Minimal Disk Footprint

Memory-backed streams store 100% of stream messages, indices, and consumer states in RAM. They produce no stream data files on disk.

### 4.1 Stream & Consumer Data (Zero Disk Usage)

No stream directory or storage files are created under `<StoreDir>/<account>/streams/<stream_name>/`:

- **No message blocks**: No `.blk` files
- **No indices**: No `.idx` or `index.db` files
- **No consumer states**: No `o.dat` files
- **No scheduling/TTL databases**: No `thw.db` or `sched.db` files

### 4.2 Raft Identity Metadata (Clustered R > 1)

To participate in Raft consensus ($R > 1$), a node must remember its Raft node identity and cluster term across process restarts. NATS writes a tiny Raft identity metadata file (`tav.idx` for term/vote index, `names.dat` / `peer.dat` for peer names) under the System Raft Directory:

```text
<StoreDir>/$SYS/_js_/<raft_group_name>/tav.idx
```

- **File size**: Only a few bytes (Raft term/vote index).
- **Scope**: Raft node identity and consensus term tracking only -- contains no message data or consumer state.
- **Raft WAL**: The Raft Write-Ahead Log for memory streams is also held in memory (`memStore`), producing no `.blk` WAL files on disk.
- **Restart behavior**: On startup, the node reads this small file to recognize its Raft peer identity, while initializing an empty `memStore` in RAM for message data.

---

## 5. Memory Protection

NATS provides three layers of memory guardrails to prevent a memory stream from taking down the node:

### 5.1 Stream Level

When a memory stream reaches its `MaxBytes` limit:

- **Discard Old** (default): NATS automatically deletes the oldest messages in RAM to free memory for new ones.
- **Discard New**: NATS rejects incoming publishes with `ErrMaxBytes`.

### 5.2 Account Level

NATS tracks memory usage per account. If an account exceeds its configured `max_memory` limit, publish requests return `ErrMemoryResourcesExceeded`.

### 5.3 Server Global Level

Configured via `jetstream { max_memory: 8GB }`. If `max_memory` is not explicitly set, NATS automatically caps JetStream memory usage to 75% of total system RAM. Before accepting a message write, NATS verifies that `memReserved + msgBytes <= config.MaxMemory`. If exceeded, the message is rejected and server stability is preserved.

### 5.4 Failure Scenario: Container Memory Mismatch

If NATS `max_memory` is set higher than the actual container/host memory limit (e.g., NATS configured for 16GB but running in a pod with an 8GB memory limit), the Linux kernel OOM Killer will terminate the `nats-server` process with `SIGKILL`.

**Best practice**: Always set NATS JetStream `max_memory` lower than the container/host cgroup memory limit, leaving headroom for Go runtime GC overhead and OS page cache.

---

## 6. Node Recovery

### 6.1 Startup Sequence

When a node hosting a memory-backed stream replica restarts:

1. **Local state load**: For clustered streams ($R > 1$), the node reads its tiny Raft metadata file (`tav.idx` for term/vote index, `names.dat` / `peer.dat` for peer names) under `<StoreDir>/$SYS/_js_/<raft_group_name>/` to reload its Raft peer identity and term. The `memStore` for stream messages and consumer states is initialized empty in RAM (0 messages) -- no `.blk`, `.idx`, `index.db`, or `o.dat` files exist under the stream directory. Local boot is instantaneous (milliseconds).
2. **Raft group re-join**: Initializes the Raft node instance and sends heartbeats to rejoin the stream's Raft group.
3. **Catchup request**: Sends an append entry response to the Raft Leader requesting catchup from the last known applied index.
4. **Leader catchup via snapshot install**: Because the memory store starts empty and the Raft WAL is also in memory (lost on restart), the leader always uses the snapshot install path. The leader calls `sendSnapshotToFollower()` to transfer the full stream state over NATS. The follower installs the snapshot into memory.
5. **Active replication**: Once caught up, the follower resumes live message replication.

### 6.2 Sync Timing and Performance Impact

Memory storage recovery always uses the snapshot install path (there is no WAL to replay from). The performance characteristics are:

| Metric | Detail |
| ------ | ------ |
| Duration | Proportional to stream size (e.g., 10GB snapshot over 1Gbps network takes approximately 80-90 seconds) |
| CPU impact | Low to moderate; leader compresses snapshot in a background goroutine using S2 compression |
| Network overhead | Moderate to high; full stream state transferred over internal cluster subjects |
| Disk I/O on leader | Controlled via disk I/O semaphores if the leader is file-backed; none if leader is also memory-backed |
| Concurrent catchup limiting | NATS limits concurrent catchups to prevent multiple catching-up followers from overwhelming leader network/CPU |

### 6.3 Traffic Behavior During Recovery

NATS JetStream uses Raft consensus with quorum routing. Client traffic (publishers and consumers) is decoupled from follower recovery.

#### Publisher Traffic

```
[Publisher] ---(Publish)---> [Raft Leader] ---(Ack)---> [Publisher]
                                  |
                        Replicates to Quorum
                                  |
                   +--------------+--------------+
                   |                             |
          [Online Follower]            [Restarting Follower]
          (Appends & ACKs)             (Catching up in background)
```

**When a follower restarts**: Zero client traffic impact. In a 3-node cluster (R=3), quorum is 2 nodes. The leader and the other active follower handle all incoming publish writes and satisfy quorum. Publishers experience no latency increase, no errors, and no dropped messages.

**When the leader restarts**:

- **Graceful shutdown**: The leader issues a Raft stepdown leadership transfer to an active follower before stopping. A new leader is elected in approximately 10-50ms.
- **Unplanned crash**: Followers detect missing leader heartbeats after the Raft election timeout (approximately 1-2 seconds) and elect a new leader. Client publishes during the brief election window return `503 No Leader Available`. Official NATS client SDKs automatically retry until the new leader is established.
- **After boot**: The old leader re-joins the cluster as a follower and catches up from the new leader in the background without affecting live traffic.

#### Consumer Traffic

- **Fetch and delivery requests**: Consumers (pull fetchers or push subscribers) automatically communicate with the active Raft Leader. They do not connect to or wait for the restarting follower.
- **Acknowledgements**: Consumer ACKs are sent to the Raft Leader, which commits the ACK floor to quorum.
- **State sync**: As the restarting follower catches up via snapshot install, consumer state is updated asynchronously in the background.

### 6.4 Recovery Summary

| Component | Status During Follower Recovery | Impact on Live Operations |
| --------- | ------------------------------- | ------------------------- |
| Stream writes (publishers) | Active, serviced by Leader + quorum | Zero impact |
| Stream reads / consumer ACKs | Active, serviced by Leader | Zero impact |
| Quorum availability | Requires R/2 + 1 nodes online (e.g., 2 out of 3) | Healthy |
| Restarting node role | Re-joins as follower, catching up in background | Does not block Leader |

---

## 7. Troubleshooting

### 7.1 Creation Failures

| Symptom | Likely Cause | Resolution |
| ------- | ------------ | ---------- |
| Stream creation fails with memory exceeded | Account or server `max_memory` limit reached | Check account and server-level JetStream memory limits with `nats account info` and `nats server info` |
| Stream creation fails after restart | Memory stream data is volatile; stream config persists but messages are lost | Expected behavior for R=1 memory streams; use R > 1 for recovery from peers |

### 7.2 Publish Failures

| Symptom | Likely Cause | Resolution |
| ------- | ------------ | ---------- |
| Publish returns `memory resources exceeded` | Server or account memory quota exhausted | Review memory allocation across streams with `nats stream ls -v`; reduce stream count or increase `max_memory` |
| Publish returns `max bytes exceeded` | Stream `MaxBytes` limit reached | Adjust `MaxBytes`, change discard policy, or consume/purge old messages |

### 7.3 Operational Limitations

| Limitation | Detail |
| ---------- | ------ |
| No backup/snapshot support | Memory-backed streams cannot be snapshotted via `nats stream backup` |
| Data loss on R=1 restart | Single-replica memory streams lose all messages on node restart; use R >= 3 for durability through replication |
| OOM risk with misconfigured limits | Set `max_memory` lower than container/host memory limit to prevent kernel OOM kills |
