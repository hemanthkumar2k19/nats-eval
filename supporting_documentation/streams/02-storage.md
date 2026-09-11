# Scope

This document details the storage architecture, storage engines, retention policies, cleanup behaviors, backup/restore mechanisms, performance characteristics, operational controls, and administrative protections of NATS JetStream streams.

## 1. Storage Capabilities and Operational Details

### 1.1 Storage Engines

NATS JetStream provides two primary underlying storage backends for persisting stream data. The storage backend is defined at stream creation time via the `storage` configuration parameter (`file` or `memory`).

#### 1.1.1 Storage Engine Comparison

| Property | File Storage (`storage: "file"`) | Memory Storage (`storage: "memory"`) |
| :--- | :--- | :--- |
| **Durability** | Persistent (survives server restarts and host reboots) | Volatile (data lost on server restart or crash) |
| **Primary Medium** | Write-Ahead Log (WAL) on disk (SSD / NVMe recommended) | Server process RAM / Heap |
| **Read Acceleration** | Operating System page cache for hot data blocks | Native RAM access latency |
| **Capacity Scale** | Disk capacity bounded (scales beyond physical RAM) | Physical RAM bounded (subject to process RAM limits) |
| **Recommended Use** | Production workloads, durable event logs, audit streams | Ephemeral caches, real-time metrics, transient state |

#### 1.1.2 S2 Compression

JetStream streams using `file` storage support transparent payload and header compression using the S2 algorithm (a high-performance extension of Snappy).

* **Block-Level Compression**: Compression is applied to message blocks before writing to disk log segments.
* **Activation Scope**: Enabling compression (`compression: "s2"`) affects newly written message blocks. Existing stored blocks are not compressed retroactively.
* **Resource Tradeoff**: Reduces disk storage footprint and write I/O bandwidth demands at the cost of minor CPU overhead during encoding and decoding.

#### 1.1.3 Direct Get (`AllowDirect`)

Direct Get allows clients to read individual stored messages directly by sequence number or subject without creating a consumer or advancing consumer sequence state.

* **Leader Offloading**: In clustered streams ($R > 1$), setting `allow_direct: true` allows all stream replicas (including non-leaders) to respond to direct read requests, preventing read bottlenecking on the Raft leader.
* **Use Cases**: Key-Value (KV) bucket storage layers, Object Store payload chunk retrieval, and administrative message inspection.

#### 1.1.4 Administrative Security & Protection Controls

JetStream streams support security and policy flags to protect stored streams from unauthorized deletion or configuration changes:

* **Sealed Streams (`sealed: true`)**: Seals stream configuration and converts the stream into a read-only archive. Messages cannot be deleted or purged, and configuration cannot be edited once sealed.
* **Deny Delete (`deny_delete: true`)**: Restricts individual message deletion (`nats stream rm`) via API or CLI.
* **Deny Purge (`deny_purge: true`)**: Restricts full or partial stream purging (`nats stream purge`) via API or CLI.

---

### 1.2 Retention Handling

Retention dictates when messages are automatically deleted from the stream (`retention` in stream configuration):

1. Limits Retention (`retention: "limits"`)
   - Default policy. Messages are retained until explicit stream limits (`max_msgs`, `max_bytes`, `max_age`, `max_msgs_per_subject`) are breached.
   - Consumers can join at any time and replay stored messages.

2. Interest Retention (`retention: "interest"`)
   - Messages are automatically removed once all active consumers registered for the stream's subjects have acknowledged (`ACK`) them.
   - If no consumers exist when a message is published, the message is removed immediately.

3. Work Queue Retention (`retention: "workqueue"`)
   - Messages are automatically removed as soon as ANY single consumer acknowledges (`ACK`) the message.
   - Designed for worker-queue pattern processing where each event is processed exactly once by a single worker.

---

### 1.3 Cleanup Policies

Cleanup policies govern how JetStream manages storage bounds and removes limit-exceeded messages:

Critical Pitfall:
- Unbounded Disk Space Exhaustion: If retention limits (`max_msgs`, `max_bytes`, `max_age`) are not configured properly on a file-backed stream, continuous message publishing will exhaust available filesystem space, causing the NATS server node to crash or fail.

1. Discard Old (`discard: "old"`)
   - Default limit-enforcement policy (`max_msgs` or `max_bytes`).
   - Oldest messages are discarded (`FirstSeq` advances forward) to make space for incoming messages.

   Number Line View (Limit = 10, Message 11 arrives):
   ```bash
   Write-Ahead-Log-Index: [1] 2 3 4 5 6 7 8 9 10 11
                              f                  l
   ```

2. Discard New (`discard: "new"`)
   - When stream limits (`max_msgs` or `max_bytes`) are reached, new incoming published messages are rejected with an error (`nats: JetStream stream limit reached`).
   - Preserves existing stored dataset without dropping older events.

---

### 1.4 Backup and Restore

NATS JetStream supports snapshotting stream state and stored messages for backup and disaster recovery operations.

Snapshot Artifacts:
- `backup.json`: Stream state, configuration metadata, consumer states, and sequence offset boundaries.
- `stream.tar.s2`: s2-compressed log file segments containing stored stream messages.

![Stream Backup and Restore](stream_backup_and_restore.png)

Backup CLI Configuration Options:
- `--[no-]consumers`: Toggles including consumer definitions, durability states, and ACK sequence positions in the backup (default: `--consumers`).
- `--chunk-size=CHUNK-SIZE`: Sets the byte size of each data chunk sent by the server during backup streaming (e.g., `1024KB`). Controls transfer packet sizing.
- `--window-size=WINDOW-SIZE`: Sets the sliding flow-control window size (outstanding unacknowledged bytes) during snapshot transfer. Tuning this option mitigates network timeouts and rate-mismatch issues over slow disks or distant links.

Key Operational Pitfalls:
- Memory-backed streams (`storage: "memory"`) cannot be snapshotted.
- The stream name cannot be modified during a restore operation (must restore under original stream name).
- Flow control can time out during backup/restore over slow disks or high-latency network links (tune `--window-size` and `--chunk-size` to prevent timeouts).

#### 1.4.1 Stream Backup to Object Store (S3)
- Covered in separate reference document - [Stream Backup to Object Store](01-01-stream_backup_object_store.md)

---

## 2. Performance Considerations

### 2.1 I/O and Storage Architecture

#### 2.1.1 Sequential Write-Ahead Log (WAL)
File-backed streams append incoming messages sequentially to disk log segments. Sequential disk access maximizes throughput on modern SSD/NVMe hardware by eliminating disk seek latencies.

#### 2.1.2 Operating System Page Cache and Read Latency
Hot messages recently written to disk are cached in the host operating system page cache. Consumers streaming real-time events experience near-RAM read performance. Catch-up consumers fetching historical data trigger disk reads, which can increase storage I/O and latency.

#### 2.1.3 Memory Pressure and Out-Of-Memory (OOM) Risks
Allocating large `memory` storage streams consumes RAM directly within the server process. Unbounded memory streams risk triggering Out-Of-Memory (OOM) process termination or garbage collection pauses under high workload memory pressure.

#### 2.1.4 Publishing Semantics and Flow Control
* **Synchronous Publish**: The client blocks until the JetStream server confirms storage persistence (and Raft quorum replication when $R > 1$).
* **Asynchronous Publish**: Clients pipeline messages asynchronously and handle confirmations via callback handlers to maximize publish throughput.

#### 2.1.5 Consensus and Replication Overhead
Clustered streams with replica count $R > 1$ require Raft quorum write confirmation before returning a publish acknowledgment. Storage write latency is bound by network RTT between cluster nodes and disk flush latency on the majority of replicas.

---

## 3. Hands-On CLI Demonstrations

### 3.1 Storage Capabilities Demos

#### 3.1.1 Creating a File-Backed Stream with S2 Compression and Direct Access

1. Setting up:
```bash
# Create EVENTS stream with file storage, S2 compression, and direct access
nats stream add EVENTS \
  --subjects="order.*","payment.*" \
  --storage=file \
  --compression=s2 \
  --allow-direct \
  --discard=old \
  --max-bytes=100MB \
  --force
```

2. Simulating the Behaviour:
```bash
# Publish sample order and payment events
nats pub order.placed "Order Payload 1"
nats pub payment.received "Payment Payload 2"
```

3. Checking the behaviour and working:
```bash
# Inspect stream configuration, storage engine type, and compression state
nats stream info EVENTS
```

#### 3.1.2 Creating a Memory-Backed Stream for Ephemeral Workloads

1. Setting up:
```bash
# Create memory-backed EVENTS stream bounded by message count
nats stream add EVENTS \
  --subjects="order.*","payment.*" \
  --storage=memory \
  --max-msgs=1000 \
  --force
```

2. Simulating the Behaviour:
```bash
# Publish sample events to memory storage
nats pub order.created "Ephemeral Order Event"
```

3. Checking the behaviour and working:
```bash
# Inspect memory stream info and RAM utilization
nats stream info EVENTS
```

#### 3.1.3 Executing Direct Access Reads

1. Setting up:
```bash
# Ensure EVENTS stream has direct access enabled
nats stream edit EVENTS --allow-direct
```

2. Simulating the Behaviour:
```bash
# Publish messages to stream subjects
nats pub order.placed "Direct Get Order Event"
nats pub payment.received "Direct Get Payment Event"
```

3. Checking the behaviour and working:
```bash
# Retrieve sequence 1 directly without creating a consumer
nats stream get EVENTS --id=1
```

#### 3.1.4 Enforcing Administrative Deletion Policies

1. Setting up:
```bash
# Edit EVENTS stream to enable deny-delete and deny-purge protections
nats stream edit EVENTS \
  --deny-delete \
  --deny-purge
```

2. Simulating the Behaviour:
```bash
# Publish a critical event
nats pub order.placed "Critical Protected Order Event"

# Attempt to delete message sequence 1 (rejected by server policy)
nats stream rm EVENTS 1 -f
```

3. Checking the behaviour and working:
```bash
# Verify stream state remains intact and protected
nats stream info EVENTS
```

---

### 3.2 Retention Handling Demos

#### 3.2.1 Limits Retention Flow

1. Setting up:
```bash
# Re-create EVENTS stream with limits retention, max-msgs=5, max-bytes=10MB
nats stream add EVENTS \
  --subjects="order.*","payment.*" \
  --retention=limits \
  --storage=file \
  --max-msgs=5 \
  --max-bytes=10MB \
  --force
```

2. Simulating the Behaviour:
```bash
# Publish 10 messages to order.placed (exceeding max-msgs=5 limit)
nats pub --jetstream order.placed --count 10 "Message {{Count}} @ {{Time}}"
```

3. Checking the behaviour and working:
```bash
# View stream state to verify limits retention (retains last 5 messages: sequences 6 to 10)
nats stream info EVENTS
```

#### 3.2.2 Interest Retention Flow

1. Setting up:
```bash
# Re-create EVENTS stream with interest retention policy
nats stream add EVENTS \
  --subjects="order.*","payment.*" \
  --retention=interest \
  --storage=file \
  --force

# Create Consumer 1 (C1) on EVENTS filtering order.*
nats consumer add EVENTS C1 \
  --filter="order.*" \
  --ack=explicit \
  --pull

# Create Consumer 2 (C2) on EVENTS filtering order.*
nats consumer add EVENTS C2 \
  --filter="order.*" \
  --ack=explicit \
  --pull
```

2. Simulating the Behaviour:
```bash
# Publish a test event
nats pub order.created "Order Event 1"

# Consume and ACK with Consumer 1 (Message retained; awaiting C2 ACK)
nats consumer next EVENTS C1

# Consume and ACK with Consumer 2 (Message purged once both C1 and C2 ACK)
nats consumer next EVENTS C2
```

3. Checking the behaviour and working:
```bash
# View stream state to confirm message was purged automatically after all registered consumers ACKed
nats stream info EVENTS
```

#### 3.2.3 Work Queue Retention Flow

1. Setting up:
```bash
# Re-create EVENTS stream with workqueue retention policy
nats stream add EVENTS \
  --subjects="order.*","payment.*" \
  --retention=workqueue \
  --storage=file \
  --force

# Create a pull worker consumer on EVENTS filtering order.*
nats consumer add EVENTS WORKER \
  --filter="order.*" \
  --ack=explicit \
  --pull
```

2. Simulating the Behaviour:
```bash
# Publish a job task to order.created
nats pub order.created "Process Payment Task"

# Consume and ACK with WORKER (Message purged immediately upon 1st ACK)
nats consumer next EVENTS WORKER
```

3. Checking the behaviour and working:
```bash
# Verify stream is empty immediately after 1st worker ACK
nats stream info EVENTS
```

---

### 3.3 Cleanup Policies Demos

#### 3.3.1 Discard Old Policy Flow

1. Setting up:
```bash
# Configure EVENTS stream with discard=old policy and max-msgs=5
nats stream add EVENTS \
  --subjects="order.*","payment.*" \
  --retention=limits \
  --discard=old \
  --max-msgs=5 \
  --storage=file \
  --force
```

2. Simulating the Behaviour:
```bash
# Publish 7 messages (exceeding the 5-message limit)
nats pub --jetstream order.placed --count 7 "Message {{Count}} @ {{Time}}"
```

3. Checking the behaviour and working:
```bash
# View stream state to confirm oldest messages 1 and 2 were dropped (FirstSeq: 3, LastSeq: 7)
nats stream info EVENTS
```

#### 3.3.2 Discard New Policy Flow

1. Setting up:
```bash
# Re-configure EVENTS stream with discard=new policy and max-msgs=5
nats stream add EVENTS \
  --subjects="order.*","payment.*" \
  --retention=limits \
  --discard=new \
  --max-msgs=5 \
  --storage=file \
  --force
```

2. Simulating the Behaviour:
```bash
# Publish 5 messages to fill stream capacity
nats pub --jetstream order.placed --count 5 "Message {{Count}} @ {{Time}}"

# Attempt to publish a 6th message (rejected by server with stream limit error)
nats pub --jetstream order.placed "Overflow Message 6"
```

3. Checking the behaviour and working:
```bash
# View stream state to confirm original 5 messages are retained (FirstSeq: 1, LastSeq: 5)
nats stream info EVENTS
```

---

### 3.4 Backup and Restore Demo

1. Setting up:
```bash
# Populate EVENTS stream with test messages
nats stream add EVENTS \
  --subjects="order.*","payment.*" \
  --storage=file \
  --force

nats pub order.placed "Order Payload 1"
nats pub payment.received "Payment Payload 2"
```

2. Simulating the Behaviour:
```bash
# Backup stream to local directory snapshot
nats stream backup EVENTS "./backups/events/$(date +%Y-%m-%d)" \
  --consumers \
  --chunk-size=1024KB \
  --window-size=8MB

# Delete the stream to simulate disaster
nats stream rm EVENTS -f

# Restore stream from backup snapshot
nats stream restore ./backups/events/$(date +%Y-%m-%d)
```

3. Checking the behaviour and working:
```bash
# Inspect restored stream configuration, message sequence pointers, and state
nats stream info EVENTS
nats stream state EVENTS
```

---

### 3.5 Storage Performance and Metrics Demos

#### 3.5.1 Benchmarking Storage Write Throughput

1. Setting up:
```bash
# Ensure EVENTS stream exists with file storage
nats stream add EVENTS \
  --subjects="order.*","payment.*" \
  --storage=file \
  --force
```

2. Simulating the Behaviour:
```bash
# Benchmark synchronous publish throughput on subject order.placed
nats bench order.placed \
  --pub 10000 \
  --size 1024 \
  --js
```

3. Checking the behaviour and working:
```bash
# Inspect publish rate, latency statistics, and stream byte growth
nats stream info EVENTS
```

#### 3.5.2 Monitoring Storage Metrics and Capacity Usage

1. Setting up:
```bash
# Ensure EVENTS stream is populated with active workload
nats pub order.placed "Metrics Verification Event"
```

2. Simulating the Behaviour:
```bash
# Query stream sequence pointers and byte allocation
nats stream state EVENTS
```

3. Checking the behaviour and working:
```bash
# List all account streams with verbose storage utilization metrics
nats stream ls -v
```