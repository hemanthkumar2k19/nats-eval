# Stream

## Storage
- All replicas of a stream must have the same storage type.

### Data
- S2 is the official standard compression algorithm across the entire NATS codebase.
    - JetStream message compression (Compression: S2)
    - Raft snapshot transfers
    - Stream Backup & Restore tarballs (.tar.s2)
    - Route & Leafnode wire compression
    - Inflight batch compression


## Pointers
- Pointers Managed on the Stream Side
    - FirstSeq / FirstTime: Sequence & timestamp of the oldest active message in the stream.
    - LastSeq / LastTime: Sequence & timestamp of the newest (most recently published) message in the stream.
    - Msgs: Total count of active messages in the stream.
    - Bytes: Total byte size of active messages in the stream.
    - NumDeleted / Deleted (dmap): Set/map of sequence numbers of deleted or purged messages between FirstSeq and LastSeq.
    - Subjects (psim): Map/tree of subjects with FirstSeq, LastSeq, and message count for each subject.

## Account
- Accounts are cluster wide scoped like namespaces
- Stream is owned by single account
- Cross-account access: While clients in other accounts can publish to or consume from a stream via NATS Exports/Imports or Stream Sources/Mirrors, the Stream entity itself always belongs exclusively to its parent Account.

## Cluster Operations

### Publishing
1. Normal Publishing (streamMsgOp)
Behavior: Each message published by a client is handled as an independent transaction.
Flow: Client sends Msg A $\rightarrow$ Proposed to Raft $\rightarrow$ Committed $\rightarrow$ Written to Disk $\rightarrow$ PubAck sent.
Characteristics:
Every message has its own Raft entry and disk flush cycle.
If a client publishes 100 messages, it creates 100 individual Raft proposals and commits.
2. Atomic Batch Publishing (batchMsgOp & batchCommitMsgOp)
Behavior: A client sends a group of messages under a single batch transaction identifier (batchId).
Key Differences:
All-Or-Nothing Atomicity: The entire batch of messages is applied as a single atomic unit. If the server crashes or the batch is interrupted mid-stream, the incomplete batch is rolled back (rejectBatchState()).
Consumer Read Isolation: Consumers cannot see or fetch any message in the batch until the entire batch has been fully committed (mset.isolateMu).
High Throughput: Instead of paying the Raft consensus and disk sync cost per message, multiple messages share a single Raft commitment and storage write cycle.

### Operations List
- NATS operates with 2 layers of Opcodes
```bash
 ┌────────────────────────────────────────────────────────────────────────┐
 │ Layer 1: Low-Level Raft Entry Types (EntryType in raft.go)            │
 │ (Controls the Raft Cluster Protocol itself)                            │
 │  - EntryNormal     : Holds application data (payload below)            │
 │  - EntrySnapshot   : Signals a state snapshot compacting the log      │
 │  - EntryPeerState  : Peer metadata / term updates                      │
 │  - EntryCatchup    : Sent to lagging followers to bring them up to date│
 │  - EntryAddPeer    : Node added to cluster                             │
 │  - EntryRemovePeer : Node removed from cluster                          │
 └───────────────────────────────────┬────────────────────────────────────┘
                                     │ Carried inside EntryNormal.Data
                                     ▼
 ┌────────────────────────────────────────────────────────────────────────┐
 │ Layer 2: JetStream Application Ops (entryOp in jetstream_cluster.go)  │
 │ (Controls Stream and Consumer State Machine)                           │
 │  - streamMsgOp / compressedStreamMsgOp                                 │
 │  - purgeStreamOp / deleteMsgOp / deleteRangeOp                         │
 │  - updateDeliveredOp / updateAcksOp / updateSkipOp                     │
 │  - assignStreamOp / removeStreamOp / updateStreamOp                   │
 └────────────────────────────────────────────────────────────────────────┘
```

### Go Internals
- internalLoop() - Single Dedicated Goroutine for all stream I/O
- Create Internal Stream Client and init Messages Channel(InBound)
- Select + Case for Checking for Message
- Get all messages in the queue for processing





