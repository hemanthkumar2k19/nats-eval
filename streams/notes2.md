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

### Write
- internalLoop() - Single Dedicated Goroutine for all stream I/O
- Create Internal Stream Client and init Messages Channel(InBound)
- Select + Case for Checking for Message
- Get all messages in the queue for processing


