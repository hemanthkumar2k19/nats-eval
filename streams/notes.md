# JetStream Streams Reference Notes

## Stream Basics and Rules
- Stream name is case-sensitive.
- Subjects bind exclusively to one stream; overlapping subject bindings across streams cause duplicate subject errors.
- Storage retention must be configured; unconstrained retention can exhaust server disk or memory.
- Live configuration updates: server allows swapping Limits and Interest retention dynamically in either direction, applying immediately to stored messages.
- Retaining WorkQueue policy is locked at creation time and cannot be altered.
- All replicas of a stream must use identical storage type (File or Memory).
- Accounts function as cluster-wide tenant namespaces:
  - Every stream belongs exclusively to a single parent Account.
  - Cross-account access is managed via NATS Exports/Imports or Stream Sources/Mirrors.

## Storage and S2 Compression
- S2 algorithm is the standard binary compression format across NATS:
  - JetStream message payload compression (Compression: S2).
  - Raft snapshot transfer payloads.
  - Stream Backup and Restore tarball archives (.tar.s2).
  - Inter-server Route and Leafnode wire transmission compression.
  - Inflight batch payload compression.

## Stream Pointers and Internal Metadata
- Pointers and metadata tracked on the stream side:
  - FirstSeq and FirstTime: Sequence number and timestamp of the oldest active message in the stream.
  - LastSeq and LastTime: Sequence number and timestamp of the newest message in the stream.
  - Msgs: Total active message count stored in the stream.
  - Bytes: Total byte footprint of all active messages in the stream.
  - NumDeleted and Deleted (dmap): Tracked sequence numbers of deleted or purged messages between FirstSeq and LastSeq.
  - Subjects (psim): Internal index map tracking FirstSeq, LastSeq, and message count per subject pattern.

## JetStream Raft Architecture
- Tracked Raft Indexes:
  - lastIndex: Latest entry sequence number appended to node Raft WAL log.
  - commitIndex: Highest entry sequence replicated to a majority quorum (R/2 + 1) of nodes.
  - appliedIndex (n.applied): Highest entry sequence executed into local stream store (memStore or fileStore).
- Control Plane (Meta Raft Group $JS.META):
  - Single cluster-wide Raft group formed by all JetStream servers in the cluster.
  - Meta Leader handles cluster orchestrations, account storage quota tracking, management API requests ($JS.API.STREAM.CREATE.*, etc.), and stream replica node assignments recorded via streamAssignment WAL entries.
- Data Plane (Stream Raft Groups):
  - Created per stream when replica count R > 1.
  - Stream Raft Leader handles message proposals, replication, and consumer acknowledgments for that specific stream.
- Internal NATS Transport (syncSubject):
  - Function syncSubjForStream() generates private internal NATS subject $JSC.SYNC.<unique_inbox>.<node_id>.
  - Raft RPCs (AppendEntries, VoteRequest, InstallSnapshot) travel as NATS binary payloads over internal system subjects.
  - Followers reply to $JSC.R.<unique_inbox>.
  - Benefits: eliminates separate TCP listening ports per stream, provides transparent node relocation, and enforces isolation within system account space.

## Replica Placement and Selection Engine (cc.selectPeerGroup)
- Hard Constraints Filtering Phase:
  - Peer list initialized from meta.Peers and randomized (rand.Shuffle) to prevent fixed ordering bias.
  - Verifies target cluster matching (Placement.Cluster or client cluster ci.Cluster).
  - Skips offline or inactive nodes (selectable()).
  - Excludes nodes marked with !jetstream tag.
  - Validates user tags: requires mandatory tags (tag) and rejects excluded tags (!tag).
  - Storage space validation: verifies available node memory/disk against stream MaxBytes.
  - Enforces MaxHAAssets limit on candidate nodes.
  - Fault domain anti-affinity (JetStreamUniqueTag / uniqueTagPrefix such as rack: or zone:): prevents placing multiple stream replicas on nodes sharing identical tag values.
- Scoring and Ranking Phase:
  - Online nodes ranked above offline nodes.
  - Sorted by available storage (avail descending - nodes with most free space first).
  - Sorted by total stream count (ns ascending - nodes hosting fewer total streams first).
  - Stable sort by HA asset density (ha ascending via slices.SortStableFunc - nodes hosting fewer R > 1 streams/consumers ranked first).
  - Picks top R nodes from sorted candidate pool.
- Multi-Cluster Fallback:
  - On JSInsufficientResourcesErr, automatically retries allocation across alternate clusters (ci.Alternates).

## Two-Layer Opcode Architecture
- Layer 1: Raft Protocol Entry Types (raft.go):
  - EntryNormal: Carries JetStream application payload.
  - EntrySnapshot: Compacts Raft log into snapshot.
  - EntryPeerState: Metadata for peer topology and term updates.
  - EntryCatchup: Data sent to bring lagging follower up to date.
  - EntryAddPeer: Adds new peer node to Raft cluster.
  - EntryRemovePeer: Removes peer node from Raft cluster.
- Layer 2: JetStream Application Operations (jetstream_cluster.go carried inside EntryNormal.Data):
  - Stream Messages: streamMsgOp, compressedStreamMsgOp, batchMsgOp, batchCommitMsgOp.
  - Message Deletions: purgeStreamOp, deleteMsgOp, deleteRangeOp.
  - Consumer State Updates: updateDeliveredOp, updateAcksOp, updateSkipOp.
  - Stream Assignments: assignStreamOp, removeStreamOp, updateStreamOp.

## Publishing Modes
- Single Message Publishing (streamMsgOp):
  - Each message processed as an independent Raft proposal.
  - Incurs individual Raft commit and disk sync cycle per published message.
- Atomic Batch Publishing (batchMsgOp & batchCommitMsgOp):
  - Multiple messages bound under single batchId.
  - All-or-nothing atomicity: incomplete batches rolled back on failure (rejectBatchState()).
  - Consumer read isolation: messages remain hidden until full batch commits (mset.isolateMu).
  - Shared consensus overhead: reduces Raft proposals and disk flushes by committing batch as single unit.

## Replication, Commit, and Campaign Lifecycle
- Inter-Node Messaging: Asynchronous NATS system messages on $SYS.RAFT.<group_id>.A (AppendEntries) and $SYS.RAFT.<group_id>.V (VoteRequest), with responses on .AR and .VR.
- Write-Ahead Log (WAL): Append-only disk log (n.wal) storing sequential entries {index, term, Type, Data}. Log consistency enforced across peers via quorum.
- Quorum Rule: Majority calculated as Floor(N/2) + 1.
- Replication and Commit Sequence:
  1. Client publishes message to stream subject.
  2. Leader wraps payload in streamMsgOp, invokes node.Propose(term, payload), appends entry to local WAL as uncommitted.
  3. Leader broadcasts AppendEntries to follower nodes.
  4. Followers check term and index consistency, append entry to local WAL, and send ACK back to leader.
  5. Leader advances commitIndex once majority quorum ACKs are received.
  6. Leader and followers execute applyStreamEntries, committing entry to local storage (mset.store.StoreRawMsg()).
  7. Leader returns PubAck JSON ({"stream":"...", "seq":...}) to client socket.
- Raft Campaign Voting Logic:
  - Follower transitions to Candidate: increments term (term++), votes for self, sets state to Candidate.
  - Broadcasts voteRequest(term, pterm, pindex, candidateID).
  - Peer grants vote if candidate term >= peer term, peer has not voted in term, and candidate log is at least as up-to-date (lastTerm, lastIndex).
  - Candidate switches to Leader upon reaching quorum votes and broadcasts sendPeerState.
- Heartbeat and Quorum Monitoring:
  - Leader transmits empty appendEntry heartbeat every 1 second.
  - Follower ACKs update peer timestamp (ps.ts).
  - Leader periodically evaluates lostQuorumLocked(); self-demotes if majority response is lost.
- Re-Election Pathways:
  - Follower election timers (1.5s to 3.0s) reset upon receiving leader entries or heartbeats.
  - Voluntary Stepdown:
    - Leader selects healthy up-to-date peer and appends EntryLeaderTransfer(peerID).
    - Nominated peer executes CampaignImmediately (10ms timer) to claim leadership instantly.
  - Unplanned Crash:
    - Followers detect missed heartbeats when election timer expires.
    - Candidate initiates Campaign, collects quorum votes, and assumes leadership.

## Go Code Internals and Server Structs
- s (*Server): Node-level server process. Controls TCP sockets, route mesh, configuration, and s.js.
- acc (*Account): Account-level tenant controller. Manages auth rules, tenant permissions, and acc.js (jsa / jsAccount) for account storage quotas and local stream lookups.
- js (*jetStream): Local JetStream subsystem engine. Manages storage engines, local queues, and cluster controller js.cluster (cc).
- cc (*jetStreamCluster): Cluster-wide JetStream controller present on every node. Contains cc.meta (local participant in $JS.META). Checked via cc.isLeader().
- Stream Goroutine Loop (internalLoop()): Single dedicated goroutine per stream processing inbound I/O messages channel via select/case statements.

## Leadership Stepdown Procedure
- Subject Endpoint: $JS.API.STREAM.LEADER.STEPDOWN.<stream_name>
- Request Handler: jsStreamLeaderStepDownRequest
- Process Flow:
  - Validates client connection and JetStream initialization.
  - Extracts ClientInfo, resolves target Account, and parses stream name token.
  - Verifies server clustering status and active Meta Raft availability.
  - Meta Leader checks stream assignment registration; missing streams return NewJSStreamNotFoundError.
  - Validates API compatibility level and account JetStream authorization.
  - Verifies Stream Raft Group has active quorum.
  - Gated to active Stream Leader node only; non-leader nodes exit silently.
  - Resolves local stream instance from account registry.
  - Parses JSON request body for optional Placement.Preferred target node.
  - Calls raft.StepDown() to append EntryLeaderTransfer and transfer leadership.
- Internal Raft Stepdown (raft.StepDown):
  - Confirms current node state is Leader under lock.
  - Verifies preferred target peer is online and active (heartbeat ACK < 3s); falls back to first healthy follower if unavailable.
  - Appends EntryLeaderTransfer with target peer ID via sendAppendEntry().
  - Demotes local node state to Follower (n.stepdown(noLeader)) and resets election timer.
  - Target follower receives EntryLeaderTransfer and invokes CampaignImmediately() for instant election.