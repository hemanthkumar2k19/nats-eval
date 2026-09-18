# Stream Cluster Operations & Administrative Controls

The contents of this file have been integrated into the cluster operations reference directory:

- [Cluster Operations Overview](../lifecycle/cluster-operations/overview.md): Details storage durability policies (`SyncAlways`), emergency quorum overrides (`RescueQuorum`), dynamic membership management (`EvictPeers`), apply channel controls (`PauseApply`), non-voting observer mode (`SetObserver`), and automated reconciliation (`meta.reconcile`).
- [Stream Leader Step-Down](../lifecycle/cluster-operations/leader-stepdown.md): Complete guide to operational impact, automatic and manual triggers, `$JS.API.STREAM.LEADER.STEPDOWN` guard checks, and Raft leadership transfer mechanics.
