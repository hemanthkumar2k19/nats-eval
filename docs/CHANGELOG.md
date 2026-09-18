# Changelog

## [2026-09-18] - Stream Developer Internals & Consistency Refactoring

### Added
- Created [`supporting_documentation/streams/stream-internals.md`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/supporting_documentation/streams/stream-internals.md) as a developer-level reference covering Stream scope, in-memory state pointers (`FirstSeq`, `LastSeq`, `Msgs`, `Bytes`, `dmap`, `psim`), account tenancy boundaries, system-wide S2 compression standards, and `StreamStore` Go interface abstractions.

### Changed
- Refined [`supporting_documentation/streams/consistency/overview.md`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/supporting_documentation/streams/consistency/overview.md) to focus 100% on Raft Consensus foundations, relocating developer-level stream pointers and S2 compression into `stream-internals.md`.
- **Reason**: Separates Raft consensus protocol mechanics from stream-level state data structures.
- **Affected area**: Documentation (`supporting_documentation/streams/`).

## [2026-09-18] - Cluster Operations & Administrative Controls Documentation

### Added
- Created [`supporting_documentation/streams/lifecycle/cluster-operations/overview.md`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/supporting_documentation/streams/lifecycle/cluster-operations/overview.md) detailing storage durability policies (`SyncAlways` vs `SyncOnFlush`), emergency quorum overrides (`RescueQuorum`), peer eviction (`EvictPeers` / `ProposeRemovePeer`), apply channel controls (`PauseApply` / `ResumeApply`), read-only observer mode (`SetObserver`), and Meta Leader automated self-healing reconciliation (`meta.reconcile`).
- Created [`supporting_documentation/streams/lifecycle/cluster-operations/leader-stepdown.md`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/supporting_documentation/streams/lifecycle/cluster-operations/leader-stepdown.md) detailing operational impact, automatic vs manual stepdown triggers, `$JS.API.STREAM.LEADER.STEPDOWN.*` handler guard checks, and internal Raft transfer mechanics (`raft.StepDown`, `EntryLeaderTransfer`, `CampaignImmediately` 10ms timer).
- **Reason**: Consolidates cluster operations and administrative controls into a dedicated subfolder structure under `supporting_documentation/streams/lifecycle/cluster-operations/`.
- **Affected area**: Documentation (`supporting_documentation/streams/lifecycle/cluster-operations/`).

## [2026-09-18] - Stream Consistency & Cluster Operations Reference Documentation

### Added
- Created [`supporting_documentation/streams/consistency/overview.md`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/supporting_documentation/streams/consistency/overview.md) detailing NATS JetStream single-leader Raft consensus, transport over internal NATS subjects (`$JSC.SYNC...`), 2-tier Raft architecture (`$JS.META` vs Stream Raft groups), index tracking (`lastIndex`, `commitIndex`, `appliedIndex`), WAL structure, majority quorum mechanics, leader elections, and core Go runtime entities (`s`, `acc`, `js`, `cc`).
- Refactored [`supporting_documentation/streams/consistency/operation-flow.md`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/supporting_documentation/streams/consistency/operation-flow.md) into a multi-layered reference document detailing Layer 1 Raft Entry Types (`EntryType`) vs Layer 2 JetStream Application Ops (`entryOp`), the unified 5-stage operation pipeline, operation deep dives (normal publishing, atomic batching with read isolation and rollback, deletes, purges, consumer state updates), stream leadership stepdown API (`$JS.API.STREAM.LEADER.STEPDOWN.*`), and internal loop concurrency (`internalLoop()`).
- **Reason**: Consolidates research and code-exploration notes into structured enterprise-grade reference documents.
- **Affected area**: Documentation (`supporting_documentation/streams/consistency/`).

## [2026-09-18] - Memory Storage Reference Documentation Clarification

### Changed
- Updated [`supporting_documentation/streams/storage/memory.md`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/supporting_documentation/streams/storage/memory.md) Section 4 and Section 6.1 with clarified Raft identity metadata details (`tav.idx`, `names.dat`/`peer.dat` under `<StoreDir>/$SYS/_js_/<raft_group_name>/`) for clustered memory streams ($R > 1$).
- Explicitly documented that stream messages, indices, and consumer states produce zero disk footprint under `<StoreDir>/<account>/streams/<stream_name>/`, while small Raft identity metadata (a few bytes) persists to disk so nodes recognize their Raft identity across restarts.
- **Reason**: Aligns memory storage documentation with exact server behavior and Raft metadata layout.
- **Affected area**: Documentation (`supporting_documentation/streams/storage/memory.md`).

## [2026-09-16] - Replica Placement via Server Tags

### Changed
- Added `server_tags: ["stream:primary"]` to [`deploy/local-nats-cluster/nats-1.conf`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/deploy/local-nats-cluster/nats-1.conf) and [`deploy/local-nats-cluster/nats-2.conf`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/deploy/local-nats-cluster/nats-2.conf). `nats-3` remains untagged to demonstrate exclusion from tag-based placement.
- Updated [`streams/stream.json`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/streams/stream.json): reduced `num_replicas` from 3 to 2, added `placement.tags: ["stream:primary"]` to constrain replica nodes to tagged servers only.
- **Reason**: Demonstrates the `Placement.Tags` capability documented in [`01-03-stream_creation_flow.md`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/supporting_documentation/streams/01-03-stream_creation_flow.md) -- client-side control over which cluster nodes host stream replicas.
- **Affected area**: Server configuration (`nats-1.conf`, `nats-2.conf`), stream configuration (`stream.json`).
- **Breaking change**: Existing EVENTS stream must be deleted and recreated (`nats stream rm EVENTS --force` then `nats stream add EVENTS --config streams/stream.json`) because replica count cannot be changed in-place.

## [2026-09-11] - Grafana Dashboard Expansion & Observability Benchmark Testing

### Added
- Expanded [`deploy/local-nats-cluster/stream-dashboard.json`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/deploy/local-nats-cluster/stream-dashboard.json) to incorporate `gnatsd_jsz_stream_messages` (Stream Message Count panel) and `gnatsd_jsz_stream_consumers` (Stream Consumer Count panel).
- Added direct NATS HTTP `/healthz` liveness probe integration (`gnatsd_healthz_status or gnatsd_up or up`) to Panel 2 in [`deploy/local-nats-cluster/stream-dashboard.json`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/deploy/local-nats-cluster/stream-dashboard.json).
- Integrated a dedicated Grafana Loki logs panel (`JetStream Raft & Server Engine Logs`) into Row 4 of [`deploy/local-nats-cluster/stream-dashboard.json`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/deploy/local-nats-cluster/stream-dashboard.json) for real-time visualization of NATS server logs, Raft leader transitions, and storage warnings.
- Added Section 3.4 (**Benchmark Workload Generation & Dashboard Verification**) in [`supporting_documentation/streams/03-observability.md`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/supporting_documentation/streams/03-observability.md) detailing concise `nats bench` and NATS CLI workload commands to populate data points across all Grafana dashboard tiles.

## [2026-09-11] - Stream Documentation Reorganization & Numbering Alignment

### Added
- Added an editable Mermaid architecture diagram (`graph TD`) in [`supporting_documentation/streams/03-observability.md`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/supporting_documentation/streams/03-observability.md) alongside the static image for easy future updates.
- Generated and embedded high-resolution architecture diagram [`supporting_documentation/streams/stream_observability_architecture.png`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/supporting_documentation/streams/stream_observability_architecture.png) into [`supporting_documentation/streams/03-observability.md`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/supporting_documentation/streams/03-observability.md) illustrating parallel telemetry flows from all 3 NATS cluster nodes to Prometheus Exporter, Fluent Bit, and the `otel-lgtm` stack.

### Changed
- Corrected the ASCII architecture diagram in [`supporting_documentation/streams/03-observability.md`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/supporting_documentation/streams/03-observability.md) to accurately illustrate Fluent Bit tailing server log files directly from all 3 NATS cluster nodes (`nats-1`, `nats-2`, `nats-3`) into Loki.
- Aligned [`supporting_documentation/streams/03-observability.md`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/supporting_documentation/streams/03-observability.md) with the standard `# Scope` header, hierarchical section numbering (`## 1.`, `### 1.1`, `## 2.`, `## 3.`), and 3-step demo structure (`1. Setting up`, `2. Simulating the Behaviour`, `3. Checking the behaviour and working`).
- Relocated all hands-on CLI demonstration workflows into dedicated end sections ([`01-lifecycle.md`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/supporting_documentation/streams/01-lifecycle.md) $\rightarrow$ `## 2. Hands-On CLI Demonstrations`, [`02-storage.md`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/supporting_documentation/streams/02-storage.md) $\rightarrow$ `## 3. Hands-On CLI Demonstrations`, [`03-observability.md`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/supporting_documentation/streams/03-observability.md) $\rightarrow$ `## 3. Hands-On CLI & Observability Demonstrations`) to keep conceptual architecture references clean and uncluttered.
- Moved `Retention Handling`, `Cleanup Policies`, and `Backup and Restore` sections cleanly from [`supporting_documentation/streams/01-lifecycle.md`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/supporting_documentation/streams/01-lifecycle.md) into [`supporting_documentation/streams/02-storage.md`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/supporting_documentation/streams/02-storage.md).
- Standardized all demonstration command workflows in [`supporting_documentation/streams/02-storage.md`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/supporting_documentation/streams/02-storage.md) into three structured steps (`1. Setting up`, `2. Simulating the Behaviour`, `3. Checking the behaviour and working`) consistently using the `EVENTS` stream and `order.*`/`payment.*` subjects.
- Enforced consistent hierarchical section numbering (`## 1.`, `### 1.1`, `#### 1.1.1`, `## 2.`, etc.) across all reference documents in [`supporting_documentation/streams`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/supporting_documentation/streams).

## [2026-09-10] - JetStream Storage Reference Documentation Update

### Added
- Comprehensive reference documentation, operational notes, and demonstration commands for JetStream storage backends (`file` vs `memory`), S2 compression, Direct Get, security flags, performance benchmarks, and retention policies in [`supporting_documentation/streams/02-storage.md`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/supporting_documentation/streams/02-storage.md).

## [2026-09-10] - Local Cluster Observability Stack Update

### Added
- Configured Fluent Bit (`fluent/fluent-bit:latest`) in [`deploy/local-nats-cluster/docker-compose.yaml`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/deploy/local-nats-cluster/docker-compose.yaml) to tail NATS server log files (`/logs/nats-*/nats.log`) and stream them directly into Loki in `otel-lgtm` on port `3100`.
- Created [`deploy/local-nats-cluster/fluent-bit.conf`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/deploy/local-nats-cluster/fluent-bit.conf) with `tail` input plugin and `loki` output plugin.
- Created Grafana dashboard schema [`deploy/local-nats-cluster/stream-dashboard.json`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/deploy/local-nats-cluster/stream-dashboard.json) covering Health, Resource Consumption, Overload Conditions, and Replication Status with optimal panel types (Stat, Gauge, Bar Gauge, Timeseries, Table).
- Added Grafana dashboard provider configuration [`deploy/local-nats-cluster/grafana-dashboards.yml`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/deploy/local-nats-cluster/grafana-dashboards.yml) and mounted dashboard files into `otel-lgtm` in [`docker-compose.yaml`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/deploy/local-nats-cluster/docker-compose.yaml) for automatic provisioning on startup.


### Changed
- Consolidated local observability infrastructure into `grafana/otel-lgtm:latest` (`otel-lgtm`) in [`deploy/local-nats-cluster/docker-compose.yaml`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/deploy/local-nats-cluster/docker-compose.yaml).
- Removed the standalone `prometheus` container service.
- Mounted [`deploy/local-nats-cluster/prometheus.yml`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/deploy/local-nats-cluster/prometheus.yml) into `/otel-lgtm/prometheus.yaml:ro` for direct scraping of `nats-exporter:7777` by the embedded Prometheus inside `otel-lgtm`.
- Configured Grafana security environment variables (`GF_SECURITY_ADMIN_USER=admin`, `GF_SECURITY_ADMIN_PASSWORD=admin`, `GF_USERS_ALLOW_SIGN_UP=false`).
- Added comprehensive telemetry documentation, metric mappings, technical justifications, and direct local browser links for Stream Observability sections 1 through 4 (Health, Resource Consumption, Overload Conditions, and Replication Status) in [`supporting_documentation/streams/03-observability.md`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/supporting_documentation/streams/03-observability.md).



