## [2026-09-22] - NATS Stream Hands-On Jupyter Presentation Notebook Refactoring

### Changed
- Refactored presentation notebook [`streams/hands-on/nats-stream-demo.ipynb`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/streams/hands-on/nats-stream-demo.ipynb):
  - **Multi-Stream Cluster Architecture**: Streams (`EVENTS` R=3, `FILE_STREAM` R=2, `MEMORY_STREAM` R=2, `LIMITS_STREAM` R=1, `INTEREST_STREAM` R=2, `WORKQUEUE_STREAM` R=3, `DISCARD_OLD_STREAM` R=1, `DISCARD_NEW_STREAM` R=2) remain active across sections to provide a rich cluster topology (`nats stream ls`) for Cluster Operations (balancing, Raft leader stepdown, live scaling, peer evacuation).
  - **Stream Sealing & Preservation**: Section 1 concludes with `EVENTS` remaining sealed and active in the cluster.
  - **Single Final Teardown**: Replaced intermediate deletions with a single comprehensive cleanup cell at the end of Section 3.
  - **Configuration Sync**: Updated [`streams/hands-on/stream-update.json`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/streams/hands-on/stream-update.json) to `"storage": "memory"` and `"num_replicas": 3` to match `stream.json`, enabling seamless in-place edit testing (`nats stream edit EVENTS --config=stream-update.json -f`).
  - **CLI Clean Flags**: Removed `-f` from creation commands (`nats stream add`) while retaining `-f` on interactive prompt commands (`edit`, `rm`, `purge`, `seal`, `stepdown`, `evacuate`).
  - **Count Template Escaping**: Escaped `{{Count}}` template variables as `{{{{Count}}}}` in notebook code cells to prevent IPython from stripping double-braces before sending to `nats pub --count`.
  - **Placement Tag & Cluster Ops Fixes**: Fixed `nats stream add PLACED_STREAM --tags="stream:primary" --defaults` flag syntax and updated all Section 3 Cluster Operations (scaling, Raft stepdown, inspection) to consistently target `PLACED_STREAM`.
  - **Step-by-Step Presentation Splits**: Split combined publish, consumer next, and stream info commands into individual code cells under Interest Retention and WorkQueue Retention for clear live demonstration.
  - **Git Ignore Cleanup**: Added `.ipynb_checkpoints/` to [`.gitignore`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/.gitignore) to exclude Jupyter auto-save checkpoint directories from version control tracking.
- **Reason**: Fix command execution failures during sequential presentation and align notebook flow with JetStream immutability rules.
- **Affected area**: Hands-on demo directory (`streams/hands-on/`) and repository configuration.

## [2026-09-22] - NATS Stream Hands-On Jupyter Presentation Notebook Setup

### Added
- Expanded presentation Jupyter Notebook [`streams/hands-on/nats-stream-demo.ipynb`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/streams/hands-on/nats-stream-demo.ipynb) aligned with [`streams/hands-on/demo.md`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/streams/hands-on/demo.md):
  - **Stream Lifecycle**: Creation, Updation (dynamic config edit & Stream Sealing write/purge lockdown), Inspection (`state`, `view`, `get`), and Sequence Handling (`--seq=4`, `--keep=3`, full purge).
  - **Storage**: File Storage durability vs Memory Storage volatility demo using container restart commands (`!podman-compose down` and `!podman-compose up -d`).
  - **Retention Policies**: Limits Retention, Interest Retention (multi-consumer ACK), and Work Queue Retention.
  - **CleanUp (Discard Policies)**: Discard Old vs Discard New policies updated dynamically in-place using `!nats stream edit EVENTS --discard=new`.
  - **Backup & Recovery**: Local directory snapshot backup and full stream restoration.
  - **Cluster Operations**: Replica Placement (tag placement constraints), Leadership Step Down (`!nats stream cluster stepdown`), Scaling Up (`--replicas=3`), Scaling Down (`--replicas=1`), Balancing (`!nats stream cluster balance`), and Evacuate Peer (`!nats stream cluster evacuate nats-1 -f`).

### Changed
- Removed the stream `add` command from post-sealing Cell 07k in `nats-stream-demo.ipynb`, keeping strictly `nats stream rm EVENTS -f 2>&1`.
- Streamlined the Memory Storage section in `nats-stream-demo.ipynb` to execute strictly `nats stream rm EVENTS -f 2>&1`, removing the stream add command for custom presentation flow.
- Added explicit stream deletion step (`nats stream rm EVENTS -f 2>&1`) right above the Inspection section in `nats-stream-demo.ipynb`.
- Updated stream seal command in `nats-stream-demo.ipynb` to use direct CLI sub-command `nats stream seal EVENTS -f 2>&1`.
- Fixed relative file path for `podman-compose` commands in `nats-stream-demo.ipynb` (`-f ../../deploy/local-nats-cluster/docker-compose.yaml`) so restart commands execute cleanly from `streams/hands-on/`.
- **Non-Interactive Execution**: Removed `-f` from creation commands (`nats stream add`) while retaining `-f` / `--force` flags on prompt commands (`nats stream edit`, `nats stream rm`, `nats stream purge`, `nats stream seal`, `nats stream cluster stepdown`, `nats stream cluster evacuate`).
- Removed embedded JSON writefile cells from the notebook for a cleaner presentation layout.
- **Reason**: Refined hands-on demo to focus strictly on Stream lifecycle and maintain clean reference documentation.
- **Affected area**: Hands-on demo directory (`streams/hands-on/`).

## [2026-09-21] - Cluster Operations & Consistency Mermaid Diagram Syntax Fix

### Fixed
- Quoted all sequence diagram participant labels containing parentheses (e.g. `participant Meta as "Meta Leader ($JS.META)"`, `participant Storage as "Storage Engine (FileStore vs MemStore)"`) across `add-node.md`, `balancing.md`, `remove-node.md`, `write.md`, `operation-flow.md`, and `overview.md`.
- Removed nested `alt`/`else` blocks containing `Note over` statements from `write.md` sequence diagram, replacing with a single linear interaction step to satisfy strict Mermaid parser rules.
- Fixed multi-participant note syntax by separating notes per participant (`Note over NodeB: ...`, `Note over NodeC: ...`).
- **Reason**: Fixes syntax parse errors (such as `Parse error on line 26: ... Expecting 'SOLID_OPEN_ARROW' ... got 'NEWLINE'`) caused by unquoted parentheses and nested `alt`/`else` note blocks in Mermaid sequence diagrams.
- **Affected area**: Documentation (`supporting_documentation/streams/lifecycle/cluster-operations/` & `supporting_documentation/streams/consistency/`).

## [2026-09-18] - Stream Cluster Operations & Consistency Documentation Refactoring

### Changed
- Refactored [`supporting_documentation/streams/consistency/overview.md`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/supporting_documentation/streams/consistency/overview.md):
  - Converted Section 2.1 transport bus diagram to an interactive Mermaid **Sequence Diagram** (`sequenceDiagram`).
  - Converted Section 3 2-tier Raft architecture diagram to a clean Mermaid **Flowchart** (`flowchart TD`).
  - Removed duplicate Section 4 (`cc.selectPeerGroup` node selection pipeline), as peer allocation is documented in detail within the Stream Creation Journey guide.
- Refactored [`supporting_documentation/streams/consistency/operation-flow.md`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/supporting_documentation/streams/consistency/operation-flow.md):
  - Consolidated 2-tier opcodes (`EntryType` vs `entryOp`) into two structured markdown tables without data loss.
  - Replaced text-based 5-stage pipeline diagram with an interactive Mermaid **Sequence Diagram** (`sequenceDiagram`).
  - Converted `internalLoop()` text block diagram in Section 4 into a clean Go code snippet (`go`).
  - Removed redundant 2-tier Raft overview section (already covered in `overview.md`).
  - Removed redundant stepdown flow section (already covered in `leader-stepdown.md`).
  - Streamlined step-by-step publish sequence text while removing duplicate text diagram.
- **Reason**: Eliminates documentation redundancy across consistency guides and improves diagram visualization quality.
- **Affected area**: Documentation (`supporting_documentation/streams/consistency/`).

## [2026-09-18] - Stream Replica Scale-Up & Peer Addition Reference Documentation

### Added
- Restructured [`supporting_documentation/streams/lifecycle/cluster-operations/add-node.md`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/supporting_documentation/streams/lifecycle/cluster-operations/add-node.md) into 4 distinct sections:
  - **Section 1 (Triggers & Operator Actions)**: Manual Client API scale-up, CLI peer addition, `$JS.META` self-healing, and operator action matrix table (Section 1 diagram removed for clean tabular representation).
  - **Section 2 (Layer 1: Conceptual Flow & Working Principles)**: 5-step interactive Mermaid sequence diagram (`sequenceDiagram`) detailing participant interactions (`Operator`, `Meta Leader`, `Target Node`, `Stream Leader`, `Raft Quorum`), non-blocking catchup, S2 snapshot streaming, and dynamic quorum expansion ($Q = \lfloor R/2 \rfloor + 1$).
  - **Section 3 (Layer 2: Go Runtime Implementation Mechanics)**: Deep dive mapping to `nats-server` Go files and functions (`s.jsClusteredStreamUpdateRequestLocked()`, `n.ProposeAddPeer()`, `InstallSnapshot()`, `n.recalcQuorum()`).
  - **Section 4 (Operational & Performance Impact)**: Comprehensive impact analysis matrix (Client Publish Latency, Network Bandwidth, Leader CPU, Leader Disk I/O, Quorum Availability, Memory Headroom).
- Updated [`supporting_documentation/streams/lifecycle/cluster-operations/overview.md`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/supporting_documentation/streams/lifecycle/cluster-operations/overview.md) summary matrix with peer addition and scale-up controls.
- **Reason**: Enhances visualization by using a Mermaid sequence diagram for conceptual flow and relying on a clean comparison table for triggers in `add-node.md`.
- **Affected area**: Documentation (`supporting_documentation/streams/lifecycle/cluster-operations/add-node.md`).

## [2026-09-18] - Stream Replica Scale-Up & Peer Addition Reference Documentation

### Added
- Restructured [`supporting_documentation/streams/lifecycle/cluster-operations/add-node.md`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/supporting_documentation/streams/lifecycle/cluster-operations/add-node.md) into 4 distinct sections:
  - **Section 1 (Triggers & Operator Actions)**: Manual Client API scale-up, CLI peer addition, `$JS.META` self-healing, and operator action matrix table (Section 1 diagram removed for clean tabular representation).
  - **Section 2 (Layer 1: Conceptual Flow & Working Principles)**: 5-step interactive Mermaid sequence diagram (`sequenceDiagram`) detailing participant interactions (`Operator`, `Meta Leader`, `Target Node`, `Stream Leader`, `Raft Quorum`), non-blocking catchup, S2 snapshot streaming, and dynamic quorum expansion ($Q = \lfloor R/2 \rfloor + 1$).
  - **Section 3 (Layer 2: Go Runtime Implementation Mechanics)**: Deep dive mapping to `nats-server` Go files and functions (`s.jsClusteredStreamUpdateRequestLocked()`, `n.ProposeAddPeer()`, `InstallSnapshot()`, `n.recalcQuorum()`).
  - **Section 4 (Operational & Performance Impact)**: Comprehensive impact analysis matrix (Client Publish Latency, Network Bandwidth, Leader CPU, Leader Disk I/O, Quorum Availability, Memory Headroom).
- Updated [`supporting_documentation/streams/lifecycle/cluster-operations/overview.md`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/supporting_documentation/streams/lifecycle/cluster-operations/overview.md) summary matrix with peer addition and scale-up controls.
- **Reason**: Enhances visualization by using a Mermaid sequence diagram for conceptual flow and relying on a clean comparison table for triggers in `add-node.md`.
- **Affected area**: Documentation (`supporting_documentation/streams/lifecycle/cluster-operations/add-node.md`).

## [2026-09-18] - End-to-End Write Path Architecture Documentation Refactoring

### Changed
- Restructured [`supporting_documentation/streams/consistency/write.md`](file:///Users/mulukahemanthkumar/Documents/dev/learning/nats/supporting_documentation/streams/consistency/write.md) into the standardized 4-part layout:
  - **Section 1 (Triggers & Client Ingress)**: Standard client publish, JS API publish, mirror/source ingestion pathways, Mermaid flowchart, and trigger comparison table.
  - **Section 2 (Layer 1: High-Level Conceptual Flow & Working Principles)**: 4-phase write path Mermaid flowchart and 5 core architectural working principles.
  - **Section 3 (Layer 2: Go Runtime & Code Implementation Mechanics)**: 4-phase Go runtime function chain Mermaid flowchart and deep-dive mapping to `nats-server` Go source files, goroutine boundaries, structures, and function calls.
  - **Section 4 (Operational & Performance Impact)**: Operational dimension matrix covering ingress throughput, batching efficiency, consensus latency, storage safety, and egress overhead.
- Replaced all text-based ASCII flow diagrams in `write.md` with standard, interactive `mermaid` flowchart blocks (`flowchart TD`).
- **Reason**: Standardizes documentation structure and improves diagram editability and rendering quality.
- **Affected area**: Documentation (`supporting_documentation/streams/consistency/write.md`).

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



