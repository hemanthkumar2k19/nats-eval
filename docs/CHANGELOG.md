# Changelog

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



