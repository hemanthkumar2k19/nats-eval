# Layer 2

# Streams

1. Lifecycle
    - Creation
        - Why and when streams are created
        - Cluster-wide creation internal process
        - Replica allocation and node-selection logic
        - Replica placement rules and hotspot avoidance
    - Update
    - Sequence Handling
    - Retention Handling
    - Cleanup Policies
    - Backup and Restore
    - Criteria for creating separate streams
    - Leader Step-Down
        - When and how leader step-down occurs
        - Operational impact during step-down
        - Manual vs automatic triggers
    - Balancing <!-- TODO -->
        - Automatic vs manual balancing behaviours <!-- TODO -->
        - When to invoke balance operations <!-- TODO -->
        - Operational impact of rebalancing <!-- TODO -->
    - Peer Removal <!-- TODO -->
        - Process for removing a peer from a stream <!-- TODO -->
        - Manual vs automatic and required operator actions <!-- TODO -->
    - Node Failure and Restart <!-- TODO -->
        - Behaviour during node failure <!-- TODO -->
        - Restart and recovery process <!-- TODO -->
        - Manual vs automatic recovery <!-- TODO -->
    - Replica Loss <!-- TODO -->
        - Detection of replica loss <!-- TODO -->
        - Recovery and re-replication <!-- TODO -->
        - Manual vs automatic and required operator actions <!-- TODO -->
    - Adding New Nodes <!-- TODO -->
        - Process for adding new nodes to a stream <!-- TODO -->
        - Data sync and catchup behaviour <!-- TODO -->
        - Manual vs automatic and required operator actions <!-- TODO -->
    - Seal and Unseal
        - Seal usage and operational meaning
        - Seal and unseal commands
        - Existence and behaviour of unseal
        - Auto-seal/unseal on failures

2. Storage
    - Storage Capabilities and Operational Details
    - Performance Considerations
    - Retention Options
    - Backup to Object Store
    - Memory Storage Recovery <!-- TODO -->
        - Behaviour after node restart for memory-backed replicas <!-- TODO -->
        - Sync timing and performance impact <!-- TODO -->
        - Traffic behaviour during recovery <!-- TODO -->
        - CLI rebalancing or replacement options <!-- TODO -->
    - Persistent Storage Layout <!-- TODO -->
        - Directory structure on mounted volumes <!-- TODO -->
        - Metadata file locations <!-- TODO -->
        - Stream definition file locations <!-- TODO -->
        - Message storage file locations <!-- TODO -->
        - JetStream log file locations <!-- TODO -->
        - SRE troubleshooting for creation and publish failures <!-- TODO -->

3. Observability
    - Health
    - Resource Consumption
    - Overload Conditions
    - Replication Status
    - Capacity Thresholds
    - Reactive Approach on new subject, stream and infra

4. Consensus
    - Replication Consistency and Acknowledgements <!-- TODO -->
        - Default ack mode (leader-only vs quorum) <!-- TODO -->
        - Available consistency configurations <!-- TODO -->
        - Impact on latency, reliability, and durability <!-- TODO -->

5. Deployment and Operational Automation <!-- TODO -->
    - Kubernetes deployment <!-- TODO -->
    - VM deployment <!-- TODO -->
    - Automation via Ansible or similar <!-- TODO -->
    - Database-style operational guidance <!-- TODO -->
    - Troubleshooting scenarios (e.g., read-only volume) <!-- TODO -->