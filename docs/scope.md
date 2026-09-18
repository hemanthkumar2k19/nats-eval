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
    - Balancing 
        - Automatic vs manual balancing behaviours
        - When to invoke balance operations 
        - Operational impact of rebalancing
    - Peer Removal
        - Process for removing a peer from a stream
        - Manual vs automatic and required operator actions
    - Node Failure and Restart 
        - Behaviour during node failure
        - Restart and recovery process
        - Manual vs automatic recovery
    - Replica Loss
        - Detection of replica loss
        - Recovery and re-replication
        - Manual vs automatic and required operator actions
    - Adding New Nodes
        - Process for adding new nodes to a stream
        - Data sync and catchup behaviour
        - Manual vs automatic and required operator actions
    - Seal and Unseal
        - Seal usage and operational meaning
        - Seal and unseal commands
        - Existence and behaviour of unseal
        - Auto-seal/unseal on failures
    - Read and Write Path
        - Cluster Operation Write - Flow
        - Cluster Operation Read - Flow

2. Storage
    - Storage Capabilities and Operational Details
    - Performance Considerations
    - Retention Options
    - Backup to Object Store
    - Memory Storage Recovery
        - Behaviour after node restart for memory-backed replicas
        - Sync timing and performance impact
        - Traffic behaviour during recovery 
        - CLI rebalancing or replacement options
    - Persistent Storage Layout
        - Directory structure on mounted volumes
        - Metadata file locations
        - Stream definition file locations
        - Message storage file locations
        - JetStream log file locations
        - SRE troubleshooting for creation and publish failures

3. Observability
    - Health
    - Resource Consumption
    - Overload Conditions
    - Replication Status
    - Capacity Thresholds
    - Reactive Approach on new subject, stream and infra

4. Consensus
    - Replication Consistency and Acknowledgements
        - Default ack mode (leader-only vs quorum)
        - Available consistency configurations
        - Impact on latency, reliability, and durability

5. Deployment and Operational Automation <!-- TODO -->
    - Kubernetes deployment <!-- TODO -->
    - VM deployment <!-- TODO -->
    - Automation via Ansible or similar <!-- TODO -->
    - Database-style operational guidance <!-- TODO -->
    - Troubleshooting scenarios (e.g., read-only volume) <!-- TODO -->