# Queries

## Streams

1. 3 Nodes, 2 Jetstream Replicas - what happens when stream replica node goes down?

## Stream Creation Internals

2. Why and when should new streams be created vs reusing existing streams?
3. How does the cluster-wide stream creation process work end to end?
4. How are replicas allocated across nodes during stream creation?
5. What is the node-selection logic for replica placement?
6. What rules govern replica placement to avoid hotspots?

## Cluster Balancing and Leader Operations

7. What automatic balancing behaviours exist in NATS JetStream clusters?
8. What manual balancing operations are available and when should they be invoked?
9. What is the operational impact of invoking a balance operation on a running cluster?
10. When should a leader step-down be triggered and what is its impact on clients?

## Stream Lifecycle Operations

11. What is the detailed flow for a leader step-down? Is it manual, automatic, or both?
12. What is the detailed flow for peer removal from a stream? What operator actions are required?
13. What happens during node failure? What is the automatic recovery process?
14. What happens on node restart? How does the stream catch up?
15. What is the behaviour when a replica is permanently lost? How is re-replication triggered?
16. What is the process for adding new nodes to an existing stream? How does data sync work?
17. For each lifecycle event above, which are manual vs automatic and what operator actions are required?

## Memory Storage Recovery

18. What happens to memory-backed stream replicas after a node restart?
19. How long does sync take for memory-backed replicas and what is the performance impact during recovery?
20. What does traffic behaviour look like during memory replica recovery (reads, writes, acks)?
21. Can CLI rebalancing or replica replacement be used for memory-backed streams?

## Replication Consistency and Acknowledgements

22. What is the default ack mode for JetStream streams - leader-only or quorum?
23. What consistency configurations are available for streams (e.g., ack quorum settings)?
24. How do different consistency configurations impact latency, reliability, and durability?

## Seal Lifecycle

25. What does sealing a stream mean operationally? When is it used?
26. What are the exact commands to seal a stream?
27. Does unseal exist? If so, what is its behaviour?
28. Are there any conditions where auto-seal or auto-unseal is triggered by failures?

## Persistent Storage Layout

29. What is the directory structure when NATS JetStream runs on a mounted volume?
30. Where are stream metadata files stored on disk?
31. Where are stream definition files stored on disk?
32. Where are message data files stored on disk?
33. Where are JetStream logs stored on disk?
34. How can an SRE use the storage layout to troubleshoot stream creation or publish failures?

## Deployment and Operational Automation

35. What are the key considerations for deploying NATS on Kubernetes vs VMs?
36. What automation approaches (Ansible, Helm, Terraform) are recommended for NATS deployment?
37. What database-style operational guidance applies to NATS (backup, restore, upgrade, rollback)?
38. What are common troubleshooting scenarios for NATS in production (e.g., read-only volume, split brain)?