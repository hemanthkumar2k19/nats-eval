# Consumer

## Deliery Policy
Consider stream FirstSeq - fs and LastSeq - ls and stream is an array [fs, ls].
- all: [fs, ls] (default)
- last: [ls, ls]
- new: []
- by_start_sequence: [seq_index, ls]
- by_start_time: [[seq_at_time, ls]
- last_per_subject: All of [ls_of_subject, ls_of_subject]


## ACK
- ACK is a way to acknowledge the messages received by the consumer.
- Ack Wait: How long (in nanoseconds) to allow messages to remain un-acknowledged before attempting redelivery

### ACK Policies
- explicit: ACK required(default of SDK and CLIs)
- all: ACK LastOne -> [fs, ls] ACKed
- none: No ACK needed


## Push Consumers
1. Flow Control
- Window Tracking: Server tracks pending un-ACKed bytes and message count (`MaxAckPending` or 50% byte threshold).
- Control Signal: Server sends a Flow Control request ping to client when threshold is hit.
- Client Reply: Client SDK auto-responds with headers containing highest received sequences (`Nats-Last-Stream-Sequence`, `Nats-Last-Consumer-Sequence`).
- Bulk ACK: Server calls `processAckMsg()` on receiving reply, bulk ACKing all messages up to target sequence to advance ACK floor and resume push delivery.


## Pointers
- ACK Floor is exclusively per consumer
    - AckFloor.Stream: Highest stream sequence fully ACKed.
    - AckFloor.Consumer: Highest consumer delivery sequence fully ACKed.
    - Delivered: Sequence of the most recently delivered message.
    - Pending: Map of un-ACKed messages above the Ack Floor.

## Consumer RAFT

- If num_replicas is omitted (or set to 0), the consumer inherits the replica count of the parent stream
- Exception: Unnamed ephemeral consumers on standard limits retention streams default to 1.
