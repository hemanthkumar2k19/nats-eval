# Consumer

## Pointers
- ACK Floor is exclusively per consumer
    - AckFloor.Stream: Highest stream sequence fully ACKed.
    - AckFloor.Consumer: Highest consumer delivery sequence fully ACKed.
    - Delivered: Sequence of the most recently delivered message.
    - Pending: Map of un-ACKed messages above the Ack Floor.
