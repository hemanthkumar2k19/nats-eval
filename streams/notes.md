# Stream

# Questions
- What, How, Why
- What happens internally when creating a stream?
- How nodes for replicas are choosen?
- Cluster Operations on Stream - Using nats stream are automatic or manual
- Node loss and other scenrios
- Additonal New Node



## Internals

### Stream Create

1. Client Sends Request to Subject: $JS.API.STREAM.CREATE.<stream_name>
2. Extract Client Info, target account, headers and msg payload