# Firehose gRPC

Firehose gRPC is intended to be used as an extraction layer for The Graph's Node.  
It implements the same gRPC interface as "canonical" ethereum-firehose implentation does but uses subsquid archives as a datasource instead of running geth node.

## Real-time data
Subsquid archives do not store latest blocks and it means that evm rpc api is still required for "real-time" data support.
