# DTX Network Transport

This directory is currently reserved for future transport abstractions.

CedarGraph's DTX (Distributed Transaction) layer uses **brpc** directly for all RPC
and network communication needs, including:

- Inter-node RPC calls
- Raft consensus messaging
- gRPC service exposure
- Cross-DC replication

brpc provides the necessary transport layer (TCP/HTTP2), connection pooling,
load balancing, and protocol support required by DTX. No additional transport
abstraction is needed at this time.

## Future Work

This directory may host a custom transport abstraction if brpc is replaced
or augmented with additional protocols (e.g., RDMA, DPDK, custom kernel-bypass
networking) in future versions.
