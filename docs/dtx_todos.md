# DTX Module TODOs and FIXMEs

**Generated:** 2026-04-10  
**Total Items:** 109 TODO/FIXME/XXX/HACK comments  
**Module:** Distributed Transaction (DTX)

## Summary by Priority

| Priority | Count | Description |
|----------|-------|-------------|
| P0 (Critical) | 12 | Must fix before production |
| P1 (High) | 35 | Should fix soon |
| P2 (Medium) | 45 | Nice to have |
| P3 (Low) | 17 | Future improvements |

---

## P0: Critical - Must Fix Before Production

### Raft Consensus
- [ ] `src/dtx/metad/admin_service.cc:53` - Propose membership change through Raft (currently just adds to transport)
- [ ] `src/dtx/metad/admin_service.cc:82` - Propose membership change through Raft for node removal
- [ ] `src/dtx/metad/admin_service.cc:250` - Create membership change log entry and propose to Raft

### Storage Implementation
- [ ] `src/dtx/storage_impl/storage_service_impl.cc:139` - Support reading at specific timestamp
- [ ] `src/dtx/storage_impl/storage_service_impl.cc:203` - Implement scan operation using CedarGraphStorage iterator
- [ ] `src/dtx/storage/storage_server.cc:225` - Start gRPC server for storage operations

### Raft Implementation
- [ ] `src/dtx/raft/raft_node_factory.cc:104` - Return actual members from node
- [ ] `src/dtx/raft/embedded_raft.cc:647` - Implement snapshot installation
- [ ] `src/dtx/raft/embedded_raft.cc:654` - Implement snapshot handling
- [ ] `src/dtx/raft/embedded_raft.cc:663` - Check if snapshot is needed

### Transaction Recovery
- [ ] `src/dtx/transaction_recovery_manager.cc:162` - Implement commit recovery
- [ ] `src/dtx/transaction_recovery_manager.cc:166` - Implement abort recovery

---

## P1: High Priority

### Service Discovery & Metadata
- [ ] `src/dtx/storage_impl/meta_service_client.cc:90` - Get actual memory usage
- [ ] `src/dtx/storage_impl/meta_service_client.cc:91` - Get actual disk usage
- [ ] `src/dtx/storage_impl/meta_service_client.cc:127` - Get actual disk usage percent
- [ ] `src/dtx/storage_impl/meta_service_client.cc:128` - Track QPS metrics
- [ ] `src/dtx/storage_impl/meta_service_client.cc:129` - Track latency metrics
- [ ] `src/dtx/meta/meta_service.cc:290` - Implement metadata operations
- [ ] `src/dtx/meta/meta_service.cc:343` - Implement cluster operations
- [ ] `src/dtx/meta/meta_service.cc:358` - Implement node failure detection
- [ ] `src/dtx/meta/meta_service.cc:379` - Implement actual connection logic

### RPC & gRPC
- [ ] `src/dtx/meta/meta_service.cc:392` - Fetch from MetaD and update cache
- [ ] `src/dtx/meta/meta_service.cc:398` - Implement partition operations
- [ ] `src/dtx/meta/meta_service.cc:409` - Implement RPC call for node registration
- [ ] `src/dtx/meta/meta_service.cc:414` - Implement RPC call for heartbeat
- [ ] `src/dtx/meta/meta_service.cc:419` - Implement RPC call for node removal
- [ ] `src/dtx/meta/meta_service.cc:424` - Implement RPC call for metadata update
- [ ] `src/dtx/meta/meta_service.cc:429` - Implement RPC call for leader election
- [ ] `src/dtx/meta/meta_service.cc:435` - Implement watch mechanism

### Partition Management
- [ ] `src/dtx/grpc/migration_executor.cc:143` - RPC call to target to reserve partition slot
- [ ] `src/dtx/grpc/migration_executor.cc:146` - RPC call to get partition size and key count
- [ ] `src/dtx/grpc/migration_executor.cc:154` - Create partition on target node
- [ ] `src/dtx/grpc/migration_executor.cc:164` - Get iterator over source partition data
- [ ] `src/dtx/grpc/migration_executor.cc:200` - RPC to source to enable dual write mode
- [ ] `src/dtx/grpc/migration_executor.cc:201` - RPC to target to start accepting writes
- [ ] `src/dtx/grpc/migration_executor.cc:220` - Replicate recent writes from source to target
- [ ] `src/dtx/grpc/migration_executor.cc:236` - RPC to source to enter read-only mode
- [ ] `src/dtx/grpc/migration_executor.cc:239` - Replicate final batch during migration
- [ ] `src/dtx/grpc/migration_executor.cc:242` - Update MetaD partition assignment
- [ ] `src/dtx/grpc/migration_executor.cc:248` - RPC to target to enable full read-write mode
- [ ] `src/dtx/grpc/migration_executor.cc:256` - Sample keys and verify checksums
- [ ] `src/dtx/grpc/migration_executor.cc:266` - RPC to source to schedule partition deletion
- [ ] `src/dtx/grpc/migration_executor.cc:286` - Implement rollback logic
- [ ] `src/dtx/grpc/migration_executor.cc:292` - RPC call to transfer batch to target node
- [ ] `src/dtx/grpc/migration_executor.cc:303` - Implement bandwidth throttling

### Storage Service
- [ ] `src/dtx/storage/storage_server_full.cc:248` - Get Raft role from actual Raft state
- [ ] `src/dtx/storage/storage_server_full.cc:249` - Get is_leader from actual Raft state
- [ ] `src/dtx/storage/storage_server_full.cc:545` - Implement proper leader detection
- [ ] `src/dtx/storage/storage_server_full.cc:546` - Get actual Raft role
- [ ] `src/dtx/storage/storage_server_full.cc:586` - Proper deserialization
- [ ] `src/dtx/storage/storage_server_full.cc:605` - Proper serialization
- [ ] `src/dtx/storage_impl/storage_service_impl.cc:433` - Track QPS metrics
- [ ] `src/dtx/storage_impl/storage_service_impl.cc:434` - Integrate with Raft for leader detection
- [ ] `src/dtx/storage_impl/storage_service_impl.cc:454` - Track flushed size

---

## P2: Medium Priority

### Storage Extensions
- [ ] `src/dtx/storage_impl/storage_service_ext.cc:271` - Implement using CedarGraphStorage scan API
- [ ] `src/dtx/storage_impl/storage_service_ext.cc:294` - Scan SST files and collect metadata
- [ ] `src/dtx/storage_impl/storage_service_ext.cc:312` - Iterate through all SST files and aggregate stats
- [ ] `src/dtx/storage_impl/storage_service_ext.cc:432` - Fill in estimated_size and tombstone_ratio
- [ ] `src/dtx/storage_impl/storage_service_ext.cc:596` - Fetch actual values from storage

### Partition Index
- [ ] `src/dtx/storage_impl/partition_index.cc:37` - Scan all SST files and extract partition metadata
- [ ] `src/dtx/storage_impl/partition_index.cc:277` - Scan MemTable for partition's keys
- [ ] `src/dtx/storage_impl/partition_index.cc:287` - Scan SST file for keys in entity range

### Partition Migrator
- [ ] `src/dtx/storage/partition_migrator.cc:270` - Prepare source partition for migration
- [ ] `src/dtx/storage/partition_migrator.cc:278` - Implement actual data copy
- [ ] `src/dtx/storage/partition_migrator.cc:286` - Implement catch-up logic
- [ ] `src/dtx/storage/partition_migrator.cc:293` - Implement traffic switching
- [ ] `src/dtx/storage/partition_migrator.cc:309` - Get target checksum
- [ ] `src/dtx/storage/partition_migrator.cc:319` - Complete migration
- [ ] `src/dtx/storage/partition_migrator.cc:327` - Implement rollback
- [ ] `src/dtx/storage/partition_migrator.cc:336` - Calculate partition checksum

### Coordinator & Partition
- [ ] `src/dtx/coordinator/partition.cc:302` - Implement actual rebalancing logic

### Meta Service Implementation
- [ ] `src/dtx/meta/meta_service_impl.cc:272` - Implement proper deserialization
- [ ] `src/dtx/meta/meta_service_impl.cc:330` - Get last_included_term from Raft

### Raft Implementation
- [ ] `src/dtx/raft/raft_node_factory.cc:171` - Create braft-based wrapper
- [ ] `src/dtx/raft/embedded_raft_impl.cc:738` - Serialize log entries
- [ ] `src/dtx/raft/embedded_raft_impl.cc:813` - Count votes
- [ ] `src/dtx/raft/embedded_raft_impl.cc:829` - Process log entries
- [ ] `src/dtx/raft/embedded_raft_impl.cc:835` - Update match_index and next_index
- [ ] `src/dtx/raft/embedded_raft.cc:262` - Send vote requests to peers
- [ ] `src/dtx/storage/raft_replication.cc:109` - Implement entry.key from CedarKey
- [ ] `src/dtx/storage/raft_replication.cc:411` - Handle apply error
- [ ] `src/dtx/storage/raft_replication.cc:546` - Implement actual RPC to peers

### Transaction Recovery
- [ ] `src/dtx/transaction_recovery_manager.cc:170` - Implement inquiry
- [ ] `src/dtx/transaction_recovery_manager.cc:205` - Implement retry logic
- [ ] `src/dtx/transaction_recovery_manager.cc:237` - Implement coordinator recovery
- [ ] `src/dtx/transaction_recovery_manager.cc:246` - Implement participant recovery
- [ ] `src/dtx/transaction_recovery_manager.cc:255` - Implement full recovery
- [ ] `src/dtx/transaction_recovery_manager.cc:264` - Implement state persistence
- [ ] `src/dtx/transaction_recovery_manager.cc:272` - Implement state loading

### Security
- [ ] `src/dtx/security/security_manager.cc:51` - Initialize OpenSSL SSL_CTX

### Multi-Raft Optimization
- [ ] `src/dtx/storage/multi_raft_optimization.cc:430` - Get leader node from group

### Failover Manager
- [ ] `src/dtx/storage/failover_manager.cc:200` - Check replica health status
- [ ] `src/dtx/storage/failover_manager.cc:219` - Implement actual leader switching logic
- [ ] `src/dtx/storage/failover_manager.cc:226` - Update MetaD partition routing info
- [ ] `src/dtx/storage/failover_manager.cc:239` - Implement lease renewal
- [ ] `src/dtx/storage/failover_manager.cc:250` - Regular leader health check
- [ ] `src/dtx/storage/failover_manager.cc:485` - Restart service
- [ ] `src/dtx/storage/failover_manager.cc:488` - Switch leader
- [ ] `src/dtx/storage/failover_manager.cc:491` - Promote replica

### Load Balancer
- [ ] `src/dtx/load_balancer.cc:74` - Implement load calculation
- [ ] `src/dtx/load_balancer.cc:97` - Implement load balancing strategy
- [ ] `src/dtx/load_balancer.cc:343` - Get actual load from node

---

## P3: Low Priority / Future Improvements

### Protocol Optimizations
- [ ] `src/dtx/protocol/version_chain.cc:422` - Implement cross-shard coordination verification
- [ ] `src/dtx/protocol/lsm_native_occ.cc:314` - Implement lightweight coordination
- [ ] `src/dtx/protocol/lsm_native_occ.cc:552` - Implement SameTemporalRange check based on temporal window

### Metrics & Monitoring
- [ ] `src/dtx/storage/metrics_collector.cc:215` - Implement actual HTTP/network sending logic
- [ ] `src/dtx/storage/metrics_collector.cc:431` - Implement HTTP POST for alerts
- [ ] `src/dtx/storage/metrics_collector.cc:570` - Evaluate alert rules

---

## Fixed TODOs (During This Refactoring)

| File | Line | Description | Resolution |
|------|------|-------------|------------|
| src/dtx/storage_impl/storage_client.cc | - | Hardcoded endpoints | Now uses ServiceRegistry |
| src/dtx/grpc/rpc_client.cc | - | Hardcoded node addresses | Now uses ServiceRegistry |
| src/dtx/metad/admin_service.cc | - | Hardcoded configuration | Now uses ConfigManager |
| src/dtx/storage/storage_server.cc | - | Hardcoded configuration | Now uses ConfigManager |

---

## Integration with Governance Layer

### Completed
- [x] ServiceRegistry integration for service discovery
- [x] ConfigManager integration for configuration management
- [x] DTX configuration schema created
- [x] TODO documentation with priorities

### Remaining Integration Work
- [ ] Implement health check integration with governance::HealthChecker
- [ ] Use governance::ServiceRegistry for all node discovery
- [ ] Migrate all configuration to governance::ConfigManager
- [ ] Remove deprecated service_discovery.cc/h (already moved to governance/)

---

## Notes

1. **Many TODOs are expected** - This is a complex distributed system under active development
2. **P0 items block production** - These must be fixed before considering the system production-ready
3. **Integration is incremental** - The governance layer integration is designed to be backward compatible
4. **Testing required** - Each fix should include appropriate unit and integration tests
