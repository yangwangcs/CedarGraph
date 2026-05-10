// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// =============================================================================
// StorageD - 分布式存储节点服务 (简化版，无 gRPC 依赖)
// =============================================================================

#ifndef CEDAR_DTX_STORAGE_SERVER_H_
#define CEDAR_DTX_STORAGE_SERVER_H_

#include <memory>
#include <unordered_map>
#include <atomic>
#include <thread>

#include "cedar/core/status.h"
#include "cedar/types/descriptor.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/dtx/types.h"
#include "cedar/dtx/meta_service.h"

namespace cedar {
namespace dtx {

// =============================================================================
// StorageD 配置
// =============================================================================

struct StorageServerConfig {
    NodeID node_id{0};
    std::string listen_address{"0.0.0.0:50051"};
    std::string advertise_address;
    std::string data_dir{"./storage_data"};
    std::vector<std::string> meta_addresses;
    std::vector<PartitionID> partitions;
    uint32_t num_partitions{256};
    uint32_t grpc_threads{4};
    uint32_t io_threads{4};
};

// =============================================================================
// 分区存储实例
// =============================================================================

class PartitionStorage {
public:
    PartitionStorage(PartitionID pid, const std::string& data_path);
    ~PartitionStorage();
    
    Status Initialize();
    Status Shutdown();
    
    Status Put(const CedarKey& key, const Descriptor& descriptor, Timestamp txn_version);
    StatusOr<Descriptor> Get(const CedarKey& key);
    Status Delete(const CedarKey& key, Timestamp txn_version);
    
    std::vector<std::pair<Timestamp, Descriptor>> Scan(
        uint64_t entity_id, Timestamp start_time, Timestamp end_time);
    
    Status BatchPut(const std::vector<std::pair<CedarKey, Descriptor>>& items, 
                    Timestamp txn_version);
    std::vector<std::optional<Descriptor>> BatchGet(const std::vector<CedarKey>& keys);
    
    Status Prepare(uint64_t txn_id, 
                   const std::vector<CedarKey>& read_set,
                   const std::vector<CedarKey>& write_set,
                   Timestamp commit_ts);
    Status Commit(uint64_t txn_id, Timestamp commit_ts);
    Status Abort(uint64_t txn_id);
    
    struct Stats {
        uint64_t data_size{0};
        uint64_t key_count{0};
        uint64_t memtable_size{0};
        uint64_t sst_count{0};
    };
    Stats GetStats() const;
    
    PartitionID GetPartitionId() const { return partition_id_; }

private:
    PartitionID partition_id_;
    std::string data_path_;
    std::unique_ptr<CedarGraphStorage> storage_;
    
    struct PendingTxn {
        uint64_t txn_id{0};
        std::vector<CedarKey> write_set;
        Timestamp commit_ts{0};
        bool prepared{false};
    };
    std::unordered_map<uint64_t, PendingTxn> pending_txns_;
    mutable std::shared_mutex txn_mutex_;
};

// =============================================================================
// 分区管理器
// =============================================================================

class PartitionManager {
public:
    PartitionManager();
    ~PartitionManager();
    
    Status Initialize(const StorageServerConfig& config);
    Status Shutdown();
    
    PartitionStorage* GetPartition(PartitionID pid);
    Status AddPartition(PartitionID pid);
    Status RemovePartition(PartitionID pid);
    std::vector<PartitionID> GetAllPartitions() const;
    
    // Stub type for partition info
    struct PartitionInfo {
        uint32_t partition_id{0};
        uint64_t data_size{0};
        uint64_t key_count{0};
        uint64_t qps{0};
        bool is_leader{false};
        std::string raft_role{"LEADER"};
    };
    std::vector<PartitionInfo> GetPartitionInfos() const;

private:
    StorageServerConfig config_;
    mutable std::shared_mutex partitions_mutex_;
    std::unordered_map<PartitionID, std::unique_ptr<PartitionStorage>> partitions_;
};

// =============================================================================
// StorageD 服务器
// =============================================================================

class StorageServer {
public:
    StorageServer();
    ~StorageServer();
    
    Status Initialize(const StorageServerConfig& config);
    void Serve();
    Status Shutdown();
    
    NodeID GetNodeId() const { return config_.node_id; }
    PartitionManager* GetPartitionManager() { return &partition_mgr_; }

private:
    Status RegisterToMetaD();
    void HeartbeatLoop();
    
    StorageServerConfig config_;
    PartitionManager partition_mgr_;
    std::unique_ptr<MetaServiceNodeClient> meta_client_;
    std::atomic<bool> running_{false};
    std::thread heartbeat_thread_;
};

// =============================================================================
// StorageD 客户端
// =============================================================================
// 注意：StorageClient 的完整定义在 storage_service_impl.h 中
// 这里只需要前向声明以避免循环依赖

// 前向声明
class StorageClient;

// =============================================================================
// StorageClient 池
// =============================================================================

// =============================================================================
// StorageClient 池 (简化版)
// 完整实现在 storage_service_impl.h 中
// =============================================================================
class StorageClientPool {
public:
    StorageClientPool();
    ~StorageClientPool();
    
    std::shared_ptr<StorageClient> GetClient(NodeID node_id, 
                                              const std::string& address);
    void CloseAll();
    void HealthCheck();

private:
    mutable std::shared_mutex clients_mutex_;
    std::unordered_map<NodeID, std::shared_ptr<StorageClient>> clients_;
};

} // namespace dtx
} // namespace cedar

#endif // CEDAR_DTX_STORAGE_SERVER_H_
