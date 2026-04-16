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
// StorageService Implementation - Shared LSM-Tree Architecture
// Multiple partitions share one LSM-Tree instance, distinguished by part_id
// =============================================================================

#ifndef CEDAR_DTX_STORAGE_SERVICE_IMPL_H_
#define CEDAR_DTX_STORAGE_SERVICE_IMPL_H_

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// Note: condition_variable is included after memory to ensure proper std namespace
#include <condition_variable>

#include "cedar/core/status.h"
#include "cedar/types/descriptor.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/storage_interface.h"
#include "cedar/dtx/types.h"
#include "cedar/dtx/transaction_state.h"

// gRPC includes
#include <grpcpp/grpcpp.h>
#include "storage_service.grpc.pb.h"
#include "meta_service.grpc.pb.h"

namespace cedar {

// Forward declaration for governance layer integration
namespace governance {
  class ServiceRegistry;
}

namespace dtx {

// Forward declarations
class MetaServiceClient;
class PartitionManager;

// Forward declaration for PartitionStorage
class StoragePartitionManager;
class StorageServiceImpl;

// =============================================================================
// PartitionStorage - Logical partition view over shared LSM-Tree
// Multiple partitions share one CedarGraphStorage instance
// =============================================================================

class PartitionStorage {
 public:
  struct StorageStats {
    PartitionID partition_id;
    size_t num_keys = 0;           // Estimated key count for this partition
    size_t disk_usage_bytes = 0;   // Estimated disk usage
    size_t num_active_txns = 0;    // Active transactions on this partition
  };

  struct PreparedTxnState {
    TxnID txn_id;
    std::vector<CedarKey> read_set;
    std::vector<CedarKey> write_set;
    Timestamp commit_ts;
    DistributedTxnState status;
  };

  // Constructor: takes partition_id and shared storage reference
  PartitionStorage(PartitionID partition_id, 
                   CedarGraphStorage* shared_storage,
                   StoragePartitionManager* manager);
  ~PartitionStorage();

  // No Open/Close - storage is managed by PartitionManager
  bool IsReady() const { return shared_storage_ != nullptr; }

  // Data operations - automatically inject part_id into keys
  Status Put(const CedarKey& key, const Descriptor& descriptor,
             Timestamp txn_version, TxnID txn_id);
  StatusOr<Descriptor> Get(const CedarKey& key, Timestamp read_time);

  // 2PC support
  Status Prepare(TxnID txn_id, const std::vector<CedarKey>& reads,
                 const std::vector<CedarKey>& writes, Timestamp commit_ts);
  Status Commit(TxnID txn_id, Timestamp commit_ts);
  Status Abort(TxnID txn_id);

  // Utility
  bool IsReadOnly() const;
  void SetReadOnly(bool readonly);
  PartitionID GetPartitionId() const { return partition_id_; }
  StorageStats GetStats() const;
  std::vector<TxnID> GetPreparedTransactions() const;

  // Key manipulation: inject/extract part_id
  CedarKey InjectPartitionId(const CedarKey& key) const;
  static PartitionID ExtractPartitionId(const CedarKey& key);

 private:
  Status WriteTxnWAL(uint64_t txn_id, const std::string& operation);

  PartitionID partition_id_;
  CedarGraphStorage* shared_storage_;  // Shared across all partitions
  StoragePartitionManager* manager_;   // For cross-partition operations
  
  std::atomic<bool> is_readonly_{false};
  mutable std::shared_mutex txn_mutex_;
  std::unordered_map<TxnID, PreparedTxnState> prepared_txns_;
};

// =============================================================================
// StoragePartitionManager - Manages multiple partitions sharing one LSM-Tree
// =============================================================================

class StoragePartitionManager {
 public:
  struct PartitionConfig {
    std::string data_root = "/tmp/cedar_storage";
    size_t max_partitions = 1024;
    size_t max_disk_usage = 0;  // 0 = unlimited
  };

  StoragePartitionManager();
  ~StoragePartitionManager();

  Status Initialize(const PartitionConfig& config);
  void Shutdown();

  // Partition lifecycle (logical only, shares same storage)
  PartitionStorage* GetPartition(PartitionID pid);
  Status AddPartition(PartitionID pid);
  Status RemovePartition(PartitionID pid);
  bool HasPartition(PartitionID pid) const;

  // Queries
  std::vector<PartitionID> GetAllPartitions() const;
  std::vector<PartitionID> GetLoadedPartitions() const;
  size_t GetPartitionCount() const;
  
  // Get shared storage
  CedarGraphStorage* GetSharedStorage() const { return shared_storage_.get(); }

  // Maintenance - operates on shared storage
  Status FlushAll();
  Status CompactPartition(PartitionID pid);
  Status CompactAll();
  
  // Statistics
  size_t GetTotalDiskUsage() const;
  
  std::string GetDataRoot() const { return config_.data_root; }

 private:
  PartitionConfig config_;
  std::unique_ptr<CedarGraphStorage> shared_storage_;  // One LSM-Tree for all partitions
  std::unordered_map<PartitionID, std::unique_ptr<PartitionStorage>> partitions_;
  mutable std::shared_mutex partitions_mutex_;
  std::atomic<bool> initialized_{false};
};

// =============================================================================
// StorageServiceImpl - gRPC service implementation for StorageD
// =============================================================================

class StorageServiceImpl final : public cedar::storage::StorageService::Service {
 public:
  explicit StorageServiceImpl(StoragePartitionManager* partition_manager);
  ~StorageServiceImpl();

  // Disable copy
  StorageServiceImpl(const StorageServiceImpl&) = delete;
  StorageServiceImpl& operator=(const StorageServiceImpl&) = delete;

  // gRPC service methods - Basic operations
  grpc::Status Put(grpc::ServerContext* context,
                   const cedar::storage::PutRequest* request,
                   cedar::storage::PutResponse* response) override;
  
  grpc::Status Get(grpc::ServerContext* context,
                   const cedar::storage::GetRequest* request,
                   cedar::storage::GetResponse* response) override;
  
  grpc::Status Delete(grpc::ServerContext* context,
                      const cedar::storage::DeleteRequest* request,
                      cedar::storage::DeleteResponse* response) override;
  
  grpc::Status Scan(grpc::ServerContext* context,
                    const cedar::storage::ScanRequest* request,
                    cedar::storage::ScanResponse* response) override;
  
  grpc::Status ScanNodeV2(grpc::ServerContext* context,
                          const cedar::storage::ScanNodeRequestV2* request,
                          cedar::storage::ScanResponse* response) override;
  
  grpc::Status ScanEdgeV2(grpc::ServerContext* context,
                          const cedar::storage::ScanEdgeRequestV2* request,
                          cedar::storage::ScanResponse* response) override;

  // Batch operations
  grpc::Status BatchPut(grpc::ServerContext* context,
                        const cedar::storage::BatchPutRequest* request,
                        cedar::storage::BatchPutResponse* response) override;
  
  grpc::Status BatchGet(grpc::ServerContext* context,
                        const cedar::storage::BatchGetRequest* request,
                        cedar::storage::BatchGetResponse* response) override;

  // 2PC distributed transaction support
  grpc::Status Prepare(grpc::ServerContext* context,
                       const cedar::storage::PrepareRequest* request,
                       cedar::storage::PrepareResponse* response) override;
  
  grpc::Status Commit(grpc::ServerContext* context,
                      const cedar::storage::CommitRequest* request,
                      cedar::storage::CommitResponse* response) override;
  
  grpc::Status Abort(grpc::ServerContext* context,
                     const cedar::storage::AbortRequest* request,
                     cedar::storage::AbortResponse* response) override;

  grpc::Status Inquire(grpc::ServerContext* context,
                       const cedar::storage::InquireRequest* request,
                       cedar::storage::InquireResponse* response) override;

  // Partition management
  grpc::Status GetPartitionInfo(grpc::ServerContext* context,
                                const cedar::storage::GetPartitionInfoRequest* request,
                                cedar::storage::GetPartitionInfoResponse* response) override;

  // Data persistence
  grpc::Status Flush(grpc::ServerContext* context,
                     const cedar::storage::FlushRequest* request,
                     cedar::storage::FlushResponse* response) override;

  // Heartbeat (bidirectional streaming)
  grpc::Status Heartbeat(grpc::ServerContext* context,
                         grpc::ServerReaderWriter<cedar::storage::HeartbeatResponse,
                                                  cedar::storage::HeartbeatRequest>* stream) override;

 private:
  // Helper methods for proto conversion
  CedarKey ProtoToCedarKey(const cedar::storage::CedarKey& proto_key);
  cedar::storage::CedarKey CedarKeyToProto(const CedarKey& key);
  Descriptor ProtoToDescriptor(const cedar::storage::Descriptor& proto_desc,
                                uint16_t column_id = 0);
  cedar::storage::Descriptor DescriptorToProto(const Descriptor& desc);

  std::unique_ptr<cedar::storage::StorageInterface> storage_interface_;
  std::vector<cedar::storage::PropertyPredicateItem> ConvertPredicates(
      const google::protobuf::RepeatedPtrField<cedar::storage::ScanPredicate>& proto_preds);

  StoragePartitionManager* partition_manager_;
};

// =============================================================================
// StorageClient - Client for connecting to StorageD nodes via gRPC
// =============================================================================

class StorageClient {
 public:
  struct ClientConfig {
    std::string server_address;
    size_t max_retries = 3;
    std::chrono::milliseconds retry_base_delay{10};
    std::chrono::milliseconds operation_timeout{5000};
  };

  StorageClient();
  ~StorageClient();
  
  // Disable copy (because of unique_ptr member)
  StorageClient(const StorageClient&) = delete;
  StorageClient& operator=(const StorageClient&) = delete;

  Status Initialize(const ClientConfig& config);
  
  // Initialize using ServiceRegistry for service discovery (governance layer integration)
  // Note: The registry must remain valid for the lifetime of the client
  Status InitializeWithDiscovery(const std::string& service_name,
                                  const governance::ServiceRegistry& registry);
  
  void Shutdown();

  // Data operations
  Status Put(const CedarKey& key, const Descriptor& descriptor,
             Timestamp txn_version, TxnID txn_id);
  StatusOr<Descriptor> Get(const CedarKey& key, Timestamp read_time);

  // Batch write method
  Status BatchPut(const std::vector<std::pair<CedarKey, Descriptor>>& items,
                  Timestamp txn_version);

  // 2PC coordinator support
  StatusOr<bool> Prepare(TxnID txn_id, const std::vector<CedarKey>& reads,
                         const std::vector<CedarKey>& writes, Timestamp commit_ts);
  Status Commit(TxnID txn_id, Timestamp commit_ts);
  Status Abort(TxnID txn_id);
  Status Inquire(TxnID txn_id, ParticipantState::State* state);

  // Health check
  bool IsConnected() const;
  Status Ping();

 private:
  // Retry with backoff for Status-returning operations
  Status RetryWithBackoff(std::function<Status()> operation);
  
  // Retry with backoff for StatusOr<T>-returning operations
  template<typename T>
  StatusOr<T> RetryWithBackoff(std::function<StatusOr<T>()> operation) {
    StatusOr<T> last_result;
    
    for (size_t attempt = 0; attempt < config_.max_retries; ++attempt) {
      last_result = operation();
      
      if (last_result.ok()) {
        return last_result;
      }
      
      // Don't retry on certain errors
      if (last_result.status().IsInvalidArgument() || 
          last_result.status().IsNotFound()) {
        return last_result;
      }
      
      // Exponential backoff
      if (attempt < config_.max_retries - 1) {
        auto delay = config_.retry_base_delay * (1 << attempt);
        std::this_thread::sleep_for(delay);
      }
    }
    
    return last_result;
  }

  ClientConfig config_;
  std::atomic<bool> connected_{false};
  std::atomic<bool> shutdown_{false};
  
  // gRPC resources
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<cedar::storage::StorageService::Stub> stub_;
};

// =============================================================================
// StorageClientPool - Pool of StorageClient connections
// =============================================================================

class StorageClientPool {
 public:
  struct PoolConfig {
    size_t min_connections = 1;
    size_t max_connections = 10;
    std::chrono::milliseconds connection_timeout{5000};
    std::chrono::milliseconds idle_timeout{60000};
  };

  StorageClientPool();
  ~StorageClientPool();

  Status Initialize(const PoolConfig& config);
  void Shutdown();

  std::shared_ptr<StorageClient> GetClient(const std::string& address);
  void ReturnClient(const std::string& address, std::shared_ptr<StorageClient> client);
  void InvalidateClient(const std::string& address, std::shared_ptr<StorageClient> client);

  // Pool statistics
  size_t GetPoolSize(const std::string& address) const;
  size_t GetTotalConnections() const;

 private:
  void EvictIdleConnections();

  PoolConfig config_;
  mutable std::shared_mutex clients_mutex_;
  std::unordered_map<std::string, std::vector<std::shared_ptr<StorageClient>>> pools_;
  std::unordered_map<std::string, std::chrono::steady_clock::time_point> last_used_;
  std::atomic<bool> shutdown_{false};
  std::thread cleanup_thread_;
};

// =============================================================================
// MetaServiceClient - Client for connecting to MetaD via gRPC
// =============================================================================

class MetaServiceClient {
 public:
  struct ClientConfig {
    std::string metad_address;
    NodeID node_id;
    std::string listen_address;
    std::string data_root;
    size_t max_partitions = 1024;
    std::chrono::seconds heartbeat_interval{5};
    std::chrono::seconds registration_timeout{10};
  };

  MetaServiceClient();
  ~MetaServiceClient();

  // Disable copy
  MetaServiceClient(const MetaServiceClient&) = delete;
  MetaServiceClient& operator=(const MetaServiceClient&) = delete;

  Status Initialize(const ClientConfig& config);
  void Shutdown();

  // Registration
  Status RegisterNode();
  Status SendHeartbeat(const std::vector<PartitionID>& partitions,
                       double cpu_usage = 0.0,
                       double memory_usage = 0.0);

  // Queries
  StatusOr<cedar::meta::PartitionAssignment> GetPartitionAssignment(
      const std::string& space_name, PartitionID partition_id);
  StatusOr<std::vector<cedar::meta::NodeInfo>> GetAliveNodes();

  // Lifecycle
  void StartHeartbeatLoop(std::function<std::vector<PartitionID>()> partition_provider);
  void StopHeartbeatLoop();

  bool IsConnected() const;

 private:
  void HeartbeatLoop(std::function<std::vector<PartitionID>()> partition_provider);

  ClientConfig config_;
  std::atomic<bool> connected_{false};
  std::atomic<bool> shutdown_{false};
  
  // gRPC resources
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<cedar::meta::MetaService::Stub> stub_;
  
  std::thread heartbeat_thread_;
  mutable std::mutex mutex_;
};

// =============================================================================
// StorageServer - Main StorageD server with full gRPC support
// =============================================================================

class StorageServer {
 public:
  struct StorageServerConfig {
    NodeID node_id;
    std::string data_root = "/tmp/cedar_storage";
    std::string listen_address = "0.0.0.0:50051";
    std::string metad_address = "127.0.0.1:50050";
    size_t max_partitions = 1024;
    std::chrono::seconds heartbeat_interval{5};
  };

  StorageServer();
  ~StorageServer();

  // Lifecycle
  Status Initialize(const StorageServerConfig& config);
  void Serve();
  Status Shutdown();

  // Getters
  NodeID GetNodeId() const { return node_id_; }
  StoragePartitionManager* GetPartitionManager() { return &partition_manager_; }
  bool IsRunning() const { return running_.load(); }

 private:
  Status RegisterToMetaD();
  void HeartbeatLoop();

  NodeID node_id_;
  StorageServerConfig config_;
  StoragePartitionManager partition_manager_;
  
  std::unique_ptr<MetaServiceClient> meta_client_;
  std::thread heartbeat_thread_;
  
  // gRPC server
  std::unique_ptr<grpc::Server> grpc_server_;
  std::unique_ptr<StorageServiceImpl> service_impl_;
  
  std::atomic<bool> running_{false};
  std::mutex shutdown_mutex_;
  std::condition_variable shutdown_cv_;
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_STORAGE_SERVICE_IMPL_H_
