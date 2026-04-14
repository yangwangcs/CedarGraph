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
// CedarGraph Storage Server (storaged) - With gRPC Service
// =============================================================================

#include <grpcpp/grpcpp.h>
#include <iostream>
#include <fstream>
#include <thread>
#include <csignal>
#include <atomic>
#include <chrono>
#include <memory>
#include <unordered_map>
#include <shared_mutex>

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/cedar_options.h"
#include "cedar/core/status.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include "cedar/dtx/storage_service_impl.h"

// gRPC generated headers
#include "storage_service.pb.h"
#include "storage_service.grpc.pb.h"

namespace cedar {
namespace dtx {
namespace storage {

// Global shutdown flag
std::atomic<bool> g_running{true};
std::unique_ptr<grpc::Server> g_grpc_server;

void SignalHandler(int signal) {
  if (signal == SIGINT || signal == SIGTERM) {
    std::cout << "\nShutting down storaged..." << std::endl;
    g_running = false;
    if (g_grpc_server) {
      g_grpc_server->Shutdown();
    }
  }
}

// =============================================================================
// Helper Functions
// =============================================================================

// Serialize cedar::Descriptor to bytes
std::string SerializeDescriptor(const cedar::Descriptor& desc) {
  std::string data;
  if (desc.GetKind() == cedar::EntryKind::InlineInt) {
    auto val = desc.AsInlineInt();
    if (val.has_value()) {
      int32_t value = val.value();
      data.assign(reinterpret_cast<const char*>(&value), sizeof(value));
    }
  } else if (desc.GetKind() == cedar::EntryKind::InlineShortStr) {
    data = desc.AsInlineShortStr();
  }
  return data;
}

// Deserialize bytes to cedar::Descriptor with column_id
cedar::Descriptor DeserializeDescriptor(const std::string& data, uint16_t column_id) {
  if (data.size() >= sizeof(int32_t)) {
    int32_t value = *reinterpret_cast<const int32_t*>(data.data());
    return cedar::Descriptor(cedar::EntryKind::InlineInt, column_id, value, sizeof(int32_t));
  }
  return cedar::Descriptor();
}

// Convert proto CedarKey to internal CedarKey
cedar::CedarKey ConvertProtoKey(const cedar::storage::CedarKey& proto_key) {
  // Use CedarKey factory method with proper parameters
  uint64_t entity_id = proto_key.entity_id();
  uint64_t timestamp = proto_key.timestamp();
  uint64_t target_id = proto_key.target_id();
  uint16_t column_id = static_cast<uint16_t>(proto_key.column_id());
  uint16_t sequence = static_cast<uint16_t>(proto_key.sequence());
  uint32_t type_flags = proto_key.type_flags();
  uint16_t partition_id = static_cast<uint16_t>(proto_key.partition_id());
  
  // Extract entity type from type_flags (high bits)
  cedar::EntityType entity_type = static_cast<cedar::EntityType>((type_flags >> 16) & 0xFF);
  uint8_t flags = static_cast<uint8_t>(type_flags & 0xFF);
  
  cedar::CedarKey key;
  key.SetEntityId(entity_id);
  key.SetTimestamp(cedar::Timestamp(timestamp));
  key.SetTargetId(target_id);
  key.SetColumnId(column_id);
  key.SetSequence(sequence);
  key.SetFlags(flags);
  key.SetPartId(partition_id);
  key.SetEntityType(static_cast<uint8_t>(entity_type));
  
  return key;
}

// =============================================================================
// gRPC Service Implementation
// =============================================================================

class StorageServiceImpl final : public cedar::storage::StorageService::Service {
 public:
  StorageServiceImpl(StoragePartitionManager* partition_manager) 
      : partition_manager_(partition_manager) {}

  // Put operation
  grpc::Status Put(grpc::ServerContext* context,
                   const cedar::storage::PutRequest* request,
                   cedar::storage::PutResponse* response) override {
    (void)context;
    
    // Extract partition_id from key
    cedar::CedarKey key = ConvertProtoKey(request->key());
    PartitionID pid = key.part_id();
    
    // Get partition storage
    PartitionStorage* partition = partition_manager_->GetPartition(pid);
    if (!partition) {
      // Auto-create partition if not exists
      Status s = partition_manager_->AddPartition(pid);
      if (!s.ok()) {
        response->set_success(false);
        response->set_error_msg("Failed to create partition: " + s.ToString());
        return grpc::Status::OK;
      }
      partition = partition_manager_->GetPartition(pid);
    }
    
    // Create descriptor from proto data with correct column_id
    cedar::Descriptor desc;
    if (request->has_descriptor_() && request->descriptor_().data().size() >= sizeof(int32_t)) {
      desc = DeserializeDescriptor(request->descriptor_().data(), key.column_id());
    }
    
    cedar::Timestamp txn_version(request->txn_version().value());
    TxnID txn_id = request->txn_id();
    
    // Execute put using PartitionStorage interface
    Status status = partition->Put(key, desc, txn_version, txn_id);
    
    response->set_success(status.ok());
    if (!status.ok()) {
      response->set_error_msg(status.ToString());
    }
    
    return grpc::Status::OK;
  }

  // Get operation
  grpc::Status Get(grpc::ServerContext* context,
                   const cedar::storage::GetRequest* request,
                   cedar::storage::GetResponse* response) override {
    (void)context;
    
    cedar::CedarKey key = ConvertProtoKey(request->key());
    PartitionID pid = key.part_id();
    
    PartitionStorage* partition = partition_manager_->GetPartition(pid);
    if (!partition) {
      response->set_success(false);
      response->set_found(false);
      response->set_error_msg("Partition not found");
      return grpc::Status::OK;
    }
    
    cedar::Timestamp read_time(key.timestamp());
    
    // Execute get
    auto result = partition->Get(key, read_time);
    
    if (result.ok()) {
      response->set_success(true);
      response->set_found(true);
      auto* proto_desc = response->mutable_descriptor_();
      proto_desc->set_data(SerializeDescriptor(result.value()));
    } else {
      response->set_success(true);
      response->set_found(false);
    }
    
    return grpc::Status::OK;
  }

  // Delete operation
  grpc::Status Delete(grpc::ServerContext* context,
                      const cedar::storage::DeleteRequest* request,
                      cedar::storage::DeleteResponse* response) override {
    (void)context;
    
    cedar::CedarKey key = ConvertProtoKey(request->key());
    PartitionID pid = key.part_id();
    
    PartitionStorage* partition = partition_manager_->GetPartition(pid);
    if (!partition) {
      response->set_success(false);
      response->set_error_msg("Partition not found");
      return grpc::Status::OK;
    }
    
    // Create tombstone descriptor
    cedar::Descriptor tombstone = cedar::Descriptor::Tombstone(key.column_id());
    cedar::Timestamp txn_version(request->txn_version().value());
    TxnID txn_id = request->txn_id();
    
    Status status = partition->Put(key, tombstone, txn_version, txn_id);
    
    response->set_success(status.ok());
    if (!status.ok()) {
      response->set_error_msg(status.ToString());
    }
    
    return grpc::Status::OK;
  }

  // Scan operation
  grpc::Status Scan(grpc::ServerContext* context,
                    const cedar::storage::ScanRequest* request,
                    cedar::storage::ScanResponse* response) override {
    (void)context;
    (void)request;
    
    // Note: Full scan not implemented in this version
    // Would need to iterate through all partitions
    
    response->set_success(true);
    return grpc::Status::OK;
  }

  // BatchPut operation
  grpc::Status BatchPut(grpc::ServerContext* context,
                        const cedar::storage::BatchPutRequest* request,
                        cedar::storage::BatchPutResponse* response) override {
    (void)context;
    
    bool all_success = true;
    
    for (const auto& item : request->items()) {
      cedar::CedarKey key = ConvertProtoKey(item.key());
      PartitionID pid = key.part_id();
      
      PartitionStorage* partition = partition_manager_->GetPartition(pid);
      if (!partition) {
        Status s = partition_manager_->AddPartition(pid);
        if (!s.ok()) {
          response->add_item_success(false);
          all_success = false;
          continue;
        }
        partition = partition_manager_->GetPartition(pid);
      }
      
      cedar::Descriptor desc;
      if (item.has_descriptor_() && item.descriptor_().data().size() >= sizeof(int32_t)) {
        desc = DeserializeDescriptor(item.descriptor_().data(), key.column_id());
      }
      
      cedar::Timestamp txn_version(request->txn_version().value());
      TxnID txn_id = request->txn_id();
      Status status = partition->Put(key, desc, txn_version, txn_id);
      
      response->add_item_success(status.ok());
      if (!status.ok()) {
        all_success = false;
      }
    }
    
    response->set_success(all_success);
    return grpc::Status::OK;
  }

  // BatchGet operation
  grpc::Status BatchGet(grpc::ServerContext* context,
                        const cedar::storage::BatchGetRequest* request,
                        cedar::storage::BatchGetResponse* response) override {
    (void)context;
    
    for (const auto& proto_key : request->keys()) {
      cedar::CedarKey key = ConvertProtoKey(proto_key);
      PartitionID pid = key.part_id();
      
      PartitionStorage* partition = partition_manager_->GetPartition(pid);
      if (!partition) {
        response->add_found(false);
        response->add_descriptors();
        continue;
      }
      
      cedar::Timestamp read_time(key.timestamp());
      auto result = partition->Get(key, read_time);
      
      if (result.ok()) {
        response->add_found(true);
        auto* proto_desc = response->add_descriptors();
        proto_desc->set_data(SerializeDescriptor(result.value()));
      } else {
        response->add_found(false);
        response->add_descriptors();
      }
    }
    
    response->set_success(true);
    return grpc::Status::OK;
  }

  // 2PC Prepare
  grpc::Status Prepare(grpc::ServerContext* context,
                       const cedar::storage::PrepareRequest* request,
                       cedar::storage::PrepareResponse* response) override {
    (void)context;
    
    TxnID txn_id = request->txn_id();
    Timestamp commit_ts(request->commit_ts());
    
    // Collect read and write sets
    std::vector<CedarKey> read_set;
    std::vector<CedarKey> write_set;
    
    for (const auto& proto_key : request->read_set()) {
      read_set.push_back(ConvertProtoKey(proto_key));
    }
    
    for (const auto& proto_key : request->write_set()) {
      write_set.push_back(ConvertProtoKey(proto_key));
    }
    
    // Determine involved partitions from write set (and read set if needed)
    std::set<PartitionID> involved_partitions;
    for (const auto& key : write_set) {
      involved_partitions.insert(key.part_id());
    }
    for (const auto& key : read_set) {
      involved_partitions.insert(key.part_id());
    }
    
    // If no partitions involved, return success (read-only transaction)
    if (involved_partitions.empty()) {
      response->set_prepared(true);
      response->set_prepared_ts(static_cast<uint64_t>(commit_ts));
      return grpc::Status::OK;
    }
    
    // Prepare on each involved partition
    bool all_prepared = true;
    std::string error_msg;
    
    for (PartitionID pid : involved_partitions) {
      PartitionStorage* partition = partition_manager_->GetPartition(pid);
      if (!partition) {
        // Auto-create partition
        Status s = partition_manager_->AddPartition(pid);
        if (!s.ok()) {
          all_prepared = false;
          error_msg = "Failed to create partition " + std::to_string(pid);
          break;
        }
        partition = partition_manager_->GetPartition(pid);
      }
      
      // Filter keys for this partition
      std::vector<CedarKey> partition_reads;
      std::vector<CedarKey> partition_writes;
      
      for (const auto& key : read_set) {
        if (key.part_id() == pid) {
          partition_reads.push_back(key);
        }
      }
      for (const auto& key : write_set) {
        if (key.part_id() == pid) {
          partition_writes.push_back(key);
        }
      }
      
      // Call Prepare on partition
      Status status = partition->Prepare(txn_id, partition_reads, partition_writes, commit_ts);
      if (!status.ok()) {
        all_prepared = false;
        error_msg = status.ToString();
        break;
      }
    }
    
    if (all_prepared) {
      response->set_prepared(true);
      response->set_prepared_ts(static_cast<uint64_t>(commit_ts));
    } else {
      response->set_prepared(false);
      response->set_error_msg(error_msg);
      // Abort prepared partitions
      for (PartitionID pid : involved_partitions) {
        PartitionStorage* partition = partition_manager_->GetPartition(pid);
        if (partition) {
          partition->Abort(txn_id);
        }
      }
    }
    
    return grpc::Status::OK;
  }

  // 2PC Commit
  grpc::Status Commit(grpc::ServerContext* context,
                      const cedar::storage::CommitRequest* request,
                      cedar::storage::CommitResponse* response) override {
    (void)context;
    
    TxnID txn_id = request->txn_id();
    Timestamp commit_ts(request->commit_ts());
    
    // Get all partitions that have this transaction prepared
    std::vector<PartitionStorage*> prepared_partitions;
    {
      std::shared_lock<std::shared_mutex> lock(txn_mutex_);
      auto it = txn_partitions_.find(txn_id);
      if (it != txn_partitions_.end()) {
        for (PartitionID pid : it->second) {
          PartitionStorage* partition = partition_manager_->GetPartition(pid);
          if (partition) {
            prepared_partitions.push_back(partition);
          }
        }
      }
    }
    
    // If no tracked partitions, try all partitions
    if (prepared_partitions.empty()) {
      // This is a fallback - in production, we'd track this better
      for (size_t pid = 0; pid < 1024; ++pid) {
        PartitionStorage* partition = partition_manager_->GetPartition(static_cast<PartitionID>(pid));
        if (partition) {
          auto prepared_txns = partition->GetPreparedTransactions();
          if (std::find(prepared_txns.begin(), prepared_txns.end(), txn_id) != prepared_txns.end()) {
            prepared_partitions.push_back(partition);
          }
        }
      }
    }
    
    // Commit on all prepared partitions
    bool all_committed = true;
    std::string error_msg;
    
    for (PartitionStorage* partition : prepared_partitions) {
      Status status = partition->Commit(txn_id, commit_ts);
      if (!status.ok()) {
        all_committed = false;
        error_msg = status.ToString();
        // Continue trying to commit others
      }
    }
    
    // Clean up tracking
    {
      std::unique_lock<std::shared_mutex> lock(txn_mutex_);
      txn_partitions_.erase(txn_id);
    }
    
    response->set_success(all_committed);
    if (!all_committed) {
      response->set_error_msg(error_msg);
    }
    
    return grpc::Status::OK;
  }

  // 2PC Abort
  grpc::Status Abort(grpc::ServerContext* context,
                     const cedar::storage::AbortRequest* request,
                     cedar::storage::AbortResponse* response) override {
    (void)context;
    
    TxnID txn_id = request->txn_id();
    
    // Get all partitions that have this transaction prepared
    std::vector<PartitionStorage*> prepared_partitions;
    {
      std::shared_lock<std::shared_mutex> lock(txn_mutex_);
      auto it = txn_partitions_.find(txn_id);
      if (it != txn_partitions_.end()) {
        for (PartitionID pid : it->second) {
          PartitionStorage* partition = partition_manager_->GetPartition(pid);
          if (partition) {
            prepared_partitions.push_back(partition);
          }
        }
      }
    }
    
    // If no tracked partitions, try all partitions
    if (prepared_partitions.empty()) {
      for (size_t pid = 0; pid < 1024; ++pid) {
        PartitionStorage* partition = partition_manager_->GetPartition(static_cast<PartitionID>(pid));
        if (partition) {
          auto prepared_txns = partition->GetPreparedTransactions();
          if (std::find(prepared_txns.begin(), prepared_txns.end(), txn_id) != prepared_txns.end()) {
            prepared_partitions.push_back(partition);
          }
        }
      }
    }
    
    // Abort on all prepared partitions
    bool all_aborted = true;
    std::string error_msg;
    
    for (PartitionStorage* partition : prepared_partitions) {
      Status status = partition->Abort(txn_id);
      if (!status.ok()) {
        all_aborted = false;
        error_msg = status.ToString();
        // Continue trying to abort others
      }
    }
    
    // Clean up tracking
    {
      std::unique_lock<std::shared_mutex> lock(txn_mutex_);
      txn_partitions_.erase(txn_id);
    }
    
    response->set_success(all_aborted);
    if (!all_aborted) {
      response->set_error_msg(error_msg);
    }
    
    return grpc::Status::OK;
  }

  // GetPartitionInfo
  grpc::Status GetPartitionInfo(grpc::ServerContext* context,
                                const cedar::storage::GetPartitionInfoRequest* request,
                                cedar::storage::GetPartitionInfoResponse* response) override {
    (void)context;
    
    PartitionID pid = request->partition_id();
    PartitionStorage* partition = partition_manager_->GetPartition(pid);
    
    if (!partition) {
      response->set_success(false);
      response->set_error_msg("Partition not found");
      return grpc::Status::OK;
    }
    
    auto stats = partition->GetStats();
    
    auto* info = response->mutable_info();
    info->set_partition_id(pid);
    info->set_data_size(stats.disk_usage_bytes);
    info->set_key_count(stats.num_keys);
    info->set_qps(0);
    info->set_is_leader(true);
    info->set_raft_role("LEADER");
    
    response->set_success(true);
    return grpc::Status::OK;
  }

  // Heartbeat (streaming)
  grpc::Status Heartbeat(grpc::ServerContext* context,
                         grpc::ServerReaderWriter<cedar::storage::HeartbeatResponse,
                                                  cedar::storage::HeartbeatRequest>* stream) override {
    (void)context;
    
    cedar::storage::HeartbeatRequest request;
    while (stream->Read(&request)) {
      cedar::storage::HeartbeatResponse response;
      response.set_success(true);
      response.set_should_transfer_leaders(false);
      stream->Write(response);
    }
    
    return grpc::Status::OK;
  }

 private:
  StoragePartitionManager* partition_manager_;
  
  // Track which partitions are involved in each transaction
  mutable std::shared_mutex txn_mutex_;
  std::unordered_map<TxnID, std::set<PartitionID>> txn_partitions_;
};

// =============================================================================
// Configuration
// =============================================================================

struct StorageConfig {
  uint32_t node_id = 0;
  std::string bind_address = "0.0.0.0:7000";
  std::string data_dir = "/tmp/cedar/storage";
  std::vector<std::pair<uint32_t, std::string>> metad_endpoints;
  uint32_t grpc_threads = 4;
};

StorageConfig ParseConfigFile(const std::string& path) {
  StorageConfig config;
  
  std::ifstream file(path);
  if (!file.is_open()) {
    std::cerr << "Failed to open config file: " << path << std::endl;
    return config;
  }
  
  std::string line;
  while (std::getline(file, line)) {
    if (line.empty() || line[0] == '#') continue;
    
    auto pos = line.find('=');
    if (pos == std::string::npos) continue;
    
    std::string key = line.substr(0, pos);
    std::string value = line.substr(pos + 1);
    
    auto trim = [](std::string& s) {
      s.erase(0, s.find_first_not_of(" \t"));
      s.erase(s.find_last_not_of(" \t") + 1);
    };
    trim(key);
    trim(value);
    
    if (key == "node_id") {
      config.node_id = std::stoul(value);
    } else if (key == "bind_address") {
      config.bind_address = value;
    } else if (key == "data_dir") {
      config.data_dir = value;
    } else if (key == "metad_endpoints") {
      size_t start = 0;
      while (start < value.size()) {
        size_t end = value.find(',', start);
        if (end == std::string::npos) end = value.size();
        
        std::string endpoint = value.substr(start, end - start);
        size_t colon = endpoint.find(':');
        
        if (colon != std::string::npos) {
          uint32_t id = std::stoul(endpoint.substr(0, colon));
          std::string addr = endpoint.substr(colon + 1);
          config.metad_endpoints.push_back({id, addr});
        }
        
        start = end + 1;
      }
    } else if (key == "grpc_threads") {
      config.grpc_threads = std::stoul(value);
    }
  }
  
  return config;
}

}  // namespace storage
}  // namespace dtx
}  // namespace cedar

// =============================================================================
// Main Entry Point
// =============================================================================

int main(int argc, char* argv[]) {
  using namespace cedar::dtx::storage;
  
  std::string config_path;
  
  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];
    if (arg == "--config" || arg == "-c") {
      if (i + 1 < argc) {
        config_path = argv[++i];
      }
    } else if (arg == "--help" || arg == "-h") {
      std::cout << "CedarGraph Storage Server (storaged) - With gRPC\n"
                << "Usage: " << argv[0] << " --config <config_file>\n"
                << "\n"
                << "Options:\n"
                << "  -c, --config <path>  Configuration file path\n"
                << "  -h, --help           Show this help\n";
      return 0;
    }
  }
  
  if (config_path.empty()) {
    std::cerr << "Error: No config file specified. Use --config <path>" << std::endl;
    return 1;
  }
  
  std::signal(SIGINT, SignalHandler);
  std::signal(SIGTERM, SignalHandler);
  
  StorageConfig config = ParseConfigFile(config_path);
  
  std::cout << "╔════════════════════════════════════════════════════════╗" << std::endl;
  std::cout << "║     CedarGraph Storage Server (storaged)               ║" << std::endl;
  std::cout << "║     With gRPC Service + 2PC Support                    ║" << std::endl;
  std::cout << "╚════════════════════════════════════════════════════════╝" << std::endl;
  std::cout << std::endl;
  std::cout << "Configuration:" << std::endl;
  std::cout << "  Node ID:    " << config.node_id << std::endl;
  std::cout << "  Bind:       " << config.bind_address << std::endl;
  std::cout << "  Data Dir:   " << config.data_dir << std::endl;
  std::cout << "  MetaD:      " << config.metad_endpoints.size() << " endpoints" << std::endl;
  std::cout << std::endl;
  
  // Initialize StoragePartitionManager
  std::unique_ptr<cedar::dtx::StoragePartitionManager> partition_manager = 
      std::make_unique<cedar::dtx::StoragePartitionManager>();
  
  cedar::dtx::StoragePartitionManager::PartitionConfig pm_config;
  pm_config.data_root = config.data_dir;
  pm_config.max_partitions = 1024;
  
  cedar::Status status = partition_manager->Initialize(pm_config);
  if (!status.ok()) {
    std::cerr << "Failed to initialize partition manager: " << status.ToString() << std::endl;
    return 1;
  }
  
  // Create default partition (0)
  status = partition_manager->AddPartition(0);
  if (!status.ok()) {
    std::cerr << "Failed to create default partition: " << status.ToString() << std::endl;
    return 1;
  }
  
  std::cout << "[1/3] Partition manager initialized" << std::endl;
  
  // Create and start gRPC server
  StorageServiceImpl grpc_service(partition_manager.get());
  
  grpc::ServerBuilder builder;
  builder.AddListeningPort(config.bind_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&grpc_service);
  
  // Set thread pool size
  builder.SetSyncServerOption(grpc::ServerBuilder::NUM_CQS, config.grpc_threads);
  
  g_grpc_server = builder.BuildAndStart();
  if (!g_grpc_server) {
    std::cerr << "Failed to start gRPC server on " << config.bind_address << std::endl;
    return 1;
  }
  
  std::cout << "[2/3] gRPC server listening on " << config.bind_address << std::endl;
  std::cout << "[3/3] Running..." << std::endl;
  std::cout << std::endl;
  
  // Monitor and print stats, with periodic flush
  std::thread stats_thread([&partition_manager]() {
    int ticks = 0;
    while (g_running) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      ticks++;
      
      auto* storage = partition_manager->GetSharedStorage();
      if (!storage) continue;
      
      // Periodic flush every 5 seconds
      if (ticks % 5 == 0) {
        storage->ForceFlush();
      }
      
      if (ticks % 10 == 0) {
        auto stats = storage->GetStats();
        std::cout << "[Stats] partitions=" << partition_manager->GetPartitionCount()
                  << " memtable=" << stats.memtable_size / 1000 
                  << "KB sst=" << stats.sst_count
                  << " data=" << (stats.sst_size / 1024 / 1024) << "MB"
                  << std::endl;
      }
    }
  });
  
  // Wait for gRPC server to finish
  g_grpc_server->Wait();
  
  // Shutdown
  std::cout << std::endl << "Shutting down..." << std::endl;
  
  g_running = false;
  stats_thread.join();
  
  partition_manager->Shutdown();
  
  std::cout << "Storage server shutdown complete." << std::endl;
  return 0;
}
