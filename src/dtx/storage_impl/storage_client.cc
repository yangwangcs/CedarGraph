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

#include "cedar/dtx/storage_service_impl.h"

#include <chrono>
#include <cstring>
#include <random>
#include <thread>

// Governance layer integration
#include "cedar/governance/service_registry.h"

namespace cedar {
namespace dtx {

// =============================================================================
// StorageClient Implementation
// =============================================================================

StorageClient::StorageClient()
    : connected_(false),
      shutdown_(false) {
  // Initialize with governance layer configuration if available
  // Service discovery will be used when InitializeWithDiscovery is called
}

StorageClient::~StorageClient() {
  Shutdown();
}

Status StorageClient::Initialize(const ClientConfig& config) {
  if (connected_.load()) {
    return Status::InvalidArgument("Client already initialized");
  }
  
  config_ = config;
  
  // Initialize gRPC channel and stub
  auto creds = cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentials(config_.tls);
  if (!creds.ok()) creds = cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentialsFromEnv();
  if (!creds.ok()) {
    return Status::IOError("Failed to create client TLS credentials: " + creds.status().ToString());
  }
  channel_ = grpc::CreateChannel(config_.server_address, creds.ValueOrDie());
  stub_ = cedar::storage::StorageService::NewStub(channel_);
  
  // Wait for channel to be ready
  auto deadline = std::chrono::system_clock::now() + config_.operation_timeout;
  if (!channel_->WaitForConnected(deadline)) {
    return Status::IOError("Failed to connect to storage server: " + 
                           config_.server_address);
  }
  
  connected_ = true;
  return Status::OK();
}

Status StorageClient::InitializeWithDiscovery(const std::string& service_name,
                                               const governance::ServiceRegistry& registry) {
  if (connected_.load()) {
    return Status::InvalidArgument("Client already initialized");
  }
  
  // Use ServiceRegistry to discover available storage services
  auto services_result = registry.Discover(service_name);
  if (!services_result.ok()) {
    return Status::IOError("Failed to discover services: " + 
                           services_result.status().ToString());
  }
  
  auto services = services_result.ValueOrDie();
  if (services.empty()) {
    return Status::NotFound("No storage services available for: " + service_name);
  }
  
  // Try to connect to healthy services in round-robin fashion
  for (const auto& service : services) {
    if (service.status != cedar::governance::ServiceStatus::kHealthy) {
      continue;
    }
    
    std::string endpoint = service.host + ":" + std::to_string(service.port);
    
    ClientConfig config;
    config.server_address = endpoint;
    // Other config values use defaults or can be loaded from ConfigManager
    
    Status s = Initialize(config);
    if (s.ok()) {
      return Status::OK();
    }
  }
  
  return Status::IOError("Failed to connect to any storage service for: " + service_name);
}

void StorageClient::Shutdown() {
  if (shutdown_.load()) {
    return;
  }
  
  shutdown_ = true;
  connected_ = false;
  
  // Clean up gRPC resources
  stub_.reset();
  channel_.reset();
}

Status StorageClient::Put(const CedarKey& key, const Descriptor& descriptor,
                         Timestamp txn_version, TxnID txn_id) {
  if (!connected_.load()) {
    return Status::IOError("Client not connected");
  }
  
  return RetryWithBackoff([&]() {
    cedar::storage::PutRequest request;
    cedar::storage::PutResponse response;
    grpc::ClientContext context;

    // Set timeout
    context.set_deadline(std::chrono::system_clock::now() + config_.operation_timeout);

    // Populate key
    auto* pb_key = request.mutable_key();
    pb_key->set_entity_id(key.entity_id());
    pb_key->set_timestamp(key.timestamp().value());
    pb_key->set_target_id(key.target_id());
    pb_key->set_column_id(key.column_id());
    pb_key->set_sequence(key.sequence());
    pb_key->set_type_flags(key.flags());
    pb_key->set_partition_id(key.part_id());

    // Populate descriptor - store raw 8 bytes
    uint64_t raw_value = descriptor.AsRaw();
    request.mutable_descriptor_()->set_data(
        reinterpret_cast<const char*>(&raw_value), sizeof(raw_value));

    // Populate transaction info
    request.mutable_txn_version()->set_value(static_cast<uint64_t>(txn_version));
    request.set_txn_id(txn_id);

    // Make gRPC call
    grpc::Status status = stub_->Put(&context, request, &response);

    if (!status.ok()) {
      return Status::IOError("gRPC Put failed: " + status.error_message());
    }

    if (!response.success()) {
      return Status::IOError("Storage error: " + response.error_msg());
    }

    return Status::OK();
  }, false);
}

StatusOr<Descriptor> StorageClient::Get(const CedarKey& key, Timestamp read_time) {
  if (!connected_.load()) {
    return Status::IOError("Client not connected");
  }
  
  return RetryWithBackoff<Descriptor>([&]() -> StatusOr<Descriptor> {
    cedar::storage::GetRequest request;
    cedar::storage::GetResponse response;
    grpc::ClientContext context;
    
    // Set timeout
    context.set_deadline(std::chrono::system_clock::now() + config_.operation_timeout);
    
    // Populate request
    auto* pb_key = request.mutable_key();
    pb_key->set_entity_id(key.entity_id());
    pb_key->set_timestamp(key.timestamp().value());
    pb_key->set_target_id(key.target_id());
    pb_key->set_column_id(key.column_id());
    pb_key->set_sequence(key.sequence());
    pb_key->set_type_flags(key.flags());
    pb_key->set_partition_id(key.part_id());
    
    // Make gRPC call
    grpc::Status status = stub_->Get(&context, request, &response);
    
    if (!status.ok()) {
      return Status::IOError("gRPC Get failed: " + status.error_message());
    }
    
    if (!response.success()) {
      return Status::IOError("Storage error: " + response.error_msg());
    }
    
    if (!response.found()) {
      return Status::NotFound("Key not found");
    }
    
    // Convert response descriptor - read raw 8 bytes
    const std::string& data = response.descriptor_().data();
    if (data.size() >= sizeof(uint64_t)) {
      uint64_t raw_value;
      std::memcpy(&raw_value, data.data(), sizeof(uint64_t));
      return Descriptor(raw_value);
    }
    
    return Descriptor();  // Return empty descriptor if no data
  });
}

Status StorageClient::Delete(const CedarKey& key, Timestamp txn_version, TxnID txn_id) {
  if (!connected_.load()) {
    return Status::IOError("Client not connected");
  }

  return RetryWithBackoff([&]() {
    cedar::storage::DeleteRequest request;
    cedar::storage::DeleteResponse response;
    grpc::ClientContext context;

    context.set_deadline(std::chrono::system_clock::now() + config_.operation_timeout);

    auto* pb_key = request.mutable_key();
    pb_key->set_entity_id(key.entity_id());
    pb_key->set_timestamp(key.timestamp().value());
    pb_key->set_target_id(key.target_id());
    pb_key->set_column_id(key.column_id());
    pb_key->set_sequence(key.sequence());
    pb_key->set_type_flags(key.flags());
    pb_key->set_partition_id(key.part_id());

    request.mutable_txn_version()->set_value(static_cast<uint64_t>(txn_version));
    request.set_txn_id(txn_id);

    grpc::Status status = stub_->Delete(&context, request, &response);

    if (!status.ok()) {
      return Status::IOError("gRPC Delete failed: " + status.error_message());
    }

    if (!response.success()) {
      return Status::IOError("Storage error: " + response.error_msg());
    }

    return Status::OK();
  }, false);
}

Status StorageClient::BatchPut(
    const std::vector<std::pair<CedarKey, Descriptor>>& items,
    Timestamp txn_version) {
  
  if (!connected_.load() || shutdown_.load()) {
    return Status::InvalidArgument("StorageClient::BatchPut", "Not connected");
  }
  
  if (items.empty()) {
    return Status::OK();
  }
  
  // Simplified implementation: send one by one
  for (const auto& [key, desc] : items) {
    Status s = Put(key, desc, txn_version, TxnID(0));
    if (!s.ok()) {
      return s;
    }
  }
  
  return Status::OK();
}

Status StorageClient::ScanNodeV2(uint64_t node_id, Timestamp start_time, Timestamp end_time,
                                 std::vector<std::pair<Timestamp, Descriptor>>* results) {
  if (!connected_.load()) {
    return Status::IOError("Client not connected");
  }

  return RetryWithBackoff([&]() {
    cedar::storage::ScanNodeRequestV2 request;
    request.set_node_id(node_id);
    request.set_start_time(start_time.value());
    request.set_end_time(end_time.value());

    cedar::storage::ScanResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + config_.operation_timeout);

    auto grpc_status = stub_->ScanNodeV2(&context, request, &response);
    if (!grpc_status.ok()) {
      return Status::IOError("Storage RPC failed: " + grpc_status.error_message());
    }
    if (!response.success()) {
      return Status::IOError(response.error_msg());
    }

    results->clear();
    for (const auto& item : response.items()) {
      const std::string& data = item.descriptor_().data();
      Descriptor desc;
      if (data.size() >= sizeof(uint64_t)) {
        uint64_t raw_value;
        std::memcpy(&raw_value, data.data(), sizeof(uint64_t));
        desc = Descriptor(raw_value);
      }
      results->emplace_back(Timestamp(item.timestamp()), std::move(desc));
    }
    return Status::OK();
  });
}

Status StorageClient::ScanEdgeV2(uint64_t node_id, uint16_t edge_type,
                                 cedar::storage::Direction direction,
                                 Timestamp start_time, Timestamp end_time,
                                 std::vector<EdgeScanEntry>* edges) {
  if (!connected_.load()) {
    return Status::IOError("Client not connected");
  }

  return RetryWithBackoff([&]() {
    cedar::storage::ScanEdgeRequestV2 request;
    request.set_node_id(node_id);
    request.set_edge_type(edge_type);
    request.set_direction(direction);
    request.set_start_time(start_time.value());
    request.set_end_time(end_time.value());

    cedar::storage::ScanResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + config_.operation_timeout);

    auto grpc_status = stub_->ScanEdgeV2(&context, request, &response);
    if (!grpc_status.ok()) {
      return Status::IOError("Storage RPC failed: " + grpc_status.error_message());
    }
    if (!response.success()) {
      return Status::IOError(response.error_msg());
    }

    edges->clear();
    for (const auto& item : response.items()) {
      EdgeScanEntry entry;
      entry.timestamp = Timestamp(item.timestamp());
      const std::string& data = item.descriptor_().data();
      if (data.size() >= sizeof(uint64_t)) {
        uint64_t raw_value;
        std::memcpy(&raw_value, data.data(), sizeof(uint64_t));
        entry.descriptor = Descriptor(raw_value);
      }
      edges->push_back(std::move(entry));
    }
    return Status::OK();
  });
}

StatusOr<bool> StorageClient::Prepare(TxnID txn_id, const std::vector<CedarKey>& reads,
                                      const std::vector<CedarKey>& writes, Timestamp commit_ts) {
  if (!connected_.load()) {
    return Status::IOError("Client not connected");
  }
  
  cedar::storage::PrepareRequest request;
  request.set_txn_id(txn_id);
  request.set_commit_ts(commit_ts.value());
  
  // Convert reads to proto
  for (const auto& key : reads) {
    auto* proto_key = request.add_read_set();
    proto_key->set_entity_id(key.entity_id());
    proto_key->set_timestamp(key.timestamp().value());
    proto_key->set_target_id(key.target_id());
    proto_key->set_column_id(key.column_id());
    proto_key->set_sequence(key.sequence());
    proto_key->set_type_flags(key.flags());
    proto_key->set_partition_id(key.part_id());
  }
  
  // Convert writes to proto
  for (const auto& key : writes) {
    auto* proto_key = request.add_write_set();
    proto_key->set_entity_id(key.entity_id());
    proto_key->set_timestamp(key.timestamp().value());
    proto_key->set_target_id(key.target_id());
    proto_key->set_column_id(key.column_id());
    proto_key->set_sequence(key.sequence());
    proto_key->set_type_flags(key.flags());
    proto_key->set_partition_id(key.part_id());
  }
  
  cedar::storage::PrepareResponse response;
  grpc::ClientContext context;
  
  // Set timeout
  auto deadline = std::chrono::system_clock::now() + config_.operation_timeout;
  context.set_deadline(deadline);
  
  grpc::Status status = stub_->Prepare(&context, request, &response);
  
  if (!status.ok()) {
    if (status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) {
      return Status::IOError("StorageClient::Prepare", "Operation timeout");
    }
    return Status::IOError("StorageClient::Prepare", status.error_message());
  }
  
  if (!response.prepared()) {
    return false;  // Participant voted to abort
  }
  
  return true;
}

Status StorageClient::Commit(TxnID txn_id, Timestamp commit_ts) {
  if (!connected_.load()) {
    return Status::IOError("Client not connected");
  }
  
  cedar::storage::CommitRequest request;
  request.set_txn_id(txn_id);
  request.set_commit_ts(commit_ts.value());
  
  cedar::storage::CommitResponse response;
  grpc::ClientContext context;
  
  auto deadline = std::chrono::system_clock::now() + config_.operation_timeout;
  context.set_deadline(deadline);
  
  grpc::Status status = stub_->Commit(&context, request, &response);
  
  if (!status.ok()) {
    if (status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) {
      return Status::IOError("StorageClient::Commit", "Operation timeout");
    }
    return Status::IOError("StorageClient::Commit", status.error_message());
  }
  
  if (!response.success()) {
    return Status::IOError("StorageClient::Commit", response.error_msg());
  }
  
  return Status::OK();
}

Status StorageClient::Abort(TxnID txn_id) {
  if (!connected_.load()) {
    return Status::IOError("Client not connected");
  }
  
  cedar::storage::AbortRequest request;
  request.set_txn_id(txn_id);
  
  cedar::storage::AbortResponse response;
  grpc::ClientContext context;
  
  auto deadline = std::chrono::system_clock::now() + config_.operation_timeout;
  context.set_deadline(deadline);
  
  grpc::Status status = stub_->Abort(&context, request, &response);
  
  if (!status.ok()) {
    if (status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) {
      return Status::IOError("StorageClient::Abort", "Operation timeout");
    }
    return Status::IOError("StorageClient::Abort", status.error_message());
  }
  
  if (!response.success()) {
    return Status::IOError("StorageClient::Abort", response.error_msg());
  }
  
  return Status::OK();
}

Status StorageClient::Inquire(TxnID txn_id, ParticipantState::State* state) {
  if (!connected_.load()) {
    return Status::IOError("Client not connected");
  }
  
  cedar::storage::InquireRequest request;
  request.set_txn_id(txn_id);
  
  cedar::storage::InquireResponse response;
  grpc::ClientContext context;
  
  auto deadline = std::chrono::system_clock::now() + config_.operation_timeout;
  context.set_deadline(deadline);
  
  grpc::Status status = stub_->Inquire(&context, request, &response);
  
  if (!status.ok()) {
    if (status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) {
      return Status::IOError("StorageClient::Inquire", "Operation timeout");
    }
    return Status::IOError("StorageClient::Inquire", status.error_message());
  }
  
  switch (response.state()) {
    case cedar::storage::InquireResponse::PREPARED:
      *state = ParticipantState::State::kPrepared;
      break;
    case cedar::storage::InquireResponse::COMMITTED:
      *state = ParticipantState::State::kCommitted;
      break;
    case cedar::storage::InquireResponse::ABORTED:
      *state = ParticipantState::State::kAborted;
      break;
    default:
      *state = ParticipantState::State::kUnknown;
      break;
  }
  
  return Status::OK();
}

bool StorageClient::IsConnected() const {
  return connected_.load() && !shutdown_.load();
}

Status StorageClient::Ping() {
  if (!connected_.load()) {
    return Status::IOError("Client not connected");
  }
  
  // Simple health check by checking channel state
  auto state = channel_->GetState(false);
  if (state == GRPC_CHANNEL_READY || state == GRPC_CHANNEL_IDLE) {
    return Status::OK();
  }
  
  // Try to reconnect
  auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(1);
  if (channel_->WaitForConnected(deadline)) {
    return Status::OK();
  }
  
  return Status::IOError("Health check failed: channel not ready");
}

// 带抖动（jitter）的指数退避
static std::chrono::milliseconds CalculateBackoffDelay(
    size_t attempt,
    std::chrono::milliseconds base_delay,
    std::chrono::milliseconds max_delay) {
  // 指数退避: base * 2^attempt
  auto delay = base_delay * (1 << std::min(attempt, size_t(10)));
  
  // 上限
  if (delay > max_delay) {
    delay = max_delay;
  }
  
  // 添加抖动: ±25% 随机变化，避免惊群效应
  static thread_local std::mt19937 rng(std::random_device{}());
  std::uniform_real_distribution<double> dist(0.75, 1.25);
  delay = std::chrono::milliseconds(static_cast<int64_t>(delay.count() * dist(rng)));
  
  return delay;
}

Status StorageClient::RetryWithBackoff(std::function<Status()> operation,
                                           bool is_idempotent) {
  Status last_status;

  for (size_t attempt = 0; attempt < config_.max_retries; ++attempt) {
    last_status = operation();

    if (last_status.ok()) {
      return Status::OK();
    }

    // 检查是否应该重试
    bool should_retry = true;

    // 业务逻辑错误不重试
    if (last_status.IsInvalidArgument() ||
        last_status.IsNotFound()) {
      should_retry = false;
    }

    // 非幂等操作在 DEADLINE_EXCEEDED 时不重试（服务器可能已经处理）
    if (!is_idempotent) {
      std::string error_msg = last_status.ToString();
      if (error_msg.find("DEADLINE_EXCEEDED") != std::string::npos) {
        should_retry = false;
      }
    }

    // 检查错误消息中的 gRPC 错误码
    std::string error_msg = last_status.ToString();
    if (error_msg.find("UNAVAILABLE") != std::string::npos ||
        error_msg.find("RESOURCE_EXHAUSTED") != std::string::npos) {
      should_retry = true;
    }

    if (!should_retry) {
      return last_status;
    }

    // 指数退避 + 抖动
    if (attempt < config_.max_retries - 1) {
      auto delay = CalculateBackoffDelay(
          attempt, config_.retry_base_delay, config_.operation_timeout / 2);

      // 检查是否正在关闭
      if (shutdown_.load()) {
        return Status::IOError("Operation aborted: client shutting down");
      }

      std::this_thread::sleep_for(delay);
    }
  }

  return last_status;
}

// =============================================================================
// StorageClientPool Implementation
// =============================================================================

StorageClientPool::StorageClientPool()
    : shutdown_(false) {}

StorageClientPool::~StorageClientPool() {
  Shutdown();
}

Status StorageClientPool::Initialize(const PoolConfig& config) {
  config_ = config;
  
  // Start cleanup thread
  cleanup_thread_ = std::thread([this]() {
    try {
      while (!shutdown_.load()) {
        std::this_thread::sleep_for(config_.idle_timeout / 2);
        EvictIdleConnections();
      }
    } catch (const std::exception& e) {
      std::cerr << "StorageClientPool cleanup thread exception: " << e.what() << std::endl;
    }
  });
  
  return Status::OK();
}

void StorageClientPool::Shutdown() {
  if (shutdown_.exchange(true)) {
    return;
  }
  
  // Wait for cleanup thread
  if (cleanup_thread_.joinable()) {
    cleanup_thread_.join();
  }
  
  // Close all connections
  std::unique_lock<std::shared_mutex> lock(clients_mutex_);
  for (auto& [address, pool] : pools_) {
    for (auto& client : pool) {
      client->Shutdown();
    }
  }
  pools_.clear();
  last_used_.clear();
}

std::shared_ptr<StorageClient> StorageClientPool::GetClient(const std::string& address) {
  std::unique_lock<std::shared_mutex> lock(clients_mutex_);

  auto& pool = pools_[address];

  // Try to find an existing healthy client
  for (auto& client : pool) {
    if (client->IsConnected()) {
      last_used_[address] = std::chrono::steady_clock::now();
      return client;
    }
  }

  // Create new client if under limit
  if (pool.size() < config_.max_connections) {
    StorageClient::ClientConfig client_config;
    client_config.server_address = address;

    auto client = std::shared_ptr<StorageClient>(new StorageClient());
    Status s = client->Initialize(client_config);
    if (s.ok()) {
      pool.push_back(client);
      last_used_[address] = std::chrono::steady_clock::now();
      return client;
    }
    std::cerr << "[StorageClientPool] Failed to initialize client for " << address
              << ": " << s.ToString() << std::endl;
  } else {
    std::cerr << "[StorageClientPool] Connection limit reached for " << address
              << " (max=" << config_.max_connections << ")" << std::endl;
  }

  return nullptr;
}

void StorageClientPool::ReturnClient(const std::string& address, 
                                     std::shared_ptr<StorageClient> client) {
  // Client automatically returned to pool (shared_ptr)
  std::unique_lock<std::shared_mutex> lock(clients_mutex_);
  last_used_[address] = std::chrono::steady_clock::now();
}

void StorageClientPool::InvalidateClient(const std::string& address,
                                         std::shared_ptr<StorageClient> client) {
  std::unique_lock<std::shared_mutex> lock(clients_mutex_);
  
  auto it = pools_.find(address);
  if (it != pools_.end()) {
    auto& pool = it->second;
    auto client_it = std::find(pool.begin(), pool.end(), client);
    if (client_it != pool.end()) {
      (*client_it)->Shutdown();
      pool.erase(client_it);
    }
  }
}

size_t StorageClientPool::GetPoolSize(const std::string& address) const {
  std::shared_lock<std::shared_mutex> lock(clients_mutex_);
  
  auto it = pools_.find(address);
  if (it != pools_.end()) {
    return it->second.size();
  }
  return 0;
}

size_t StorageClientPool::GetTotalConnections() const {
  std::shared_lock<std::shared_mutex> lock(clients_mutex_);
  
  size_t total = 0;
  for (const auto& [address, pool] : pools_) {
    total += pool.size();
  }
  return total;
}

void StorageClientPool::EvictIdleConnections() {
  std::unique_lock<std::shared_mutex> lock(clients_mutex_);
  
  auto now = std::chrono::steady_clock::now();
  
  for (auto& [address, pool] : pools_) {
    auto last_it = last_used_.find(address);
    if (last_it != last_used_.end()) {
      auto idle_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
          now - last_it->second);
      
      // Remove idle connections beyond minimum
      if (idle_duration > config_.idle_timeout && pool.size() > config_.min_connections) {
        size_t to_remove = pool.size() - config_.min_connections;
        for (size_t i = 0; i < to_remove && !pool.empty(); ++i) {
          pool.back()->Shutdown();
          pool.pop_back();
        }
      }
    }
  }
}

}  // namespace dtx
}  // namespace cedar
