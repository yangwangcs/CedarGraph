// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Query Storage Client Implementation

#include "cedar/queryd/query_storage_client.h"

#include <chrono>
#include <thread>

// 包含 dtx StorageClient 头文件
#include "cedar/dtx/storage_service_impl.h"

// 包含 StorageService proto 头文件 (for RemoteRPCNodeClient)
#include "storage_service.grpc.pb.h"

// 包含 Cypher parser 和 AST 头文件
#include "cedar/cypher/parser.h"
#include "cedar/cypher/ast.h"
#include "cedar/cypher/expression_evaluator.h"
#include "cedar/queryd/storage_execution_context.h"

namespace cedar {
namespace queryd {

using namespace std::chrono;

// ============================================================================
// Query Storage Client
// ============================================================================

QueryStorageClient::QueryStorageClient(const Options& options)
    : options_(options) {
}

QueryStorageClient::~QueryStorageClient() = default;

void QueryStorageClient::SetBaseClient(
    std::shared_ptr<cedar::dtx::StorageClient> base_client) {
  base_client_ = std::move(base_client);
  use_base_client_ = true;
}

Status QueryStorageClient::Init(const std::string& meta_service_address) {
  if (meta_service_address.empty()) {
    return Status::InvalidArgument("Meta service address is empty");
  }
  
  // Check if address has valid format (host:port)
  size_t colon_pos = meta_service_address.find(':');
  if (colon_pos == std::string::npos || colon_pos == 0) {
    return Status::InvalidArgument("Invalid meta service address format, expected host:port");
  }
  
  // Node discovery requires a MetaD gRPC client (MetaServiceGrpcClient).
  // For now, callers should use RegisterNode() to manually register partitions,
  // or integrate with MetaServiceGrpcClient at a higher layer.
  
  return Status::OK();
}

void QueryStorageClient::RegisterNode(uint32_t partition_id, 
                                 const std::string& node_address) {
  std::unique_lock<std::shared_mutex> lock(nodes_mutex_);
  partition_routing_[partition_id] = node_address;
}

Status QueryStorageClient::Get(const CedarKey& key,
                               Descriptor* descriptor,
                               bool* found) {
  if (use_base_client_ && base_client_) {
    // Use underlying dtx::StorageClient
    auto result = base_client_->Get(key, key.timestamp());
    if (!result.ok()) {
      if (result.status().IsNotFound()) {
        *found = false;
        return Status::OK();
      }
      return result.status();
    }
    
    *descriptor = std::move(result.value());
    *found = true;
    return Status::OK();
  }
  
  // Independent mode - not implemented
  return Status::NotSupported("Independent mode not implemented, use SetBaseClient");
}

Status QueryStorageClient::Put(const CedarKey& key, const Descriptor& descriptor) {
  if (use_base_client_ && base_client_) {
    // Use underlying dtx::StorageClient
    // Note: dtx::StorageClient requires txn_version and txn_id
    // For queries, we might not have transaction context
    (void)key;
    (void)descriptor;
    return Status::NotSupported("Put not supported in query mode");
  }
  
  return Status::NotSupported("Independent mode not implemented");
}

Status QueryStorageClient::Delete(const CedarKey& key) {
  (void)key;
  return Status::NotSupported("Delete not supported in query mode");
}

Status QueryStorageClient::BatchGet(const std::vector<CedarKey>& keys,
                  std::vector<Descriptor>* descriptors,
                  std::vector<bool>* found) {
  descriptors->clear();
  found->clear();
  
  for (const auto& key : keys) {
    Descriptor desc;
    bool f = false;
    Status s = Get(key, &desc, &f);
    if (!s.ok()) {
      return s;
    }
    
    descriptors->push_back(std::move(desc));
    found->push_back(f);
  }
  
  return Status::OK();
}

Status QueryStorageClient::ScanNode(
    uint64_t node_id,
    Timestamp as_of_time,
    std::vector<std::pair<Timestamp, Descriptor>>* versions) {
  if (use_base_client_ && base_client_) {
    return base_client_->ScanNodeV2(node_id, Timestamp::Min(), as_of_time, versions);
  }
  return Status::NotSupported("Independent mode not implemented, use SetBaseClient");
}

Status QueryStorageClient::ScanOutEdges(
    uint64_t node_id,
    uint16_t edge_type,
    Timestamp as_of_time,
    std::vector<EdgeScanEntry>* edges) {
  if (use_base_client_ && base_client_) {
    return base_client_->ScanEdgeV2(
        node_id, edge_type, cedar::storage::Direction::OUTGOING,
        Timestamp::Min(), as_of_time, edges);
  }
  return Status::NotSupported("Independent mode not implemented, use SetBaseClient");
}

Status QueryStorageClient::ScanInEdges(
    uint64_t node_id,
    uint16_t edge_type,
    Timestamp as_of_time,
    std::vector<EdgeScanEntry>* edges) {
  if (use_base_client_ && base_client_) {
    return base_client_->ScanEdgeV2(
        node_id, edge_type, cedar::storage::Direction::INCOMING,
        Timestamp::Min(), as_of_time, edges);
  }
  return Status::NotSupported("Independent mode not implemented, use SetBaseClient");
}

Status QueryStorageClient::GetAtTime(uint64_t entity_id,
                                    EntityType entity_type,
                                    Timestamp snapshot_ts,
                                    Descriptor* descriptor,
                                    bool* found) {
  // Build CedarKey for lookup
  CedarKey key;
  key.SetEntityId(entity_id);
  key.SetTimestamp(snapshot_ts);
  key.SetEntityType(static_cast<uint8_t>(entity_type));
  
  return Get(key, descriptor, found);
}

Status QueryStorageClient::ParallelBatchGet(
    const std::vector<std::pair<uint32_t, uint64_t>>& partition_entity_pairs,
    EntityType entity_type,
    Timestamp timestamp,
    std::vector<std::pair<bool, Descriptor>>* results) {
  
  results->clear();
  results->reserve(partition_entity_pairs.size());
  
  // Sequential implementation for now
  for (const auto& [partition_id, entity_id] : partition_entity_pairs) {
    (void)partition_id;
    
    CedarKey key;
    key.SetEntityId(entity_id);
    key.SetTimestamp(timestamp);
    key.SetEntityType(static_cast<uint8_t>(entity_type));
    
    Descriptor desc;
    bool found;
    Status s = Get(key, &desc, &found);
    if (!s.ok()) {
      return s;
    }
    results->emplace_back(found, std::move(desc));
  }
  
  return Status::OK();
}

std::unique_ptr<CedarScan> QueryStorageClient::CreateScan(Timestamp snapshot_ts) {
  // CedarScan needs LsmEngine - return nullptr for now
  (void)snapshot_ts;
  return nullptr;
}

// ============================================================================
// NodeClient implementation
// ============================================================================

class NodeClientImpl : public QueryStorageClient::NodeClient {
 public:
  explicit NodeClientImpl(QueryStorageClient* client, uint32_t partition_id)
      : client_(client), partition_id_(partition_id) {}

  Status ScanEntity(uint64_t entity_id,
                    EntityType entity_type,
                    Timestamp start_ts,
                    Timestamp end_ts,
                    std::vector<std::pair<Timestamp, Descriptor>>* results) override {
    (void)entity_type;
    (void)start_ts;
    // Bridge to ScanNode using end_ts as the as-of-time
    return client_->ScanNode(entity_id, end_ts, results);
  }

  Status ExecuteSubQuery(
      const std::string& query_fragment,
      const std::unordered_map<std::string, cypher::Value>& parameters,
      cypher::ResultSet* result) override {
    cypher::CypherParser parser(query_fragment);
    auto stmt = parser.ParseStatement();
    if (!stmt) {
      return Status::InvalidArgument("Parse failed: " + parser.GetError());
    }

    // Build execution plan
    auto plan = cypher::ExecutionPlanBuilder::Build(stmt);
    if (!plan) {
      return Status::InvalidArgument("Plan build failed");
    }

    // Create storage-backed execution context
    StorageBackedExecutionContext ctx(client_, partition_id_);
    ctx.variables.reserve(parameters.size());
    for (const auto& [name, val] : parameters) {
      ctx.SetVariable(name, val);
    }

    // Initialize and execute
    if (!plan->Init(&ctx)) {
      return Status::IOError("Plan initialization failed");
    }

    result->columns.clear();
    result->records.clear();
    result->error = std::nullopt;

    while (auto record = plan->Next()) {
      result->records.push_back(*record);
    }

    // Extract column names from RETURN clause
    for (const auto& clause : stmt->clauses) {
      if (clause->clause_type == cypher::ClauseType::RETURN) {
        auto* ret = static_cast<cypher::ReturnClause*>(clause.get());
        for (const auto& item : ret->items) {
          result->columns.push_back(item.alias.value_or("column"));
        }
        break;
      }
    }

    return Status::OK();
  }

 private:
  QueryStorageClient* client_;
  uint32_t partition_id_;
};

// Helper: convert proto QueryValue to cypher::Value
static cypher::Value ProtoValueToCypherValue(const cedar::storage::QueryValue& pv) {
  if (pv.has_bool_val()) return cypher::Value(pv.bool_val());
  if (pv.has_int_val()) return cypher::Value(pv.int_val());
  if (pv.has_float_val()) return cypher::Value(pv.float_val());
  if (pv.has_string_val()) return cypher::Value(pv.string_val());
  return cypher::Value();
}

// Helper: convert cypher::Value to proto QueryValue
static void CypherValueToProtoValue(const cypher::Value& cv, cedar::storage::QueryValue* pv) {
  switch (cv.Type()) {
    case cypher::ValueType::kBool:
      pv->set_bool_val(cv.GetBool());
      break;
    case cypher::ValueType::kInt:
      pv->set_int_val(cv.GetInt());
      break;
    case cypher::ValueType::kFloat:
      pv->set_float_val(cv.GetFloat());
      break;
    case cypher::ValueType::kString:
      pv->set_string_val(cv.GetString());
      break;
    default:
      // Null / others -> leave as default (null)
      break;
  }
}

class RemoteRPCNodeClient : public QueryStorageClient::NodeClient {
 public:
  RemoteRPCNodeClient(std::shared_ptr<grpc::Channel> channel,
                      uint32_t partition_id)
      : stub_(cedar::storage::StorageService::NewStub(channel)),
        partition_id_(partition_id) {}

  Status ScanEntity(uint64_t entity_id,
                    EntityType entity_type,
                    Timestamp start_ts,
                    Timestamp end_ts,
                    std::vector<std::pair<Timestamp, Descriptor>>* results) override {
    (void)entity_id;
    (void)entity_type;
    (void)start_ts;
    (void)end_ts;
    (void)results;
    return Status::NotSupported(
        "RemoteRPCNodeClient::ScanEntity is not supported. Use ExecuteSubQuery "
        "with a MATCH/RETURN fragment instead.");
  }

  Status ExecuteSubQuery(
      const std::string& query_fragment,
      const std::unordered_map<std::string, cypher::Value>& parameters,
      cypher::ResultSet* result) override {
    cedar::storage::ExecuteSubQueryRequest req;
    req.set_query_fragment(query_fragment);
    req.set_partition_id(partition_id_);
    req.set_accept_streaming(true);

    auto* params = req.mutable_parameters();
    for (const auto& [name, val] : parameters) {
      CypherValueToProtoValue(val, &(*params)[name]);
    }

    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
    auto reader = stub_->ExecuteSubQuery(&ctx, req);

    cedar::storage::SubQueryResultBatch batch;
    result->records.clear();
    result->columns.clear();
    result->error = std::nullopt;

    while (reader->Read(&batch)) {
      if (result->columns.empty() && batch.columns_size() > 0) {
        for (const auto& col : batch.columns()) {
          result->columns.push_back(col);
        }
      }
      for (const auto& row : batch.records()) {
        cypher::Record record;
        for (int i = 0; i < row.values_size() && i < static_cast<int>(result->columns.size()); ++i) {
          record.Set(result->columns[i], ProtoValueToCypherValue(row.values(i)));
        }
        result->records.push_back(std::move(record));
      }
      if (batch.is_last()) break;
    }

    grpc::Status status = reader->Finish();
    if (!status.ok()) {
      return Status::IOError("ExecuteSubQuery RPC failed: " + status.error_message());
    }
    return Status::OK();
  }

 private:
  std::unique_ptr<cedar::storage::StorageService::Stub> stub_;
  uint32_t partition_id_;
};

std::shared_ptr<QueryStorageClient::NodeClient> QueryStorageClient::GetNodeClient(
    uint32_t partition_id) {
  if (IsLocalPartition(partition_id)) {
    return std::make_shared<NodeClientImpl>(this, partition_id);
  }
  auto channel = GetOrCreateChannel(partition_id);
  if (!channel) {
    // Fallback to local execution if channel unavailable
    return std::make_shared<NodeClientImpl>(this, partition_id);
  }
  return std::make_shared<RemoteRPCNodeClient>(channel, partition_id);
}

Status QueryStorageClient::HealthCheck() {
  if (!use_base_client_ || !base_client_) {
    return Status::IOError("Storage client not initialized");
  }
  
  if (base_client_->IsConnected()) {
    return Status::OK();
  }
  return Status::IOError("Base client not connected");
}

QueryStorageClient::Stats QueryStorageClient::GetStats() const {
  std::lock_guard<std::mutex> lock(stats_mutex_);
  return stats_;
}

bool QueryStorageClient::CheckCircuitBreaker(const std::string& node_address) {
  std::lock_guard<std::mutex> lock(cb_mutex_);
  
  auto it = circuit_breakers_.find(node_address);
  if (it == circuit_breakers_.end()) {
    return false;  // New node, circuit is closed by default
  }
  
  auto& cb = it->second;
  if (cb.open.load()) {
    // Check if recovery timeout has passed
    auto elapsed = duration_cast<seconds>(
        steady_clock::now() - cb.last_failure);
    if (elapsed > options_.recovery_timeout) {
      cb.open = false;
      cb.failures = 0;
      return false;
    }
    return true;  // Circuit is open
  }
  return false;
}

void QueryStorageClient::RecordSuccess(const std::string& node_address) {
  std::lock_guard<std::mutex> lock(cb_mutex_);
  
  auto& cb = circuit_breakers_[node_address];
  cb.failures = 0;
  cb.open = false;
}

void QueryStorageClient::RecordFailure(const std::string& node_address) {
  std::lock_guard<std::mutex> lock(cb_mutex_);
  
  auto& cb = circuit_breakers_[node_address];
  cb.failures++;
  cb.last_failure = steady_clock::now();
  
  if (cb.failures >= options_.failure_threshold) {
    cb.open = true;
    {
      std::lock_guard<std::mutex> stats_lock(stats_mutex_);
      stats_.circuit_breaker_opens++;
    }
  }
}

// ============================================================================
// Query Cache
// ============================================================================

QueryCache::QueryCache(const Options& options) : options_(options) {}

bool QueryCache::Get(const CedarKey& key, Descriptor* descriptor) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto now = steady_clock::now();
  // Use entity_id + timestamp as cache key to avoid collisions
  CacheKey cache_key{key.entity_id(), static_cast<uint64_t>(key.timestamp())};
  auto it = cache_.find(cache_key);
  
  if (it != cache_.end()) {
    if (it->second.expires_at > now) {
      *descriptor = it->second.descriptor;
      return true;
    }
    // Expired
    cache_.erase(it);
  }
  
  return false;
}

void QueryCache::Put(const CedarKey& key, const Descriptor& descriptor) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  EvictIfNeeded();
  
  CacheEntry entry;
  entry.descriptor = descriptor;
  entry.expires_at = steady_clock::now() + options_.ttl;
  
  cache_[CacheKey{key.entity_id(), static_cast<uint64_t>(key.timestamp())}] = std::move(entry);
}

void QueryCache::Invalidate(const CedarKey& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  cache_.erase(CacheKey{key.entity_id(), static_cast<uint64_t>(key.timestamp())});
}

void QueryCache::Clear() {
  std::lock_guard<std::mutex> lock(mutex_);
  cache_.clear();
}

void QueryCache::EvictIfNeeded() {
  if (cache_.size() < options_.max_entries) {
    return;
  }
  
  // Simple eviction: remove 10% of entries
  size_t to_evict = cache_.size() / 10;
  auto it = cache_.begin();
  for (size_t i = 0; i < to_evict && it != cache_.end(); ++i) {
    it = cache_.erase(it);
  }
}

void QueryStorageClient::MarkPartitionLocal(uint32_t partition_id) {
  std::unique_lock<std::shared_mutex> lock(local_partitions_mutex_);
  local_partition_ids_.insert(partition_id);
}

bool QueryStorageClient::IsLocalPartition(uint32_t partition_id) const {
  std::shared_lock<std::shared_mutex> lock(local_partitions_mutex_);
  return local_partition_ids_.find(partition_id) != local_partition_ids_.end();
}

std::shared_ptr<grpc::Channel> QueryStorageClient::GetOrCreateChannel(
    uint32_t partition_id) {
  {
    std::shared_lock<std::shared_mutex> lock(channels_mutex_);
    auto it = partition_channels_.find(partition_id);
    if (it != partition_channels_.end()) {
      return it->second;
    }
  }
  std::string address;
  {
    std::shared_lock<std::shared_mutex> lock(nodes_mutex_);
    auto it = partition_routing_.find(partition_id);
    if (it == partition_routing_.end()) {
      return nullptr;
    }
    address = it->second;
  }
  auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
  {
    std::unique_lock<std::shared_mutex> lock(channels_mutex_);
    partition_channels_[partition_id] = channel;
  }
  return channel;
}

}  // namespace queryd
}  // namespace cedar
