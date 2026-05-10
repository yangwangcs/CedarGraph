// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Query Storage Client Implementation

#include "cedar/queryd/query_storage_client.h"

#include <chrono>
#include <thread>

// 包含 dtx StorageClient 头文件
#include "cedar/dtx/storage_service_impl.h"

// 包含 Cypher parser 和 AST 头文件
#include "cedar/cypher/parser.h"
#include "cedar/cypher/ast.h"

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
  explicit NodeClientImpl(QueryStorageClient* client) : client_(client) {}

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
    (void)parameters;
    if (!result) {
      return Status::InvalidArgument("result pointer is null");
    }

    // Parse the query fragment to determine operation type
    cypher::CypherParser parser(query_fragment);
    auto stmt = parser.ParseStatement();
    if (!stmt) {
      return Status::InvalidArgument("Failed to parse sub-query: " + parser.GetError());
    }

    // Determine query type from AST
    bool is_match = false;
    bool has_return = false;
    std::string entity_alias;
    uint16_t edge_type = 0;
    cypher::Direction direction = cypher::Direction::OUTGOING;

    for (const auto& clause : stmt->clauses) {
      if (clause->clause_type == cypher::ClauseType::MATCH) {
        is_match = true;
        auto* match = static_cast<cypher::MatchClause*>(clause.get());
        if (!match->patterns.empty() && !match->patterns[0].elements.empty()) {
          // First element in the path pattern should be a NodePattern
          if (auto* node_pattern = std::get_if<cypher::NodePattern>(&match->patterns[0].elements[0])) {
            entity_alias = node_pattern->variable;
          }
        }
      } else if (clause->clause_type == cypher::ClauseType::RETURN) {
        has_return = true;
      }
    }

    if (!is_match || !has_return) {
      return Status::NotSupported("Only MATCH...RETURN sub-queries are supported");
    }

    // For now, implement a full partition scan (node_id = 0 means all nodes)
    // This is the minimal viable implementation for cross-partition queries.
    std::vector<std::pair<Timestamp, Descriptor>> versions;
    Status s = client_->ScanNode(0, Timestamp::Max(), &versions);
    if (!s.ok()) {
      return s;
    }

    // Convert scan results to cypher::ResultSet records
    for (const auto& [ts, desc] : versions) {
      (void)ts;
      cypher::Record record;
      if (!entity_alias.empty()) {
        cypher::Node node;
        node.id = desc.AsRaw();
        record.values[entity_alias] = cypher::Value(std::move(node));
      }
      result->records.push_back(std::move(record));
    }

    return Status::OK();
  }

 private:
  QueryStorageClient* client_;
};

std::shared_ptr<QueryStorageClient::NodeClient> QueryStorageClient::GetNodeClient(
    uint32_t partition_id) {
  (void)partition_id;
  return std::make_shared<NodeClientImpl>(this);
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

}  // namespace queryd
}  // namespace cedar
