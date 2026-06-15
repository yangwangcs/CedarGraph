// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Query Storage Client Implementation

#include "cedar/queryd/query_storage_client.h"

#include <chrono>
#include <future>
#include <iostream>
#include <thread>
#include <unordered_set>

// 包含 dtx StorageClient 头文件
#include "cedar/dtx/storage_service_impl.h"
#include "cedar/dtx/raft/grpc_tls.h"

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
    // Use underlying dtx::StorageClient with an auto-generated txn_version
    // (current wall-clock time) and txn_id = 0 for single-key writes in
    // query path. Full transactional writes should go through the 2PC engine.
    uint64_t now_us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    return base_client_->Put(key, descriptor, Timestamp(now_us), 0);
  }
  
  return Status::NotSupported("Independent mode not implemented");
}

Status QueryStorageClient::Delete(const CedarKey& key) {
  if (use_base_client_ && base_client_) {
    uint64_t now_us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    return base_client_->Delete(key, Timestamp(now_us), 0);
  }

  return Status::NotSupported("Independent mode not implemented");
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
  
  // Independent mode: route via gRPC to StorageD
  uint32_t partition_id = 0;
  if (partition_count_ > 0) {
    partition_id = static_cast<uint32_t>(node_id % partition_count_);
  }
  
  auto channel = GetOrCreateChannel(partition_id);
  if (!channel) {
    // Fallback: try any registered partition
    std::shared_lock<std::shared_mutex> lock(nodes_mutex_);
    if (!partition_routing_.empty()) {
      channel = GetOrCreateChannel(partition_routing_.begin()->first);
    }
    if (!channel) {
      return Status::NotFound("No storage node registered for partition " + std::to_string(partition_id));
    }
  }
  
  auto stub = cedar::storage::StorageService::NewStub(channel);
  cedar::storage::ScanNodeRequestV2 req;
  req.set_node_id(node_id);
  req.set_start_time(0);
  req.set_end_time(as_of_time.value());
  req.set_partition_id(partition_id);
  
  cedar::storage::ScanResponse resp;
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
  auto status = stub->ScanNodeV2(&ctx, req, &resp);
  
  if (!status.ok()) {
    return Status::IOError("ScanNodeV2 RPC failed: " + status.error_message());
  }
  if (!resp.success()) {
    return Status::IOError("ScanNodeV2 failed: " + resp.error_msg());
  }
  
  versions->clear();
  for (const auto& item : resp.items()) {
    Descriptor desc;
    const std::string& data = item.descriptor_().data();
    if (data.size() >= sizeof(uint64_t)) {
      uint64_t raw;
      std::memcpy(&raw, data.data(), sizeof(uint64_t));
      desc = Descriptor(raw);
    }
    versions->emplace_back(Timestamp(item.timestamp()), std::move(desc));
  }
  return Status::OK();
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
  
  // Independent mode: route via gRPC to StorageD
  uint32_t partition_id = 0;
  if (partition_count_ > 0) {
    partition_id = static_cast<uint32_t>(node_id % partition_count_);
  }
  
  auto channel = GetOrCreateChannel(partition_id);
  if (!channel) {
    std::shared_lock<std::shared_mutex> lock(nodes_mutex_);
    if (!partition_routing_.empty()) {
      channel = GetOrCreateChannel(partition_routing_.begin()->first);
    }
    if (!channel) {
      return Status::NotFound("No storage node registered for partition " + std::to_string(partition_id));
    }
  }
  
  auto stub = cedar::storage::StorageService::NewStub(channel);
  cedar::storage::ScanEdgeRequestV2 req;
  req.set_node_id(node_id);
  req.set_edge_type(edge_type);
  req.set_direction(cedar::storage::Direction::OUTGOING);
  req.set_start_time(0);
  req.set_end_time(as_of_time.value());
  req.set_partition_id(partition_id);
  
  cedar::storage::ScanResponse resp;
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
  auto status = stub->ScanEdgeV2(&ctx, req, &resp);
  
  if (!status.ok()) {
    return Status::IOError("ScanEdgeV2 RPC failed: " + status.error_message());
  }
  if (!resp.success()) {
    return Status::IOError("ScanEdgeV2 failed: " + resp.error_msg());
  }
  
  edges->clear();
  for (const auto& item : resp.items()) {
    EdgeScanEntry entry;
    entry.timestamp = Timestamp(item.timestamp());
    const std::string& data = item.descriptor_().data();
    if (data.size() >= sizeof(uint64_t)) {
      uint64_t raw;
      std::memcpy(&raw, data.data(), sizeof(uint64_t));
      entry.descriptor = Descriptor(raw);
    }
    edges->push_back(std::move(entry));
  }
  return Status::OK();
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
  
  // Independent mode: route via gRPC to StorageD
  uint32_t partition_id = 0;
  if (partition_count_ > 0) {
    partition_id = static_cast<uint32_t>(node_id % partition_count_);
  }
  
  auto channel = GetOrCreateChannel(partition_id);
  if (!channel) {
    std::shared_lock<std::shared_mutex> lock(nodes_mutex_);
    if (!partition_routing_.empty()) {
      channel = GetOrCreateChannel(partition_routing_.begin()->first);
    }
    if (!channel) {
      return Status::NotFound("No storage node registered for partition " + std::to_string(partition_id));
    }
  }
  
  auto stub = cedar::storage::StorageService::NewStub(channel);
  cedar::storage::ScanEdgeRequestV2 req;
  req.set_node_id(node_id);
  req.set_edge_type(edge_type);
  req.set_direction(cedar::storage::Direction::INCOMING);
  req.set_start_time(0);
  req.set_end_time(as_of_time.value());
  req.set_partition_id(partition_id);
  
  cedar::storage::ScanResponse resp;
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
  auto status = stub->ScanEdgeV2(&ctx, req, &resp);
  
  if (!status.ok()) {
    return Status::IOError("ScanEdgeV2 RPC failed: " + status.error_message());
  }
  if (!resp.success()) {
    return Status::IOError("ScanEdgeV2 failed: " + resp.error_msg());
  }
  
  edges->clear();
  for (const auto& item : resp.items()) {
    EdgeScanEntry entry;
    entry.timestamp = Timestamp(item.timestamp());
    const std::string& data = item.descriptor_().data();
    if (data.size() >= sizeof(uint64_t)) {
      uint64_t raw;
      std::memcpy(&raw, data.data(), sizeof(uint64_t));
      entry.descriptor = Descriptor(raw);
    }
    edges->push_back(std::move(entry));
  }
  return Status::OK();
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
  
  if (partition_entity_pairs.empty()) {
    return Status::OK();
  }
  
  // Limit concurrency to avoid thread explosion on large batches.
  size_t max_concurrency = std::thread::hardware_concurrency();
  if (max_concurrency == 0) max_concurrency = 4;
  
  // Process in chunks to bound parallelism.
  size_t index = 0;
  while (index < partition_entity_pairs.size()) {
    size_t chunk_size = std::min(max_concurrency, partition_entity_pairs.size() - index);
    std::vector<std::future<std::tuple<bool, Descriptor, Status>>> futures;
    futures.reserve(chunk_size);
    
    for (size_t i = 0; i < chunk_size; ++i) {
      uint64_t entity_id = partition_entity_pairs[index + i].second;
      futures.push_back(std::async(std::launch::async,
          [this, entity_id, entity_type, timestamp]() {
            CedarKey key;
            key.SetEntityId(entity_id);
            key.SetTimestamp(timestamp);
            key.SetEntityType(static_cast<uint8_t>(entity_type));
            Descriptor desc;
            bool found;
            Status s = Get(key, &desc, &found);
            return std::make_tuple(found, std::move(desc), s);
          }));
    }
    
    for (auto& f : futures) {
      auto [found, desc, s] = f.get();
      if (!s.ok()) {
        return s;
      }
      results->emplace_back(found, std::move(desc));
    }
    
    index += chunk_size;
  }
  
  return Status::OK();
}

std::unique_ptr<CedarScan> QueryStorageClient::CreateScan(Timestamp snapshot_ts) {
  // CedarScan needs LsmEngine - return nullptr for now
  (void)snapshot_ts;
  return nullptr;
}

Status QueryStorageClient::ScanLabel(const std::string& space_name,
                                      const std::string& label,
                                      uint64_t min_id,
                                      uint64_t max_id,
                                      uint64_t limit,
                                      std::vector<uint64_t>* entity_ids) {
  if (use_base_client_ && base_client_) {
    return Status::NotSupported(
        "ScanLabel not available via base_client_, use independent mode");
  }

  entity_ids->clear();
  std::unordered_set<uint64_t> seen;

  // Collect all partition IDs to query
  std::vector<uint32_t> partition_ids;
  {
    std::shared_lock<std::shared_mutex> lock(nodes_mutex_);
    if (partition_routing_.empty()) {
      return Status::NotFound("No storage node registered for ScanLabel");
    }
    for (const auto& [pid, _] : partition_routing_) {
      partition_ids.push_back(pid);
    }
  }

  for (uint32_t pid : partition_ids) {
    auto channel = GetOrCreateChannel(pid);
    if (!channel) continue;

    auto stub = cedar::storage::StorageService::NewStub(channel);
    cedar::storage::ScanLabelRequest request;
    request.set_space_name(space_name);
    request.set_label(label);
    request.set_min_id(min_id);
    request.set_max_id(max_id);
    request.set_limit(limit);

    cedar::storage::ScanLabelResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));

    grpc::Status status = stub->ScanLabel(&context, request, &response);
    if (!status.ok() || !response.success()) {
      continue;
    }

    for (uint64_t id : response.entity_ids()) {
      if (seen.insert(id).second) {
        entity_ids->push_back(id);
      }
    }

    if (entity_ids->size() >= limit) {
      break;
    }
  }

  return entity_ids->empty() ? Status::NotFound("No entities found for label") : Status::OK();
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

    // Extract label from query for label-aware scanning
    std::string label;
    size_t match_pos = query_fragment.find("MATCH");
    if (match_pos != std::string::npos) {
      size_t colon_pos = query_fragment.find(":", match_pos);
      size_t close_pos = query_fragment.find(")", colon_pos);
      if (colon_pos != std::string::npos && close_pos != std::string::npos) {
        label = query_fragment.substr(colon_pos + 1, close_pos - colon_pos - 1);
        label.erase(0, label.find_first_not_of(" \t\n"));
        label.erase(label.find_last_not_of(" \t\n") + 1);
      }
    }

    // Create storage-backed execution context with extracted label
    StorageBackedExecutionContext ctx(client_, partition_id_, "default", label);
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

// ============================================================================
// Circuit breaker helpers
// ============================================================================

class UnavailableNodeClient : public QueryStorageClient::NodeClient {
 public:
  explicit UnavailableNodeClient(const std::string& node_address)
      : node_address_(node_address) {}

  Status ScanEntity(uint64_t /*entity_id*/,
                    EntityType /*entity_type*/,
                    Timestamp /*start_ts*/,
                    Timestamp /*end_ts*/,
                    std::vector<std::pair<Timestamp, Descriptor>>* /*results*/) override {
    return Status::Unavailable("Circuit breaker open for node " + node_address_);
  }

  Status ExecuteSubQuery(
      const std::string& /*query_fragment*/,
      const std::unordered_map<std::string, cypher::Value>& /*parameters*/,
      cypher::ResultSet* /*result*/) override {
    return Status::Unavailable("Circuit breaker open for node " + node_address_);
  }

 private:
  std::string node_address_;
};

class CircuitBreakerTrackingNodeClient : public QueryStorageClient::NodeClient {
 public:
  CircuitBreakerTrackingNodeClient(
      QueryStorageClient* client,
      const std::string& node_address,
      std::shared_ptr<NodeClient> inner)
      : client_(client), node_address_(node_address), inner_(std::move(inner)) {}

  Status ScanEntity(uint64_t entity_id,
                    EntityType entity_type,
                    Timestamp start_ts,
                    Timestamp end_ts,
                    std::vector<std::pair<Timestamp, Descriptor>>* results) override {
    return inner_->ScanEntity(entity_id, entity_type, start_ts, end_ts, results);
  }

  Status ExecuteSubQuery(
      const std::string& query_fragment,
      const std::unordered_map<std::string, cypher::Value>& parameters,
      cypher::ResultSet* result) override {
    Status s = inner_->ExecuteSubQuery(query_fragment, parameters, result);
    if (client_) {
      client_->ReportNodeResult(node_address_, s.ok());
    }
    return s;
  }

 private:
  QueryStorageClient* client_;
  std::string node_address_;
  std::shared_ptr<NodeClient> inner_;
};

std::shared_ptr<QueryStorageClient::NodeClient> QueryStorageClient::GetNodeClient(
    uint32_t partition_id) {
  if (IsLocalPartition(partition_id)) {
    return std::make_shared<NodeClientImpl>(this, partition_id);
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
  if (CheckCircuitBreaker(address)) {
    return std::make_shared<UnavailableNodeClient>(address);
  }
  auto channel = GetOrCreateChannel(partition_id);
  if (!channel) {
    // Fallback to local execution if channel unavailable
    return std::make_shared<NodeClientImpl>(this, partition_id);
  }
  auto inner = std::make_shared<RemoteRPCNodeClient>(channel, partition_id);
  return std::make_shared<CircuitBreakerTrackingNodeClient>(
      this, address, std::move(inner));
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

std::string QueryStorageClient::GetNodeAddress(uint32_t partition_id) const {
  std::shared_lock<std::shared_mutex> lock(nodes_mutex_);
  auto it = partition_routing_.find(partition_id);
  if (it != partition_routing_.end()) {
    return it->second;
  }
  return "";
}

void QueryStorageClient::ReportNodeResult(const std::string& node_address, bool success) {
  if (success) {
    RecordSuccess(node_address);
  } else {
    RecordFailure(node_address);
  }
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
  auto creds = cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentialsFromEnv();
  if (!creds.ok()) {
    std::cerr << "[QueryStorageClient] TLS credential error: "
              << creds.status().ToString() << std::endl;
    return nullptr;
  }
  auto channel = grpc::CreateChannel(address, creds.ValueOrDie());
  {
    std::unique_lock<std::shared_mutex> lock(channels_mutex_);
    partition_channels_[partition_id] = channel;
  }
  return channel;
}

}  // namespace queryd
}  // namespace cedar
