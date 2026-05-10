// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Query Service Implementation - Full Version

#include "cedar/queryd/query_service.h"

#include <chrono>
#include <iomanip>
#include <random>
#include <sstream>

#include <grpc/grpc.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include "cedar/queryd/distributed_executor.h"
#include "cedar/queryd/query_storage_client.h"
#include "cedar/queryd/meta_client.h"
#include "cedar/cypher/parser.h"

// 包含生成的 protobuf 代码
#include "query_service.pb.h"
#include "query_service.grpc.pb.h"

namespace cedar {
namespace queryd {

using namespace std::chrono;
using grpc::ServerContext;
using grpc::Status;
using grpc::ServerWriter;

// ============================================================================
// QueryServiceImpl - gRPC Service Implementation
// ============================================================================

class QueryServiceImpl::Impl {
 public:
  Impl(std::shared_ptr<QueryStorageClient> storage_client,
       std::shared_ptr<QueryMetaClient> meta_client,
       const QueryServiceImpl::Options& options)
      : storage_client_(storage_client),
        meta_client_(meta_client),
        options_(options) {}

  Status Init() {
    executor_ = std::make_unique<DistributedExecutor>(
        storage_client_.get(),
        meta_client_.get(),
        options_.executor_workers);
    
    plan_cache_ = std::make_unique<QueryPlanCache>(options_.plan_cache_size);
    
    return Status::OK();
  }

  // gRPC methods
  Status ExecuteQuery(ServerContext* context,
                      const cedar::query::ExecuteQueryRequest* request,
                      cedar::query::ExecuteQueryResponse* response) {
    
    auto start = steady_clock::now();
    std::string query_id = GenerateQueryId();
    
    // Rate limiting
    if (!AcquireQuerySlot()) {
      response->set_success(false);
      response->set_error_msg("Too many concurrent queries");
      return grpc::Status::OK;
    }
    
    // Track active query
    {
      std::lock_guard<std::mutex> lock(queries_mutex_);
      ActiveQuery aq;
      aq.query_id = query_id;
      aq.query_text = request->query();
      aq.start_time = start;
      aq.client_address = context->peer();
      active_queries_[query_id] = std::move(aq);
    }
    
    // Cleanup on exit
    auto cleanup = [&]() {
      ReleaseQuerySlot();
      {
        std::lock_guard<std::mutex> lock(queries_mutex_);
        active_queries_.erase(query_id);
      }
    };
    
    // Check for EXPLAIN
    if (request->explain_only()) {
      std::string explain_output;
      cedar::Status s = executor_->ExecuteExplain(request->query(), &explain_output);
      
      cleanup();
      
      if (!s.ok()) {
        response->set_success(false);
        response->set_error_msg(s.ToString());
        RecordQueryCompletion(query_id, 
            duration_cast<microseconds>(steady_clock::now() - start).count(), 
            false);
        return grpc::Status::OK;
      }
      
      response->set_success(true);
      response->set_execution_plan(explain_output);
      response->set_query_id(query_id);
      
      RecordQueryCompletion(query_id,
          duration_cast<microseconds>(steady_clock::now() - start).count(),
          true);
      return grpc::Status::OK;
    }
    
    // Setup execution context
    DistributedExecutionContext ctx;
    ctx.query_id = query_id;
    ctx.session_id = request->session_id();
    ctx.start_time_us = duration_cast<microseconds>(
        start.time_since_epoch()).count();
    ctx.timeout_ms = request->timeout_ms() > 0 ? 
        request->timeout_ms() : options_.max_query_timeout_ms;
    
    // Parse consistency level
    switch (request->consistency()) {
      case cedar::query::ConsistencyLevel::READ_YOUR_WRITES:
        ctx.consistency = DistributedExecutionContext::Consistency::kReadYourWrites;
        break;
      case cedar::query::ConsistencyLevel::EVENTUAL:
        ctx.consistency = DistributedExecutionContext::Consistency::kEventual;
        break;
      case cedar::query::ConsistencyLevel::STRONG:
        ctx.consistency = DistributedExecutionContext::Consistency::kStrong;
        break;
    }
    
    // Convert parameters
    auto parameters = ConvertParameters(request->parameters());
    
    // Execute query
    cypher::ResultSet result;
    cedar::Status s = executor_->Execute(request->query(), parameters, &ctx, &result);
    
    cleanup();
    
    auto latency_us = duration_cast<microseconds>(steady_clock::now() - start).count();
    
    if (!s.ok()) {
      response->set_success(false);
      response->set_error_msg(s.ToString());
      RecordQueryCompletion(query_id, latency_us, false);
      return grpc::Status::OK;
    }
    
    // Build response
    response->set_success(true);
    response->set_query_id(query_id);
    
    // Convert result set
    auto* proto_result = response->mutable_result_set();
    
    // Add columns
    for (const auto& col : result.columns) {
      proto_result->add_columns(col);
    }
    
    // Add rows
    for (const auto& record : result.records) {
      auto* row = proto_result->add_rows();
      for (const auto& col : result.columns) {
        auto it = record.values.find(col);
        if (it != record.values.end()) {
          *row->add_values() = ConvertToProtoValue(it->second);
        } else {
          row->add_values()->mutable_null_val();
        }
      }
    }
    
    proto_result->set_total_rows(result.records.size());
    
    // Set stats
    auto* stats = response->mutable_stats();
    stats->set_execution_time_us(latency_us);
    stats->set_rows_scanned(ctx.stats.rows_scanned.load());
    stats->set_rows_returned(ctx.stats.rows_returned.load());
    stats->set_storage_nodes_accessed(ctx.stats.storage_nodes_accessed.load());
    stats->set_network_roundtrips(ctx.stats.network_roundtrips.load());
    
    RecordQueryCompletion(query_id, latency_us, true);
    
    return grpc::Status::OK;
  }

  Status Traverse(ServerContext* context,
                  const cedar::query::TraverseRequest* request,
                  cedar::query::TraverseResponse* response) {
    
    if (context->IsCancelled()) return Status::CANCELLED;
    
    // Convert direction
    cypher::Direction dir;
    switch (request->direction()) {
      case cedar::query::TraverseRequest::OUTGOING:
        dir = cypher::Direction::kOut;
        break;
      case cedar::query::TraverseRequest::INCOMING:
        dir = cypher::Direction::kIn;
        break;
      case cedar::query::TraverseRequest::BOTH:
        dir = cypher::Direction::kBoth;
        break;
    }
    
    // Convert edge types
    std::vector<uint16_t> edge_types;
    for (auto type : request->edge_types()) {
      edge_types.push_back(static_cast<uint16_t>(type));
    }
    
    // Execute traversal
    std::vector<std::unique_ptr<cypher::Path>> paths;
    cedar::Status s = executor_->Traverse(
        request->start_node_id(),
        dir,
        edge_types,
        request->max_depth(),
        request->max_branch(),
        request->as_of_timestamp(),
        &paths);
    
    if (!s.ok()) {
      response->set_success(false);
      response->set_error_msg(s.ToString());
      return grpc::Status::OK;
    }
    
    // Convert paths to response
    response->set_success(true);
    response->set_nodes_visited(static_cast<uint32_t>(paths.size()));
    
    for (const auto& path : paths) {
      auto* proto_path = response->add_paths();
      proto_path->set_length(path->length);
    }
    
    return grpc::Status::OK;
  }

  Status Health(ServerContext* context,
                const cedar::query::HealthRequest* request,
                cedar::query::HealthResponse* response) {
    
    if (context->IsCancelled()) return Status::CANCELLED;
    
    bool healthy = true;
    
    // Check components
    bool executor_healthy = executor_ != nullptr;
    bool storage_healthy = storage_client_->HealthCheck().ok();
    bool meta_healthy = meta_client_->GetCachedClusterState() != nullptr;
    
    healthy = executor_healthy && storage_healthy && meta_healthy;
    
    response->set_healthy(healthy);
    response->set_status(healthy ? "healthy" : "degraded");
    response->set_parser_healthy(true);
    response->set_planner_healthy(true);
    response->set_executor_healthy(executor_healthy);
    response->set_storage_client_healthy(storage_healthy);
    response->set_meta_client_healthy(meta_healthy);
    response->set_active_queries(current_queries_.load());
    
    if (request->detailed()) {
      // Add more detailed stats
    }
    
    return grpc::Status::OK;
  }

  ServiceStats GetStats() const {
    ServiceStats stats;
    stats.total_queries = total_queries_.load();
    stats.failed_queries = failed_queries_.load();
    stats.active_queries = current_queries_.load();
    
    auto cache_stats = plan_cache_->GetStats();
    stats.cache_size = cache_stats.size;
    stats.cache_hit_rate = cache_stats.hit_rate;
    
    return stats;
  }

 private:
  std::shared_ptr<QueryStorageClient> storage_client_;
  std::shared_ptr<QueryMetaClient> meta_client_;
  QueryServiceImpl::Options options_;
  
  std::unique_ptr<DistributedExecutor> executor_;
  std::unique_ptr<QueryPlanCache> plan_cache_;
  
  // Query tracking
  mutable std::mutex queries_mutex_;
  std::unordered_map<std::string, ActiveQuery> active_queries_;
  
  // Stats
  mutable std::mutex stats_mutex_;
  std::atomic<uint64_t> total_queries_{0};
  std::atomic<uint64_t> failed_queries_{0};
  std::atomic<uint64_t> total_latency_us_{0};
  
  // Rate limiting
  std::atomic<uint32_t> current_queries_{0};
  std::condition_variable query_cv_;
  mutable std::mutex query_mutex_;
  
  std::string GenerateQueryId() {
    static std::atomic<uint64_t> counter{0};
    
    auto now = steady_clock::now();
    auto nanos = duration_cast<nanoseconds>(now.time_since_epoch()).count();
    uint64_t seq = counter++;
    
    std::ostringstream oss;
    oss << std::hex << std::setfill('0')
        << std::setw(16) << nanos
        << std::setw(8) << seq;
    
    return oss.str();
  }
  
  std::unordered_map<std::string, cypher::Value> ConvertParameters(
      const cedar::query::QueryParameters& proto_params) {
    
    std::unordered_map<std::string, cypher::Value> result;
    
    for (const auto& [key, proto_value] : proto_params.params()) {
      result[key] = ConvertFromProtoValue(proto_value);
    }
    
    return result;
  }
  
  cedar::query::Value ConvertToProtoValue(const cypher::Value& value) {
    cedar::query::Value proto;
    
    switch (value.Type()) {
      case cypher::ValueType::kNull:
        proto.mutable_null_val();
        break;
      case cypher::ValueType::kBool:
        proto.set_bool_val(value.GetBool());
        break;
      case cypher::ValueType::kInt:
        proto.set_int_val(value.GetInt());
        break;
      case cypher::ValueType::kFloat:
        proto.set_float_val(value.GetFloat());
        break;
      case cypher::ValueType::kString:
        proto.set_string_val(value.GetString());
        break;
      case cypher::ValueType::kList:
        for (const auto& item : value.GetList()) {
          *proto.mutable_list_val()->add_items() = ConvertToProtoValue(item);
        }
        break;
      case cypher::ValueType::kMap:
        for (const auto& [k, v] : value.GetMap()) {
          (*proto.mutable_map_val()->mutable_items())[k] = ConvertToProtoValue(v);
        }
        break;
      default:
        proto.mutable_null_val();
    }
    
    return proto;
  }
  
  cypher::Value ConvertFromProtoValue(const cedar::query::Value& proto_value) {
    using cedar::query::Value;
    
    switch (proto_value.value_type_case()) {
      case Value::kBoolVal:
        return cypher::Value(proto_value.bool_val());
      case Value::kIntVal:
        return cypher::Value(proto_value.int_val());
      case Value::kFloatVal:
        return cypher::Value(proto_value.float_val());
      case Value::kStringVal:
        return cypher::Value(proto_value.string_val());
      case Value::kBytesVal:
        return cypher::Value(proto_value.bytes_val());
      case Value::kListVal: {
        std::vector<cypher::Value> list;
        for (const auto& item : proto_value.list_val().items()) {
          list.push_back(ConvertFromProtoValue(item));
        }
        return cypher::Value(list);
      }
      case Value::kMapVal: {
        std::map<std::string, cypher::Value> map;
        for (const auto& [key, val] : proto_value.map_val().items()) {
          map[key] = ConvertFromProtoValue(val);
        }
        return cypher::Value(map);
      }
      case Value::kNullVal:
      default:
        return cypher::Value();
    }
  }
  
  void RecordQueryCompletion(const std::string& query_id, 
                             uint64_t latency_us,
                             bool success) {
    (void)query_id;
    total_queries_++;
    total_latency_us_ += latency_us;
    
    if (!success) {
      failed_queries_++;
    }
  }
  
  bool AcquireQuerySlot() {
    if (current_queries_.load() >= options_.max_concurrent_queries) {
      return false;
    }
    
    current_queries_++;
    return true;
  }
  
  void ReleaseQuerySlot() {
    current_queries_--;
  }
};

// ============================================================================
// QueryServiceImpl - Public Interface
// ============================================================================

QueryServiceImpl::QueryServiceImpl(
    std::shared_ptr<QueryStorageClient> storage_client,
    std::shared_ptr<QueryMetaClient> meta_client,
    const Options& options)
    : impl_(std::make_unique<Impl>(storage_client, meta_client, options)) {}

QueryServiceImpl::~QueryServiceImpl() = default;

Status QueryServiceImpl::Init() {
  return impl_->Init();
}

grpc::Status QueryServiceImpl::ExecuteQuery(
    grpc::ServerContext* context,
    const cedar::query::ExecuteQueryRequest* request,
    cedar::query::ExecuteQueryResponse* response) {
  return impl_->ExecuteQuery(context, request, response);
}

grpc::Status QueryServiceImpl::Traverse(
    grpc::ServerContext* context,
    const cedar::query::TraverseRequest* request,
    cedar::query::TraverseResponse* response) {
  return impl_->Traverse(context, request, response);
}

grpc::Status QueryServiceImpl::Health(
    grpc::ServerContext* context,
    const cedar::query::HealthRequest* request,
    cedar::query::HealthResponse* response) {
  return impl_->Health(context, request, response);
}

QueryServiceImpl::ServiceStats QueryServiceImpl::GetStats() const {
  return impl_->GetStats();
}

// ============================================================================
// Query Server
// ============================================================================

QueryServer::QueryServer(const Options& options) : options_(options) {}

QueryServer::~QueryServer() {
  Stop();
}

Status QueryServer::Init() {
  // Create clients
  storage_client_ = std::make_shared<QueryStorageClient>();
  
  QueryMetaClient::Options meta_options;
  meta_options.meta_service_address = options_.meta_service_address;
  meta_client_ = std::make_shared<QueryMetaClient>(meta_options);
  
  Status s = meta_client_->Init();
  if (!s.ok()) {
    return s;
  }
  
  // Create service implementation
  service_impl_ = std::make_unique<QueryServiceImpl>(
      storage_client_,
      meta_client_,
      options_.service_options);
  
  s = service_impl_->Init();
  if (!s.ok()) {
    return s;
  }
  
  return Status::OK();
}

Status QueryServer::Start() {
  grpc::ServerBuilder builder;
  
  // Configure server
  builder.AddListeningPort(options_.listen_address, 
                           grpc::InsecureServerCredentials());
  builder.RegisterService(service_impl_.get());
  
  // Set max message size
  builder.SetMaxReceiveMessageSize(options_.max_message_size_mb * 1024 * 1024);
  builder.SetMaxSendMessageSize(options_.max_message_size_mb * 1024 * 1024);
  
  // Build and start
  grpc_server_ = builder.BuildAndStart();
  
  if (!grpc_server_) {
    return Status::IOError("Failed to start gRPC server");
  }
  
  running_ = true;
  
  // Register with meta service
  meta_client_->RegisterQueryD(options_.listen_address);
  
  return Status::OK();
}

Status QueryServer::Stop() {
  if (grpc_server_) {
    grpc_server_->Shutdown();
  }
  running_ = false;
  return Status::OK();
}

void QueryServer::Wait() {
  if (grpc_server_) {
    grpc_server_->Wait();
  }
}

std::string QueryServer::GetListenAddress() const {
  return options_.listen_address;
}

bool QueryServer::IsRunning() const {
  return running_;
}

}  // namespace queryd
}  // namespace cedar
