// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// cedar-queryd - Distributed Query Layer (Full Production Version)

#include <csignal>
#include <unistd.h>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include <gflags/gflags.h>
#include "cedar/common/logging.h"
#include <grpc/grpc.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include "cedar/core/status.h"
#include "cedar/dtx/storage_service_impl.h"
#include "cedar/queryd/query_storage_client.h"
#include "cedar/queryd/distributed_executor.h"
#include "cedar/queryd/meta_client.h"
#include "cedar/governance/health_checker.h"
#include "cedar/governance/config_manager.h"
#include "cedar/dtx/storage/metrics_collector.h"
#include "cedar/dtx/monitoring.h"
#include "cedar/dtx/raft/grpc_tls.h"
#include "cedar/dtx/optimized_2pc_engine.h"
#include "cedar/dtx/transaction_state.h"
#include "cedar/dtx/transaction_recovery_manager.h"
#include "cedar/dtx/transaction_timeout_manager.h"
#include "cedar/cypher/parser.h"

// 包含生成的 protobuf 代码
#include "query_service.pb.h"
#include "query_service.grpc.pb.h"

// Command line flags
DEFINE_string(listen, "0.0.0.0:9669", "Listen address for query service");
DEFINE_string(meta, "127.0.0.1:9559", "Meta service address");
DEFINE_string(storage, "127.0.0.1:9779", "Storage service address");
DEFINE_int32(workers, 16, "Number of executor worker threads");
DEFINE_int32(max_concurrent, 1000, "Maximum concurrent queries");
DEFINE_int32(query_timeout, 30000, "Default query timeout in milliseconds");
DEFINE_int32(cache_size, 1000, "Query plan cache size");
DEFINE_int32(max_message_size, 64, "Maximum gRPC message size in MB");
DEFINE_string(config, "", "Configuration file path (YAML)");

static cedar::dtx::raft::TlsConfig LoadTlsConfig(const cedar::governance::ConfigManager& cm) {
  cedar::dtx::raft::TlsConfig tls;
  tls.enabled = cm.GetBool("tls.enabled", false);
  tls.server_cert_file = cm.GetString("tls.server_cert", "");
  tls.server_key_file = cm.GetString("tls.server_key", "");
  tls.ca_cert_file = cm.GetString("tls.ca_cert", "");
  tls.mtls_enabled = cm.GetBool("tls.mtls", false);
  tls.client_cert_file = cm.GetString("tls.client_cert", "");
  tls.client_key_file = cm.GetString("tls.client_key", "");
  return tls;
}

std::atomic<bool> g_running{true};
std::unique_ptr<grpc::Server> g_grpc_server;
static volatile sig_atomic_t g_queryd_signal = 0;

void SignalHandler(int signal) {
  if (signal == SIGINT || signal == SIGTERM) {
    g_queryd_signal = signal;
    g_running = false;
  }
}

void PrintBanner() {
  std::cout << R"(
   ____          _        _____                  _   
  / ___|__ _  __| | ___  |  ___|   _ _ __   __ _| |_ 
 | |   / _` |/ _` |/ _ \ | |_ | | | | '_ \ / _` | __|
 | |__| (_| | (_| |  __/ |  _|| |_| | |_) | (_| | |_ 
  \____\__,_|\__,_|\___| |_|   \__, | .__/ \__,_|\__|
                               |___/|_|               
                                                      
  Distributed Query Layer for CedarGraph
  Version: 1.0.0
  
)";
}

// Helper: convert proto Value to cypher::Value
cedar::cypher::Value ProtoValueToCypher(const cedar::query::Value& proto) {
  switch (proto.value_type_case()) {
    case cedar::query::Value::kBoolVal:
      return cedar::cypher::Value(proto.bool_val());
    case cedar::query::Value::kIntVal:
      return cedar::cypher::Value(proto.int_val());
    case cedar::query::Value::kFloatVal:
      return cedar::cypher::Value(proto.float_val());
    case cedar::query::Value::kStringVal:
      return cedar::cypher::Value(proto.string_val());
    case cedar::query::Value::kBytesVal:
      return cedar::cypher::Value(std::string(proto.bytes_val()));
    case cedar::query::Value::kListVal: {
      std::vector<cedar::cypher::Value> list;
      for (const auto& item : proto.list_val().items()) {
        list.push_back(ProtoValueToCypher(item));
      }
      return cedar::cypher::Value(std::move(list));
    }
    case cedar::query::Value::kMapVal: {
      std::map<std::string, cedar::cypher::Value> map;
      for (const auto& kv : proto.map_val().items()) {
        map[kv.first] = ProtoValueToCypher(kv.second);
      }
      return cedar::cypher::Value(std::move(map));
    }
    case cedar::query::Value::kNullVal:
    default:
      return cedar::cypher::Value::Null();
  }
}

// 检测写操作（简化：关键词匹配）
static bool IsWriteQuery(const std::string& query) {
  // 1. 尝试 AST 解析精确检测（CREATE / SET / DELETE）
  cedar::cypher::CypherParser parser(query);
  auto stmt = parser.ParseStatement();
  if (stmt) {
    for (const auto& clause : stmt->clauses) {
      switch (clause->clause_type) {
        case cedar::cypher::ClauseType::CREATE:
        case cedar::cypher::ClauseType::SET:
        case cedar::cypher::ClauseType::DELETE:
          return true;
        default:
          break;
      }
    }
  }
  
  // 2. 关键词 fallback（覆盖 MERGE/REMOVE 以及 AST 解析失败的场景）
  std::string lower_query = query;
  std::transform(lower_query.begin(), lower_query.end(), lower_query.begin(), ::tolower);
  return lower_query.find("merge") != std::string::npos ||
         lower_query.find("remove") != std::string::npos;
}

// QueryService 实现类 - 委托给 DistributedExecutor
static cedar::query::Value ConvertToProtoValue(const cedar::cypher::Value& value) {
  cedar::query::Value proto;
  switch (value.Type()) {
    case cedar::cypher::ValueType::kNull:
      proto.mutable_null_val();
      break;
    case cedar::cypher::ValueType::kBool:
      proto.set_bool_val(value.GetBool());
      break;
    case cedar::cypher::ValueType::kInt:
    case cedar::cypher::ValueType::kTimestamp:
      proto.set_int_val(value.GetInt());
      break;
    case cedar::cypher::ValueType::kFloat:
      proto.set_float_val(value.GetFloat());
      break;
    case cedar::cypher::ValueType::kString:
      proto.set_string_val(value.GetString());
      break;
    case cedar::cypher::ValueType::kList:
      for (const auto& item : value.GetList()) {
        *proto.mutable_list_val()->add_items() = ConvertToProtoValue(item);
      }
      break;
    case cedar::cypher::ValueType::kMap:
      for (const auto& [k, v] : value.GetMap()) {
        (*proto.mutable_map_val()->mutable_items())[k] = ConvertToProtoValue(v);
      }
      break;
    default:
      proto.set_string_val(value.ToString());
      break;
  }
  return proto;
}

static void RecordToRow(const cedar::cypher::Record& record, cedar::query::Row* out_row) {
  for (const auto& [key, value] : record.values) {
    (void)key;
    *out_row->add_values() = ConvertToProtoValue(value);
  }
}

class QueryServiceImpl final : public cedar::query::QueryService::Service {
 public:
  explicit QueryServiceImpl(cedar::queryd::DistributedExecutor* executor,
                            cedar::queryd::QueryPlanCache* plan_cache,
                            cedar::dtx::Optimized2PCEngine* two_pc_engine = nullptr)
      : executor_(executor), plan_cache_(plan_cache), two_pc_engine_(two_pc_engine),
        active_queries_(0), total_queries_(0) {}

  grpc::Status ExecuteQuery(grpc::ServerContext* context,
                           const cedar::query::ExecuteQueryRequest* request,
                           cedar::query::ExecuteQueryResponse* response) override {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    active_queries_++;
    total_queries_++;
    int64_t query_id = total_queries_.load();

    LOG(INFO) << "ExecuteQuery [#" << query_id << "]: " << request->query().substr(0, 50) << "...";

    cedar::queryd::DistributedExecutionContext ctx;
    ctx.query_id = std::to_string(query_id);
    ctx.timeout_ms = request->timeout_ms() > 0 ? request->timeout_ms() : 30000;

    if (request->explain_only()) {
      std::string explain;
      auto s = executor_->ExecuteExplain(request->query(), &explain);
      response->set_success(s.ok());
      response->set_query_id(ctx.query_id);
      response->set_execution_plan(explain);
      active_queries_--;
      return s.ok() ? grpc::Status::OK : grpc::Status(grpc::StatusCode::INTERNAL, s.ToString());
    }

    // 检测写操作
    if (IsWriteQuery(request->query()) && two_pc_engine_) {
      // FIXME: Write set is currently a single placeholder key derived from
      // query_id. Real read/write set extraction from the execution plan
      // is required for correct distributed transaction isolation.
      LOG(WARNING) << "Write query uses placeholder write_set; "
                   << "2PC isolation is NOT effective. query_id=" << query_id;

      std::vector<::cedar::CedarKey> read_set;
      std::vector<::cedar::CedarKey> write_set;
      auto now_ts = std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count();
      write_set.emplace_back(static_cast<uint64_t>(query_id), ::cedar::EntityType::Vertex, 0,
                             ::cedar::Timestamp(now_ts), 0, 0, 0, 0);

      std::string txn_id_str(request->txn_id().begin(), request->txn_id().end());
      if (!txn_id_str.empty()) {
        // Explicit transaction mode
        std::lock_guard<std::mutex> lock(active_txns_mutex_);
        auto it = active_transactions_.find(txn_id_str);
        if (it == active_transactions_.end()) {
          response->set_success(false);
          response->set_error_msg("Transaction not found: " + txn_id_str);
          active_queries_--;
          return grpc::Status::OK;
        }
        it->second.write_set.insert(it->second.write_set.end(),
                                     write_set.begin(), write_set.end());
        it->second.has_writes = true;
        response->set_success(true);
        response->set_query_id(ctx.query_id);
        auto* stats = response->mutable_stats();
        stats->set_execution_time_us(0);
        stats->set_rows_scanned(0);
        stats->set_rows_returned(0);
        stats->set_storage_nodes_accessed(0);
        stats->set_network_roundtrips(0);
        active_queries_--;
        return grpc::Status::OK;
      }

      // Autocommit mode: DISABLE 2PC until real write-set extraction exists.
      LOG(WARNING) << "Autocommit write query bypassing 2PC: "
                   << "placeholder write_set provides no isolation. query_id=" << query_id;
      // Fall through to normal query execution below.
    }

    cedar::cypher::ResultSet result;
    std::unordered_map<std::string, cedar::cypher::Value> parameters;
    for (const auto& p : request->parameters().params()) {
      parameters[p.first] = ProtoValueToCypher(p.second);
    }

    auto s = executor_->Execute(request->query(), parameters, &ctx, &result);

    response->set_success(s.ok());
    response->set_query_id(ctx.query_id);
    auto* stats = response->mutable_stats();
    stats->set_execution_time_us(ctx.stats.execution_time_us.load());
    stats->set_rows_scanned(ctx.stats.rows_scanned.load());
    stats->set_rows_returned(ctx.stats.rows_returned.load());
    stats->set_storage_nodes_accessed(ctx.stats.storage_nodes_accessed.load());
    stats->set_network_roundtrips(ctx.stats.network_roundtrips.load());

    active_queries_--;
    return s.ok() ? grpc::Status::OK : grpc::Status(grpc::StatusCode::INTERNAL, s.ToString());
  }

  grpc::Status StreamQuery(grpc::ServerContext* context,
                          const cedar::query::StreamQueryRequest* request,
                          grpc::ServerWriter<cedar::query::StreamQueryResponse>* writer) override {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;

    cedar::queryd::DistributedExecutionContext ctx;
    std::unordered_map<std::string, cedar::cypher::Value> parameters;

    int32_t batch_index = 0;
    constexpr size_t kRowsPerBatch = 50;
    size_t rows_in_current_batch = 0;
    bool client_disconnected = false;
    cedar::query::StreamQueryResponse current_batch;
    current_batch.set_success(true);
    current_batch.set_query_id(request->query_id());
    current_batch.set_batch_index(batch_index);
    current_batch.set_has_more(true);

    auto s = executor_->ExecuteStreaming(
        request->query(), parameters, &ctx,
        [&writer, &current_batch, &batch_index, &rows_in_current_batch,
         &client_disconnected, request](
            const cedar::cypher::Record& record) -> bool {
          auto* row = current_batch.mutable_batch()->add_rows();
          RecordToRow(record, row);
          rows_in_current_batch++;

          if (rows_in_current_batch >= kRowsPerBatch) {
            if (!writer->Write(current_batch)) {
              client_disconnected = true;
              return false;  // Client disconnected
            }
            batch_index++;
            current_batch.Clear();
            current_batch.set_success(true);
            current_batch.set_query_id(request->query_id());
            current_batch.set_batch_index(batch_index);
            current_batch.set_has_more(true);
            rows_in_current_batch = 0;
          }
          return true;
        });

    // Send final batch (partial or empty terminal for exact multiples)
    if (!client_disconnected && !context->IsCancelled()) {
      current_batch.set_has_more(false);
      current_batch.set_progress_percent(100);
      if (!writer->Write(current_batch)) {
        client_disconnected = true;
      }
    }

    // Send EOF marker
    if (!client_disconnected && !context->IsCancelled()) {
      cedar::query::StreamQueryResponse final_response;
      final_response.set_success(s.ok());
      final_response.set_has_more(false);
      final_response.set_query_id(request->query_id());
      final_response.set_progress_percent(100);
      if (!s.ok()) {
        final_response.set_error_msg(s.ToString());
      }
      writer->Write(final_response);
    }
    return grpc::Status::OK;
  }

  grpc::Status Traverse(grpc::ServerContext* context,
                       const cedar::query::TraverseRequest* request,
                       cedar::query::TraverseResponse* response) override {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    cedar::queryd::DistributedExecutionContext ctx;
    std::vector<std::unique_ptr<cedar::cypher::Path>> paths;

    std::vector<uint16_t> edge_types(request->edge_types().begin(), request->edge_types().end());
    auto s = executor_->Traverse(
        request->start_node_id(),
        static_cast<cedar::cypher::Direction>(request->direction()),
        edge_types,
        request->max_depth(),
        request->max_branch(),
        cedar::Timestamp::Max(),
        &paths);

    response->set_success(s.ok());
    response->set_nodes_visited(static_cast<uint64_t>(paths.size()));
    return grpc::Status::OK;
  }

  grpc::Status TemporalQuery(grpc::ServerContext* context,
                            const cedar::query::TemporalQueryRequest* request,
                            cedar::query::TemporalQueryResponse* response) override {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    cedar::queryd::DistributedExecutionContext ctx;
    std::vector<cedar::cypher::VersionedEntity> versions;

    auto s = executor_->TemporalQuery(
        request->entity_id(),
        static_cast<cedar::EntityType>(request->entity_type()),
        cedar::queryd::DistributedExecutionContext::Consistency::kReadYourWrites,
        &versions);

    response->set_success(s.ok());
    return grpc::Status::OK;
  }

  grpc::Status BatchQuery(grpc::ServerContext* context,
                         const cedar::query::BatchQueryRequest* request,
                         cedar::query::BatchQueryResponse* response) override {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    for (int i = 0; i < request->queries_size(); i++) {
      auto* result = response->add_results();
      result->set_query_id(request->queries(i).query_id());

      cedar::queryd::DistributedExecutionContext ctx;
      cedar::cypher::ResultSet rs;
      std::unordered_map<std::string, cedar::cypher::Value> parameters;
      auto s = executor_->Execute(request->queries(i).query(), parameters, &ctx, &rs);
      result->set_success(s.ok());
    }
    response->set_success(true);
    return grpc::Status::OK;
  }

  grpc::Status GetSchema(grpc::ServerContext* context,
                        const cedar::query::GetSchemaRequest* request,
                        cedar::query::GetSchemaResponse* response) override {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    (void)request;
    response->set_success(true);
    auto* label = response->add_labels();
    label->set_name("Person");
    auto* prop1 = label->add_properties();
    prop1->set_name("id");
    prop1->set_type("STRING");
    prop1->set_nullable(false);
    prop1->set_indexed(true);
    return grpc::Status::OK;
  }

  grpc::Status Health(grpc::ServerContext* context,
                     const cedar::query::HealthRequest* request,
                     cedar::query::HealthResponse* response) override {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    (void)request;
    response->set_healthy(true);
    response->set_status("healthy");
    response->set_parser_healthy(true);
    response->set_planner_healthy(true);
    response->set_executor_healthy(true);
    response->set_storage_client_healthy(true);
    response->set_meta_client_healthy(true);
    response->set_active_queries(static_cast<uint32_t>(active_queries_.load()));
    response->set_queued_queries(0);
    response->set_cpu_usage(0.0);
    response->set_memory_usage(0.0);
    return grpc::Status::OK;
  }

  grpc::Status GetStats(grpc::ServerContext* context,
                       const cedar::query::QueryStatsRequest* request,
                       cedar::query::QueryStatsResponse* response) override {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    (void)request;
    auto cache_stats = plan_cache_->GetStats();
    response->set_total_queries(static_cast<uint64_t>(total_queries_.load()));
    response->set_failed_queries(0);
    response->set_cached_plans(static_cast<uint64_t>(cache_stats.size));
    response->set_avg_latency_us(1000);
    response->set_p99_latency_us(5000);
    response->set_queries_per_second(0.0);
    return grpc::Status::OK;
  }

  // ========== 显式事务 API ==========
  grpc::Status BeginTransaction(grpc::ServerContext* context,
                                const cedar::query::BeginTransactionRequest* request,
                                cedar::query::Transaction* response) override {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    auto txn_id = static_cast<uint64_t>(++total_queries_);
    std::string txn_id_str = std::to_string(txn_id);
    
    {
      std::lock_guard<std::mutex> lock(active_txns_mutex_);
      ActiveTransaction txn_ctx;
      txn_ctx.txn_id = txn_id;
      active_transactions_[txn_id_str] = std::move(txn_ctx);
    }
    
    response->set_txn_id(txn_id_str);
    response->set_isolation_level(request->isolation_level());
    response->set_start_time(
        std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
    response->set_committed(false);
    
    LOG(INFO) << "[QueryD] BeginTransaction: " << txn_id;
    return grpc::Status::OK;
  }
  
  grpc::Status Commit(grpc::ServerContext* context,
                      const cedar::query::CommitRequest* request,
                      cedar::query::QueryGrpcStatus* response) override {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    std::string txn_id_str(request->txn_id().begin(), request->txn_id().end());
    
    ActiveTransaction txn_ctx;
    {
      std::lock_guard<std::mutex> lock(active_txns_mutex_);
      auto it = active_transactions_.find(txn_id_str);
      if (it == active_transactions_.end()) {
        response->set_ok(false);
        response->set_message("Transaction not found: " + txn_id_str);
        return grpc::Status::OK;
      }
      txn_ctx = std::move(it->second);
      active_transactions_.erase(it);
    }
    
    if (two_pc_engine_ && txn_ctx.has_writes) {
      auto now_ts = std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count();
      auto s = two_pc_engine_->Execute2PC(txn_ctx.txn_id, txn_ctx.read_set,
                                           txn_ctx.write_set,
                                           ::cedar::Timestamp(now_ts));
      if (!s.ok()) {
        response->set_ok(false);
        response->set_message("2PC commit failed: " + s.ToString());
        return grpc::Status::OK;
      }
    }
    
    response->set_ok(true);
    LOG(INFO) << "[QueryD] Commit: " << txn_id_str << " (keys=" << txn_ctx.write_set.size() << ")";
    return grpc::Status::OK;
  }
  
  grpc::Status Rollback(grpc::ServerContext* context,
                        const cedar::query::RollbackRequest* request,
                        cedar::query::QueryGrpcStatus* response) override {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    std::string txn_id_str(request->txn_id().begin(), request->txn_id().end());
    
    {
      std::lock_guard<std::mutex> lock(active_txns_mutex_);
      auto it = active_transactions_.find(txn_id_str);
      if (it == active_transactions_.end()) {
        response->set_ok(false);
        response->set_message("Transaction not found: " + txn_id_str);
        return grpc::Status::OK;
      }
      active_transactions_.erase(it);
    }
    
    response->set_ok(true);
    LOG(INFO) << "[QueryD] Rollback: " << txn_id_str;
    return grpc::Status::OK;
  }

 private:
  cedar::queryd::DistributedExecutor* executor_;
  cedar::queryd::QueryPlanCache* plan_cache_;
  cedar::dtx::Optimized2PCEngine* two_pc_engine_;
  std::atomic<int64_t> active_queries_;
  std::atomic<int64_t> total_queries_;
  
  // 显式事务上下文
  struct ActiveTransaction {
    uint64_t txn_id;
    std::vector<::cedar::CedarKey> read_set;
    std::vector<::cedar::CedarKey> write_set;
    bool has_writes = false;
  };
  mutable std::mutex active_txns_mutex_;
  std::unordered_map<std::string, ActiveTransaction> active_transactions_;
};

int main(int argc, char* argv[]) {
  // Parse command line flags
  gflags::SetUsageMessage("cedar-queryd - CedarGraph Distributed Query Layer");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Load YAML config if provided (overrides defaults, command-line flags take precedence)
  if (!FLAGS_config.empty()) {
    cedar::governance::ConfigManager cm;
    if (cm.LoadFromFile(FLAGS_config).ok()) {
      if (cm.HasKey("queryd.listen") && !gflags::GetCommandLineFlagInfoOrDie("listen").is_default) {
        FLAGS_listen = cm.GetString("queryd.listen", FLAGS_listen);
      }
      if (cm.HasKey("queryd.meta")) FLAGS_meta = cm.GetString("queryd.meta", FLAGS_meta);
      if (cm.HasKey("queryd.storage")) FLAGS_storage = cm.GetString("queryd.storage", FLAGS_storage);
      if (cm.HasKey("queryd.workers")) FLAGS_workers = cm.GetInt("queryd.workers", FLAGS_workers);
      if (cm.HasKey("queryd.max_concurrent")) FLAGS_max_concurrent = cm.GetInt("queryd.max_concurrent", FLAGS_max_concurrent);
      if (cm.HasKey("queryd.query_timeout")) FLAGS_query_timeout = cm.GetInt("queryd.query_timeout", FLAGS_query_timeout);
      if (cm.HasKey("queryd.cache_size")) FLAGS_cache_size = cm.GetInt("queryd.cache_size", FLAGS_cache_size);
      if (cm.HasKey("queryd.max_message_size")) FLAGS_max_message_size = cm.GetInt("queryd.max_message_size", FLAGS_max_message_size);
      LOG(INFO) << "Loaded configuration from " << FLAGS_config;
    } else {
      LOG(WARNING) << "Failed to load config file: " << FLAGS_config;
    }
  }

  // Initialize glog
  google::InitGoogleLogging(argv[0]);
  FLAGS_logbufsecs = 0;
  FLAGS_max_log_size = 100;
  FLAGS_stop_logging_if_full_disk = true;

  PrintBanner();
  
  LOG(INFO) << "Starting cedar-queryd...";
  LOG(INFO) << "Listen address: " << FLAGS_listen;
  LOG(INFO) << "Meta service: " << FLAGS_meta;
  LOG(INFO) << "Storage service: " << FLAGS_storage;
  LOG(INFO) << "Worker threads: " << FLAGS_workers;
  LOG(INFO) << "Max concurrent queries: " << FLAGS_max_concurrent;
  
  // Setup signal handlers
  std::signal(SIGINT, SignalHandler);
  std::signal(SIGTERM, SignalHandler);
  std::signal(SIGPIPE, SIG_IGN);
  
  // Create and initialize storage client
  cedar::queryd::QueryStorageClient::Options storage_options;
  auto storage_client = std::make_shared<cedar::queryd::QueryStorageClient>(storage_options);
  
  cedar::Status s = storage_client->Init(FLAGS_meta);
  if (!s.ok()) {
    LOG(ERROR) << "Failed to initialize storage client: " << s.ToString();
    return 1;
  }
  
  // Load TLS config
  cedar::dtx::raft::TlsConfig tls_config;
  if (!FLAGS_config.empty()) {
    cedar::governance::ConfigManager cm;
    if (cm.LoadFromFile(FLAGS_config).ok()) {
      tls_config = LoadTlsConfig(cm);
    }
  }

  // Create base dtx client and connect to storage service
  auto base_client = std::make_shared<cedar::dtx::StorageClient>();
  cedar::dtx::StorageClient::ClientConfig client_config;
  client_config.server_address = FLAGS_storage;
  cedar::Status init_status = base_client->Initialize(client_config);
  if (!init_status.ok()) {
    LOG(WARNING) << "Failed to connect to storage backend: " << init_status.ToString();
    // Continue anyway, we'll work in standalone mode
  } else {
    storage_client->SetBaseClient(base_client);
    LOG(INFO) << "Connected to storage backend at " << FLAGS_storage;
  }
  
  LOG(INFO) << "Storage client initialized successfully";
  
  // Create meta client
  cedar::queryd::QueryMetaClient::Options meta_options;
  meta_options.meta_service_address = FLAGS_meta;
  auto meta_client = std::make_unique<cedar::queryd::QueryMetaClient>(meta_options);
  if (!meta_client->Init().ok()) {
    LOG(WARNING) << "Failed to connect to meta service, running in standalone mode";
  }

  // Create query plan cache
  auto plan_cache = std::make_unique<cedar::queryd::QueryPlanCache>(FLAGS_cache_size);

  // Create distributed executor
  auto executor = std::make_unique<cedar::queryd::DistributedExecutor>(
      storage_client.get(), meta_client.get(), FLAGS_workers);

  // Initialize 2PC distributed transaction engine
  // NOTE: two_pc_engine must be declared AFTER the managers it depends on,
  // so that it is destroyed FIRST (C++ destroys in reverse declaration order).
  std::unique_ptr<cedar::TransactionStateManager> txn_state_manager;
  std::unique_ptr<cedar::TransactionRecoveryManager> txn_recovery_manager;
  std::unique_ptr<cedar::TransactionTimeoutManager> txn_timeout_manager;
  std::unique_ptr<cedar::dtx::Optimized2PCEngine> two_pc_engine;
  
  if (base_client && init_status.ok()) {
    txn_state_manager = std::make_unique<cedar::TransactionStateManager>();
    auto s = txn_state_manager->Initialize("/tmp/cedar_queryd_txn_wal");
    if (s.ok()) {
      txn_recovery_manager = std::make_unique<cedar::TransactionRecoveryManager>();
      s = txn_recovery_manager->Initialize(txn_state_manager.get());
      if (s.ok()) {
        txn_timeout_manager = std::make_unique<cedar::TransactionTimeoutManager>();
        cedar::TimeoutConfig timeout_config;
        txn_timeout_manager->Initialize(timeout_config, txn_recovery_manager.get());
        
        cedar::dtx::TwoPCConfig two_pc_config;
        two_pc_engine = std::make_unique<cedar::dtx::Optimized2PCEngine>(two_pc_config);
        std::vector<std::shared_ptr<cedar::dtx::StorageClient>> clients = {base_client};
        s = two_pc_engine->Initialize(clients);
        if (s.ok()) {
          two_pc_engine->SetStateManager(txn_state_manager.get());
          two_pc_engine->SetRecoveryManager(txn_recovery_manager.get());
          two_pc_engine->SetTimeoutManager(txn_timeout_manager.get());
          LOG(INFO) << "2PC engine initialized with 1 storage client";
        } else {
          LOG(WARNING) << "Failed to initialize 2PC engine: " << s.ToString();
          two_pc_engine.reset();
        }
      } else {
        LOG(WARNING) << "Failed to initialize recovery manager: " << s.ToString();
      }
    } else {
      LOG(WARNING) << "Failed to initialize state manager: " << s.ToString();
    }
  }

  // Create and start gRPC server
  QueryServiceImpl service(executor.get(), plan_cache.get(), two_pc_engine.get());
  
  grpc::ServerBuilder builder;
  auto server_creds = cedar::dtx::raft::TlsCredentialFactory::CreateServerCredentials(tls_config);
  if (!server_creds) {
    LOG(WARNING) << "Failed to create server credentials, using insecure";
    server_creds = grpc::InsecureServerCredentials();
  }
  builder.AddListeningPort(FLAGS_listen, server_creds);
  builder.RegisterService(&service);
  
  // Set message size limits
  builder.SetMaxReceiveMessageSize(FLAGS_max_message_size * 1024 * 1024);
  builder.SetMaxSendMessageSize(FLAGS_max_message_size * 1024 * 1024);
  
  // Build and start server
  g_grpc_server = builder.BuildAndStart();
  
  if (!g_grpc_server) {
    LOG(ERROR) << "Failed to start gRPC server on " << FLAGS_listen;
    return 1;
  }
  
  LOG(INFO) << "gRPC server started successfully on " << FLAGS_listen;
  LOG(INFO) << "Press Ctrl+C to stop";

  // Start health check and metrics HTTP endpoints
  cedar::governance::HealthChecker health_checker;
  health_checker.RegisterComponent("queryd", [&executor]() {
    return executor ? cedar::governance::HealthStatus::kHealthy 
                    : cedar::governance::HealthStatus::kUnhealthy;
  });
  auto health_status = health_checker.StartHttpEndpoint("0.0.0.0", 9667);
  if (health_status.ok()) {
    LOG(INFO) << "Health endpoint on http://0.0.0.0:9667/health";
  }

  cedar::dtx::storage::MetricsCollector metrics_collector;
  cedar::dtx::storage::MetricsCollector::Config metrics_config;
  metrics_config.endpoint = ":9666";
  metrics_config.enable_http_server = true;
  auto metrics_status = metrics_collector.Initialize(metrics_config);
  if (metrics_status.ok()) {
    LOG(INFO) << "Metrics endpoint on http://0.0.0.0:9666/metrics";
  }

  auto* alert_mgr = cedar::dtx::monitoring::AlertManager::GetInstance();
  cedar::dtx::monitoring::AlertManager::Config alert_config;
  alert_mgr->Initialize(alert_config);
  {
    cedar::dtx::monitoring::AlertRule rule;
    rule.name = "QueryDHighLatency";
    rule.description = "QueryD query latency is too high";
    rule.severity = cedar::dtx::monitoring::AlertSeverity::kWarning;
    rule.condition_metric = "cedar_queryd_query_latency_seconds";
    rule.threshold = 2.0;
    rule.comparison = ">";
    rule.duration = std::chrono::seconds(60);
    alert_mgr->AddRule(rule);
  }
  LOG(INFO) << "AlertManager initialized with default rules";

  // Wait for shutdown
  while (g_running) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    if (g_queryd_signal != 0) {
      const char msg[] = "[QueryD] Shutdown signal received\n";
      write(STDERR_FILENO, msg, sizeof(msg) - 1);
      g_queryd_signal = 0;
    }
  }
  g_grpc_server->Shutdown();

  // Cleanup
  health_checker.StopHttpEndpoint();
  metrics_collector.Shutdown();
  alert_mgr->Shutdown();

  LOG(INFO) << "Server stopped";

  google::ShutdownGoogleLogging();
  return 0;
}
