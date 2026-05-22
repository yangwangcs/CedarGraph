// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#include "cedar/service/graph_service_router.h"

#include <algorithm>
#include <chrono>
#include <iostream>
#include <regex>
#include <sstream>
#include <thread>

#include "cedar/cypher/parser.h"
#include "cedar/cypher/fingerprint.h"

namespace cedar {
namespace service {

using namespace cedar::query;
using namespace cedar::meta;
using namespace cedar::storage;

GraphServiceRouter::GraphServiceRouter() 
    : cypher_engine_(std::make_unique<cypher::CypherEngine>(nullptr)) {}

GraphServiceRouter::~GraphServiceRouter() {
  Stop();
}

Status GraphServiceRouter::Initialize(const std::string& meta_server_addr,
                                         const std::string& gcn_server_addr) {
  // 连接到 MetaD (支持多地址 failover)
  meta_client_ = std::make_unique<cedar::dtx::MetaServiceGrpcClient>();
  std::vector<std::string> meta_addresses;
  std::stringstream addr_stream(meta_server_addr);
  std::string addr;
  while (std::getline(addr_stream, addr, ',')) {
    addr.erase(0, addr.find_first_not_of(" \t"));
    addr.erase(addr.find_last_not_of(" \t") + 1);
    if (!addr.empty()) meta_addresses.push_back(addr);
  }
  if (meta_addresses.empty()) {
    meta_addresses.push_back(meta_server_addr);
  }

  auto connect_status = meta_client_->Connect(meta_addresses);
  if (!connect_status.ok()) {
    return Status::InvalidArgument("Failed to connect to MetaD: " + connect_status.ToString());
  }

  std::cerr << "[GraphD] Connected to MetaD at " << meta_server_addr << std::endl;
  
  // 初始化 GCN 路由（如果配置了）
  gcn_router_ = std::make_shared<cedar::gcn::ScatterGatherRouter>();
  if (!gcn_server_addr.empty()) {
    auto gcn_creds = cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentials(tls_config_);
    if (!gcn_creds) gcn_creds = cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentialsFromEnv();
    auto gcn_channel = grpc::CreateChannel(gcn_server_addr, gcn_creds);
    gcn_router_->RegisterPeer(gcn_server_addr, gcn_channel);
    gcn_peer_addresses_.push_back(gcn_server_addr);
    std::cerr << "[GraphD] Registered GCN " << gcn_server_addr << " in router" << std::endl;
  }
  
  // 初始加载分区映射
  RefreshPartitionMap();
  
  // 初始化查询缓存
  cedar::query::QueryCacheConfig cache_config;
  cache_config.max_entries = 10000;
  cache_config.max_memory_bytes = 100 * 1024 * 1024;  // 100MB
  cache_config.default_ttl_seconds = 60;
  query_cache_ = std::make_unique<cedar::query::QueryCache>(cache_config);
  
  std::cerr << "[GraphD] Query cache initialized" << std::endl;

  // 初始化 2PC 分布式事务引擎
  auto s = Initialize2PCEngine();
  if (!s.ok()) {
    std::cerr << "[GraphD] FATAL: Failed to initialize 2PC engine: " << s.ToString() << std::endl;
    std::abort();
  }
  
  return Status::OK();
}

Status GraphServiceRouter::Start() {
  running_ = true;
  
  // 启动分区映射刷新线程
  refresh_thread_ = std::thread(&GraphServiceRouter::PartitionMapRefreshLoop, this);

  // 恢复未完成的分布式事务
  if (txn_recovery_manager_) {
    auto recovery_result = txn_recovery_manager_->RecoverAllPendingTransactions();
    if (!recovery_result.ok()) {
      std::cerr << "[GraphD] Transaction recovery warning: " << recovery_result.ToString() << std::endl;
    } else {
      std::cerr << "[GraphD] Transaction recovery completed" << std::endl;
    }
  }
  
  std::cerr << "[GraphD] Router started" << std::endl;
  return Status::OK();
}

Status GraphServiceRouter::Stop() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  if (refresh_thread_.joinable()) {
    refresh_thread_.join();
  }

  Shutdown2PCEngine();

  // Stop MetaD client health monitor
  if (meta_client_) {
    meta_client_.reset();
  }

  std::cerr << "[GraphD] Router stopped" << std::endl;
  return Status::OK();
}

// ========== gRPC 方法实现 ==========

grpc::Status GraphServiceRouter::ExecuteQuery(grpc::ServerContext* context,
                                              const ExecuteQueryRequest* request,
                                              ExecuteQueryResponse* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }
  
  auto start_time = std::chrono::steady_clock::now();
  stats_.total_queries++;
  stats_.active_queries++;
  
  // RAII guard to decrement active_queries on exit
  struct ActiveQueryGuard {
    std::atomic<uint64_t>* counter;
    ~ActiveQueryGuard() { counter->fetch_sub(1); }
  } active_guard{&stats_.active_queries};
  
  // 构建缓存键
  if (!request->explain_only()) {
    cedar::query::CacheKey cache_key;
    cache_key.query_fingerprint = GenerateQueryFingerprint(request->query());
    cache_key.partition_hash = 0;  // 简化处理
    cache_key.as_of_timestamp = 0;
    
    // 尝试从缓存获取
    auto cached_result = query_cache_->Get(cache_key);
    if (cached_result.ok()) {
      *response->mutable_result_set() = cached_result.ValueOrDie();
      response->set_success(true);
      
      auto* stats = response->mutable_stats();
      stats->set_execution_time_us(0);
      
      return grpc::Status::OK;
    }
  }
  
  // 解析查询
  QueryRouteContext route_ctx;
  route_ctx.query = request->query();
  
  auto parse_status = ParseQueryForRouting(request->query(), &route_ctx);
  if (!parse_status.ok()) {
    stats_.failed_queries++;
    response->set_success(false);
    response->set_error_msg(parse_status.ToString());
    return grpc::Status::OK;
  }
  
  // 设置查询 ID
  static std::atomic<uint64_t> query_counter{0};
  response->set_query_id(std::to_string(++query_counter));
  
  ResultSet result_set;
  
  // 检测写操作（简化：关键词匹配）
  if (IsWriteQuery(request->query())) {
    std::shared_lock<std::shared_mutex> engine_lock(engine_mutex_);
    if (!two_pc_engine_) {
      stats_.failed_queries++;
      response->set_success(false);
      response->set_error_msg("Write operations require distributed transaction engine, which is not initialized");
      return grpc::Status::OK;
    }
    // 构建简化的 write_set（使用 route_ctx 中的 entity_ids）
    std::vector<::cedar::CedarKey> read_set;
    std::vector<::cedar::CedarKey> write_set;
    auto now_ts = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    for (uint64_t entity_id : route_ctx.entity_ids) {
      write_set.emplace_back(entity_id, ::cedar::EntityType::Vertex, 0,
                             ::cedar::Timestamp(now_ts), 0, 0, 0,
                             CalculatePartition(entity_id));
    }
    
    // 检查是否在显式事务中
    std::string txn_id_str(request->txn_id().begin(), request->txn_id().end());
    if (!txn_id_str.empty()) {
      // 显式事务模式：累积 write_set 到事务上下文
      {
        std::lock_guard<std::mutex> lock(active_txns_mutex_);
        auto it = active_transactions_.find(txn_id_str);
        if (it == active_transactions_.end()) {
          stats_.failed_queries++;
          response->set_success(false);
          response->set_error_msg("Transaction not found: " + txn_id_str);
          return grpc::Status::OK;
        }
        it->second.write_set.insert(it->second.write_set.end(),
                                     write_set.begin(), write_set.end());
        it->second.has_writes = true;
      }
      result_set.set_total_rows(static_cast<int32_t>(route_ctx.entity_ids.size()));
    } else {
      // 自动事务模式（autocommit）：立即执行 2PC
      auto txn_id = next_txn_id_.fetch_add(1);
      auto write_status = two_pc_engine_->Execute2PC(txn_id, read_set, write_set,
                                                      ::cedar::Timestamp(now_ts));
      if (!write_status.ok()) {
        stats_.failed_queries++;
        response->set_success(false);
        response->set_error_msg("Distributed write failed: " + write_status.ToString());
        return grpc::Status::OK;
      }
      result_set.set_total_rows(static_cast<int32_t>(route_ctx.entity_ids.size()));
    }
  } else {
    // 执行查询
    if (route_ctx.target_partitions.empty()) {
      // 无特定分区：SCAN / AGGREGATE 需要广播到所有已知分区
      if (route_ctx.query_type == QueryType::SCAN ||
          route_ctx.query_type == QueryType::AGGREGATE) {
        std::shared_lock<std::shared_mutex> lock(partition_map_mutex_);
        for (const auto& [part_id, _route] : partition_cache_) {
          route_ctx.target_partitions.push_back(part_id);
        }
      }
      
      if (route_ctx.target_partitions.empty()) {
        // 无已知分区，返回空结果
        result_set.set_total_rows(0);
      } else {
        for (uint32_t part_id : route_ctx.target_partitions) {
          ExecutePartitionQuery(request->query(), part_id, route_ctx, &result_set);
        }
      }
    } else {
      // 执行分区查询并聚合结果
      for (uint32_t part_id : route_ctx.target_partitions) {
        ExecutePartitionQuery(request->query(), part_id, route_ctx, &result_set);
      }
    }
    
    // 读查询 read_set 累积（显式事务模式）
    std::string txn_id_str(request->txn_id().begin(), request->txn_id().end());
    if (!txn_id_str.empty() && !route_ctx.entity_ids.empty()) {
      std::lock_guard<std::mutex> lock(active_txns_mutex_);
      auto it = active_transactions_.find(txn_id_str);
      if (it != active_transactions_.end()) {
        auto now_ts = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
        for (uint64_t entity_id : route_ctx.entity_ids) {
          it->second.read_set.emplace_back(entity_id, ::cedar::EntityType::Vertex, 0,
                                           ::cedar::Timestamp(now_ts), 0, 0, 0,
                                           CalculatePartition(entity_id));
        }
      }
    }
  }
  
  // ========================================================================
  // 后处理：聚合、排序、分页（跨分区结果统一处理）
  // ========================================================================
  if (route_ctx.has_aggregate && result_set.rows_size() > 0) {
    // 计算聚合值，替换为多行 → 单行
    cedar::query::ResultSet aggregated;
    auto* agg_row = aggregated.add_rows();
    auto* agg_val = agg_row->add_values();
    
    const std::string& func = route_ctx.aggregate_function;
    
    if (func == "count") {
      agg_val->set_int_val(result_set.rows_size());
    } else if (func == "sum" || func == "avg") {
      double total = 0.0;
      int64_t count = 0;
      for (const auto& row : result_set.rows()) {
        if (row.values_size() > 0) {
          const auto& v = row.values(0);
          if (v.value_type_case() == cedar::query::Value::kIntVal) {
            total += static_cast<double>(v.int_val());
            count++;
          } else if (v.value_type_case() == cedar::query::Value::kFloatVal) {
            total += v.float_val();
            count++;
          }
        }
      }
      if (func == "avg" && count > 0) {
        agg_val->set_float_val(total / static_cast<double>(count));
      } else {
        agg_val->set_float_val(total);
      }
    } else if (func == "min" || func == "max") {
      bool first = true;
      double best = 0.0;
      for (const auto& row : result_set.rows()) {
        if (row.values_size() > 0) {
          const auto& v = row.values(0);
          double val = 0.0;
          bool has_val = false;
          if (v.value_type_case() == cedar::query::Value::kIntVal) {
            val = static_cast<double>(v.int_val());
            has_val = true;
          } else if (v.value_type_case() == cedar::query::Value::kFloatVal) {
            val = v.float_val();
            has_val = true;
          }
          if (has_val) {
            if (first) {
              best = val;
              first = false;
            } else if (func == "min" && val < best) {
              best = val;
            } else if (func == "max" && val > best) {
              best = val;
            }
          }
        }
      }
      if (!first) {
        agg_val->set_float_val(best);
      } else {
        agg_val->mutable_null_val();
      }
    }
    
    if (!route_ctx.return_columns.empty()) {
      aggregated.add_columns(route_ctx.return_columns[0]);
    }
    aggregated.set_total_rows(1);
    result_set = std::move(aggregated);
  }
  
  // ORDER BY 后处理
  if (route_ctx.has_order_by && result_set.rows_size() > 1) {
    auto& rows = *result_set.mutable_rows();
    std::sort(rows.begin(), rows.end(),
      [&route_ctx](const cedar::query::Row& a, const cedar::query::Row& b) {
        if (a.values_size() == 0 || b.values_size() == 0) return false;
        const auto& av = a.values(0);
        const auto& bv = b.values(0);
        bool less = false;
        // 简单比较：优先 int，其次 float，其次 string
        if (av.value_type_case() == cedar::query::Value::kIntVal &&
            bv.value_type_case() == cedar::query::Value::kIntVal) {
          less = av.int_val() < bv.int_val();
        } else if (av.value_type_case() == cedar::query::Value::kFloatVal &&
                   bv.value_type_case() == cedar::query::Value::kFloatVal) {
          less = av.float_val() < bv.float_val();
        } else if (av.value_type_case() == cedar::query::Value::kStringVal &&
                   bv.value_type_case() == cedar::query::Value::kStringVal) {
          less = av.string_val() < bv.string_val();
        }
        return route_ctx.order_ascending ? less : !less;
      });
  }
  
  // LIMIT / SKIP 后处理
  if (route_ctx.has_limit || route_ctx.skip > 0) {
    auto& rows = *result_set.mutable_rows();
    int64_t start = route_ctx.skip;
    if (start < 0) start = 0;
    if (start >= static_cast<int64_t>(rows.size())) {
      rows.Clear();
    } else {
      int64_t end = rows.size();
      if (route_ctx.has_limit && route_ctx.limit >= 0) {
        end = std::min(end, start + route_ctx.limit);
      }
      if (start > 0 || end < static_cast<int64_t>(rows.size())) {
        std::vector<cedar::query::Row> sliced;
        sliced.reserve(end - start);
        for (int64_t i = start; i < end; ++i) {
          sliced.push_back(std::move(rows[i]));
        }
        rows.Clear();
        for (auto& r : sliced) {
          *rows.Add() = std::move(r);
        }
      }
    }
    result_set.set_total_rows(static_cast<int32_t>(rows.size()));
  }
  
  // 填充响应
  response->set_success(true);
  *response->mutable_result_set() = result_set;
  
  // 执行统计
  auto end_time = std::chrono::steady_clock::now();
  auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
      end_time - start_time).count();
  
  auto* stats = response->mutable_stats();
  stats->set_execution_time_us(latency_us);
  stats->set_rows_scanned(result_set.total_rows());
  stats->set_rows_returned(result_set.rows_size());
  stats->set_storage_nodes_accessed(route_ctx.target_partitions.size());
  
  stats_.total_latency_us += latency_us;
  RecordLatency(static_cast<uint64_t>(latency_us));
  
  // 将结果放入缓存
  if (!request->explain_only() && query_cache_ != nullptr) {
    cedar::query::CacheKey cache_key;
    cache_key.query_fingerprint = GenerateQueryFingerprint(request->query());
    cache_key.partition_hash = 0;
    cache_key.as_of_timestamp = 0;
    query_cache_->Put(cache_key, response->result_set());
  }
  
  // EXPLAIN 模式
  if (request->explain_only()) {
    std::stringstream plan;
    plan << "Execution Plan:\n";
    plan << "  Query: " << request->query().substr(0, 50) << "...\n";
    
    const char* type_str = "SCAN";
    switch (route_ctx.query_type) {
      case QueryType::POINT_LOOKUP: type_str = "POINT_LOOKUP"; break;
      case QueryType::SCAN: type_str = "SCAN"; break;
      case QueryType::NEIGHBOR_TRAVERSAL: type_str = "NEIGHBOR_TRAVERSAL"; break;
      case QueryType::AGGREGATE: type_str = "AGGREGATE"; break;
      default:
        std::cerr << "[GraphServiceRouter] Unknown query type" << std::endl;
        break;
    }
    plan << "  Query Type: " << type_str << "\n";
    plan << "  Target Partitions: " << route_ctx.target_partitions.size() << "\n";
    for (auto part_id : route_ctx.target_partitions) {
      plan << "    - Partition " << part_id << "\n";
    }
    if (route_ctx.query_type == QueryType::NEIGHBOR_TRAVERSAL) {
      plan << "  Traversal: start_node=" << route_ctx.start_node_id
           << ", hops=" << route_ctx.hops
           << ", edge_type=" << route_ctx.edge_type << "\n";
    }
    if (route_ctx.has_aggregate) {
      plan << "  Aggregate: " << route_ctx.aggregate_function
           << "(" << (route_ctx.aggregate_column.empty() ? "*" : route_ctx.aggregate_column)
           << ")\n";
    }
    if (route_ctx.has_order_by) {
      plan << "  Order By: " << route_ctx.order_by_column
           << " " << (route_ctx.order_ascending ? "ASC" : "DESC") << "\n";
    }
    if (route_ctx.has_limit) {
      plan << "  Limit: " << route_ctx.limit;
      if (route_ctx.skip > 0) plan << " OFFSET " << route_ctx.skip;
      plan << "\n";
    }
    response->set_execution_plan(plan.str());
  }
  
  return grpc::Status::OK;
}

grpc::Status GraphServiceRouter::Traverse(grpc::ServerContext* context,
                                          const TraverseRequest* request,
                                          TraverseResponse* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }

  // 优先路由到 GCN（支持多 GCN Scatter-Gather）
  if (gcn_router_ && !gcn_peer_addresses_.empty()) {
    cedar::gcn::TraversalRequest gcn_request;
    gcn_request.set_trace_id("graphd-traverse");
    gcn_request.set_root_entity_id(request->start_node_id());
    gcn_request.set_query_time(request->as_of_timestamp() > 0
                                ? request->as_of_timestamp() : UINT64_MAX);
    gcn_request.set_max_hops(request->max_depth() > 0 ? request->max_depth() : 3);
    gcn_request.set_edge_type(request->edge_types_size() > 0 ? request->edge_types(0) : 0);

    auto gcn_response = gcn_router_->ScatterTraversalByEntity(gcn_request);
    if (gcn_response.success()) {
      response->set_success(true);
      response->set_nodes_visited(gcn_response.visited_entity_ids_size());
      for (const auto& entity_id : gcn_response.visited_entity_ids()) {
        auto* path = response->add_paths();
        path->add_nodes()->set_id(entity_id);
      }
      return grpc::Status::OK;
    }
    // GCN 失败时回退到 StorageD
  }
  
  // 回退：路由到 StorageD
  uint32_t partition_id = CalculatePartition(request->start_node_id());
  
  auto route_result = GetPartitionRoute(partition_id);
  if (!route_result.ok()) {
    response->set_success(false);
    response->set_error_msg("Failed to get partition route: " + 
                           route_result.status().ToString());
    return grpc::Status::OK;
  }
  
  auto route = route_result.ValueOrDie();
  
  auto stub = GetStorageStub(route.leader_node);
  if (!stub) {
    response->set_success(false);
    response->set_error_msg("Failed to connect to storage node: " + route.leader_node);
    return grpc::Status::OK;
  }
  
  ScanRequest scan_request;
  scan_request.set_entity_id(request->start_node_id());
  
  if (request->as_of_timestamp() > 0) {
    scan_request.set_start_time(request->as_of_timestamp());
    scan_request.set_end_time(request->as_of_timestamp());
  } else {
    scan_request.set_start_time(0);
    scan_request.set_end_time(UINT64_MAX);
  }
  
  scan_request.set_partition_id(partition_id);
  
  ScanResponse scan_response;
  grpc::ClientContext client_context;
  client_context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
  auto grpc_status = stub->Scan(&client_context, scan_request, &scan_response);
  
  if (!grpc_status.ok()) {
    response->set_success(false);
    response->set_error_msg("Storage query failed: " + grpc_status.error_message());
    return grpc::Status::OK;
  }
  
  if (!scan_response.success()) {
    response->set_success(false);
    response->set_error_msg(scan_response.error_msg());
    return grpc::Status::OK;
  }
  
  response->set_success(true);
  response->set_nodes_visited(scan_response.items_size());
  
  for (const auto& item : scan_response.items()) {
    auto* path = response->add_paths();
    (void)path;
    (void)item;
  }
  
  return grpc::Status::OK;
}

grpc::Status GraphServiceRouter::TemporalQuery(grpc::ServerContext* context,
                                               const TemporalQueryRequest* request,
                                               TemporalQueryResponse* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }

  // 计算实体分区
  uint32_t partition_id = CalculatePartition(request->entity_id());
  
  // 获取分区路由
  auto route_result = GetPartitionRoute(partition_id);
  if (!route_result.ok()) {
    response->set_success(false);
    response->set_error_msg(route_result.status().ToString());
    return grpc::Status::OK;
  }
  
  auto route = route_result.ValueOrDie();
  auto stub = GetStorageStub(route.leader_node);
  
  if (!stub) {
    response->set_success(false);
    response->set_error_msg("Failed to connect to storage");
    return grpc::Status::OK;
  }
  
  // 构建 Scan 请求
  ScanRequest scan_request;
  scan_request.set_entity_id(request->entity_id());
  scan_request.set_partition_id(partition_id);
  
  // 根据查询类型设置时间范围
  switch (request->query_type()) {
    case TemporalQueryRequest::AS_OF:
      scan_request.set_start_time(request->timestamp());
      scan_request.set_end_time(request->timestamp());
      break;
    case TemporalQueryRequest::VERSION_HISTORY:
      scan_request.set_start_time(0);
      scan_request.set_end_time(UINT64_MAX);
      break;
    case TemporalQueryRequest::CHANGES_IN_RANGE:
      scan_request.set_start_time(request->start_time());
      scan_request.set_end_time(request->end_time());
      break;
    case TemporalQueryRequest::LATEST:
    default:
      scan_request.set_start_time(0);
      scan_request.set_end_time(UINT64_MAX);
      break;
  }
  
  ScanResponse scan_response;
  grpc::ClientContext client_context;
  client_context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
  auto status = stub->Scan(&client_context, scan_request, &scan_response);
  
  if (!status.ok() || !scan_response.success()) {
    response->set_success(false);
    response->set_error_msg(status.ok() ? scan_response.error_msg() : status.error_message());
    return grpc::Status::OK;
  }
  
  // 转换结果
  response->set_success(true);
  for (const auto& item : scan_response.items()) {
    auto* version = response->add_versions();
    version->set_timestamp(item.timestamp());
    // version->set_version_number(...);
    // version->set_is_deleted(...);
    // 解析属性...
    (void)version;
  }
  
  return grpc::Status::OK;
}

grpc::Status GraphServiceRouter::Health(grpc::ServerContext* context,
                                        const HealthRequest* request,
                                        HealthResponse* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }
  (void)request;
  
  // 检查 MetaD 连接
  auto meta_nodes = meta_client_->GetAliveNodes();
  bool meta_healthy = meta_nodes.ok();
  
  response->set_healthy(meta_healthy);
  response->set_status(meta_healthy ? "healthy" : "degraded");
  response->set_meta_client_healthy(meta_healthy);
  response->set_parser_healthy(true);
  response->set_planner_healthy(true);
  response->set_executor_healthy(true);
  response->set_storage_client_healthy(meta_healthy);  // 依赖 MetaD
  
  response->set_active_queries(stats_.active_queries.load());
  response->set_queued_queries(0);
  
  return grpc::Status::OK;
}

grpc::Status GraphServiceRouter::GetStats(grpc::ServerContext* context,
                                          const QueryStatsRequest* request,
                                          QueryStatsResponse* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }
  (void)request;
  
  uint64_t total = stats_.total_queries.load();
  uint64_t failed = stats_.failed_queries.load();
  uint64_t total_latency = stats_.total_latency_us.load();
  
  response->set_total_queries(total);
  response->set_failed_queries(failed);
  response->set_cached_plans(stats_.cached_plans.load());
  
  if (total > 0) {
    response->set_avg_latency_us(total_latency / total);
  }
  
  response->set_p99_latency_us(GetP99Latency());
  response->set_queries_per_second(GetQPS());
  
  return grpc::Status::OK;
}

grpc::Status GraphServiceRouter::StreamQuery(grpc::ServerContext* context,
                                             const StreamQueryRequest* request,
                                             grpc::ServerWriter<StreamQueryResponse>* writer) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }
  
  // Execute the query and stream results in batches
  ExecuteQueryRequest exec_request;
  exec_request.set_query(request->query());
  *exec_request.mutable_parameters() = request->parameters();
  exec_request.set_explain_only(false);
  
  ExecuteQueryResponse exec_response;
  auto status = ExecuteQuery(context, &exec_request, &exec_response);
  if (!status.ok()) {
    return status;
  }
  
  if (!exec_response.success()) {
    StreamQueryResponse error_response;
    error_response.set_success(false);
    error_response.set_error_msg(exec_response.error_msg());
    if (!writer->Write(error_response)) {
      return grpc::Status(grpc::StatusCode::INTERNAL, "Stream write failed");
    }
    return grpc::Status::OK;
  }

  // Stream result rows in batches
  const auto& result_set = exec_response.result_set();
  constexpr size_t kBatchSize = 100;
  size_t total_rows = static_cast<size_t>(result_set.rows_size());

  for (size_t offset = 0; offset < total_rows; offset += kBatchSize) {
    StreamQueryResponse batch;
    batch.set_success(true);
    batch.set_query_id(request->query_id());
    batch.set_batch_index(static_cast<int32_t>(offset / kBatchSize));
    batch.set_has_more(offset + kBatchSize < total_rows);

    size_t end = std::min(offset + kBatchSize, total_rows);
    for (size_t i = offset; i < end; ++i) {
      *batch.mutable_batch()->add_rows() = result_set.rows(i);
    }

    if (!writer->Write(batch)) {
      return grpc::Status(grpc::StatusCode::INTERNAL, "Stream write failed");
    }
  }

  // Send terminal empty batch for zero-row results or to mark stream end
  if (total_rows == 0) {
    StreamQueryResponse empty_batch;
    empty_batch.set_success(true);
    empty_batch.set_query_id(request->query_id());
    empty_batch.set_batch_index(0);
    empty_batch.set_has_more(false);
    empty_batch.set_progress_percent(100);
    if (!writer->Write(empty_batch)) {
      return grpc::Status(grpc::StatusCode::INTERNAL, "Stream write failed");
    }
  }

  return grpc::Status();
}

grpc::Status GraphServiceRouter::BatchQuery(grpc::ServerContext* context,
                                            const BatchQueryRequest* request,
                                            BatchQueryResponse* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }
  
  for (const auto& item : request->queries()) {
    auto* result = response->add_results();
    result->set_query_id(item.query_id());
    
    ExecuteQueryRequest single_request;
    single_request.set_query(item.query());
    *single_request.mutable_parameters() = item.parameters();
    
    ExecuteQueryResponse single_response;
    grpc::ServerContext dummy_context;
    auto status = ExecuteQuery(&dummy_context, &single_request, &single_response);
    
    if (status.ok()) {
      result->set_success(single_response.success());
      result->set_error_msg(single_response.error_msg());
      *result->mutable_result_set() = single_response.result_set();
      *result->mutable_stats() = single_response.stats();
    } else {
      result->set_success(false);
      result->set_error_msg(status.error_message());
    }
  }
  
  response->set_success(true);
  return grpc::Status::OK;
}

grpc::Status GraphServiceRouter::GetSchema(grpc::ServerContext* context,
                                           const cedar::query::GetSchemaRequest* request,
                                           cedar::query::GetSchemaResponse* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }

  if (!meta_client_) {
    response->set_success(false);
    response->set_error_msg("MetaD client not initialized");
    return grpc::Status::OK;
  }

  cedar::meta::GetSchemaRequest meta_req;
  meta_req.set_space_name("default");  // TODO: multi-space support
  for (const auto& label : request->labels()) {
    meta_req.add_labels(label);
  }

  cedar::meta::GetSchemaResponse meta_resp;
  grpc::ClientContext meta_ctx;
  meta_ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

  auto stub = meta_client_->GetStub();
  if (!stub) {
    response->set_success(false);
    response->set_error_msg("No MetaD connection");
    return grpc::Status::OK;
  }

  auto status = stub->GetSchema(&meta_ctx, meta_req, &meta_resp);
  if (!status.ok()) {
    response->set_success(false);
    response->set_error_msg("MetaD schema query failed: " + status.error_message());
    return grpc::Status::OK;
  }

  response->set_success(meta_resp.success());
  if (!meta_resp.success()) {
    response->set_error_msg(meta_resp.error_msg());
    return grpc::Status::OK;
  }

  for (const auto& proto_label : meta_resp.labels()) {
    auto* out = response->add_labels();
    out->set_name(proto_label.name());
    for (const auto& proto_prop : proto_label.properties()) {
      auto* out_prop = out->add_properties();
      out_prop->set_name(proto_prop.name());
      out_prop->set_type(proto_prop.type());
      out_prop->set_nullable(proto_prop.nullable());
      out_prop->set_indexed(proto_prop.indexed());
    }
    for (const auto& idx : proto_label.indexes()) {
      out->add_indexes(idx);
    }
  }
  return grpc::Status::OK;
}

// ========== 私有方法 ==========

Status GraphServiceRouter::ParseQueryForRouting(const std::string& query, 
                                                QueryRouteContext* route_ctx) {
  // 使用 Cypher 解析器解析查询
  cypher::CypherParser parser(query);
  auto stmt = parser.ParseStatement();
  
  if (!stmt) {
    return Status::InvalidArgument("Parse error: " + parser.GetError());
  }
  
  // 保留原有的正则启发式提取（兜底）
  std::regex id_pattern(R"(id\s*\(\s*\w+\s*\)\s*=\s*(\d+))", std::regex::icase);
  std::smatch match;
  std::string::const_iterator search_start(query.cbegin());
  while (std::regex_search(search_start, query.cend(), match, id_pattern)) {
    uint64_t entity_id = std::stoull(match[1].str());
    route_ctx->entity_ids.push_back(entity_id);
    route_ctx->target_partitions.push_back(CalculatePartition(entity_id));
    search_start = match.suffix().first;
  }
  
  // ==========================================================================
  // Cypher AST 分析 - 识别查询模式、聚合、排序、分页
  // ==========================================================================
  bool has_match = false;
  bool has_where_id_filter = false;
  
  for (const auto& clause : stmt->clauses) {
    if (!clause) continue;
    
    switch (clause->clause_type) {
      // ---- MATCH 子句分析 ----
      case cypher::ClauseType::MATCH: {
        has_match = true;
        auto* match_clause = static_cast<cypher::MatchClause*>(clause.get());
        
        for (const auto& pattern : match_clause->patterns) {
          if (pattern.elements.empty()) continue;
          
          // 分析路径模式的第一个元素
          // 单节点模式: [(n)] → SCAN
          // 邻域模式: [(n), -[e]->, (m)] → NEIGHBOR_TRAVERSAL
          if (pattern.elements.size() >= 3) {
            // 多元素路径 → 邻域遍历
            route_ctx->query_type = QueryType::NEIGHBOR_TRAVERSAL;
            
            // 提取关系信息
            for (size_t i = 1; i < pattern.elements.size(); i += 2) {
              if (std::holds_alternative<cypher::RelationshipPattern>(pattern.elements[i])) {
                auto& rel = std::get<cypher::RelationshipPattern>(pattern.elements[i]);
                route_ctx->direction = rel.direction;
                if (!rel.types.empty()) {
                  // 将第一个边类型字符串 hash 为 uint32_t（简化）
                  route_ctx->edge_type = std::hash<std::string>{}(rel.types[0]) & 0xFFFFFFFF;
                }
                // 计算跳数（min_hops/max_hops 默认为 1）
                if (rel.max_hops.has_value()) {
                  route_ctx->hops = static_cast<uint32_t>(rel.max_hops.value());
                } else if (rel.min_hops.has_value()) {
                  route_ctx->hops = static_cast<uint32_t>(rel.min_hops.value());
                }
              }
            }
          }
        }
        break;
      }
      
      // ---- WHERE 子句分析 ----
      case cypher::ClauseType::WHERE: {
        auto* where_clause = static_cast<cypher::WhereClause*>(clause.get());
        if (where_clause->condition) {
          // 检查是否是 id(n) = xxx 的比较
          if (where_clause->condition->expr_type == cypher::ExprType::COMPARISON) {
            auto* cmp = static_cast<cypher::ComparisonExpr*>(where_clause->condition.get());
            if (cmp->op == cypher::ComparisonExpr::EQ) {
              has_where_id_filter = true;
              // 已经通过正则提取了 entity_ids
              if (!route_ctx->entity_ids.empty()) {
                route_ctx->start_node_id = route_ctx->entity_ids[0];
              }
            }
          }
        }
        break;
      }
      
      // ---- RETURN 子句分析 ----
      case cypher::ClauseType::RETURN: {
        auto* return_clause = static_cast<cypher::ReturnClause*>(clause.get());
        route_ctx->return_all = return_clause->all;
        
        for (const auto& item : return_clause->items) {
          if (!item.expression) continue;
          
          if (item.expression->expr_type == cypher::ExprType::FUNCTION_CALL) {
            auto* func = static_cast<cypher::FunctionCallExpr*>(item.expression.get());
            std::string func_name = func->name;
            // 统一转小写比较
            std::transform(func_name.begin(), func_name.end(), func_name.begin(), ::tolower);
            
            if (func_name == "count" || func_name == "sum" || 
                func_name == "avg" || func_name == "min" || func_name == "max") {
              route_ctx->has_aggregate = true;
              route_ctx->aggregate_function = func_name;
              route_ctx->query_type = QueryType::AGGREGATE;
              
              // 提取聚合列（如果有参数）
              if (!func->arguments.empty()) {
                auto& arg = func->arguments[0];
                if (arg->expr_type == cypher::ExprType::VARIABLE) {
                  route_ctx->aggregate_column = 
                      static_cast<cypher::VariableExpr*>(arg.get())->name;
                } else if (arg->expr_type == cypher::ExprType::PROPERTY) {
                  route_ctx->aggregate_column = 
                      static_cast<cypher::PropertyExpr*>(arg.get())->property;
                }
              }
            }
            
            // 列别名
            if (item.alias.has_value()) {
              route_ctx->return_columns.push_back(item.alias.value());
            } else {
              route_ctx->return_columns.push_back(func_name + "(...)");
            }
          } else if (item.expression->expr_type == cypher::ExprType::VARIABLE) {
            route_ctx->return_columns.push_back(
                static_cast<cypher::VariableExpr*>(item.expression.get())->name);
          } else if (item.expression->expr_type == cypher::ExprType::PROPERTY) {
            route_ctx->return_columns.push_back(
                static_cast<cypher::PropertyExpr*>(item.expression.get())->property);
          }
        }
        break;
      }
      
      // ---- ORDER BY 子句分析 ----
      case cypher::ClauseType::ORDER_BY: {
        auto* order_clause = static_cast<cypher::OrderByClause*>(clause.get());
        route_ctx->has_order_by = true;
        if (!order_clause->items.empty()) {
          auto& first = order_clause->items[0];
          if (first.expression) {
            if (first.expression->expr_type == cypher::ExprType::VARIABLE) {
              route_ctx->order_by_column = 
                  static_cast<cypher::VariableExpr*>(first.expression.get())->name;
            } else if (first.expression->expr_type == cypher::ExprType::PROPERTY) {
              route_ctx->order_by_column = 
                  static_cast<cypher::PropertyExpr*>(first.expression.get())->property;
            }
          }
          route_ctx->order_ascending = first.ascending;
        }
        break;
      }
      
      // ---- LIMIT 子句分析 ----
      case cypher::ClauseType::LIMIT: {
        auto* limit_clause = static_cast<cypher::LimitClause*>(clause.get());
        route_ctx->has_limit = true;
        if (limit_clause->expression && 
            limit_clause->expression->expr_type == cypher::ExprType::LITERAL) {
          auto* lit = static_cast<cypher::LiteralExpr*>(limit_clause->expression.get());
          if (lit->value.IsInt()) {
            route_ctx->limit = lit->value.GetInt();
          } else if (lit->value.IsFloat()) {
            route_ctx->limit = static_cast<int64_t>(lit->value.GetFloat());
          }
        }
        break;
      }
      
      // ---- SKIP 子句分析 ----
      case cypher::ClauseType::SKIP: {
        auto* skip_clause = static_cast<cypher::SkipClause*>(clause.get());
        if (skip_clause->expression && 
            skip_clause->expression->expr_type == cypher::ExprType::LITERAL) {
          auto* lit = static_cast<cypher::LiteralExpr*>(skip_clause->expression.get());
          if (lit->value.IsInt()) {
            route_ctx->skip = lit->value.GetInt();
          } else if (lit->value.IsFloat()) {
            route_ctx->skip = static_cast<int64_t>(lit->value.GetFloat());
          }
        }
        break;
      }
      
      default:
        break;
    }
  }
  
  // ---- 确定查询类型 ----
  if (route_ctx->has_aggregate) {
    route_ctx->query_type = QueryType::AGGREGATE;
  } else if (route_ctx->query_type == QueryType::NEIGHBOR_TRAVERSAL) {
    // 已经是邻域遍历
  } else if (has_where_id_filter && !route_ctx->entity_ids.empty()) {
    route_ctx->query_type = QueryType::POINT_LOOKUP;
  } else {
    route_ctx->query_type = QueryType::SCAN;
  }
  
  // 如果没有找到特定 ID 的目标分区，后续处理为全部分区
  
  // 检查时态约束
  auto temporal_clause = parser.GetTemporalClause();
  if (temporal_clause) {
    route_ctx->has_temporal_constraint = true;
  }
  
  return Status::OK();
}

uint32_t GraphServiceRouter::CalculatePartition(uint64_t entity_id) {
  // 如果双模式分区策略已初始化，使用它
  if (partition_strategy_) {
    // 构建一个临时的 CedarKey 用于分区计算
    cedar::CedarKey key = cedar::CedarKey::Vertex(
        entity_id, 
        cedar::VertexColumnId(0), 
        cedar::Timestamp::Now());
    
    return partition_strategy_->ComputePartition(key, kNumPartitions);
  }
  
  // 默认使用简单的哈希
  return static_cast<uint32_t>(entity_id % kNumPartitions);
}

StatusOr<PartitionRoute> GraphServiceRouter::GetPartitionRoute(uint32_t partition_id) {
  // 先查缓存
  {
    std::shared_lock<std::shared_mutex> lock(partition_map_mutex_);
    auto it = partition_cache_.find(partition_id);
    if (it != partition_cache_.end()) {
      return it->second;
    }
  }
  
  // 缓存未命中，从 MetaD 获取
  auto assign_result = meta_client_->GetPartitionAssignment("default", partition_id);
  if (!assign_result.ok()) {
    return Status::InvalidArgument("MetaD error: " + assign_result.status().ToString());
  }
  
  // 构建路由信息
  PartitionRoute route;
  route.partition_id = partition_id;

  const auto& assignment = assign_result.value();
  auto leader_addr = GetNodeAddress(assignment.leader_node);
  if (!leader_addr.ok()) {
    return leader_addr.status();
  }
  route.leader_node = leader_addr.value();

  for (uint32_t replica : assignment.follower_nodes) {
    auto replica_addr = GetNodeAddress(replica);
    if (!replica_addr.ok()) {
      return replica_addr.status();
    }
    route.replicas.push_back(replica_addr.value());
  }
  
  // 更新缓存
  {
    std::unique_lock<std::shared_mutex> lock(partition_map_mutex_);
    partition_cache_[partition_id] = route;
  }
  
  return route;
}

std::shared_ptr<cedar::storage::StorageService::Stub>
GraphServiceRouter::GetStorageStub(const std::string& node_addr) {
  // Fast path: read-only lookup with shared lock
  {
    std::shared_lock<std::shared_mutex> lock(stubs_mutex_);
    auto it = storage_stubs_.find(node_addr);
    if (it != storage_stubs_.end()) {
      auto ch_it = storage_channels_.find(node_addr);
      if (ch_it == storage_channels_.end() ||
          ch_it->second->GetState(false) != GRPC_CHANNEL_TRANSIENT_FAILURE) {
        return it->second;
      }
      // Channel in TRANSIENT_FAILURE: fall through to recreate
    }
  }

  // Slow path: create connection without holding the lock
  auto storage_creds = cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentials(tls_config_);
  if (!storage_creds) storage_creds = cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentialsFromEnv();
  auto channel = grpc::CreateChannel(node_addr, storage_creds);
  std::shared_ptr<cedar::storage::StorageService::Stub> stub(
      cedar::storage::StorageService::NewStub(channel).release());

  // Insert with exclusive lock
  std::unique_lock<std::shared_mutex> lock(stubs_mutex_);
  auto it = storage_stubs_.find(node_addr);
  if (it != storage_stubs_.end()) {
    return it->second;  // Another thread created it first
  }
  // Evict oldest entry if cache is full
  if (storage_stubs_.size() >= kMaxCachedStubs) {
    auto evict_it = storage_stubs_.begin();
    storage_channels_.erase(evict_it->first);
    storage_stubs_.erase(evict_it);
  }
  storage_stubs_[node_addr] = stub;
  storage_channels_[node_addr] = channel;
  lock.unlock();  // Release lock before blocking I/O
  
  // 同步创建 StorageClient 供 2PC 引擎使用 (在锁外执行阻塞I/O)
  {
    std::shared_lock<std::shared_mutex> engine_lock(engine_mutex_);
    if (two_pc_engine_) {
      auto client = std::make_shared<cedar::dtx::StorageClient>();
      cedar::dtx::StorageClient::ClientConfig client_config;
      client_config.server_address = node_addr;
      client_config.tls = tls_config_;
      auto s = client->Initialize(client_config);
      if (s.ok()) {
        two_pc_engine_->AddClient(client);
        storage_clients_.push_back(client);
      } else {
        std::cerr << "[GraphD] Failed to create StorageClient for 2PC: " << node_addr
                  << " - " << s.ToString() << std::endl;
      }
    }
  }
  
  return stub;
}

StatusOr<std::string> GraphServiceRouter::GetNodeAddress(uint32_t node_id) {
  // Fast path: check local cache
  {
    std::shared_lock<std::shared_mutex> lock(node_map_mutex_);
    auto it = node_address_cache_.find(node_id);
    if (it != node_address_cache_.end()) {
      return it->second;
    }
  }

  // Slow path: fetch from MetaD
  if (meta_client_) {
    auto nodes_result = meta_client_->GetAliveNodes();
    if (nodes_result.ok()) {
      std::unique_lock<std::shared_mutex> lock(node_map_mutex_);
      for (const auto& node : nodes_result.value()) {
        node_address_cache_[node.node_id] = node.address;
      }
      auto it = node_address_cache_.find(node_id);
      if (it != node_address_cache_.end()) {
        return it->second;
      }
    }
  }

  return Status::IOError("Node address unknown and MetaD unreachable");
}

namespace {

// Helper: Convert Cedar Descriptor to query Value protobuf
void DescriptorToQueryValue(const cedar::Descriptor& desc,
                            cedar::query::Value* out) {
  switch (desc.GetKind()) {
    case cedar::EntryKind::InlineInt: {
      auto v = desc.AsInlineInt();
      if (v.has_value()) {
        out->set_int_val(v.value());
      } else {
        out->mutable_null_val();
      }
      break;
    }
    case cedar::EntryKind::InlineFloat: {
      auto v = desc.AsInlineFloat();
      if (v.has_value()) {
        out->set_float_val(v.value());
      } else {
        out->mutable_null_val();
      }
      break;
    }
    case cedar::EntryKind::InlineShortStr: {
      out->set_string_val(desc.AsInlineShortStr());
      break;
    }
    case cedar::EntryKind::Tombstone: {
      out->mutable_null_val();
      break;
    }
    default: {
      // ExternalRef, EdgeRef, Metadata -> expose raw bytes
      uint64_t raw = desc.AsRaw();
      out->set_bytes_val(std::string(reinterpret_cast<const char*>(&raw), sizeof(raw)));
      break;
    }
  }
}

}  // namespace

Status GraphServiceRouter::ExecutePartitionQuery(
    const std::string& query,
    uint32_t partition_id,
    const QueryRouteContext& route_ctx,
    cedar::query::ResultSet* result) {
  // Get partition route
  auto route_result = GetPartitionRoute(partition_id);
  if (!route_result.ok()) {
    std::cerr << "[GraphD] Failed to get route for partition " << partition_id
              << ": " << route_result.status().ToString() << std::endl;
    return route_result.status();
  }
  auto route = route_result.ValueOrDie();

  // Get storage stub
  auto stub = GetStorageStub(route.leader_node);
  if (!stub) {
    std::cerr << "[GraphD] Failed to get storage stub for partition " << partition_id
              << " at " << route.leader_node << std::endl;
    return Status::IOError("Failed to connect to storage node");
  }

  size_t rows_added = 0;

  switch (route_ctx.query_type) {
    // ========================================================================
    // POINT_LOOKUP: 单点精确查询
    // ========================================================================
    case QueryType::POINT_LOOKUP: {
      for (uint64_t entity_id : route_ctx.entity_ids) {
        if (CalculatePartition(entity_id) != partition_id) {
          continue;
        }

        cedar::storage::GetRequest get_req;
        auto* key = get_req.mutable_key();
        key->set_entity_id(entity_id);
        key->set_partition_id(partition_id);
        key->set_timestamp(UINT64_MAX);

        cedar::storage::GetResponse get_resp;
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
        auto grpc_status = stub->Get(&ctx, get_req, &get_resp);

        if (!grpc_status.ok() || !get_resp.success() || !get_resp.found()) {
          continue;
        }

        auto* row = result->add_rows();
        auto* val = row->add_values();
        const std::string& data = get_resp.descriptor_().data();
        if (data.size() >= sizeof(uint64_t)) {
          uint64_t raw;
          std::memcpy(&raw, data.data(), sizeof(uint64_t));
          cedar::Descriptor desc(raw);
          DescriptorToQueryValue(desc, val);
        } else {
          val->mutable_null_val();
        }
        rows_added++;
      }
      break;
    }

    // ========================================================================
    // SCAN: 全分区扫描
    // ========================================================================
    case QueryType::SCAN: {
      cedar::storage::ScanNodeRequestV2 scan_req;
      scan_req.set_node_id(0);  // scan all nodes
      scan_req.set_start_time(0);
      scan_req.set_end_time(UINT64_MAX);
      scan_req.set_partition_id(partition_id);

      cedar::storage::ScanResponse scan_resp;
      grpc::ClientContext ctx;
      ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
      auto grpc_status = stub->ScanNodeV2(&ctx, scan_req, &scan_resp);

      if (grpc_status.ok() && scan_resp.success()) {
        for (const auto& item : scan_resp.items()) {
          auto* row = result->add_rows();
          auto* val = row->add_values();
          const std::string& data = item.descriptor_().data();
          if (data.size() >= sizeof(uint64_t)) {
            uint64_t raw;
            std::memcpy(&raw, data.data(), sizeof(uint64_t));
            cedar::Descriptor desc(raw);
            DescriptorToQueryValue(desc, val);
          } else {
            val->mutable_null_val();
          }
          rows_added++;
        }
      }
      break;
    }

    // ========================================================================
    // NEIGHBOR_TRAVERSAL: 邻域遍历
    // ========================================================================
    case QueryType::NEIGHBOR_TRAVERSAL: {
      uint64_t start_id = route_ctx.start_node_id;
      if (start_id == 0 && !route_ctx.entity_ids.empty()) {
        start_id = route_ctx.entity_ids[0];
      }
      if (start_id == 0 || CalculatePartition(start_id) != partition_id) {
        break;  // 起始节点不在此分区
      }

      // 调用 ScanEdgeV2 获取邻域边
      cedar::storage::ScanEdgeRequestV2 edge_req;
      edge_req.set_node_id(start_id);
      edge_req.set_edge_type(route_ctx.edge_type);
      edge_req.set_direction(static_cast<cedar::storage::Direction>(route_ctx.direction));
      edge_req.set_start_time(0);
      edge_req.set_end_time(UINT64_MAX);
      edge_req.set_partition_id(partition_id);

      cedar::storage::ScanResponse edge_resp;
      grpc::ClientContext ctx;
      ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
      auto grpc_status = stub->ScanEdgeV2(&ctx, edge_req, &edge_resp);

      if (!grpc_status.ok() || !edge_resp.success()) {
        std::cerr << "[GraphD] ScanEdgeV2 failed for partition " << partition_id
                  << ": " << grpc_status.error_message() << std::endl;
        break;
      }

      // 对每个邻接边，获取目标节点
      for (const auto& item : edge_resp.items()) {
        uint64_t neighbor_id = 0;
        const std::string& data = item.descriptor_().data();
        if (data.size() >= sizeof(uint64_t)) {
          std::memcpy(&neighbor_id, data.data(), sizeof(uint64_t));
        }
        if (neighbor_id == 0) continue;

        cedar::storage::GetRequest get_req;
        auto* key = get_req.mutable_key();
        key->set_entity_id(neighbor_id);
        key->set_partition_id(partition_id);
        key->set_timestamp(UINT64_MAX);

        cedar::storage::GetResponse get_resp;
        grpc::ClientContext get_ctx;
        get_ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
        auto get_status = stub->Get(&get_ctx, get_req, &get_resp);

        if (get_status.ok() && get_resp.success() && get_resp.found()) {
          auto* row = result->add_rows();
          auto* val = row->add_values();
          const std::string& ndata = get_resp.descriptor_().data();
          if (ndata.size() >= sizeof(uint64_t)) {
            uint64_t raw;
            std::memcpy(&raw, ndata.data(), sizeof(uint64_t));
            cedar::Descriptor desc(raw);
            DescriptorToQueryValue(desc, val);
          } else {
            val->mutable_null_val();
          }
          rows_added++;
        }
      }
      break;
    }

    // ========================================================================
    // AGGREGATE: 聚合查询 - 返回原始行，由上层计算聚合
    // ========================================================================
    case QueryType::AGGREGATE: {
      // 聚合查询的数据收集策略：先 SCAN 收集所有原始行
      cedar::storage::ScanNodeRequestV2 scan_req;
      scan_req.set_node_id(0);
      scan_req.set_start_time(0);
      scan_req.set_end_time(UINT64_MAX);
      scan_req.set_partition_id(partition_id);

      cedar::storage::ScanResponse scan_resp;
      grpc::ClientContext ctx;
      ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
      auto grpc_status = stub->ScanNodeV2(&ctx, scan_req, &scan_resp);

      if (grpc_status.ok() && scan_resp.success()) {
        for (const auto& item : scan_resp.items()) {
          auto* row = result->add_rows();
          auto* val = row->add_values();
          const std::string& data = item.descriptor_().data();
          if (data.size() >= sizeof(uint64_t)) {
            uint64_t raw;
            std::memcpy(&raw, data.data(), sizeof(uint64_t));
            cedar::Descriptor desc(raw);
            DescriptorToQueryValue(desc, val);
          } else {
            val->mutable_null_val();
          }
          rows_added++;
        }
      }
      break;
    }
    default:
      std::cerr << "[GraphServiceRouter] Unknown query type" << std::endl;
      return Status::InvalidArgument("GraphServiceRouter", "Unknown query type");
  }

  result->set_total_rows(result->total_rows() + static_cast<int32_t>(rows_added));
  return Status::OK();
}

Status GraphServiceRouter::RefreshPartitionMap() {
  if (!meta_client_) {
    return Status::OK();
  }

  auto result = meta_client_->GetSpacePartitionMap("default");
  if (!result.ok()) {
    std::cerr << "[GraphServiceRouter] Failed to refresh partition map: "
              << result.status().ToString() << std::endl;
    return result.status();
  }

  const auto& partition_map = result.value();

  std::unordered_map<uint32_t, PartitionRoute> new_cache;
  for (const auto& entry : partition_map.assignments) {
    const auto& assignment = entry.second;
    PartitionRoute route;
    route.partition_id = entry.first;
    auto leader = GetNodeAddress(assignment.leader_node);
    if (!leader.ok()) {
      std::cerr << "[GraphServiceRouter] Failed to resolve leader node "
                << assignment.leader_node << " for partition " << entry.first
                << ": " << leader.status().ToString() << std::endl;
      continue;
    }
    route.leader_node = leader.value();
    for (uint32_t replica : assignment.follower_nodes) {
      auto addr = GetNodeAddress(replica);
      if (!addr.ok()) {
        std::cerr << "[GraphServiceRouter] Failed to resolve replica node "
                  << replica << " for partition " << entry.first
                  << ": " << addr.status().ToString() << std::endl;
        continue;
      }
      route.replicas.push_back(addr.value());
    }
    new_cache[entry.first] = std::move(route);
  }

  std::unique_lock<std::shared_mutex> lock(partition_map_mutex_);
  partition_cache_.swap(new_cache);
  partition_cache_version_ = partition_map.version;
}

void GraphServiceRouter::PartitionMapRefreshLoop() {
  std::chrono::seconds backoff(1);
  while (running_) {
    for (int i = 0; i < partition_refresh_interval_.count() && running_; ++i) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    if (!running_) break;

    auto status = RefreshPartitionMap();
    if (!status.ok()) {
      backoff = std::min(backoff * 2, std::chrono::seconds(300));
      std::this_thread::sleep_for(backoff);
    } else {
      backoff = std::chrono::seconds(1);
    }
  }
}

// ========== 2PC 分布式事务引擎 ==========

Status GraphServiceRouter::Initialize2PCEngine() {
  // 1. 创建 StorageClient 连接池
  // 优先基于当前已缓存的 storage stubs
  {
    std::shared_lock<std::shared_mutex> lock(stubs_mutex_);
    for (const auto& [addr, stub] : storage_stubs_) {
      (void)stub;
      auto client = std::make_shared<cedar::dtx::StorageClient>();
      cedar::dtx::StorageClient::ClientConfig client_config;
      client_config.server_address = addr;
      client_config.tls = tls_config_;
      auto s = client->Initialize(client_config);
      if (!s.ok()) {
        std::cerr << "[GraphD] Failed to create StorageClient for " << addr
                  << ": " << s.ToString() << std::endl;
        continue;
      }
      storage_clients_.push_back(client);
      std::cerr << "[GraphD] StorageClient connected to " << addr << std::endl;
    }
  }
  
  // 如果 stubs 为空，尝试从 MetaD 获取所有存活节点
  if (storage_clients_.empty() && meta_client_) {
    auto nodes_result = meta_client_->GetAliveNodes();
    if (nodes_result.ok()) {
      for (const auto& node : nodes_result.value()) {
        if (node.state == cedar::dtx::NodeInfo::State::kOnline) {
          auto client = std::make_shared<cedar::dtx::StorageClient>();
          cedar::dtx::StorageClient::ClientConfig client_config;
          client_config.server_address = node.address;
          client_config.tls = tls_config_;
          auto s = client->Initialize(client_config);
          if (!s.ok()) {
            std::cerr << "[GraphD] Failed to create StorageClient for " << node.address
                      << ": " << s.ToString() << std::endl;
            continue;
          }
          storage_clients_.push_back(client);
          std::cerr << "[GraphD] StorageClient connected to " << node.address << std::endl;
        }
      }
    }
  }
  
  if (storage_clients_.empty()) {
    std::cerr << "[GraphD] No storage clients available, 2PC engine not initialized" << std::endl;
    return Status::OK();  // Non-fatal
  }
  
  // 2. 初始化 TransactionStateManager（WAL）
  txn_state_manager_ = std::make_unique<cedar::TransactionStateManager>();
  auto s = txn_state_manager_->Initialize(txn_wal_dir_);
  if (!s.ok()) {
    std::cerr << "[GraphD] Failed to initialize TransactionStateManager: " << s.ToString() << std::endl;
    txn_state_manager_.reset();
    storage_clients_.clear();
    return s;
  }
  
  // 3. 初始化 TransactionRecoveryManager
  txn_recovery_manager_ = std::make_unique<cedar::TransactionRecoveryManager>();
  s = txn_recovery_manager_->Initialize(txn_state_manager_.get());
  if (!s.ok()) {
    std::cerr << "[GraphD] Failed to initialize TransactionRecoveryManager: " << s.ToString() << std::endl;
    txn_recovery_manager_.reset();
    txn_state_manager_->Shutdown();
    txn_state_manager_.reset();
    storage_clients_.clear();
    return s;
  }
  
  // 4. 初始化 TransactionTimeoutManager
  txn_timeout_manager_ = std::make_unique<cedar::TransactionTimeoutManager>();
  cedar::TimeoutConfig timeout_config;
  txn_timeout_manager_->Initialize(timeout_config, txn_recovery_manager_.get());
  
  // 5. 初始化 Optimized2PCEngine
  {
    cedar::dtx::TwoPCConfig two_pc_config;
    std::unique_lock<std::shared_mutex> lock(engine_mutex_);
    two_pc_engine_ = std::make_unique<cedar::dtx::Optimized2PCEngine>(two_pc_config);
    s = two_pc_engine_->Initialize(storage_clients_);
    if (!s.ok()) {
      std::cerr << "[GraphD] Failed to initialize Optimized2PCEngine: " << s.ToString() << std::endl;
      two_pc_engine_.reset();
    } else {
      two_pc_engine_->SetStateManager(txn_state_manager_.get());
      two_pc_engine_->SetRecoveryManager(txn_recovery_manager_.get());
      two_pc_engine_->SetTimeoutManager(txn_timeout_manager_.get());
    }
  }
  if (!s.ok()) {
    txn_timeout_manager_->Shutdown();
    txn_timeout_manager_.reset();
    txn_recovery_manager_->Shutdown();
    txn_recovery_manager_.reset();
    txn_state_manager_->Shutdown();
    txn_state_manager_.reset();
    storage_clients_.clear();
    return s;
  }
  
  std::cerr << "[GraphD] 2PC engine initialized with " << storage_clients_.size()
            << " storage clients" << std::endl;
  return Status::OK();
}

void GraphServiceRouter::Shutdown2PCEngine() {
  // 注意：Optimized2PCEngine 的析构函数会调用 timeout_manager_ 和 recovery_manager_ 的 Shutdown()
  // 所以先 reset engine，避免 double shutdown
  {
    std::unique_lock<std::shared_mutex> lock(engine_mutex_);
    if (two_pc_engine_) {
      two_pc_engine_.reset();
    }
  }
  if (txn_timeout_manager_) {
    txn_timeout_manager_.reset();
  }
  if (txn_recovery_manager_) {
    txn_recovery_manager_.reset();
  }
  if (txn_state_manager_) {
    txn_state_manager_->Shutdown();
    txn_state_manager_.reset();
  }
  storage_clients_.clear();
}

grpc::Status GraphServiceRouter::BeginTransaction(grpc::ServerContext* context,
                                                    const cedargrpc::BeginTransactionRequest* request,
                                                    cedargrpc::Transaction* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }
  (void)request;

  {
    std::lock_guard<std::mutex> lock(active_txns_mutex_);
    if (active_transactions_.size() >= kMaxActiveTransactions) {
      return grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "Too many active transactions");
    }
  }

  auto txn_id = next_txn_id_.fetch_add(1);
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
  
  if (txn_state_manager_) {
    txn_state_manager_->CreateTransaction(txn_id, std::vector<uint16_t>{});
  }
  
  std::cerr << "[GraphD] BeginTransaction: " << txn_id << std::endl;
  return grpc::Status::OK;
}

grpc::Status GraphServiceRouter::Commit(grpc::ServerContext* context,
                                        const cedargrpc::CommitRequest* request,
                                        cedargrpc::GrpcStatus* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }
  
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
  
  // 执行 2PC 提交（如果事务有累积的 write_set）
  {
    std::shared_lock<std::shared_mutex> lock(engine_mutex_);
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
  }
  
  if (txn_state_manager_) {
    txn_state_manager_->UpdateState(txn_ctx.txn_id, cedar::TxnState::kCommitted);
  }
  
  response->set_ok(true);
  std::cerr << "[GraphD] Commit: " << txn_id_str << " (keys=" << txn_ctx.write_set.size() << ")" << std::endl;
  return grpc::Status::OK;
}

grpc::Status GraphServiceRouter::Rollback(grpc::ServerContext* context,
                                          const cedargrpc::RollbackRequest* request,
                                          cedargrpc::GrpcStatus* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }
  
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
  
  if (txn_state_manager_) {
    auto txn_id = std::stoull(txn_id_str);
    txn_state_manager_->UpdateState(txn_id, cedar::TxnState::kAborted);
  }
  
  response->set_ok(true);
  std::cerr << "[GraphD] Rollback: " << txn_id_str << std::endl;
  return grpc::Status::OK;
}

bool GraphServiceRouter::IsWriteQuery(const std::string& query) const {
  // 1. 尝试 AST 解析精确检测（CREATE / SET / DELETE）
  cypher::CypherParser parser(query);
  auto stmt = parser.ParseStatement();
  if (stmt) {
    for (const auto& clause : stmt->clauses) {
      switch (clause->clause_type) {
        case cypher::ClauseType::CREATE:
        case cypher::ClauseType::SET:
        case cypher::ClauseType::DELETE:
          return true;
        default:
          break;
      }
    }
    // AST 未检测到写子句，但 MERGE/REMOVE 尚未被解析器支持
    // 回退到关键词检测以覆盖 MERGE/REMOVE
  }
  
  // 2. 关键词 fallback（覆盖 MERGE/REMOVE 以及 AST 解析失败的场景）
  std::string lower_query = query;
  std::transform(lower_query.begin(), lower_query.end(), lower_query.begin(), ::tolower);
  return lower_query.find("merge") != std::string::npos ||
         lower_query.find("remove") != std::string::npos;
}

Status GraphServiceRouter::ExecuteDistributedWrite(
    const std::vector<::cedar::CedarKey>& read_set,
    const std::vector<::cedar::CedarKey>& write_set) {
  std::shared_lock<std::shared_mutex> lock(engine_mutex_);
  if (!two_pc_engine_) {
    return Status::InvalidArgument("2PC engine not initialized");
  }
  if (write_set.empty()) {
    return Status::OK();
  }
  
  auto txn_id = next_txn_id_.fetch_add(1);
  auto now_ts = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
  
  return two_pc_engine_->Execute2PC(txn_id, read_set, write_set,
                                     ::cedar::Timestamp(now_ts));
}

std::string GraphServiceRouter::GenerateQueryFingerprint(const std::string& query) {
  return cedar::cypher::ComputeFingerprint(query);
}

uint64_t GraphServiceRouter::CalculatePartitionHash(const std::vector<uint32_t>& partition_ids) {
  uint64_t hash = 0;
  for (uint32_t id : partition_ids) {
    hash = hash * 31 + id;
  }
  return hash;
}

// ========== 双模式分区策略管理 ==========

Status GraphServiceRouter::InitializeDualModePartition(
    const cedar::dtx::DualModePartitionStrategy::Config& config) {
  
  partition_strategy_ = std::make_unique<cedar::dtx::DualModePartitionStrategy>(config);
  
  std::cerr << "[GraphD] Dual-mode partition strategy initialized: " 
            << partition_strategy_->Name() << std::endl;
  
  return Status::OK();
}

Status GraphServiceRouter::SetPartitionMode(cedar::dtx::DualModePartitionStrategy::Mode mode) {
  if (!partition_strategy_) {
    return Status::InvalidArgument("Dual-mode partition strategy not initialized");
  }
  
  partition_strategy_->SetMode(mode);
  
  std::cerr << "[GraphD] Partition mode switched to: " 
            << partition_strategy_->Name() << std::endl;
  
  return Status::OK();
}

cedar::dtx::DualModePartitionStrategy::Mode GraphServiceRouter::GetPartitionMode() const {
  if (!partition_strategy_) {
    return cedar::dtx::DualModePartitionStrategy::Mode::STATIC_HASH;
  }
  
  return partition_strategy_->GetMode();
}

void GraphServiceRouter::ReportQueryStats(bool is_temporal_query, bool has_locality) {
  if (partition_strategy_) {
    partition_strategy_->UpdateQueryStats(is_temporal_query, has_locality);
  }
}

void GraphServiceRouter::RecordLatency(uint64_t latency_us) {
  std::lock_guard<std::mutex> lock(latency_mutex_);
  if (latency_history_.size() < kLatencyHistorySize) {
    latency_history_.push_back(latency_us);
  } else {
    latency_history_[latency_history_pos_] = latency_us;
    latency_history_pos_ = (latency_history_pos_ + 1) % kLatencyHistorySize;
  }
}

uint64_t GraphServiceRouter::GetP99Latency() const {
  std::lock_guard<std::mutex> lock(latency_mutex_);
  if (latency_history_.empty()) return 0;
  
  std::vector<uint64_t> sorted = latency_history_;
  std::sort(sorted.begin(), sorted.end());
  size_t idx = (sorted.size() * 99) / 100;
  if (idx >= sorted.size()) idx = sorted.size() - 1;
  return sorted[idx];
}

uint64_t GraphServiceRouter::GetQPS() const {
  std::lock_guard<std::mutex> lock(latency_mutex_);
  if (latency_history_.empty()) return 0;
  
  // Count queries in the last 1 second using recent history
  // Since we don't store timestamps, approximate by assuming uniform rate
  // over the measurement window. Use total_queries as a rough proxy.
  return stats_.total_queries.load() / 60;  // Approximate average QPS over 1 minute
}

}  // namespace service
}  // namespace cedar
