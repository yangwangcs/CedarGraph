// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#include "graph_service_router.h"

#include <algorithm>
#include <chrono>
#include <iostream>
#include <regex>
#include <sstream>
#include <thread>

#include "cedar/cypher/parser.h"

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
  // 连接到 MetaD
  auto channel = grpc::CreateChannel(meta_server_addr, grpc::InsecureChannelCredentials());
  meta_stub_ = MetaService::NewStub(channel);
  
  // 测试连接
  GetAliveNodesRequest request;
  GetAliveNodesResponse response;
  grpc::ClientContext context;
  auto status = meta_stub_->GetAliveNodes(&context, request, &response);
  
  if (!status.ok()) {
    return Status::InvalidArgument("Failed to connect to MetaD: " + status.error_message());
  }
  
  std::cout << "[GraphD] Connected to MetaD at " << meta_server_addr << std::endl;
  
  // 连接到 GCN（如果配置了）
  if (!gcn_server_addr.empty()) {
    std::lock_guard<std::mutex> lock(gcn_mutex_);
    gcn_server_addr_ = gcn_server_addr;
    auto gcn_channel = grpc::CreateChannel(gcn_server_addr, grpc::InsecureChannelCredentials());
    gcn_stub_ = cedar::gcn::GcnService::NewStub(gcn_channel);
    std::cout << "[GraphD] Connected to GCN at " << gcn_server_addr << std::endl;
  }
  
  // 初始加载分区映射
  RefreshPartitionMap();
  
  // 初始化查询缓存
  cedar::query::QueryCacheConfig cache_config;
  cache_config.max_entries = 10000;
  cache_config.max_memory_bytes = 100 * 1024 * 1024;  // 100MB
  cache_config.default_ttl_seconds = 60;
  query_cache_ = std::make_unique<cedar::query::QueryCache>(cache_config);
  
  std::cout << "[GraphD] Query cache initialized" << std::endl;
  
  return Status::OK();
}

Status GraphServiceRouter::Start() {
  running_ = true;
  
  // 启动分区映射刷新线程
  refresh_thread_ = std::thread(&GraphServiceRouter::PartitionMapRefreshLoop, this);
  
  std::cout << "[GraphD] Router started" << std::endl;
  return Status::OK();
}

Status GraphServiceRouter::Stop() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }
  
  if (refresh_thread_.joinable()) {
    refresh_thread_.join();
  }
  
  std::cout << "[GraphD] Router stopped" << std::endl;
  return Status::OK();
}

// ========== gRPC 方法实现 ==========

grpc::Status GraphServiceRouter::ExecuteQuery(grpc::ServerContext* context,
                                              const ExecuteQueryRequest* request,
                                              ExecuteQueryResponse* response) {
  (void)context;
  
  auto start_time = std::chrono::steady_clock::now();
  stats_.total_queries++;
  
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
  
  // 执行查询
  ResultSet result_set;
  if (route_ctx.target_partitions.empty()) {
    // 无特定分区，广播到所有分区或执行元数据查询
    // 简化：返回空结果
    result_set.set_total_rows(0);
  } else {
    // 执行分区查询并聚合结果
    for (uint32_t part_id : route_ctx.target_partitions) {
      ExecutePartitionQuery(request->query(), part_id, &result_set);
    }
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
    plan << "  Target Partitions: " << route_ctx.target_partitions.size() << "\n";
    for (auto part_id : route_ctx.target_partitions) {
      plan << "    - Partition " << part_id << "\n";
    }
    response->set_execution_plan(plan.str());
  }
  
  return grpc::Status::OK;
}

std::shared_ptr<cedar::gcn::GcnService::Stub> GraphServiceRouter::GetGcnStub() {
  std::lock_guard<std::mutex> lock(gcn_mutex_);
  return gcn_stub_;
}

grpc::Status GraphServiceRouter::Traverse(grpc::ServerContext* context,
                                          const TraverseRequest* request,
                                          TraverseResponse* response) {
  (void)context;
  
  // 优先路由到 GCN（本地图计算节点）
  auto gcn_stub = GetGcnStub();
  if (gcn_stub) {
    cedar::gcn::TraversalRequest gcn_request;
    gcn_request.set_trace_id("graphd-traverse");
    gcn_request.set_root_entity_id(request->start_node_id());
    gcn_request.set_query_time(request->as_of_timestamp() > 0 ? request->as_of_timestamp() : UINT64_MAX);
    gcn_request.set_max_hops(request->max_depth() > 0 ? request->max_depth() : 3);
    gcn_request.set_edge_type(request->edge_types_size() > 0 ? request->edge_types(0) : 0);
    gcn_request.set_required_version(0);
    
    cedar::gcn::TraversalResponse gcn_response;
    grpc::ClientContext client_context;
    auto grpc_status = gcn_stub->Traverse(&client_context, gcn_request, &gcn_response);
    
    if (grpc_status.ok() && gcn_response.success()) {
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
  (void)context;
  
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
  (void)context;
  (void)request;
  
  // 检查 MetaD 连接
  GetAliveNodesRequest meta_request;
  GetAliveNodesResponse meta_response;
  grpc::ClientContext meta_context;
  auto meta_status = meta_stub_->GetAliveNodes(&meta_context, meta_request, &meta_response);
  
  bool meta_healthy = meta_status.ok();
  
  response->set_healthy(meta_healthy);
  response->set_status(meta_healthy ? "healthy" : "degraded");
  response->set_meta_client_healthy(meta_healthy);
  response->set_parser_healthy(true);
  response->set_planner_healthy(true);
  response->set_executor_healthy(true);
  response->set_storage_client_healthy(meta_healthy);  // 依赖 MetaD
  
  response->set_active_queries(0);  // TODO: 跟踪活跃查询
  response->set_queued_queries(0);
  
  return grpc::Status::OK;
}

grpc::Status GraphServiceRouter::GetStats(grpc::ServerContext* context,
                                          const QueryStatsRequest* request,
                                          QueryStatsResponse* response) {
  (void)context;
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
  
  // TODO: 计算 P99 延迟和 QPS
  response->set_p99_latency_us(0);
  response->set_queries_per_second(0);
  
  return grpc::Status::OK;
}

grpc::Status GraphServiceRouter::StreamQuery(grpc::ServerContext* context,
                                             const StreamQueryRequest* request,
                                             grpc::ServerWriter<StreamQueryResponse>* writer) {
  (void)context;
  (void)request;
  (void)writer;
  // TODO: 实现流式查询
  return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "Stream query not implemented");
}

grpc::Status GraphServiceRouter::BatchQuery(grpc::ServerContext* context,
                                            const BatchQueryRequest* request,
                                            BatchQueryResponse* response) {
  (void)context;
  
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
                                           const GetSchemaRequest* request,
                                           GetSchemaResponse* response) {
  (void)context;
  (void)request;
  // TODO: 从 MetaD 获取 Schema
  response->set_success(true);
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
  
  // 检查是否有 ID 直接查找
  // 简单启发式：从 WHERE id(n) = xxx 中提取 ID
  std::regex id_pattern(R"(id\s*\(\s*\w+\s*\)\s*=\s*(\d+))", std::regex::icase);
  std::smatch match;
  std::string::const_iterator search_start(query.cbegin());
  
  while (std::regex_search(search_start, query.cend(), match, id_pattern)) {
    uint64_t entity_id = std::stoull(match[1].str());
    route_ctx->entity_ids.push_back(entity_id);
    route_ctx->target_partitions.push_back(CalculatePartition(entity_id));
    search_start = match.suffix().first;
  }
  
  // 如果没有找到特定 ID，可能需要广播到所有分区
  // 简化：如果 target_partitions 为空，后续处理为全部分区
  
  // 检查时态约束
  auto temporal_clause = parser.GetTemporalClause();
  if (temporal_clause) {
    route_ctx->has_temporal_constraint = true;
    // TODO: 提取具体时态约束
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
    std::lock_guard<std::mutex> lock(partition_map_mutex_);
    auto it = partition_cache_.find(partition_id);
    if (it != partition_cache_.end()) {
      return it->second;
    }
  }
  
  // 缓存未命中，从 MetaD 获取
  GetPartitionAssignmentRequest request;
  request.set_space_name("default");  // TODO: 支持多 space
  request.set_partition_id(partition_id);
  
  GetPartitionAssignmentResponse response;
  grpc::ClientContext context;
  auto status = meta_stub_->GetPartitionAssignment(&context, request, &response);
  
  if (!status.ok()) {
    return Status::InvalidArgument("MetaD error: " + status.error_message());
  }
  
  if (!response.success()) {
    return Status::InvalidArgument(response.error_msg());
  }
  
  // 构建路由信息
  PartitionRoute route;
  route.partition_id = partition_id;
  
  const auto& assignment = response.assignment();
  // leader_node 是 uint32，需要转换为地址
  // TODO: 从 MetaD 获取节点地址映射
  route.leader_node = "127.0.0.1:" + std::to_string(9779 + assignment.leader_node() % 3);
  
  for (uint32_t replica : assignment.follower_nodes()) {
    route.replicas.push_back("127.0.0.1:" + std::to_string(9779 + replica % 3));
  }
  
  // 更新缓存
  {
    std::lock_guard<std::mutex> lock(partition_map_mutex_);
    partition_cache_[partition_id] = route;
  }
  
  return route;
}

std::shared_ptr<cedar::storage::StorageService::Stub> 
GraphServiceRouter::GetStorageStub(const std::string& node_addr) {
  std::lock_guard<std::mutex> lock(stubs_mutex_);
  
  auto it = storage_stubs_.find(node_addr);
  if (it != storage_stubs_.end()) {
    return it->second;
  }
  
  // 创建新连接
  auto channel = grpc::CreateChannel(node_addr, grpc::InsecureChannelCredentials());
  std::shared_ptr<cedar::storage::StorageService::Stub> stub(
      cedar::storage::StorageService::NewStub(channel).release());
  storage_stubs_[node_addr] = stub;
  
  return stub;
}

Status GraphServiceRouter::ExecutePartitionQuery(const std::string& query,
                                                 uint32_t partition_id,
                                                 cedar::query::ResultSet* result) {
  (void)query;
  // TODO: 实现实际的分区查询执行
  // 1. 获取分区路由
  // 2. 发送查询到 StorageD
  // 3. 聚合结果
  
  result->set_total_rows(result->total_rows() + 0);  // 占位
  return Status::OK();
}

void GraphServiceRouter::RefreshPartitionMap() {
  // TODO: 从 MetaD 批量获取分区映射
  // GetSpacePartitionMapRequest request;
  // request.set_space_name("default");
  // ...
}

void GraphServiceRouter::PartitionMapRefreshLoop() {
  while (running_) {
    for (int i = 0; i < partition_refresh_interval_.count() && running_; ++i) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    if (!running_) break;
    
    RefreshPartitionMap();
  }
}

std::string GraphServiceRouter::GenerateQueryFingerprint(const std::string& query) {
  std::string normalized = query;
  
  // 转小写
  std::transform(normalized.begin(), normalized.end(), normalized.begin(), ::tolower);
  
  // 去除多余空格
  std::regex multiple_spaces("\\s+");
  normalized = std::regex_replace(normalized, multiple_spaces, " ");
  
  return normalized;
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
  
  std::cout << "[GraphD] Dual-mode partition strategy initialized: " 
            << partition_strategy_->Name() << std::endl;
  
  return Status::OK();
}

Status GraphServiceRouter::SetPartitionMode(cedar::dtx::DualModePartitionStrategy::Mode mode) {
  if (!partition_strategy_) {
    return Status::InvalidArgument("Dual-mode partition strategy not initialized");
  }
  
  partition_strategy_->SetMode(mode);
  
  std::cout << "[GraphD] Partition mode switched to: " 
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

}  // namespace service
}  // namespace cedar
