// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#ifndef CEDAR_SERVICE_GRAPH_SERVICE_ROUTER_H_
#define CEDAR_SERVICE_GRAPH_SERVICE_ROUTER_H_

#include <grpcpp/grpcpp.h>
#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "query_service.grpc.pb.h"
#include "meta_service.grpc.pb.h"
#include "storage_service.grpc.pb.h"
#include "gcn_service.grpc.pb.h"
#include "cedar/cypher/cypher_engine.h"
#include "query/query_cache.h"
#include "cedar/dtx/partition.h"

namespace cedar {
namespace service {

// 分区路由信息
struct PartitionRoute {
  uint32_t partition_id;
  std::string leader_node;      // "127.0.0.1:9779"
  std::vector<std::string> replicas;
};

// 查询路由上下文
struct QueryRouteContext {
  std::string query;
  std::unordered_map<std::string, cedar::query::Value> parameters;
  
  // 解析出的实体 ID（用于分区计算）
  std::vector<uint64_t> entity_ids;
  std::vector<uint64_t> edge_ids;
  
  // 需要访问的分区
  std::vector<uint32_t> target_partitions;
  
  // 时态约束
  uint64_t as_of_timestamp = 0;  // 0 = latest
  bool has_temporal_constraint = false;
};

// GraphD 查询路由服务
class GraphServiceRouter final : public cedar::query::QueryService::Service {
 public:
  GraphServiceRouter();
  ~GraphServiceRouter() override;

  // 禁止拷贝
  GraphServiceRouter(const GraphServiceRouter&) = delete;
  GraphServiceRouter& operator=(const GraphServiceRouter&) = delete;

  // 初始化
  Status Initialize(const std::string& meta_server_addr,
                    const std::string& gcn_server_addr = "");
  
  // 启动后台任务
  Status Start();
  
  // 停止
  Status Stop();

  // ========== gRPC 方法实现 ==========
  
  grpc::Status ExecuteQuery(grpc::ServerContext* context,
                            const cedar::query::ExecuteQueryRequest* request,
                            cedar::query::ExecuteQueryResponse* response) override;

  grpc::Status StreamQuery(grpc::ServerContext* context,
                           const cedar::query::StreamQueryRequest* request,
                           grpc::ServerWriter<cedar::query::StreamQueryResponse>* writer) override;

  grpc::Status Traverse(grpc::ServerContext* context,
                        const cedar::query::TraverseRequest* request,
                        cedar::query::TraverseResponse* response) override;

  grpc::Status TemporalQuery(grpc::ServerContext* context,
                             const cedar::query::TemporalQueryRequest* request,
                             cedar::query::TemporalQueryResponse* response) override;

  grpc::Status BatchQuery(grpc::ServerContext* context,
                          const cedar::query::BatchQueryRequest* request,
                          cedar::query::BatchQueryResponse* response) override;

  grpc::Status GetSchema(grpc::ServerContext* context,
                         const cedar::query::GetSchemaRequest* request,
                         cedar::query::GetSchemaResponse* response) override;

  grpc::Status Health(grpc::ServerContext* context,
                      const cedar::query::HealthRequest* request,
                      cedar::query::HealthResponse* response) override;

  grpc::Status GetStats(grpc::ServerContext* context,
                        const cedar::query::QueryStatsRequest* request,
                        cedar::query::QueryStatsResponse* response) override;

  // ========== 双模式分区策略管理 ==========
  
  // 初始化双模式分区策略
  Status InitializeDualModePartition(const cedar::dtx::DualModePartitionStrategy::Config& config);
  
  // 切换分区模式
  Status SetPartitionMode(cedar::dtx::DualModePartitionStrategy::Mode mode);
  
  // 获取当前分区模式
  cedar::dtx::DualModePartitionStrategy::Mode GetPartitionMode() const;
  
  // 上报查询统计（用于AUTO模式）
  void ReportQueryStats(bool is_temporal_query, bool has_locality);

 private:
  // 解析查询并提取路由信息
  Status ParseQueryForRouting(const std::string& query, 
                              QueryRouteContext* route_ctx);
  
  // 计算实体 ID 对应的分区
  uint32_t CalculatePartition(uint64_t entity_id);
  
  // 从 MetaD 获取分区路由
  StatusOr<PartitionRoute> GetPartitionRoute(uint32_t partition_id);
  
  // 获取 StorageD 客户端 stub
  std::shared_ptr<cedar::storage::StorageService::Stub> GetStorageStub(
      const std::string& node_addr);
  
  // 获取 GCN 客户端 stub
  std::shared_ptr<cedar::gcn::GcnService::Stub> GetGcnStub();
  
  // 执行单分区查询
  Status ExecutePartitionQuery(const std::string& query,
                               uint32_t partition_id,
                               cedar::query::ResultSet* result);
  
  // 刷新分区映射缓存
  void RefreshPartitionMap();
  
  // 后台刷新线程
  void PartitionMapRefreshLoop();

  // MetaD 客户端
  std::unique_ptr<cedar::meta::MetaService::Stub> meta_stub_;
  
  // StorageD 客户端缓存
  std::mutex stubs_mutex_;
  std::unordered_map<std::string, std::shared_ptr<cedar::storage::StorageService::Stub>> 
      storage_stubs_;
  
  // GCN 客户端
  std::mutex gcn_mutex_;
  std::string gcn_server_addr_;
  std::shared_ptr<cedar::gcn::GcnService::Stub> gcn_stub_;
  
  // 分区映射缓存
  std::mutex partition_map_mutex_;
  std::unordered_map<uint32_t, PartitionRoute> partition_cache_;
  uint64_t partition_cache_version_ = 0;
  
  // Cypher 引擎（用于本地解析和计划）
  std::unique_ptr<cypher::CypherEngine> cypher_engine_;
  
  // 查询缓存
  std::unique_ptr<cedar::query::QueryCache> query_cache_;
  
  // 辅助方法
  std::string GenerateQueryFingerprint(const std::string& query);
  uint64_t CalculatePartitionHash(const std::vector<uint32_t>& partition_ids);
  
  // 统计信息
  struct QueryStats {
    std::atomic<uint64_t> total_queries{0};
    std::atomic<uint64_t> failed_queries{0};
    std::atomic<uint64_t> cached_plans{0};
    std::atomic<uint64_t> total_latency_us{0};
  } stats_;
  
  std::atomic<bool> running_{false};
  std::thread refresh_thread_;
  
  // 配置
  std::chrono::seconds partition_refresh_interval_{30};
  static constexpr uint32_t kNumPartitions = 32768;
  
  // 双模式分区策略（如果使用）
  std::unique_ptr<cedar::dtx::DualModePartitionStrategy> partition_strategy_;
};

}  // namespace service
}  // namespace cedar

#endif  // CEDAR_SERVICE_GRAPH_SERVICE_ROUTER_H_
