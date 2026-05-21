// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#ifndef CEDAR_SERVICE_GRAPH_SERVICE_ROUTER_H_
#define CEDAR_SERVICE_GRAPH_SERVICE_ROUTER_H_

#include <grpcpp/grpcpp.h>
#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "query_service.grpc.pb.h"
#include "meta_service.grpc.pb.h"
#include "storage_service.grpc.pb.h"
#include "gcn_service.grpc.pb.h"
#include "cedar_graph.grpc.pb.h"
#include "cedar/cypher/cypher_engine.h"
#include "cedar/cypher/ast.h"
#include "query/query_cache.h"
#include "cedar/dtx/partition.h"
#include "cedar/dtx/raft/grpc_tls.h"
#include "cedar/dtx/meta_service_grpc.h"
#include "cedar/dtx/optimized_2pc_engine.h"
#include "cedar/dtx/storage_service_impl.h"
#include "cedar/dtx/transaction_state.h"
#include "cedar/dtx/transaction_recovery_manager.h"
#include "cedar/dtx/transaction_timeout_manager.h"
#include "cedar/gcn/scatter_gather_router.h"

namespace cedar {
namespace service {

// 分区路由信息
struct PartitionRoute {
  uint32_t partition_id;
  std::string leader_node;      // "127.0.0.1:9779"
  std::vector<std::string> replicas;
};

// 查询类型枚举
enum class QueryType {
  POINT_LOOKUP,       // 单点精确查询: MATCH (n) WHERE id(n) = xxx
  SCAN,               // 全分区扫描: MATCH (n) RETURN n
  NEIGHBOR_TRAVERSAL, // 邻域遍历: MATCH (n)-[e]->(m) WHERE id(n) = xxx
  AGGREGATE,          // 聚合查询: RETURN count(*), sum(...)
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
  
  // ---- Cypher 查询引擎扩展 ----
  QueryType query_type = QueryType::SCAN;
  
  // 邻域遍历参数
  uint64_t start_node_id = 0;           // 遍历起始节点 ID（0 = 未指定）
  uint32_t edge_type = 0;               // 0 = 所有边类型
  cedar::cypher::Direction direction = cedar::cypher::Direction::OUTGOING;
  uint32_t hops = 1;                    // 跳数（默认 1 跳）
  
  // RETURN 信息
  std::vector<std::string> return_columns;
  bool return_all = false;              // RETURN *
  
  // 聚合信息
  std::string aggregate_function;       // "count", "sum", "avg", "min", "max"
  std::string aggregate_column;         // 聚合的列名（空 = count(*)）
  bool has_aggregate = false;
  
  // 排序与分页
  std::string order_by_column;
  bool order_ascending = true;
  int64_t limit = -1;                   // -1 = 无限制
  int64_t skip = 0;                     // OFFSET
  bool has_order_by = false;
  bool has_limit = false;
};

// GraphD 查询路由服务
class GraphServiceRouter final : public cedar::query::QueryService::Service,
                                    public cedargrpc::CedarGraphService::Service {
 public:
  GraphServiceRouter();
  ~GraphServiceRouter() override;

  // 禁止拷贝
  GraphServiceRouter(const GraphServiceRouter&) = delete;
  GraphServiceRouter& operator=(const GraphServiceRouter&) = delete;

  // 初始化
  Status Initialize(const std::string& meta_server_addr,
                    const std::string& gcn_server_addr = "");
  
  // 设置事务 WAL 目录（默认 /tmp/cedar_graphd_txn_wal）
  void SetTxnWALDir(const std::string& dir) { txn_wal_dir_ = dir; }

  // 设置 TLS 配置
  void SetTlsConfig(const cedar::dtx::raft::TlsConfig& config) { tls_config_ = config; }

  // 启动后台任务
  Status Start();

  // 停止
  Status Stop();

  // 刷新分区映射缓存
  Status RefreshPartitionMap();

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

  // ========== 显式事务 API (CedarGraphService) ==========
  
  grpc::Status BeginTransaction(grpc::ServerContext* context,
                                const cedargrpc::BeginTransactionRequest* request,
                                cedargrpc::Transaction* response) override;
  
  grpc::Status Commit(grpc::ServerContext* context,
                      const cedargrpc::CommitRequest* request,
                      cedargrpc::GrpcStatus* response) override;
  
  grpc::Status Rollback(grpc::ServerContext* context,
                        const cedargrpc::RollbackRequest* request,
                        cedargrpc::GrpcStatus* response) override;

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
  cedar::dtx::raft::TlsConfig tls_config_;

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
  
  // GCN 客户端
  std::shared_ptr<cedar::gcn::ScatterGatherRouter> gcn_router_;
  std::vector<std::string> gcn_peer_addresses_;
  
  // 执行单分区查询
  Status ExecutePartitionQuery(const std::string& query,
                               uint32_t partition_id,
                               const QueryRouteContext& route_ctx,
                               cedar::query::ResultSet* result);
  
  // 后台刷新线程
  void PartitionMapRefreshLoop();

  // 从 MetaD 获取节点地址（带本地缓存）
  StatusOr<std::string> GetNodeAddress(uint32_t node_id);

  // MetaD 客户端 (带 failover)
  std::unique_ptr<cedar::dtx::MetaServiceGrpcClient> meta_client_;
  
  // StorageD 客户端缓存
  std::shared_mutex stubs_mutex_;
  std::unordered_map<std::string, std::shared_ptr<cedar::storage::StorageService::Stub>>
      storage_stubs_;
  std::unordered_map<std::string, std::shared_ptr<grpc::Channel>>
      storage_channels_;
  
  // 分区映射缓存
  mutable std::shared_mutex partition_map_mutex_;
  std::unordered_map<uint32_t, PartitionRoute> partition_cache_;
  uint64_t partition_cache_version_ = 0;

  // 节点地址缓存
  mutable std::shared_mutex node_map_mutex_;
  std::unordered_map<uint32_t, std::string> node_address_cache_;
  
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
    std::atomic<uint64_t> active_queries{0};
  } stats_;
  
  // Latency history for P99 calculation (circular buffer, protected by latency_mutex_)
  static constexpr size_t kLatencyHistorySize = 10000;
  mutable std::mutex latency_mutex_;
  std::vector<uint64_t> latency_history_;
  size_t latency_history_pos_ = 0;
  
  void RecordLatency(uint64_t latency_us);
  uint64_t GetP99Latency() const;
  uint64_t GetQPS() const;
  
  std::atomic<bool> running_{false};
  std::thread refresh_thread_;
  
  // 配置
  std::chrono::seconds partition_refresh_interval_{30};
  static constexpr uint32_t kNumPartitions = 32768;
  std::string txn_wal_dir_ = "/tmp/cedar_graphd_txn_wal";
  static constexpr size_t kMaxCachedStubs = 100;
  static constexpr size_t kMaxActiveTransactions = 10000;
  
  // 双模式分区策略（如果使用）
  std::unique_ptr<cedar::dtx::DualModePartitionStrategy> partition_strategy_;

  // ========== 分布式事务 2PC 引擎 ==========
  mutable std::shared_mutex engine_mutex_;
  std::unique_ptr<cedar::dtx::Optimized2PCEngine> two_pc_engine_;
  std::unique_ptr<cedar::TransactionStateManager> txn_state_manager_;
  std::unique_ptr<cedar::TransactionRecoveryManager> txn_recovery_manager_;
  std::unique_ptr<cedar::TransactionTimeoutManager> txn_timeout_manager_;
  std::vector<std::shared_ptr<cedar::dtx::StorageClient>> storage_clients_;
  std::atomic<uint64_t> next_txn_id_{1};
  
  // 活跃显式事务上下文（用于多语句事务）
  struct ActiveTransaction {
    uint64_t txn_id;
    std::vector<::cedar::CedarKey> read_set;
    std::vector<::cedar::CedarKey> write_set;
    bool has_writes = false;
  };
  mutable std::mutex active_txns_mutex_;
  std::unordered_map<std::string, ActiveTransaction> active_transactions_;

  // 初始化 2PC 引擎和 StorageClient 连接池
  Status Initialize2PCEngine();
  void Shutdown2PCEngine();

  // 检测查询是否为写操作（简化：关键词匹配）
  bool IsWriteQuery(const std::string& query) const;

  // 执行分布式写事务（供未来完整 Cypher 解析后调用）
  Status ExecuteDistributedWrite(const std::vector<::cedar::CedarKey>& read_set,
                                 const std::vector<::cedar::CedarKey>& write_set);
};

}  // namespace service
}  // namespace cedar

#endif  // CEDAR_SERVICE_GRAPH_SERVICE_ROUTER_H_
