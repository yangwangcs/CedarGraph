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

// =============================================================================
// Partition Management - GLTR核心组件
// =============================================================================

#ifndef CEDAR_DTX_PARTITION_H_
#define CEDAR_DTX_PARTITION_H_

#include <cstdint>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <shared_mutex>
#include <atomic>

#include "cedar/types/cedar_key.h"
#include "cedar/dtx/types.h"
#include "cedar/core/status.h"

// Forward declarations for dual-mode partition strategy
namespace cedar {
namespace partition {
class StaticHashStrategy;
class MTHStreamStrategy;
struct GraphEvent;
}  // namespace partition
}  // namespace cedar

namespace cedar {
namespace dtx {

// 前向声明
class GraphTopology;

/**
 * @brief 分区元数据
 * 
 * 存储分区的拓扑信息、负载状态和统计信息
 */
struct PartitionMeta {
  PartitionID partition_id{kInvalidPartitionID};
  NodeID primary_node{kInvalidNodeID};           // 主节点
  std::vector<NodeID> replicas;                  // 副本节点列表
  
  // 拓扑信息（用于GLTR）
  std::unordered_set<SubgraphID> subgraphs;      // 包含的子图集合
  std::atomic<uint64_t> vertex_count{0};         // 顶点数量估计
  std::atomic<uint64_t> edge_count{0};           // 边数量估计
  
  // 负载信息（原子操作保证线程安全）
  std::atomic<uint64_t> txn_rate{0};             // 当前事务率 (TPS)
  std::atomic<uint64_t> conflict_rate{0};        // 冲突率
  std::atomic<uint64_t> queue_depth{0};          // 队列深度
  
  // 统计信息
  std::atomic<double> avg_latency_ms{0.0};       // 平均延迟
  
  // 自定义拷贝构造函数（因为std::atomic不能复制）
  PartitionMeta(const PartitionMeta& other)
      : partition_id(other.partition_id),
        primary_node(other.primary_node),
        replicas(other.replicas),
        subgraphs(other.subgraphs),
        vertex_count(other.vertex_count.load()),
        edge_count(other.edge_count.load()),
        txn_rate(other.txn_rate.load()),
        conflict_rate(other.conflict_rate.load()),
        queue_depth(other.queue_depth.load()),
        avg_latency_ms(other.avg_latency_ms.load()),
        hot_key_count(other.hot_key_count),
        locality_score(other.locality_score) {}
  
  PartitionMeta& operator=(const PartitionMeta& other) {
    if (this != &other) {
      partition_id = other.partition_id;
      primary_node = other.primary_node;
      replicas = other.replicas;
      subgraphs = other.subgraphs;
      vertex_count.store(other.vertex_count.load());
      edge_count.store(other.edge_count.load());
      txn_rate.store(other.txn_rate.load());
      conflict_rate.store(other.conflict_rate.load());
      queue_depth.store(other.queue_depth.load());
      avg_latency_ms.store(other.avg_latency_ms.load());
      hot_key_count = other.hot_key_count;
      locality_score = other.locality_score;
    }
    return *this;
  }
  uint64_t hot_key_count{0};                     // 热点Key数量
  double locality_score{0.0};                    // 局部性评分 (0-1)
  
  // 构造函数
  PartitionMeta() = default;
  explicit PartitionMeta(PartitionID pid) : partition_id(pid) {}
  
  // 添加子图
  void AddSubgraph(SubgraphID sid) {
    subgraphs.insert(sid);
  }
  
  // 检查是否包含子图
  bool ContainsSubgraph(SubgraphID sid) const {
    return subgraphs.count(sid) > 0;
  }
  
  // 更新负载统计
  void UpdateLoadStats(uint64_t tps, uint64_t conflicts) {
    txn_rate.store(tps, std::memory_order_relaxed);
    conflict_rate.store(conflicts, std::memory_order_relaxed);
  }
  
  // 序列化（用于持久化）
  std::string Serialize() const;
  static PartitionMeta Deserialize(const std::string& data);
};

/**
 * @brief 分区负载统计
 * 
 * 用于动态重平衡决策
 */
struct PartitionLoadStats {
  PartitionID partition_id{kInvalidPartitionID};
  
  // 负载指标
  uint64_t data_size_bytes{0};      // 数据大小
  uint64_t key_count{0};            // Key数量
  uint64_t hot_key_count{0};        // 热点Key数量
  double cpu_usage{0.0};            // CPU使用率
  double memory_usage{0.0};         // 内存使用率
  
  // 事务指标
  uint64_t txn_count_1min{0};       // 1分钟事务数
  uint64_t conflict_count_1min{0};  // 1分钟冲突数
  double avg_latency_ms{0.0};       // 平均延迟
  double p99_latency_ms{0.0};       // P99延迟
  
  // 计算负载分数（越高越需要迁移）
  double ComputeLoadScore() const;
};

/**
 * @brief 子图边界信息
 * 
 * GLTR使用此结构快速判断事务是否需要跨分区协调
 */
struct SubgraphBoundary {
  SubgraphID subgraph_id{kInvalidSubgraphID};
  std::unordered_set<PartitionID> partitions;           // 子图涉及的分区
  std::unordered_set<uint64_t> boundary_vertex_ids;     // 边界顶点（跨分区边）
  std::unordered_set<uint64_t, std::hash<uint64_t>> internal_vertex_ids;  // 内部顶点
  
  // 快速判断：Key是否在此子图内
  bool Contains(const CedarKey& key) const {
    return internal_vertex_ids.count(key.entity_id()) > 0 ||
           boundary_vertex_ids.count(key.entity_id()) > 0;
  }
  
  // 判断是否为边界顶点
  bool IsBoundaryVertex(uint64_t vertex_id) const {
    return boundary_vertex_ids.count(vertex_id) > 0;
  }
  
  // 判断事务是否完全在子图内（仅涉及一个分区）
  bool IsLocalTransaction(const std::vector<CedarKey>& keys) const;
  
  // 判断事务是否涉及跨分区边
  bool IsCrossPartition(const std::vector<CedarKey>& keys) const;
  
  // 获取涉及的分区列表
  std::vector<PartitionID> GetInvolvedPartitions(const std::vector<CedarKey>& keys) const;
};

/**
 * @brief 分区分配策略接口
 */
class PartitionStrategy {
 public:
  virtual ~PartitionStrategy() = default;
  
  // 计算Key的分区
  virtual PartitionID ComputePartition(const CedarKey& key, 
                                        PartitionID num_partitions) = 0;
  
  // 策略名称
  virtual std::string Name() const = 0;
};

/**
 * @brief 哈希分区策略（默认）
 */
class HashPartitionStrategy : public PartitionStrategy {
 public:
  PartitionID ComputePartition(const CedarKey& key, 
                                PartitionID num_partitions) override {
    return HashToPartition(key, num_partitions);
  }
  
  std::string Name() const override { return "HashPartition"; }
};

/**
 * @brief 范围分区策略
 */
class RangePartitionStrategy : public PartitionStrategy {
 public:
  PartitionID ComputePartition(const CedarKey& key, 
                                PartitionID num_partitions) override {
    // 基于entity_id的范围分区
    // 使用向上取整的range_size，确保最大值的Key也能映射到有效分区
    uint64_t range_size = (std::numeric_limits<uint64_t>::max() / num_partitions) + 1;
    PartitionID pid = static_cast<PartitionID>(key.entity_id() / range_size);
    // 确保不超出范围（处理边界情况）
    return std::min(pid, static_cast<PartitionID>(num_partitions - 1));
  }
  
  std::string Name() const override { return "RangePartition"; }
};

/**
 * @brief 图感知分区策略（GLTR核心）
 * 
 * 基于图拓扑结构的分区，优化遍历局部性
 */
class GraphAwarePartitionStrategy : public PartitionStrategy {
 public:
  // 初始化（需要图拓扑信息）
  Status Initialize(const GraphTopology& topology, 
                     PartitionID num_partitions,
                     double imbalance_factor = 1.05);
  
  PartitionID ComputePartition(const CedarKey& key, 
                                PartitionID num_partitions) override;
  
  std::string Name() const override { return "GraphAwarePartition"; }
  
  // 获取顶点所属子图
  SubgraphID GetSubgraph(uint64_t vertex_id) const;
  
 private:
  // 顶点 -> 分区映射
  std::unordered_map<uint64_t, PartitionID> vertex_partition_map_;
  
  // 顶点 -> 子图映射
  std::unordered_map<uint64_t, SubgraphID> vertex_subgraph_map_;
  
  bool initialized_{false};
};

/**
 * @brief 双模式分区策略（StaticHash + MTHStream）
 * 
 * 支持运行时切换两种分区模式：
 * - StaticHash: 简单的 hash(vid) % n
 * - MTHStream: 基于时态感知的 Count-Min Sketch
 */
class DualModePartitionStrategy : public PartitionStrategy {
 public:
  enum class Mode {
    STATIC_HASH,    // Simple hash(vid) % n
    MTH_STREAM,     // Temporal-aware sketch-based
    AUTO            // Auto-select based on workload
  };
  
  struct Config {
    Mode mode = Mode::STATIC_HASH;
    PartitionID num_partitions = 32768;
    
    // MTH-specific configuration
    size_t sketch_capacity = 1000000;
    double mth_alpha = 1.0;
    double mth_beta = 1.0;
    double mth_gamma = 0.0;
    double mth_eta = 0.0;
    double temporal_alpha = 0.01;
    int sketch_depth = 3;
    int sketch_width = 64;
    double fast_path_threshold = 0.6;
    double load_relaxation = 0.0;
    int decay_interval = 0;
    double decay_factor = 0.95;
    
    // Auto-switch thresholds
    uint64_t temporal_query_threshold = 100;
    double locality_ratio_threshold = 0.7;
  };
  
  explicit DualModePartitionStrategy(const Config& config);
  ~DualModePartitionStrategy() override;
  
  // PartitionStrategy interface
  PartitionID ComputePartition(const CedarKey& key, 
                                PartitionID num_partitions) override;
  std::string Name() const override;
  
  // Mode management
  void SetMode(Mode mode);
  Mode GetMode() const { return mode_.load(); }
  
  // Stats for AUTO mode
  void UpdateQueryStats(bool is_temporal_query, bool has_locality);
  
  // Get statistics
  std::string GetStats() const;

 private:
  Config config_;
  std::atomic<Mode> mode_;
  
  // Sub-strategies (PIMPL idiom to avoid header bloat)
  struct SubStrategies;
  std::unique_ptr<SubStrategies> sub_;
  
  // Query statistics for AUTO mode
  struct QueryStats {
    std::atomic<uint64_t> total{0};
    std::atomic<uint64_t> temporal{0};
    std::atomic<uint64_t> locality{0};
  } stats_;
  
  // Initialize sub-strategies
  void InitializeSubStrategies();
};

/**
 * @brief 分区管理器
 * 
 * 管理集群的所有分区信息，负责分区分配、负载均衡和元数据维护
 */
class PartitionManager {
 public:
  // 构造函数
  explicit PartitionManager(const DTxConfig& config);
  
  // 禁止拷贝
  PartitionManager(const PartitionManager&) = delete;
  PartitionManager& operator=(const PartitionManager&) = delete;
  
  // 初始化分区
  Status Initialize(PartitionID num_partitions, 
                     std::unique_ptr<PartitionStrategy> strategy);
  
  // 初始化双模式分区
  Status InitializeDualMode(const DualModePartitionStrategy::Config& config);
  
  // 获取当前分区策略（返回DualModePartitionStrategy如果可用）
  DualModePartitionStrategy* GetDualModeStrategy() const;
  
  // 切换分区模式（仅当使用双模式策略时有效）
  Status SetPartitionMode(DualModePartitionStrategy::Mode mode);
  
  // 获取当前分区模式
  DualModePartitionStrategy::Mode GetPartitionMode() const;
  
  // 上报查询统计（用于AUTO模式）
  void ReportQueryStats(bool is_temporal_query, bool has_locality);
  
  // 获取Key的分区
  PartitionID GetPartition(const CedarKey& key) const;
  
  // 获取分区元数据
  std::shared_ptr<PartitionMeta> GetPartitionMeta(PartitionID pid) const;
  
  // 获取分区的Leader节点
  NodeID GetPartitionLeader(PartitionID pid) const;
  
  // 设置分区的Leader节点
  Status SetPartitionLeader(PartitionID pid, NodeID node_id);
  
  // Assign all partitions to the given nodes using round-robin placement.
  // Clears any existing node->partition mappings before assigning.
  Status AssignPartitionsToNodes(const std::vector<NodeID>& node_ids);
  
  // 计算一组Key涉及的分区
  std::vector<PartitionID> GetPartitionsForKeys(
      const std::vector<CedarKey>& keys) const;
  
  // 判断事务是否需要跨分区协调
  bool NeedsCoordination(const std::vector<CedarKey>& keys) const {
    return GetPartitionsForKeys(keys).size() > 1;
  }
  
  // 获取子图边界信息
  std::shared_ptr<SubgraphBoundary> GetSubgraphBoundary(SubgraphID sid) const;
  
  // 更新分区负载统计
  void UpdatePartitionStats(PartitionID pid, const PartitionLoadStats& stats);
  
  // 动态重平衡（运行时调用）
  Status RebalanceIfNeeded();
  
  // Rebalance migration plan
  struct MigrationPlan {
    PartitionID partition_id;
    NodeID from_node;
    NodeID to_node;
  };
  
  // Compute a count-based rebalance plan.
  std::vector<MigrationPlan> ComputeRebalancePlan() const;
  
  // Migrate a partition's leader to a new node (metadata only).
  Status MigratePartition(PartitionID pid, NodeID new_node);
  
  // 获取所有分区列表
  std::vector<PartitionID> GetAllPartitions() const;
  
  // 获取节点负责的分区列表
  std::vector<PartitionID> GetPartitionsOnNode(NodeID node_id) const;
  
  // 上报负载信息（由各节点调用）
  void ReportLoad(PartitionID pid, const PartitionLoadStats& stats);
  
 private:
  DTxConfig config_;
  
  // 分区数量
  PartitionID num_partitions_{0};
  
  // 分区策略
  std::unique_ptr<PartitionStrategy> strategy_;
  
  // 分区元数据（PartitionID -> PartitionMeta）
  mutable std::shared_mutex meta_mutex_;
  std::unordered_map<PartitionID, std::shared_ptr<PartitionMeta>> partition_metas_;
  
  // 子图边界信息
  mutable std::shared_mutex subgraph_mutex_;
  std::unordered_map<SubgraphID, std::shared_ptr<SubgraphBoundary>> subgraph_boundaries_;
  
  // 节点 -> 分区映射
  mutable std::shared_mutex node_partition_mutex_;
  std::unordered_map<NodeID, std::vector<PartitionID>> node_partitions_;
};

/**
 * @brief CedarKey分区操作辅助类
 * 
 * 提供便捷的CedarKey分区操作方法
 */
class CedarKeyPartitionHelper {
 public:
  // 获取Key的分区ID
  static PartitionID GetPartitionID(const CedarKey& key) {
    return key.part_id();
  }
  
  // 设置Key的分区ID（返回修改后的Key）
  static CedarKey SetPartitionID(CedarKey key, PartitionID pid) {
    key.SetPartId(pid);
    return key;
  }
  
  // 检查Key是否属于指定分区
  static bool BelongsToPartition(const CedarKey& key, PartitionID pid) {
    return key.part_id() == pid;
  }
  
  // 批量设置分区ID
  static std::vector<CedarKey> SetPartitionIDs(
      const std::vector<CedarKey>& keys, 
      PartitionID pid);
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_PARTITION_H_
