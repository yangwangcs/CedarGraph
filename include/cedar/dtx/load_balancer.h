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
// Load Balancer - 负载均衡器
// =============================================================================

#ifndef CEDAR_DTX_LOAD_BALANCER_H_
#define CEDAR_DTX_LOAD_BALANCER_H_

#include <cstdint>
#include <condition_variable>
#include <vector>
#include <unordered_map>
#include <memory>
#include <thread>
#include <atomic>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"
#include "cedar/dtx/meta_service.h"

namespace cedar {
namespace dtx {

// =============================================================================
// 负载统计
// =============================================================================

/**
 * @brief 分区负载信息
 */
struct PartitionLoad {
    PartitionID partition_id;
    uint64_t data_size_bytes{0};      // 数据大小
    uint64_t key_count{0};            // Key 数量
    uint64_t qps{0};                  // 每秒查询数
    double avg_latency_ms{0.0};       // 平均延迟
    uint64_t leader_count{0};         // Leader 数量（用于节点负载）
};

/**
 * @brief 节点负载信息
 */
struct NodeLoad {
    NodeID node_id;
    double cpu_usage{0.0};
    double memory_usage{0.0};
    double disk_usage{0.0};
    uint64_t total_data_size{0};
    size_t leader_count{0};
    size_t partition_count{0};
    uint64_t total_qps{0};
};

/**
 * @brief 集群负载报告
 */
struct ClusterLoadReport {
    std::string space_name;
    std::vector<NodeLoad> node_loads;
    std::vector<PartitionLoad> partition_loads;
    uint64_t total_data_size{0};
    size_t total_partitions{0};
    double avg_node_load{0.0};
    double max_node_load{0.0};
    double min_node_load{0.0};
    double load_variance{0.0};  // 负载方差
};

// =============================================================================
// 负载均衡任务
// =============================================================================

/**
 * @brief 均衡任务类型
 */
enum class BalanceTaskType : uint8_t {
    kTransferLeader = 1,     // 转移 Leader
    kMigratePartition = 2,   // 迁移分区
    kAddReplica = 3,         // 添加副本
    kRemoveReplica = 4,      // 移除副本
};

/**
 * @brief 均衡任务
 */
struct BalanceTask {
    BalanceTaskType type;
    PartitionID partition_id;
    NodeID source_node;
    NodeID target_node;
    std::string reason;
    uint64_t estimated_data_size{0};  // 预估数据大小（用于进度跟踪）
};

/**
 * @brief 均衡计划
 */
struct BalancePlan {
    std::string space_name;
    std::vector<BalanceTask> tasks;
    uint64_t total_data_to_move{0};
    double expected_load_variance_after{0.0};
};

// =============================================================================
// 负载均衡策略
// =============================================================================

/**
 * @brief 负载均衡策略接口
 */
class BalanceStrategy {
public:
    virtual ~BalanceStrategy() = default;
    
    // 分析是否需要均衡
    virtual bool NeedsRebalance(const ClusterLoadReport& report) = 0;
    
    // 生成均衡计划
    virtual BalancePlan GeneratePlan(const ClusterLoadReport& report) = 0;
    
    // 策略名称
    virtual std::string Name() const = 0;
};

/**
 * @brief Leader 均衡策略
 * 
 * 目标：每个节点的 Leader 数量大致相等
 */
class LeaderBalanceStrategy : public BalanceStrategy {
public:
    bool NeedsRebalance(const ClusterLoadReport& report) override;
    BalancePlan GeneratePlan(const ClusterLoadReport& report) override;
    std::string Name() const override { return "LeaderBalance"; }
    
private:
    static constexpr double kImbalanceThreshold = 2.0;  // 最大差值阈值
};

/**
 * @brief 数据均衡策略
 * 
 * 目标：每个节点的数据量大致相等
 */
class DataBalanceStrategy : public BalanceStrategy {
public:
    bool NeedsRebalance(const ClusterLoadReport& report) override;
    BalancePlan GeneratePlan(const ClusterLoadReport& report) override;
    std::string Name() const override { return "DataBalance"; }
    
private:
    static constexpr double kImbalanceRatio = 1.5;  // 最大/最小比值阈值
};

/**
 * @brief QPS 均衡策略
 * 
 * 目标：每个节点的 QPS 大致相等
 */
class QpsBalanceStrategy : public BalanceStrategy {
public:
    bool NeedsRebalance(const ClusterLoadReport& report) override;
    BalancePlan GeneratePlan(const ClusterLoadReport& report) override;
    std::string Name() const override { return "QpsBalance"; }
    
private:
    static constexpr double kImbalanceRatio = 2.0;
};

/**
 * @brief 复合均衡策略
 * 
 * 综合考虑 Leader、数据量、QPS
 */
class CompositeBalanceStrategy : public BalanceStrategy {
public:
    bool NeedsRebalance(const ClusterLoadReport& report) override;
    BalancePlan GeneratePlan(const ClusterLoadReport& report) override;
    std::string Name() const override { return "CompositeBalance"; }
    
private:
    LeaderBalanceStrategy leader_strategy_;
    DataBalanceStrategy data_strategy_;
    QpsBalanceStrategy qps_strategy_;
};

// =============================================================================
// 负载均衡执行器
// =============================================================================

/**
 * @brief 任务执行状态
 */
enum class TaskExecutionState : uint8_t {
    kPending = 0,       // 等待执行
    kInProgress = 1,    // 执行中
    kCompleted = 2,     // 完成
    kFailed = 3,        // 失败
    kCancelled = 4,     // 取消
};

struct TaskExecutionStatus {
    BalanceTask task;
    TaskExecutionState state{TaskExecutionState::kPending};
    std::string error_msg;
    double progress_percent{0.0};  // 0-100
    std::chrono::system_clock::time_point start_time;
    std::chrono::system_clock::time_point end_time;
};

/**
 * @brief 分区迁移执行器
 * 
 * 负责实际执行分区迁移任务
 */
class PartitionMigrationExecutor {
public:
    PartitionMigrationExecutor() = default;
    ~PartitionMigrationExecutor() = default;
    
    // 初始化
    Status Initialize(MetadataService* meta_service);
    
    // 执行迁移任务
    StatusOr<TaskExecutionStatus> ExecuteTask(const BalanceTask& task);
    
    // 取消正在执行的任务
    Status CancelTask(PartitionID partition_id);
    
    // 获取任务状态
    StatusOr<TaskExecutionStatus> GetTaskStatus(PartitionID partition_id) const;
    
    // 获取所有任务状态
    std::vector<TaskExecutionStatus> GetAllTaskStatuses() const;

private:
    // 执行 Leader 转移
    Status ExecuteTransferLeader(const BalanceTask& task, TaskExecutionStatus* status);
    
    // 执行分区迁移
    Status ExecuteMigratePartition(const BalanceTask& task, TaskExecutionStatus* status);
    
    // 执行添加副本
    Status ExecuteAddReplica(const BalanceTask& task, TaskExecutionStatus* status);
    
    // 执行移除副本
    Status ExecuteRemoveReplica(const BalanceTask& task, TaskExecutionStatus* status);
    
    MetadataService* meta_service_{nullptr};
    
    mutable std::shared_mutex tasks_mutex_;
    std::unordered_map<PartitionID, TaskExecutionStatus> active_tasks_;
};

// =============================================================================
// 负载均衡器
// =============================================================================

/**
 * @brief 负载均衡器配置
 */
struct LoadBalancerConfig {
    bool auto_balance_enabled{true};           // 是否自动均衡
    uint64_t check_interval_sec{300};          // 检查间隔（5分钟）
    uint64_t min_balance_interval_sec{600};    // 最小均衡间隔（10分钟）
    uint32_t max_concurrent_tasks{3};          // 最大并发任务数
    uint64_t max_data_move_per_run{10737418240ULL};  // 单次最大迁移数据量 (10GB)
    bool balance_during_peak{false};           // 高峰期是否均衡
    uint64_t peak_hours_start{8};              // 高峰期开始（小时）
    uint64_t peak_hours_end{22};               // 高峰期结束（小时）
};

/**
 * @brief 负载均衡器
 * 
 * 自动监控集群负载并执行均衡操作
 */
class LoadBalancer {
public:
    LoadBalancer();
    ~LoadBalancer();
    
    // 初始化
    Status Initialize(const LoadBalancerConfig& config, 
                      MetadataService* meta_service,
                      std::unique_ptr<BalanceStrategy> strategy);
    
    // 启动/停止
    Status Start();
    Status Stop();
    
    // 手动触发均衡检查
    Status TriggerBalanceCheck();
    
    // 获取当前负载报告
    StatusOr<ClusterLoadReport> GetCurrentLoadReport(const std::string& space_name);
    
    // 获取均衡历史
    std::vector<BalancePlan> GetBalanceHistory() const;
    
    // 获取正在执行的任务
    std::vector<TaskExecutionStatus> GetActiveTasks() const;
    
    // 切换均衡策略
    Status SetStrategy(std::unique_ptr<BalanceStrategy> strategy);

private:
    // 后台检查循环
    void BalanceCheckLoop();
    
    // 收集负载信息
    StatusOr<ClusterLoadReport> CollectLoadReport(const std::string& space_name);
    
    // 执行均衡计划
    Status ExecuteBalancePlan(const BalancePlan& plan);
    
    // 检查是否在高峰期
    bool IsPeakHours() const;
    
    LoadBalancerConfig config_;
    MetadataService* meta_service_{nullptr};
    std::unique_ptr<BalanceStrategy> strategy_;
    
    PartitionMigrationExecutor executor_;
    
    std::atomic<bool> running_{false};
    std::mutex check_mutex_;
    std::condition_variable check_cv_;
    std::thread check_thread_;
    
    mutable std::shared_mutex history_mutex_;
    std::vector<BalancePlan> balance_history_;
    
    std::atomic<std::chrono::system_clock::time_point> last_balance_time_;
};

} // namespace dtx
} // namespace cedar

#endif // CEDAR_DTX_LOAD_BALANCER_H_
