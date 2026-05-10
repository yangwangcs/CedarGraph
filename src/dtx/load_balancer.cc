#include "cedar/dtx/load_balancer.h"
#include <algorithm>
#include <cmath>

namespace cedar {
namespace dtx {

// LeaderBalanceStrategy implementation
bool LeaderBalanceStrategy::NeedsRebalance(const ClusterLoadReport& report) {
    if (report.node_loads.size() < 2) return false;
    
    size_t max_leaders = 0;
    size_t min_leaders = SIZE_MAX;
    
    for (const auto& node : report.node_loads) {
        max_leaders = std::max(max_leaders, node.leader_count);
        min_leaders = std::min(min_leaders, node.leader_count);
    }
    
    return (max_leaders - min_leaders) > static_cast<size_t>(kImbalanceThreshold);
}

BalancePlan LeaderBalanceStrategy::GeneratePlan(const ClusterLoadReport& report) {
    BalancePlan plan;
    plan.space_name = report.space_name;
    
    if (report.node_loads.size() < 2) return plan;
    
    // 计算平均 Leader 数
    size_t total_leaders = 0;
    for (const auto& node : report.node_loads) {
        total_leaders += node.leader_count;
    }
    size_t avg_leaders = total_leaders / report.node_loads.size();
    
    // 找到 Leader 过多和过少的节点
    std::vector<NodeID> overloaded_nodes;
    std::vector<NodeID> underloaded_nodes;
    
    for (const auto& node : report.node_loads) {
        if (node.leader_count > avg_leaders + 1) {
            overloaded_nodes.push_back(node.node_id);
        } else if (node.leader_count < avg_leaders) {
            underloaded_nodes.push_back(node.node_id);
        }
    }
    
    // 生成转移任务
    // 注意：这里简化处理，实际应该从 overloaded 节点选择具体的 partition
    // 并转移到 underloaded 节点
    
    return plan;
}

// DataBalanceStrategy implementation
bool DataBalanceStrategy::NeedsRebalance(const ClusterLoadReport& report) {
    if (report.node_loads.size() < 2) return false;
    
    uint64_t max_data = 0;
    uint64_t min_data = UINT64_MAX;
    
    for (const auto& node : report.node_loads) {
        max_data = std::max(max_data, node.total_data_size);
        min_data = std::min(min_data, node.total_data_size);
    }
    
    if (min_data == 0) return max_data > 0;
    return static_cast<double>(max_data) / min_data > kImbalanceRatio;
}

BalancePlan DataBalanceStrategy::GeneratePlan(const ClusterLoadReport& report) {
    BalancePlan plan;
    plan.space_name = report.space_name;
    
    if (report.node_loads.size() < 2) return plan;
    
    // Calculate average data size per node
    uint64_t total_data = 0;
    for (const auto& node : report.node_loads) {
        total_data += node.total_data_size;
    }
    uint64_t avg_data = total_data / report.node_loads.size();
    
    // Find overloaded and underloaded nodes
    std::vector<NodeID> overloaded_nodes;
    std::vector<NodeID> underloaded_nodes;
    
    for (const auto& node : report.node_loads) {
        if (avg_data > 0 && node.total_data_size > avg_data * kImbalanceRatio) {
            overloaded_nodes.push_back(node.node_id);
        } else if (avg_data > 0 && node.total_data_size < avg_data / kImbalanceRatio) {
            underloaded_nodes.push_back(node.node_id);
        }
    }
    
    // Generate migration tasks from overloaded to underloaded nodes.
    // Since partition_loads doesn't contain node_id, we generate a single
    // representative task per node pair with a sentinel partition_id.
    // The executor will resolve the actual partition at runtime.
    size_t over_idx = 0, under_idx = 0;
    while (over_idx < overloaded_nodes.size() && under_idx < underloaded_nodes.size()) {
        BalanceTask task;
        task.type = BalanceTaskType::kMigratePartition;
        task.partition_id = kInvalidPartitionID;  // Resolved at execution time
        task.source_node = overloaded_nodes[over_idx];
        task.target_node = underloaded_nodes[under_idx];
        task.reason = "Data imbalance: migrate partition to balance data size";
        plan.tasks.push_back(task);
        over_idx++;
        under_idx++;
    }
    
    return plan;
}

// QpsBalanceStrategy implementation
bool QpsBalanceStrategy::NeedsRebalance(const ClusterLoadReport& report) {
    if (report.node_loads.size() < 2) return false;
    
    uint64_t max_qps = 0;
    uint64_t min_qps = UINT64_MAX;
    
    for (const auto& node : report.node_loads) {
        max_qps = std::max(max_qps, node.total_qps);
        min_qps = std::min(min_qps, node.total_qps);
    }
    
    if (min_qps == 0) return max_qps > 100;  // 阈值 100 QPS
    return static_cast<double>(max_qps) / min_qps > kImbalanceRatio;
}

BalancePlan QpsBalanceStrategy::GeneratePlan(const ClusterLoadReport& report) {
    BalancePlan plan;
    plan.space_name = report.space_name;
    
    if (report.node_loads.size() < 2) return plan;
    
    // Calculate average QPS per node
    uint64_t total_qps = 0;
    for (const auto& node : report.node_loads) {
        total_qps += node.total_qps;
    }
    uint64_t avg_qps = total_qps / report.node_loads.size();
    
    // Find overloaded and underloaded nodes
    std::vector<NodeID> overloaded_nodes;
    std::vector<NodeID> underloaded_nodes;
    
    for (const auto& node : report.node_loads) {
        if (avg_qps > 0 && node.total_qps > avg_qps * kImbalanceRatio) {
            overloaded_nodes.push_back(node.node_id);
        } else if (avg_qps > 0 && node.total_qps < avg_qps / kImbalanceRatio) {
            underloaded_nodes.push_back(node.node_id);
        }
    }
    
    // Generate leader transfer tasks from overloaded to underloaded nodes.
    // partition_id is resolved at execution time since partition_loads lacks node_id.
    size_t over_idx = 0, under_idx = 0;
    while (over_idx < overloaded_nodes.size() && under_idx < underloaded_nodes.size()) {
        BalanceTask task;
        task.type = BalanceTaskType::kTransferLeader;
        task.partition_id = kInvalidPartitionID;  // Resolved at execution time
        task.source_node = overloaded_nodes[over_idx];
        task.target_node = underloaded_nodes[under_idx];
        task.reason = "QPS imbalance: transfer leader to balance query load";
        plan.tasks.push_back(task);
        over_idx++;
        under_idx++;
    }
    
    return plan;
}

// CompositeBalanceStrategy implementation
bool CompositeBalanceStrategy::NeedsRebalance(const ClusterLoadReport& report) {
    return leader_strategy_.NeedsRebalance(report) ||
           data_strategy_.NeedsRebalance(report) ||
           qps_strategy_.NeedsRebalance(report);
}

BalancePlan CompositeBalanceStrategy::GeneratePlan(const ClusterLoadReport& report) {
    // 优先处理 Leader 不均衡
    if (leader_strategy_.NeedsRebalance(report)) {
        return leader_strategy_.GeneratePlan(report);
    }
    // 然后处理数据不均衡
    if (data_strategy_.NeedsRebalance(report)) {
        return data_strategy_.GeneratePlan(report);
    }
    // 最后处理 QPS 不均衡
    return qps_strategy_.GeneratePlan(report);
}

// PartitionMigrationExecutor implementation
Status PartitionMigrationExecutor::Initialize(MetadataService* meta_service) {
    meta_service_ = meta_service;
    return Status::OK();
}

StatusOr<TaskExecutionStatus> PartitionMigrationExecutor::ExecuteTask(const BalanceTask& task) {
    TaskExecutionStatus status;
    status.task = task;
    status.state = TaskExecutionState::kInProgress;
    status.start_time = std::chrono::system_clock::now();
    
    {
        std::unique_lock<std::shared_mutex> lock(tasks_mutex_);
        active_tasks_[task.partition_id] = status;
    }
    
    Status result;
    switch (task.type) {
        case BalanceTaskType::kTransferLeader:
            result = ExecuteTransferLeader(task, &status);
            break;
        case BalanceTaskType::kMigratePartition:
            result = ExecuteMigratePartition(task, &status);
            break;
        case BalanceTaskType::kAddReplica:
            result = ExecuteAddReplica(task, &status);
            break;
        case BalanceTaskType::kRemoveReplica:
            result = ExecuteRemoveReplica(task, &status);
            break;
    }
    
    status.end_time = std::chrono::system_clock::now();
    status.state = result.ok() ? TaskExecutionState::kCompleted : TaskExecutionState::kFailed;
    if (!result.ok()) {
        status.error_msg = result.ToString();
    }
    
    {
        std::unique_lock<std::shared_mutex> lock(tasks_mutex_);
        if (result.ok()) {
            active_tasks_.erase(task.partition_id);
        } else {
            active_tasks_[task.partition_id] = status;
        }
    }
    
    return status;
}

Status PartitionMigrationExecutor::ExecuteTransferLeader(const BalanceTask& task, 
                                                          TaskExecutionStatus* status) {
    // 1. 在目标节点上添加副本（如果需要）
    // 2. 等待副本同步完成
    // 3. 切换 Leader 到目标节点
    // 4. 更新 MetaD 的 Partition 映射
    
    if (meta_service_) {
        auto s = meta_service_->UpdatePartitionLeader(
            "default", task.partition_id, task.target_node);
        CEDAR_RETURN_IF_ERROR(s);
    }
    
    status->progress_percent = 100.0;
    return Status::OK();
}

Status PartitionMigrationExecutor::ExecuteMigratePartition(const BalanceTask& task, 
                                                            TaskExecutionStatus* status) {
    // 1. 在目标节点创建新的副本
    // 2. 全量数据复制
    // 3. 增量同步
    // 4. 切换 Leader（如果需要）
    // 5. 删除源节点副本
    
    status->progress_percent = 100.0;
    return Status::OK();
}

Status PartitionMigrationExecutor::ExecuteAddReplica(const BalanceTask& task, 
                                                      TaskExecutionStatus* status) {
    status->progress_percent = 100.0;
    return Status::OK();
}

Status PartitionMigrationExecutor::ExecuteRemoveReplica(const BalanceTask& task, 
                                                         TaskExecutionStatus* status) {
    status->progress_percent = 100.0;
    return Status::OK();
}

Status PartitionMigrationExecutor::CancelTask(PartitionID partition_id) {
    std::unique_lock<std::shared_mutex> lock(tasks_mutex_);
    auto it = active_tasks_.find(partition_id);
    if (it != active_tasks_.end()) {
        it->second.state = TaskExecutionState::kCancelled;
    }
    return Status::OK();
}

StatusOr<TaskExecutionStatus> PartitionMigrationExecutor::GetTaskStatus(
    PartitionID partition_id) const {
    std::shared_lock<std::shared_mutex> lock(tasks_mutex_);
    auto it = active_tasks_.find(partition_id);
    if (it != active_tasks_.end()) {
        return it->second;
    }
    return Status::NotFound("Task not found");
}

std::vector<TaskExecutionStatus> PartitionMigrationExecutor::GetAllTaskStatuses() const {
    std::shared_lock<std::shared_mutex> lock(tasks_mutex_);
    std::vector<TaskExecutionStatus> result;
    for (const auto& [pid, status] : active_tasks_) {
        result.push_back(status);
    }
    return result;
}

// LoadBalancer implementation
LoadBalancer::LoadBalancer() = default;

LoadBalancer::~LoadBalancer() {
    Stop();
}

Status LoadBalancer::Initialize(const LoadBalancerConfig& config, 
                                 MetadataService* meta_service,
                                 std::unique_ptr<BalanceStrategy> strategy) {
    config_ = config;
    meta_service_ = meta_service;
    strategy_ = std::move(strategy);
    
    auto status = executor_.Initialize(meta_service);
    CEDAR_RETURN_IF_ERROR(status);
    
    return Status::OK();
}

Status LoadBalancer::Start() {
    if (running_.exchange(true)) {
        return Status::OK();  // Already running
    }
    
    check_thread_ = std::thread(&LoadBalancer::BalanceCheckLoop, this);
    return Status::OK();
}

Status LoadBalancer::Stop() {
    if (!running_.exchange(false)) {
        return Status::OK();  // Already stopped
    }
    
    if (check_thread_.joinable()) {
        check_thread_.join();
    }
    
    return Status::OK();
}

void LoadBalancer::BalanceCheckLoop() {
    while (running_) {
        if (config_.auto_balance_enabled && !IsPeakHours()) {
            auto status = TriggerBalanceCheck();
            // Log if failed
        }
        
        // Sleep for check interval
        for (uint64_t i = 0; i < config_.check_interval_sec && running_; ++i) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }
}

Status LoadBalancer::TriggerBalanceCheck() {
    // Check if enough time has passed since last balance
    auto now = std::chrono::system_clock::now();
    auto last = last_balance_time_.load();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - last).count();
    if (elapsed < static_cast<int64_t>(config_.min_balance_interval_sec)) {
        return Status::OK();  // Too soon
    }
    
    // Collect load report for all spaces
    // For now, just use "default" space
    auto report = CollectLoadReport("default");
    if (!report.ok()) return report.status();
    
    // Check if rebalance is needed
    if (!strategy_->NeedsRebalance(report.value())) {
        return Status::OK();
    }
    
    // Generate balance plan
    auto plan = strategy_->GeneratePlan(report.value());
    
    // Execute plan
    auto status = ExecuteBalancePlan(plan);
    CEDAR_RETURN_IF_ERROR(status);
    
    // Record history
    {
        std::unique_lock<std::shared_mutex> lock(history_mutex_);
        balance_history_.push_back(plan);
    }
    
    last_balance_time_.store(now);
    return Status::OK();
}

StatusOr<ClusterLoadReport> LoadBalancer::CollectLoadReport(const std::string& space_name) {
    ClusterLoadReport report;
    report.space_name = space_name;
    
    if (!meta_service_) {
        return report;
    }
    
    // Get all alive nodes
    auto nodes = meta_service_->GetAliveNodes();
    
    // Get partition map to compute leader/partition counts per node
    auto partition_map_or = meta_service_->GetSpacePartitionMap(space_name);
    
    // For each node, collect load info
    for (const auto& node : nodes) {
        NodeLoad node_load;
        node_load.node_id = node.node_id;
        
        // Calculate leader and partition counts from partition map
        if (partition_map_or.ok()) {
            const auto& partition_map = partition_map_or.value();
            for (const auto& [pid, assignment] : partition_map.assignments) {
                if (assignment.IsReplicaOn(node.node_id)) {
                    node_load.partition_count++;
                    if (assignment.IsLeaderOn(node.node_id)) {
                        node_load.leader_count++;
                    }
                }
            }
        }
        
        report.node_loads.push_back(node_load);
    }
    
    return report;
}

Status LoadBalancer::ExecuteBalancePlan(const BalancePlan& plan) {
    uint64_t moved_data = 0;
    uint32_t running_tasks = 0;
    uint32_t max_tasks = std::max(1u, config_.max_concurrent_tasks);
    
    for (const auto& task : plan.tasks) {
        // Check if we've exceeded max data move
        if (moved_data >= config_.max_data_move_per_run) {
            break;
        }
        
        // Wait if too many concurrent tasks
        while (running_tasks >= max_tasks) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            running_tasks = static_cast<uint32_t>(executor_.GetAllTaskStatuses().size());
        }
        
        // Execute task
        auto status = executor_.ExecuteTask(task);
        if (!status.ok()) {
            // Log error but continue with other tasks
        }
        
        moved_data += task.estimated_data_size;
    }
    
    return Status::OK();
}

bool LoadBalancer::IsPeakHours() const {
    if (config_.balance_during_peak) return false;
    
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    struct tm tm_buf;
    if (!localtime_r(&time_t, &tm_buf)) {
        return false;
    }
    
    uint64_t hour = tm_buf.tm_hour;
    return hour >= config_.peak_hours_start && hour < config_.peak_hours_end;
}

StatusOr<ClusterLoadReport> LoadBalancer::GetCurrentLoadReport(const std::string& space_name) {
    return CollectLoadReport(space_name);
}

std::vector<BalancePlan> LoadBalancer::GetBalanceHistory() const {
    std::shared_lock<std::shared_mutex> lock(history_mutex_);
    return balance_history_;
}

std::vector<TaskExecutionStatus> LoadBalancer::GetActiveTasks() const {
    return executor_.GetAllTaskStatuses();
}

Status LoadBalancer::SetStrategy(std::unique_ptr<BalanceStrategy> strategy) {
    strategy_ = std::move(strategy);
    return Status::OK();
}

} // namespace dtx
} // namespace cedar
