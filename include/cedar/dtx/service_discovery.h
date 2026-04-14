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
// Service Discovery & Health Check - 服务发现与健康检查
// =============================================================================
// Features:
// - Phi Accrual 故障检测算法
// - Gossip 协议传播节点状态
// - 服务注册与发现
// - 动态配置更新
// =============================================================================

#ifndef CEDAR_DTX_SERVICE_DISCOVERY_H_
#define CEDAR_DTX_SERVICE_DISCOVERY_H_

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"

namespace cedar {
namespace dtx {

// =============================================================================
// 节点状态
// =============================================================================

enum class NodeHealthStatus : uint8_t {
  kHealthy = 0,      // 健康
  kSuspected = 1,    // 疑似故障（Phi Accrual）
  kUnhealthy = 2,    // 确认故障
  kOffline = 3,      // 离线
  kMaintenance = 4,  // 维护模式
};

inline std::string NodeHealthStatusToString(NodeHealthStatus status) {
  switch (status) {
    case NodeHealthStatus::kHealthy: return "Healthy";
    case NodeHealthStatus::kSuspected: return "Suspected";
    case NodeHealthStatus::kUnhealthy: return "Unhealthy";
    case NodeHealthStatus::kOffline: return "Offline";
    case NodeHealthStatus::kMaintenance: return "Maintenance";
    default: return "Unknown";
  }
}

struct NodeInfo {
  NodeID id{0};
  std::string address;
  std::string data_center;
  std::string rack;
  std::vector<std::string> tags;
  
  // 资源信息
  uint64_t memory_bytes{0};
  uint32_t cpu_cores{0};
  uint64_t disk_bytes{0};
  
  // 状态
  NodeHealthStatus health{NodeHealthStatus::kHealthy};
  std::chrono::steady_clock::time_point last_heartbeat;
  std::chrono::steady_clock::time_point registered_at;
  
  // 负载信息
  double cpu_usage{0.0};
  double memory_usage{0.0};
  double disk_usage{0.0};
  uint64_t active_connections{0};
  double qps{0.0};
};

// =============================================================================
// Phi Accrual 故障检测器
// =============================================================================

class PhiAccrualFailureDetector {
 public:
  struct Config {
    uint32_t window_size{1000};           // 心跳历史窗口大小
    double phi_threshold{8.0};            // 故障检测阈值
    std::chrono::milliseconds min_std_dev{100};  // 最小标准差
    std::chrono::milliseconds heartbeat_interval{5000};  // 期望心跳间隔
  };
  
  explicit PhiAccrualFailureDetector(const Config& config);
  
  // 记录心跳到达
  void RecordHeartbeat(NodeID node_id);
  
  // 计算 phi 值
  double ComputePhi(NodeID node_id) const;
  
  // 检查是否疑似故障
  bool IsSuspected(NodeID node_id) const;
  
  // 检查是否确认故障
  bool IsFailed(NodeID node_id) const;
  
  // 移除节点
  void RemoveNode(NodeID node_id);
  
  // 获取节点最后心跳时间
  std::chrono::steady_clock::time_point GetLastHeartbeat(NodeID node_id) const;

 private:
  struct HeartbeatHistory {
    std::vector<std::chrono::milliseconds> intervals;
    std::chrono::steady_clock::time_point last_heartbeat;
    size_t index{0};  // 循环缓冲区索引
  };
  
  Config config_;
  mutable std::mutex mutex_;
  std::unordered_map<NodeID, HeartbeatHistory> histories_;
};

// =============================================================================
// Gossip 协议
// =============================================================================

class GossipProtocol {
 public:
  struct Config {
    uint32_t gossip_interval_ms{1000};        // Gossip 间隔
    uint32_t gossip_fanout{3};                // 每次随机选择的节点数
    uint32_t max_gossip_payload_size{65536};  // 最大 payload 大小
    bool enable_encryption{false};            // 是否加密
  };
  
  using StateUpdateHandler = std::function<void(NodeID, const NodeInfo&)>;
  
  GossipProtocol();
  ~GossipProtocol();
  
  // 禁止拷贝
  GossipProtocol(const GossipProtocol&) = delete;
  GossipProtocol& operator=(const GossipProtocol&) = delete;
  
  Status Initialize(const Config& config, NodeID self_id);
  void Shutdown();
  
  // 添加/移除节点
  void AddNode(const NodeInfo& info);
  void RemoveNode(NodeID node_id);
  
  // 更新本节点状态
  void UpdateSelfInfo(const NodeInfo& info);
  
  // 获取所有节点状态
  std::vector<NodeInfo> GetAllNodes() const;
  std::vector<NodeInfo> GetHealthyNodes() const;
  std::optional<NodeInfo> GetNode(NodeID node_id) const;
  
  // 注册状态更新回调
  void RegisterUpdateHandler(StateUpdateHandler handler);

 private:
  // Gossip 循环
  void GossipLoop();
  
  // 发送 gossip 消息
  void SendGossipTo(NodeID target);
  
  // 处理收到的 gossip
  void OnGossipReceived(NodeID from, const std::vector<NodeInfo>& states);
  
  // 合并状态（取较新的）
  NodeInfo MergeState(const NodeInfo& local, const NodeInfo& remote);
  
  Config config_;
  NodeID self_id_{0};
  std::atomic<bool> running_{false};
  
  mutable std::mutex nodes_mutex_;
  std::unordered_map<NodeID, NodeInfo> nodes_;
  std::unordered_set<NodeID> seeds_;  // 种子节点
  
  std::vector<StateUpdateHandler> update_handlers_;
  mutable std::mutex handlers_mutex_;
  
  std::thread gossip_thread_;
  std::mt19937 rng_{std::random_device{}()};
};

// =============================================================================
// 服务注册表
// =============================================================================

class ServiceRegistry {
 public:
  struct ServiceInstance {
    std::string service_name;
    NodeID node_id;
    std::string address;
    uint32_t port{0};
    std::map<std::string, std::string> metadata;
    std::chrono::steady_clock::time_point registered_at;
    std::chrono::steady_clock::time_point expires_at;
  };
  
  ServiceRegistry();
  ~ServiceRegistry();
  
  // 服务注册
  Status RegisterService(const ServiceInstance& instance,
                         std::chrono::seconds ttl = std::chrono::seconds(30));
  
  // 服务注销
  Status DeregisterService(const std::string& service_name, NodeID node_id);
  
  // 服务发现
  std::vector<ServiceInstance> DiscoverService(const std::string& service_name) const;
  
  // 获取服务实例
  std::optional<ServiceInstance> GetServiceInstance(const std::string& service_name,
                                                     NodeID node_id) const;
  
  // 续约（心跳）
  Status RenewLease(const std::string& service_name, NodeID node_id);
  
  // 后台清理过期服务
  void CleanupExpiredServices();

 private:
  mutable std::mutex services_mutex_;
  std::unordered_map<std::string, std::vector<ServiceInstance>> services_;
  
  std::atomic<bool> running_{false};
  std::thread cleanup_thread_;
};

// =============================================================================
// 集群状态管理器
// =============================================================================

class ClusterStateManager {
 public:
  struct Config {
    std::chrono::milliseconds heartbeat_interval{5000};
    std::chrono::milliseconds health_check_interval{1000};
    double phi_threshold{8.0};
    bool enable_gossip{true};
    GossipProtocol::Config gossip_config;
  };
  
  using NodeStateChangeHandler = 
      std::function<void(NodeID, NodeHealthStatus old_status, NodeHealthStatus new_status)>;
  
  ClusterStateManager();
  ~ClusterStateManager();
  
  // 禁止拷贝
  ClusterStateManager(const ClusterStateManager&) = delete;
  ClusterStateManager& operator=(const ClusterStateManager&) = delete;
  
  Status Initialize(const Config& config, NodeID self_id);
  void Shutdown();
  
  // 注册/注销本节点
  Status RegisterSelf(const NodeInfo& info);
  void DeregisterSelf();
  
  // 发送心跳
  void SendHeartbeat();
  
  // 获取集群视图
  std::vector<NodeInfo> GetClusterView() const;
  std::vector<NodeInfo> GetHealthyNodes() const;
  size_t GetClusterSize() const;
  
  // 查询节点
  std::optional<NodeInfo> GetNodeInfo(NodeID node_id) const;
  NodeHealthStatus GetNodeHealth(NodeID node_id) const;
  bool IsNodeHealthy(NodeID node_id) const;
  
  // 选举协调者（最低 ID 的健康节点）
  std::optional<NodeID> ElectCoordinator() const;
  
  // 注册状态变更回调
  void RegisterStateChangeHandler(NodeStateChangeHandler handler);
  
  // 统计信息
  struct Stats {
    uint64_t heartbeats_sent{0};
    uint64_t heartbeats_received{0};
    uint64_t state_changes{0};
    uint64_t suspected_count{0};
    uint64_t failed_count{0};
  };
  Stats GetStats() const;

 private:
  // 后台任务
  void HeartbeatLoop();
  void HealthCheckLoop();
  
  // 处理状态变更
  void OnNodeStateChanged(NodeID node_id, NodeHealthStatus new_status);
  
  Config config_;
  NodeID self_id_{0};
  std::atomic<bool> initialized_{false};
  std::atomic<bool> running_{false};
  
  // 组件
  std::unique_ptr<PhiAccrualFailureDetector> failure_detector_;
  std::unique_ptr<GossipProtocol> gossip_;
  std::unique_ptr<ServiceRegistry> registry_;
  
  // 节点状态
  mutable std::mutex nodes_mutex_;
  std::unordered_map<NodeID, NodeInfo> nodes_;
  std::unordered_map<NodeID, NodeHealthStatus> last_health_status_;
  
  // 回调
  std::vector<NodeStateChangeHandler> state_change_handlers_;
  mutable std::mutex handlers_mutex_;
  
  // 后台线程
  std::thread heartbeat_thread_;
  std::thread health_check_thread_;
  
  // 统计
  mutable std::mutex stats_mutex_;
  Stats stats_;
};

// =============================================================================
// 便捷函数
// =============================================================================

ClusterStateManager* GetGlobalClusterStateManager();

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_SERVICE_DISCOVERY_H_
