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
// Service Discovery - 服务自动发现
// 实现 GraphD 自动发现 StorageD 节点并注册
// =============================================================================

#ifndef CEDAR_DTX_SERVICE_DISCOVERY_H_
#define CEDAR_DTX_SERVICE_DISCOVERY_H_

#include <string>
#include <vector>
#include <memory>
#include <functional>
#include <thread>
#include <atomic>
#include <mutex>

#include "cedar/core/status.h"

namespace cedar {
namespace dtx {

// 存储节点信息
struct StorageNodeInfo {
  std::string host;
  int port;
  std::string container_name;  // Docker 容器名
  std::string ip_address;
  int64_t register_time;
  bool is_healthy;
  
  std::string GetEndpoint() const {
    return host + ":" + std::to_string(port);
  }
};

// 发现配置
struct ServiceDiscoveryConfig {
  // 发现方式
  bool enable_docker_discovery = true;      // 通过 Docker API 发现
  bool enable_dns_discovery = true;         // 通过 DNS 发现
  bool enable_consul_discovery = false;     // 通过 Consul 发现
  
  // Docker 发现配置
  std::string docker_socket = "/var/run/docker.sock";
  std::string container_name_prefix = "cedar-storaged";
  
  // DNS 发现配置
  std::vector<std::string> dns_names = {
    "storaged0", "storaged1", "storaged2",
    "storaged3", "storaged4", "storaged5"
  };
  int storaged_port = 9779;
  
  // 健康检查配置
  int health_check_interval_seconds = 30;
  int health_check_timeout_ms = 5000;
  
  // 自动注册配置
  bool auto_register = true;                // 自动注册发现的节点
  int register_retry_times = 3;
};

// 服务发现回调
using NodeDiscoveredCallback = std::function<void(const StorageNodeInfo&)>;
using NodeHealthChangedCallback = std::function<void(const StorageNodeInfo&, bool healthy)>;

// =============================================================================
// ServiceDiscovery - 服务发现管理器
// =============================================================================

class ServiceDiscovery {
 public:
  explicit ServiceDiscovery(const ServiceDiscoveryConfig& config);
  ~ServiceDiscovery();
  
  // 禁止拷贝
  ServiceDiscovery(const ServiceDiscovery&) = delete;
  ServiceDiscovery& operator=(const ServiceDiscovery&) = delete;
  
  // 初始化
  Status Initialize();
  
  // 启动/停止发现服务
  Status Start();
  void Stop();
  
  // 手动触发发现
  std::vector<StorageNodeInfo> DiscoverNow();
  
  // 获取已发现的节点
  std::vector<StorageNodeInfo> GetDiscoveredNodes();
  
  // 获取健康的节点
  std::vector<StorageNodeInfo> GetHealthyNodes();
  
  // 设置回调
  void SetNodeDiscoveredCallback(NodeDiscoveredCallback callback);
  void SetNodeHealthChangedCallback(NodeHealthChangedCallback callback);
  
  // 检查特定节点健康状态
  bool CheckNodeHealth(const StorageNodeInfo& node);
  
  // 统计信息
  struct Stats {
    int total_discovered = 0;
    int healthy_nodes = 0;
    int registered_nodes = 0;
    int64_t last_discovery_time = 0;
  };
  Stats GetStats() const;
  
 private:
  // 发现方法
  std::vector<StorageNodeInfo> DiscoverViaDocker();
  std::vector<StorageNodeInfo> DiscoverViaDNS();
  std::vector<StorageNodeInfo> DiscoverViaConsul();
  
  // 后台任务
  void DiscoveryLoop();
  void HealthCheckLoop();
  
  // 合并节点列表
  void MergeNodes(const std::vector<StorageNodeInfo>& new_nodes);
  
  // 内部状态
  ServiceDiscoveryConfig config_;
  std::atomic<bool> running_{false};
  std::atomic<bool> initialized_{false};
  
  // 节点列表
  std::vector<StorageNodeInfo> discovered_nodes_;
  mutable std::mutex nodes_mutex_;
  
  // 后台线程
  mutable std::mutex thread_mutex_;
  std::thread discovery_thread_;
  std::thread health_check_thread_;
  
  // 回调
  mutable std::mutex callback_mutex_;
  NodeDiscoveredCallback discovered_callback_;
  NodeHealthChangedCallback health_changed_callback_;
  
  // 统计
  mutable std::mutex stats_mutex_;
  Stats stats_;
};

// =============================================================================
// ClusterInitializer - 集群初始化器
// =============================================================================

class ClusterInitializer {
 public:
  struct Config {
    std::vector<std::string> meta_servers;
    bool auto_discover_storaged = true;
    int init_timeout_seconds = 120;
    int retry_interval_seconds = 5;
  };
  
  explicit ClusterInitializer(const Config& config);
  ~ClusterInitializer();
  
  // 初始化集群
  Status InitializeCluster();
  
  // 等待 MetaD 就绪
  Status WaitForMetaD();
  
  // 自动发现并注册存储节点
  Status AutoDiscoverAndRegister();
  
  // 手动注册存储节点
  Status RegisterStorageNodes(const std::vector<StorageNodeInfo>& nodes);
  
  // 检查集群状态
  bool IsClusterReady();
  
 private:
  Config config_;
  std::unique_ptr<ServiceDiscovery> service_discovery_;
};

// =============================================================================
// 便捷函数
// =============================================================================

// 创建默认的服务发现配置
ServiceDiscoveryConfig CreateDefaultDiscoveryConfig();

// 快速发现存储节点（同步版本）
std::vector<StorageNodeInfo> QuickDiscoverStorageNodes();

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_SERVICE_DISCOVERY_H_
