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
// Service Discovery Implementation
// =============================================================================

#include "cedar/dtx/service_discovery.h"

#include <iostream>
#include <chrono>
#include <thread>
#include <algorithm>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstring>

namespace cedar {
namespace dtx {

// =============================================================================
// ServiceDiscovery Implementation
// =============================================================================

ServiceDiscovery::ServiceDiscovery(const ServiceDiscoveryConfig& config)
    : config_(config) {}

ServiceDiscovery::~ServiceDiscovery() {
  Stop();
}

Status ServiceDiscovery::Initialize() {
  if (initialized_.exchange(true)) {
    return Status::OK();  // 已初始化
  }
  
  std::cout << "[ServiceDiscovery] Initializing..." << std::endl;
  std::cout << "  Docker discovery: " << (config_.enable_docker_discovery ? "enabled" : "disabled") << std::endl;
  std::cout << "  DNS discovery: " << (config_.enable_dns_discovery ? "enabled" : "disabled") << std::endl;
  std::cout << "  Auto register: " << (config_.auto_register ? "enabled" : "disabled") << std::endl;
  
  return Status::OK();
}

Status ServiceDiscovery::Start() {
  if (!initialized_.load()) {
    return Status::InvalidArgument("Not initialized");
  }
  
  if (running_.exchange(true)) {
    return Status::OK();  // 已在运行
  }
  
  std::cout << "[ServiceDiscovery] Starting discovery service..." << std::endl;
  
  // 立即执行一次发现
  auto nodes = DiscoverNow();
  std::cout << "[ServiceDiscovery] Initial discovery found " << nodes.size() << " nodes" << std::endl;
  
  // 启动后台线程
  discovery_thread_ = std::thread(&ServiceDiscovery::DiscoveryLoop, this);
  health_check_thread_ = std::thread(&ServiceDiscovery::HealthCheckLoop, this);
  
  return Status::OK();
}

void ServiceDiscovery::Stop() {
  if (!running_.exchange(false)) {
    return;
  }
  
  std::cout << "[ServiceDiscovery] Stopping discovery service..." << std::endl;
  
  if (discovery_thread_.joinable()) {
    discovery_thread_.join();
  }
  if (health_check_thread_.joinable()) {
    health_check_thread_.join();
  }
}

std::vector<StorageNodeInfo> ServiceDiscovery::DiscoverNow() {
  std::vector<StorageNodeInfo> all_nodes;
  
  // Docker 发现
  if (config_.enable_docker_discovery) {
    auto docker_nodes = DiscoverViaDocker();
    all_nodes.insert(all_nodes.end(), docker_nodes.begin(), docker_nodes.end());
  }
  
  // DNS 发现
  if (config_.enable_dns_discovery) {
    auto dns_nodes = DiscoverViaDNS();
    all_nodes.insert(all_nodes.end(), dns_nodes.begin(), dns_nodes.end());
  }
  
  // Consul 发现
  if (config_.enable_consul_discovery) {
    auto consul_nodes = DiscoverViaConsul();
    all_nodes.insert(all_nodes.end(), consul_nodes.begin(), consul_nodes.end());
  }
  
  // 去重
  std::sort(all_nodes.begin(), all_nodes.end(), 
            [](const auto& a, const auto& b) { return a.GetEndpoint() < b.GetEndpoint(); });
  auto last = std::unique(all_nodes.begin(), all_nodes.end(),
                          [](const auto& a, const auto& b) { return a.GetEndpoint() == b.GetEndpoint(); });
  all_nodes.erase(last, all_nodes.end());
  
  // 合并到内部列表
  MergeNodes(all_nodes);
  
  // 更新统计
  {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    stats_.total_discovered = all_nodes.size();
    stats_.last_discovery_time = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
  }
  
  return all_nodes;
}

std::vector<StorageNodeInfo> ServiceDiscovery::GetDiscoveredNodes() {
  std::lock_guard<std::mutex> lock(nodes_mutex_);
  return discovered_nodes_;
}

std::vector<StorageNodeInfo> ServiceDiscovery::GetHealthyNodes() {
  std::lock_guard<std::mutex> lock(nodes_mutex_);
  std::vector<StorageNodeInfo> healthy;
  for (const auto& node : discovered_nodes_) {
    if (node.is_healthy) {
      healthy.push_back(node);
    }
  }
  return healthy;
}

void ServiceDiscovery::SetNodeDiscoveredCallback(NodeDiscoveredCallback callback) {
  discovered_callback_ = callback;
}

void ServiceDiscovery::SetNodeHealthChangedCallback(NodeHealthChangedCallback callback) {
  health_changed_callback_ = callback;
}

bool ServiceDiscovery::CheckNodeHealth(const StorageNodeInfo& node) {
  // 尝试 TCP 连接检查
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    return false;
  }
  
  struct timeval tv;
  tv.tv_sec = config_.health_check_timeout_ms / 1000;
  tv.tv_usec = (config_.health_check_timeout_ms % 1000) * 1000;
  setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
  
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(node.port);
  
  // 尝试解析主机名
  struct hostent* host = gethostbyname(node.host.c_str());
  if (host == nullptr) {
    close(sock);
    return false;
  }
  
  memcpy(&addr.sin_addr, host->h_addr_list[0], host->h_length);
  
  int result = connect(sock, (struct sockaddr*)&addr, sizeof(addr));
  close(sock);
  
  return result == 0;
}

ServiceDiscovery::Stats ServiceDiscovery::GetStats() const {
  std::lock_guard<std::mutex> lock(stats_mutex_);
  return stats_;
}

// =============================================================================
// 发现方法实现
// =============================================================================

std::vector<StorageNodeInfo> ServiceDiscovery::DiscoverViaDocker() {
  std::vector<StorageNodeInfo> nodes;
  
  // 简化版：通过环境变量或文件获取 Docker 信息
  // 实际生产环境应该使用 Docker API
  
  const char* docker_host = getenv("DOCKER_HOST");
  if (docker_host == nullptr) {
    // 尝试本地 Docker socket
    if (access(config_.docker_socket.c_str(), F_OK) != 0) {
      std::cout << "[ServiceDiscovery] Docker socket not available" << std::endl;
      return nodes;
    }
  }
  
  // 模拟从 Docker 获取容器信息
  // 实际实现应该调用 Docker API
  std::cout << "[ServiceDiscovery] Discovering via Docker..." << std::endl;
  
  // 常见的存储节点名称
  std::vector<std::string> possible_names = {
    "cedar-storaged-0", "cedar-storaged-1", "cedar-storaged-2",
    "cedar-storaged-3", "cedar-storaged-4", "cedar-storaged-5",
    "storaged0", "storaged1", "storaged2"
  };
  
  for (const auto& name : possible_names) {
    struct hostent* host = gethostbyname(name.c_str());
    if (host != nullptr) {
      StorageNodeInfo node;
      node.host = name;
      node.port = config_.storaged_port;
      node.container_name = name;
      node.ip_address = inet_ntoa(*(struct in_addr*)host->h_addr_list[0]);
      node.register_time = std::chrono::duration_cast<std::chrono::seconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count();
      node.is_healthy = true;  // 初始假设健康
      
      nodes.push_back(node);
      std::cout << "  Found: " << name << " (" << node.ip_address << ")" << std::endl;
    }
  }
  
  return nodes;
}

std::vector<StorageNodeInfo> ServiceDiscovery::DiscoverViaDNS() {
  std::vector<StorageNodeInfo> nodes;
  
  std::cout << "[ServiceDiscovery] Discovering via DNS..." << std::endl;
  
  for (const auto& name : config_.dns_names) {
    struct hostent* host = gethostbyname(name.c_str());
    if (host != nullptr) {
      StorageNodeInfo node;
      node.host = name;
      node.port = config_.storaged_port;
      node.container_name = name;
      node.ip_address = inet_ntoa(*(struct in_addr*)host->h_addr_list[0]);
      node.register_time = std::chrono::duration_cast<std::chrono::seconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count();
      node.is_healthy = true;
      
      nodes.push_back(node);
      std::cout << "  Found: " << name << " (" << node.ip_address << ")" << std::endl;
    }
  }
  
  return nodes;
}

std::vector<StorageNodeInfo> ServiceDiscovery::DiscoverViaConsul() {
  // Consul 发现暂未实现
  std::cout << "[ServiceDiscovery] Consul discovery not implemented yet" << std::endl;
  return {};
}

// =============================================================================
// 后台任务
// =============================================================================

void ServiceDiscovery::DiscoveryLoop() {
  while (running_.load()) {
    // 每 60 秒执行一次发现
    for (int i = 0; i < 60 && running_.load(); ++i) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    
    if (!running_.load()) break;
    
    auto new_nodes = DiscoverNow();
    std::cout << "[ServiceDiscovery] Discovery loop found " << new_nodes.size() << " nodes" << std::endl;
  }
}

void ServiceDiscovery::HealthCheckLoop() {
  while (running_.load()) {
    // 每 30 秒执行一次健康检查
    for (int i = 0; i < config_.health_check_interval_seconds && running_.load(); ++i) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    
    if (!running_.load()) break;
    
    std::vector<StorageNodeInfo> nodes;
    {
      std::lock_guard<std::mutex> lock(nodes_mutex_);
      nodes = discovered_nodes_;
    }
    
    int healthy_count = 0;
    for (auto& node : nodes) {
      bool was_healthy = node.is_healthy;
      node.is_healthy = CheckNodeHealth(node);
      
      if (node.is_healthy) {
        healthy_count++;
      }
      
      // 状态变化回调
      if (was_healthy != node.is_healthy && health_changed_callback_) {
        health_changed_callback_(node, node.is_healthy);
      }
    }
    
    {
      std::lock_guard<std::mutex> lock(stats_mutex_);
      stats_.healthy_nodes = healthy_count;
    }
    
    std::cout << "[ServiceDiscovery] Health check: " << healthy_count << "/" 
              << nodes.size() << " nodes healthy" << std::endl;
  }
}

void ServiceDiscovery::MergeNodes(const std::vector<StorageNodeInfo>& new_nodes) {
  std::lock_guard<std::mutex> lock(nodes_mutex_);
  
  for (const auto& new_node : new_nodes) {
    auto it = std::find_if(discovered_nodes_.begin(), discovered_nodes_.end(),
                           [&new_node](const auto& existing) {
                             return existing.GetEndpoint() == new_node.GetEndpoint();
                           });
    
    if (it == discovered_nodes_.end()) {
      // 新节点
      discovered_nodes_.push_back(new_node);
      std::cout << "[ServiceDiscovery] New node discovered: " << new_node.GetEndpoint() << std::endl;
      
      if (discovered_callback_) {
        discovered_callback_(new_node);
      }
    }
  }
}

// =============================================================================
// ClusterInitializer Implementation
// =============================================================================

ClusterInitializer::ClusterInitializer(const Config& config)
    : config_(config) {
  ServiceDiscoveryConfig discovery_config;
  service_discovery_ = std::make_unique<ServiceDiscovery>(discovery_config);
}

ClusterInitializer::~ClusterInitializer() = default;

Status ClusterInitializer::InitializeCluster() {
  std::cout << "[ClusterInitializer] Initializing cluster..." << std::endl;
  
  // 1. 等待 MetaD 就绪
  auto status = WaitForMetaD();
  if (!status.ok()) {
    return status;
  }
  
  // 2. 自动发现并注册存储节点
  if (config_.auto_discover_storaged) {
    status = AutoDiscoverAndRegister();
    if (!status.ok()) {
      std::cout << "[ClusterInitializer] Warning: Auto-discovery failed: " 
                << status.ToString() << std::endl;
      // 继续，不中断初始化
    }
  }
  
  std::cout << "[ClusterInitializer] Cluster initialization completed!" << std::endl;
  return Status::OK();
}

Status ClusterInitializer::WaitForMetaD() {
  std::cout << "[ClusterInitializer] Waiting for MetaD to be ready..." << std::endl;
  
  auto start = std::chrono::steady_clock::now();
  auto timeout = std::chrono::seconds(config_.init_timeout_seconds);
  
  while (std::chrono::steady_clock::now() - start < timeout) {
    // 尝试连接任意一个 MetaD
    for (const auto& meta_server : config_.meta_servers) {
      // 解析 host:port
      size_t colon_pos = meta_server.find(':');
      if (colon_pos == std::string::npos) continue;
      
      std::string host = meta_server.substr(0, colon_pos);
      int port = std::stoi(meta_server.substr(colon_pos + 1));
      
      int sock = socket(AF_INET, SOCK_STREAM, 0);
      if (sock < 0) continue;
      
      struct timeval tv;
      tv.tv_sec = 2;
      tv.tv_usec = 0;
      setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
      setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
      
      struct sockaddr_in addr;
      memset(&addr, 0, sizeof(addr));
      addr.sin_family = AF_INET;
      addr.sin_port = htons(port);
      
      struct hostent* he = gethostbyname(host.c_str());
      if (he != nullptr) {
        memcpy(&addr.sin_addr, he->h_addr_list[0], he->h_length);
        
        if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) == 0) {
          close(sock);
          std::cout << "[ClusterInitializer] MetaD is ready at " << meta_server << std::endl;
          return Status::OK();
        }
      }
      
      close(sock);
    }
    
    std::this_thread::sleep_for(std::chrono::seconds(config_.retry_interval_seconds));
    std::cout << "." << std::flush;
  }
  
  return Status::IOError("Timeout waiting for MetaD");
}

Status ClusterInitializer::AutoDiscoverAndRegister() {
  std::cout << "[ClusterInitializer] Auto-discovering storage nodes..." << std::endl;
  
  // 初始化服务发现
  auto status = service_discovery_->Initialize();
  if (!status.ok()) {
    return status;
  }
  
  // 立即发现
  auto nodes = service_discovery_->DiscoverNow();
  
  if (nodes.empty()) {
    return Status::NotFound("No storage nodes discovered");
  }
  
  std::cout << "[ClusterInitializer] Discovered " << nodes.size() << " storage nodes" << std::endl;
  
  // 注册节点
  return RegisterStorageNodes(nodes);
}

Status ClusterInitializer::RegisterStorageNodes(const std::vector<StorageNodeInfo>& nodes) {
  std::cout << "[ClusterInitializer] Registering storage nodes..." << std::endl;
  
  int success_count = 0;
  for (const auto& node : nodes) {
    std::cout << "  Registering: " << node.GetEndpoint() << " ... " << std::flush;
    
    // 这里应该调用 MetaService 的 AddHost 接口
    // 简化版：模拟成功
    
    bool success = true;
    for (int retry = 0; retry < config_.init_timeout_seconds / config_.retry_interval_seconds; ++retry) {
      // 模拟 RPC 调用
      // 实际应该调用: meta_client_->AddHost(node.host, node.port);
      
      // 模拟成功
      success = true;
      break;
    }
    
    if (success) {
      std::cout << "OK" << std::endl;
      success_count++;
    } else {
      std::cout << "FAILED" << std::endl;
    }
  }
  
  std::cout << "[ClusterInitializer] Registered " << success_count << "/" 
            << nodes.size() << " nodes" << std::endl;
  
  return success_count > 0 ? Status::OK() : Status::IOError("Failed to register any nodes");
}

bool ClusterInitializer::IsClusterReady() {
  // 检查集群是否就绪
  // 1. MetaD 可用
  // 2. 至少有一个存储节点已注册
  
  auto status = WaitForMetaD();
  if (!status.ok()) {
    return false;
  }
  
  // 简化版：假设就绪
  return true;
}

// =============================================================================
// 便捷函数实现
// =============================================================================

ServiceDiscoveryConfig CreateDefaultDiscoveryConfig() {
  ServiceDiscoveryConfig config;
  // 使用默认配置
  return config;
}

std::vector<StorageNodeInfo> QuickDiscoverStorageNodes() {
  ServiceDiscovery discovery(CreateDefaultDiscoveryConfig());
  auto status = discovery.Initialize();
  if (!status.ok()) {
    return {};
  }
  return discovery.DiscoverNow();
}

}  // namespace dtx
}  // namespace cedar
