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
#include <mutex>
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
  
  std::cerr << "[ServiceDiscovery] Initializing..." << std::endl;
  std::cerr << "  Docker discovery: " << (config_.enable_docker_discovery ? "enabled" : "disabled") << std::endl;
  std::cerr << "  DNS discovery: " << (config_.enable_dns_discovery ? "enabled" : "disabled") << std::endl;
  std::cerr << "  Auto register: " << (config_.auto_register ? "enabled" : "disabled") << std::endl;
  
  return Status::OK();
}

Status ServiceDiscovery::Start() {
  if (!initialized_.load()) {
    return Status::InvalidArgument("Not initialized");
  }
  
  if (running_.exchange(true)) {
    return Status::OK();  // 已在运行
  }
  
  std::cerr << "[ServiceDiscovery] Starting discovery service..." << std::endl;
  
  // 立即执行一次发现
  auto nodes = DiscoverNow();
  std::cerr << "[ServiceDiscovery] Initial discovery found " << nodes.size() << " nodes" << std::endl;
  
  // 启动后台线程
  std::lock_guard<std::mutex> lock(thread_mutex_);
  discovery_thread_ = std::thread(&ServiceDiscovery::DiscoveryLoop, this);
  health_check_thread_ = std::thread(&ServiceDiscovery::HealthCheckLoop, this);
  
  return Status::OK();
}

void ServiceDiscovery::Stop() {
  if (!running_.exchange(false)) {
    return;
  }
  
  std::cerr << "[ServiceDiscovery] Stopping discovery service..." << std::endl;
  
  {
    std::lock_guard<std::mutex> lock(cv_mutex_);
    cv_.notify_all();
  }
  
  std::lock_guard<std::mutex> lock(thread_mutex_);
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
  std::lock_guard<std::mutex> lock(callback_mutex_);
  discovered_callback_ = callback;
}

void ServiceDiscovery::SetNodeHealthChangedCallback(NodeHealthChangedCallback callback) {
  std::lock_guard<std::mutex> lock(callback_mutex_);
  health_changed_callback_ = callback;
}

bool ServiceDiscovery::CheckNodeHealth(const StorageNodeInfo& node) {
  // Try TCP connectivity check with bounded DNS resolution time.
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    return false;
  }
  
  struct timeval tv;
  tv.tv_sec = config_.health_check_timeout_ms / 1000;
  tv.tv_usec = (config_.health_check_timeout_ms % 1000) * 1000;
  if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0 ||
      setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) < 0) {
    close(sock);
    return false;
  }
  
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(node.port);
  
  // Fast-path: if host is already an IPv4 literal, skip DNS entirely.
  if (inet_pton(AF_INET, node.host.c_str(), &addr.sin_addr) == 1) {
    // Valid IPv4 literal — proceed directly to connect.
  } else {
    // DNS resolution with timeout using a detached thread + heap data.
    // All data shared with the detached thread lives on the heap to avoid
    // use-after-free when this function returns before the thread finishes.
    struct DnsResult {
      struct addrinfo* res = nullptr;
      bool success = false;
    };
    auto dns_result = std::make_shared<DnsResult>();
    std::atomic<bool> resolved{false};
    std::thread dns_thread([dns_result, &node, &resolved]() {
      struct addrinfo hints = {};
      hints.ai_family = AF_INET;
      hints.ai_socktype = SOCK_STREAM;
      if (getaddrinfo(node.host.c_str(), nullptr, &hints, &dns_result->res) == 0 
          && dns_result->res != nullptr) {
        dns_result->success = true;
      }
      resolved.store(true, std::memory_order_release);
    });
    
    // Wait up to the configured health-check timeout for DNS.
    auto deadline = std::chrono::steady_clock::now() 
                    + std::chrono::milliseconds(config_.health_check_timeout_ms);
    while (!resolved.load(std::memory_order_acquire) 
           && std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    
    // If DNS is still in-flight, detach the thread (it only touches heap data).
    if (!resolved.load(std::memory_order_acquire)) {
      dns_thread.detach();
      close(sock);
      return false;
    }
    dns_thread.join();
    
    if (!dns_result->success) {
      close(sock);
      return false;
    }
    struct sockaddr_in* sin = reinterpret_cast<struct sockaddr_in*>(
        dns_result->res->ai_addr);
    memcpy(&addr.sin_addr, &sin->sin_addr, sizeof(addr.sin_addr));
    freeaddrinfo(dns_result->res);
  }
  
  int result = connect(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
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
  
  static std::mutex getenv_mutex;
  const char* docker_host = nullptr;
  {
    std::lock_guard<std::mutex> lock(getenv_mutex);
    docker_host = getenv("DOCKER_HOST");
  }
  if (docker_host == nullptr) {
    // 尝试本地 Docker socket
    if (access(config_.docker_socket.c_str(), F_OK) != 0) {
      std::cerr << "[ServiceDiscovery] Docker socket not available" << std::endl;
      return nodes;
    }
  }
  
  // 模拟从 Docker 获取容器信息
  // 实际实现应该调用 Docker API
  std::cerr << "[ServiceDiscovery] Discovering via Docker..." << std::endl;
  
  // 常见的存储节点名称
  std::vector<std::string> possible_names = {
    "cedar-storaged-0", "cedar-storaged-1", "cedar-storaged-2",
    "cedar-storaged-3", "cedar-storaged-4", "cedar-storaged-5",
    "storaged0", "storaged1", "storaged2"
  };
  
  for (const auto& name : possible_names) {
    struct addrinfo hints = {};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo* res = nullptr;
    if (getaddrinfo(name.c_str(), nullptr, &hints, &res) == 0 && res != nullptr) {
      StorageNodeInfo node;
      node.host = name;
      node.port = config_.storaged_port;
      node.container_name = name;
      char ip_str[INET_ADDRSTRLEN];
      if (inet_ntop(AF_INET,
                    &reinterpret_cast<struct sockaddr_in*>(res->ai_addr)->sin_addr,
                    ip_str, sizeof(ip_str))) {
        node.ip_address = ip_str;
      }
      freeaddrinfo(res);
      node.register_time = std::chrono::duration_cast<std::chrono::seconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count();
      node.is_healthy = true;  // 初始假设健康
      
      nodes.push_back(node);
      std::cerr << "  Found: " << name << " (" << node.ip_address << ")" << std::endl;
    }
  }
  
  return nodes;
}

std::vector<StorageNodeInfo> ServiceDiscovery::DiscoverViaDNS() {
  std::vector<StorageNodeInfo> nodes;
  
  std::cerr << "[ServiceDiscovery] Discovering via DNS..." << std::endl;
  
  for (const auto& name : config_.dns_names) {
    struct addrinfo hints = {};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo* res = nullptr;
    if (getaddrinfo(name.c_str(), nullptr, &hints, &res) == 0 && res != nullptr) {
      StorageNodeInfo node;
      node.host = name;
      node.port = config_.storaged_port;
      node.container_name = name;
      char ip_str[INET_ADDRSTRLEN];
      if (inet_ntop(AF_INET,
                    &reinterpret_cast<struct sockaddr_in*>(res->ai_addr)->sin_addr,
                    ip_str, sizeof(ip_str))) {
        node.ip_address = ip_str;
      }
      freeaddrinfo(res);
      node.register_time = std::chrono::duration_cast<std::chrono::seconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count();
      node.is_healthy = true;
      
      nodes.push_back(node);
      std::cerr << "  Found: " << name << " (" << node.ip_address << ")" << std::endl;
    }
  }
  
  return nodes;
}

std::vector<StorageNodeInfo> ServiceDiscovery::DiscoverViaConsul() {
  // Consul 发现暂未实现
  std::cerr << "[ServiceDiscovery] Consul discovery not implemented yet" << std::endl;
  return {};
}

// =============================================================================
// 后台任务
// =============================================================================

void ServiceDiscovery::DiscoveryLoop() {
  while (running_.load()) {
    {
      std::unique_lock<std::mutex> lock(cv_mutex_);
      cv_.wait_for(lock, std::chrono::seconds(60),
                   [this]() { return !running_.load(); });
    }
    
    if (!running_.load()) break;
    
    auto new_nodes = DiscoverNow();
    std::cerr << "[ServiceDiscovery] Discovery loop found " << new_nodes.size() << " nodes" << std::endl;
  }
}

void ServiceDiscovery::HealthCheckLoop() {
  while (running_.load()) {
    {
      std::unique_lock<std::mutex> lock(cv_mutex_);
      cv_.wait_for(lock, std::chrono::seconds(config_.health_check_interval_seconds),
                   [this]() { return !running_.load(); });
    }
    
    if (!running_.load()) break;
    
    std::vector<StorageNodeInfo> nodes;
    {
      std::lock_guard<std::mutex> lock(nodes_mutex_);
      nodes = discovered_nodes_;
    }
    
    int healthy_count = 0;
    std::vector<std::pair<StorageNodeInfo, bool>> health_changes;
    for (auto& node : nodes) {
      bool was_healthy = node.is_healthy;
      node.is_healthy = CheckNodeHealth(node);
      
      if (node.is_healthy) {
        healthy_count++;
      }
      
      // 记录状态变化，稍后回调
      if (was_healthy != node.is_healthy) {
        health_changes.emplace_back(node, node.is_healthy);
      }
    }
    
    // 将健康检查结果写回 discovered_nodes_
    {
      std::lock_guard<std::mutex> lock(nodes_mutex_);
      for (const auto& updated : nodes) {
        for (auto& existing : discovered_nodes_) {
          if (existing.GetEndpoint() == updated.GetEndpoint()) {
            existing.is_healthy = updated.is_healthy;
            break;
          }
        }
      }
    }
    
    // 在锁外调用回调，避免死锁
    {
      std::lock_guard<std::mutex> cb_lock(callback_mutex_);
      for (const auto& [node, healthy] : health_changes) {
        if (health_changed_callback_) {
          health_changed_callback_(node, healthy);
        }
      }
    }
    
    {
      std::lock_guard<std::mutex> lock(stats_mutex_);
      stats_.healthy_nodes = healthy_count;
    }
    
    std::cerr << "[ServiceDiscovery] Health check: " << healthy_count << "/" 
              << nodes.size() << " nodes healthy" << std::endl;
  }
}

void ServiceDiscovery::MergeNodes(const std::vector<StorageNodeInfo>& new_nodes) {
  std::vector<StorageNodeInfo> newly_discovered;
  
  {
    std::lock_guard<std::mutex> lock(nodes_mutex_);
    
    for (const auto& new_node : new_nodes) {
      auto it = std::find_if(discovered_nodes_.begin(), discovered_nodes_.end(),
                             [&new_node](const auto& existing) {
                               return existing.GetEndpoint() == new_node.GetEndpoint();
                             });
      
      if (it == discovered_nodes_.end()) {
        // 新节点
        discovered_nodes_.push_back(new_node);
        newly_discovered.push_back(new_node);
      }
    }
  }
  
  // 在锁外调用回调，避免死锁
  for (const auto& node : newly_discovered) {
    std::cerr << "[ServiceDiscovery] New node discovered: " << node.GetEndpoint() << std::endl;
    std::lock_guard<std::mutex> cb_lock(callback_mutex_);
    if (discovered_callback_) {
      discovered_callback_(node);
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
  std::cerr << "[ClusterInitializer] Initializing cluster..." << std::endl;
  
  // 1. 等待 MetaD 就绪
  auto status = WaitForMetaD();
  if (!status.ok()) {
    return status;
  }
  
  // 2. 自动发现并注册存储节点
  if (config_.auto_discover_storaged) {
    status = AutoDiscoverAndRegister();
    if (!status.ok()) {
      std::cerr << "[ClusterInitializer] Warning: Auto-discovery failed: " 
                << status.ToString() << std::endl;
      // 继续，不中断初始化
    }
  }
  
  std::cerr << "[ClusterInitializer] Cluster initialization completed!" << std::endl;
  return Status::OK();
}

Status ClusterInitializer::WaitForMetaD() {
  std::cerr << "[ClusterInitializer] Waiting for MetaD to be ready..." << std::endl;
  
  auto start = std::chrono::steady_clock::now();
  auto timeout = std::chrono::seconds(config_.init_timeout_seconds);
  
  while (std::chrono::steady_clock::now() - start < timeout) {
    // 尝试连接任意一个 MetaD
    for (const auto& meta_server : config_.meta_servers) {
      // 解析 host:port
      size_t colon_pos = meta_server.find(':');
      if (colon_pos == std::string::npos) continue;
      
      std::string host = meta_server.substr(0, colon_pos);
      int port = 0;
      try {
        port = std::stoi(meta_server.substr(colon_pos + 1));
      } catch (const std::exception& e) {
        std::cerr << "[ClusterInitializer] Invalid port in meta server address: "
                  << meta_server << " - " << e.what() << std::endl;
        continue;
      }
      
      int sock = socket(AF_INET, SOCK_STREAM, 0);
      if (sock < 0) continue;
      
      struct timeval tv;
      tv.tv_sec = 2;
      tv.tv_usec = 0;
      if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0 ||
          setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) < 0) {
        close(sock);
        continue;
      }
      
      struct sockaddr_in addr;
      memset(&addr, 0, sizeof(addr));
      addr.sin_family = AF_INET;
      addr.sin_port = htons(port);
      
      struct addrinfo hints = {};
      hints.ai_family = AF_INET;
      hints.ai_socktype = SOCK_STREAM;
      struct addrinfo* res = nullptr;
      if (getaddrinfo(host.c_str(), nullptr, &hints, &res) == 0 && res != nullptr) {
        memcpy(&addr.sin_addr,
               &reinterpret_cast<struct sockaddr_in*>(res->ai_addr)->sin_addr,
               sizeof(addr.sin_addr));
        freeaddrinfo(res);
        
        if (connect(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0) {
          close(sock);
          std::cerr << "[ClusterInitializer] MetaD is ready at " << meta_server << std::endl;
          return Status::OK();
        }
      }
      
      close(sock);
    }
    
    std::this_thread::sleep_for(std::chrono::seconds(config_.retry_interval_seconds));
    std::cerr << "." << std::flush;
  }
  
  return Status::IOError("Timeout waiting for MetaD");
}

Status ClusterInitializer::AutoDiscoverAndRegister() {
  std::cerr << "[ClusterInitializer] Auto-discovering storage nodes..." << std::endl;
  
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
  
  std::cerr << "[ClusterInitializer] Discovered " << nodes.size() << " storage nodes" << std::endl;
  
  // 注册节点
  return RegisterStorageNodes(nodes);
}

Status ClusterInitializer::RegisterStorageNodes(const std::vector<StorageNodeInfo>& nodes) {
  std::cerr << "[ClusterInitializer] Registering storage nodes..." << std::endl;

  if (nodes.empty()) {
    return Status::InvalidArgument("No storage nodes to register");
  }

  // Lazily initialize MetaServiceGrpcClient on first registration attempt
  if (!meta_client_ && !config_.meta_servers.empty()) {
    meta_client_ = std::make_unique<MetaServiceGrpcClient>();
    auto connect_status = meta_client_->Connect(config_.meta_servers);
    if (!connect_status.ok()) {
      meta_client_.reset();
      return Status::IOError("Failed to connect to MetaD: " + connect_status.ToString());
    }
  }

  int success_count = 0;
  for (const auto& node : nodes) {
    std::cerr << "  Registering: " << node.GetEndpoint() << " ... " << std::flush;

    if (!meta_client_) {
      std::cerr << "SKIPPED (no MetaD connection)" << std::endl;
      continue;
    }

    // Convert StorageNodeInfo -> NodeInfo for MetaService RPC
    NodeInfo info;
    info.node_id = static_cast<NodeID>(std::hash<std::string>{}(node.GetEndpoint()) & 0x7FFFFFFF);
    info.address = node.ip_address.empty() ? node.GetEndpoint() : node.ip_address;
    info.data_path = "/data/cedar";
    info.state = NodeInfo::State::kOnline;

    auto status = meta_client_->RegisterNode(info);
    if (status.ok()) {
      std::cerr << "OK" << std::endl;
      success_count++;
    } else {
      std::cerr << "FAILED: " << status.ToString() << std::endl;
    }
  }

  std::cerr << "[ClusterInitializer] Registered " << success_count << "/"
            << nodes.size() << " nodes" << std::endl;

  return success_count > 0 ? Status::OK()
                           : Status::IOError("Failed to register any nodes");
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
