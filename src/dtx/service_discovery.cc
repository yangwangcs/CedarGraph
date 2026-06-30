#include "cedar/core/logging.h"
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

#include <algorithm>
#include <arpa/inet.h>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <iostream>
#include <mutex>
#include <netdb.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

namespace cedar {
namespace dtx {

namespace {

struct TimedDnsResult {
  std::mutex mutex;
  std::condition_variable cv;
  struct addrinfo* res = nullptr;
  bool success = false;
  bool done = false;

  ~TimedDnsResult() {
    if (res) {
      freeaddrinfo(res);
      res = nullptr;
    }
  }
};

bool ResolveIPv4WithTimeout(const std::string& host,
                            std::chrono::milliseconds timeout,
                            in_addr* out_addr) {
  if (!out_addr || timeout.count() <= 0) {
    return false;
  }

  if (inet_pton(AF_INET, host.c_str(), out_addr) == 1) {
    return true;
  }

  auto dns_result = std::make_shared<TimedDnsResult>();
  auto host_copy = std::make_shared<std::string>(host);

  std::thread dns_thread([dns_result, host_copy]() {
    struct addrinfo hints = {};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo* resolved = nullptr;
    bool success = getaddrinfo(host_copy->c_str(), nullptr, &hints, &resolved) == 0
                   && resolved != nullptr;

    {
      std::lock_guard<std::mutex> lock(dns_result->mutex);
      dns_result->res = resolved;
      dns_result->success = success;
      dns_result->done = true;
    }
    dns_result->cv.notify_all();
  });

  {
    std::unique_lock<std::mutex> lock(dns_result->mutex);
    if (!dns_result->cv.wait_for(lock, timeout, [&dns_result]() {
          return dns_result->done;
        })) {
      dns_thread.detach();
      return false;
    }
  }

  dns_thread.join();

  std::lock_guard<std::mutex> lock(dns_result->mutex);
  if (!dns_result->success || dns_result->res == nullptr) {
    return false;
  }

  auto* sin = reinterpret_cast<struct sockaddr_in*>(dns_result->res->ai_addr);
  memcpy(out_addr, &sin->sin_addr, sizeof(*out_addr));
  return true;
}

bool ParseHostPort(const std::string& endpoint, std::string* host, int* port) {
  if (!host || !port) {
    return false;
  }

  size_t colon_pos = endpoint.rfind(':');
  if (colon_pos == std::string::npos || colon_pos == 0 ||
      colon_pos + 1 >= endpoint.size()) {
    return false;
  }

  std::string parsed_host = endpoint.substr(0, colon_pos);
  int parsed_port = 0;
  try {
    size_t consumed = 0;
    parsed_port = std::stoi(endpoint.substr(colon_pos + 1), &consumed);
    if (consumed != endpoint.size() - colon_pos - 1) {
      return false;
    }
  } catch (const std::exception&) {
    return false;
  }

  if (parsed_port <= 0 || parsed_port > 65535) {
    return false;
  }

  *host = std::move(parsed_host);
  *port = parsed_port;
  return true;
}

}  // namespace

// =============================================================================
// ServiceDiscovery Implementation
// =============================================================================

ServiceDiscovery::ServiceDiscovery(const ServiceDiscoveryConfig& config)
    : config_(config) {}

ServiceDiscovery::~ServiceDiscovery() {
  Stop();
}

Status ServiceDiscovery::Initialize() {
  if (config_.storaged_port <= 0 || config_.storaged_port > 65535) {
    return Status::InvalidArgument("storaged_port must be in range 1..65535");
  }
  if (config_.health_check_interval_seconds <= 0) {
    return Status::InvalidArgument("health_check_interval_seconds must be positive");
  }
  if (config_.health_check_timeout_ms <= 0) {
    return Status::InvalidArgument("health_check_timeout_ms must be positive");
  }
  if (config_.register_retry_times < 0) {
    return Status::InvalidArgument("register_retry_times must not be negative");
  }

  if (initialized_.exchange(true)) {
    return Status::OK();  // 已初始化
  }
  
  CEDAR_LOG_ERROR() << "[ServiceDiscovery] Initializing..." << std::endl;
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
  
  CEDAR_LOG_ERROR() << "[ServiceDiscovery] Starting discovery service..." << std::endl;
  
  // 立即执行一次发现
  auto nodes = DiscoverNow();
  CEDAR_LOG_ERROR() << "[ServiceDiscovery] Initial discovery found " << nodes.size() << " nodes" << std::endl;
  
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
  
  CEDAR_LOG_ERROR() << "[ServiceDiscovery] Stopping discovery service..." << std::endl;
  
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

  if (!ResolveIPv4WithTimeout(node.host,
                              std::chrono::milliseconds(
                                  config_.health_check_timeout_ms),
                              &addr.sin_addr)) {
    close(sock);
    return false;
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
      CEDAR_LOG_ERROR() << "[ServiceDiscovery] Docker socket not available" << std::endl;
      return nodes;
    }
  }
  
  // 模拟从 Docker 获取容器信息
  // 实际实现应该调用 Docker API
  CEDAR_LOG_ERROR() << "[ServiceDiscovery] Discovering via Docker..." << std::endl;
  
  // 常见的存储节点名称
  std::vector<std::string> possible_names = {
    "cedar-storaged-0", "cedar-storaged-1", "cedar-storaged-2",
    "cedar-storaged-3", "cedar-storaged-4", "cedar-storaged-5",
    "storaged0", "storaged1", "storaged2"
  };
  
  for (const auto& name : possible_names) {
    in_addr resolved_addr;
    if (ResolveIPv4WithTimeout(
            name, std::chrono::milliseconds(config_.health_check_timeout_ms),
            &resolved_addr)) {
      StorageNodeInfo node;
      node.host = name;
      node.port = config_.storaged_port;
      node.container_name = name;
      char ip_str[INET_ADDRSTRLEN];
      if (inet_ntop(AF_INET, &resolved_addr, ip_str, sizeof(ip_str))) {
        node.ip_address = ip_str;
      }
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
  
  CEDAR_LOG_ERROR() << "[ServiceDiscovery] Discovering via DNS..." << std::endl;
  
  for (const auto& name : config_.dns_names) {
    in_addr resolved_addr;
    if (ResolveIPv4WithTimeout(
            name, std::chrono::milliseconds(config_.health_check_timeout_ms),
            &resolved_addr)) {
      StorageNodeInfo node;
      node.host = name;
      node.port = config_.storaged_port;
      node.container_name = name;
      char ip_str[INET_ADDRSTRLEN];
      if (inet_ntop(AF_INET, &resolved_addr, ip_str, sizeof(ip_str))) {
        node.ip_address = ip_str;
      }
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
  CEDAR_LOG_ERROR() << "[ServiceDiscovery] Consul discovery not implemented yet" << std::endl;
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
    CEDAR_LOG_ERROR() << "[ServiceDiscovery] Discovery loop found " << new_nodes.size() << " nodes" << std::endl;
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
    
    NodeHealthChangedCallback health_changed_callback;
    {
      std::lock_guard<std::mutex> cb_lock(callback_mutex_);
      health_changed_callback = health_changed_callback_;
    }
    if (health_changed_callback) {
      for (const auto& [node, healthy] : health_changes) {
        try {
          health_changed_callback(node, healthy);
        } catch (const std::exception& e) {
          CEDAR_LOG_ERROR() << "[ServiceDiscovery] Health callback exception: "
                            << e.what() << std::endl;
        } catch (...) {
          CEDAR_LOG_ERROR() << "[ServiceDiscovery] Health callback unknown exception"
                            << std::endl;
        }
      }
    }
    
    {
      std::lock_guard<std::mutex> lock(stats_mutex_);
      stats_.healthy_nodes = healthy_count;
    }
    
    CEDAR_LOG_ERROR() << "[ServiceDiscovery] Health check: " << healthy_count << "/" 
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
  
  NodeDiscoveredCallback discovered_callback;
  {
    std::lock_guard<std::mutex> cb_lock(callback_mutex_);
    discovered_callback = discovered_callback_;
  }

  // 在锁外调用回调，避免死锁
  for (const auto& node : newly_discovered) {
    CEDAR_LOG_ERROR() << "[ServiceDiscovery] New node discovered: " << node.GetEndpoint() << std::endl;
    if (discovered_callback) {
      try {
        discovered_callback(node);
      } catch (const std::exception& e) {
        CEDAR_LOG_ERROR() << "[ServiceDiscovery] Discovery callback exception: "
                          << e.what() << std::endl;
      } catch (...) {
        CEDAR_LOG_ERROR() << "[ServiceDiscovery] Discovery callback unknown exception"
                          << std::endl;
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
  CEDAR_LOG_ERROR() << "[ClusterInitializer] Initializing cluster..." << std::endl;
  
  // 1. 等待 MetaD 就绪
  auto status = WaitForMetaD();
  if (!status.ok()) {
    return status;
  }
  
  // 2. 自动发现并注册存储节点
  if (config_.auto_discover_storaged) {
    status = AutoDiscoverAndRegister();
    if (!status.ok()) {
      CEDAR_LOG_ERROR() << "[ClusterInitializer] Warning: Auto-discovery failed: " 
                << status.ToString() << std::endl;
      // 继续，不中断初始化
    }
  }
  
  CEDAR_LOG_ERROR() << "[ClusterInitializer] Cluster initialization completed!" << std::endl;
  return Status::OK();
}

Status ClusterInitializer::WaitForMetaD() {
  CEDAR_LOG_ERROR() << "[ClusterInitializer] Waiting for MetaD to be ready..." << std::endl;

  if (config_.meta_servers.empty()) {
    return Status::InvalidArgument("No MetaD servers configured");
  }
  if (config_.init_timeout_seconds <= 0) {
    return Status::InvalidArgument("init_timeout_seconds must be positive");
  }
  if (config_.retry_interval_seconds <= 0) {
    return Status::InvalidArgument("retry_interval_seconds must be positive");
  }
  
  auto start = std::chrono::steady_clock::now();
  auto timeout = std::chrono::seconds(config_.init_timeout_seconds);
  auto deadline = start + timeout;
  
  while (std::chrono::steady_clock::now() < deadline) {
    // 尝试连接任意一个 MetaD
    for (const auto& meta_server : config_.meta_servers) {
      // 解析 host:port
      std::string host;
      int port = 0;
      if (!ParseHostPort(meta_server, &host, &port)) {
        CEDAR_LOG_ERROR() << "[ClusterInitializer] Invalid port in meta server address: "
                          << meta_server << std::endl;
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

      if (ResolveIPv4WithTimeout(host, std::chrono::seconds(2),
                                 &addr.sin_addr)) {
        if (connect(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0) {
          close(sock);
          CEDAR_LOG_ERROR() << "[ClusterInitializer] MetaD is ready at " << meta_server << std::endl;
          return Status::OK();
        }
      }
      
      close(sock);
    }

    auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
      break;
    }
    auto sleep_time = std::min<std::chrono::steady_clock::duration>(
        std::chrono::seconds(config_.retry_interval_seconds), deadline - now);
    std::this_thread::sleep_for(sleep_time);
    std::cerr << "." << std::flush;
  }
  
  return Status::IOError("Timeout waiting for MetaD");
}

Status ClusterInitializer::AutoDiscoverAndRegister() {
  CEDAR_LOG_ERROR() << "[ClusterInitializer] Auto-discovering storage nodes..." << std::endl;
  
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
  
  CEDAR_LOG_ERROR() << "[ClusterInitializer] Discovered " << nodes.size() << " storage nodes" << std::endl;
  
  // 注册节点
  return RegisterStorageNodes(nodes);
}

Status ClusterInitializer::RegisterStorageNodes(const std::vector<StorageNodeInfo>& nodes) {
  CEDAR_LOG_ERROR() << "[ClusterInitializer] Registering storage nodes..." << std::endl;

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

  CEDAR_LOG_ERROR() << "[ClusterInitializer] Registered " << success_count << "/"
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
