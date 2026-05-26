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
// gRPC Connection Pool Implementation
// =============================================================================

#include "cedar/dtx/grpc_connection_pool.h"
#include "cedar/dtx/raft/grpc_tls.h"

#include <iostream>
#include <chrono>
#include <grpcpp/grpcpp.h>

namespace cedar {
namespace dtx {

// =============================================================================
// PooledChannel Implementation
// =============================================================================

PooledChannel::PooledChannel(const std::string& address, 
                              std::shared_ptr<grpc::Channel> channel)
    : address_(address), 
      channel_(channel),
      create_time_(std::chrono::steady_clock::now()),
      use_count_(0) {}

PooledChannel::~PooledChannel() = default;

bool PooledChannel::IsHealthy() const {
  if (!channel_) return false;
  
  // 检查连接状态
  auto state = channel_->GetState(false);
  return state == GRPC_CHANNEL_READY || state == GRPC_CHANNEL_IDLE;
}

// =============================================================================
// GrpcConnectionPool Implementation
// =============================================================================

GrpcConnectionPool::GrpcConnectionPool(const Config& config) 
    : config_(config),
      in_use_count_(0),
      total_acquisitions_(0),
      total_releases_(0),
      total_wait_time_us_(0),
      shutdown_(false) {}

GrpcConnectionPool::GrpcConnectionPool() 
    : GrpcConnectionPool(Config()) {}

GrpcConnectionPool::~GrpcConnectionPool() {
  Shutdown();
}

Status GrpcConnectionPool::Initialize(const std::vector<std::string>& endpoints) {
  std::cerr << "[ConnectionPool] Initializing with " << endpoints.size() 
            << " endpoints..." << std::endl;
  
  endpoints_ = endpoints;
  
  // 为每个端点创建最小连接数
  for (const auto& endpoint : endpoints_) {
    std::cerr << "  Creating connections for: " << endpoint << std::endl;
    
    for (size_t i = 0; i < config_.min_connections_per_endpoint; ++i) {
      auto conn = CreateConnection(endpoint);
      if (conn) {
        available_connections_.push(conn);
        endpoint_connections_[endpoint].push_back(conn);
      }
    }
  }
  
  std::cerr << "[ConnectionPool] Created " << available_connections_.size() 
            << " initial connections" << std::endl;
  
  // 启动后台线程
  if (config_.health_check_interval.count() > 0) {
    health_check_thread_ = std::thread(&GrpcConnectionPool::HealthCheckLoop, this);
  }
  
  cleanup_thread_ = std::thread(&GrpcConnectionPool::IdleCleanupLoop, this);
  
  // 预热连接
  if (config_.enable_connection_warmup) {
    return WarmupConnections();
  }
  
  return Status::OK();
}

std::shared_ptr<PooledChannel> GrpcConnectionPool::Acquire() {
  auto start = std::chrono::steady_clock::now();
  
  std::unique_lock<std::mutex> lock(mutex_);
  
  // 等待可用连接
  cv_.wait(lock, [this] {
    return shutdown_ || !available_connections_.empty();
  });
  
  if (shutdown_) {
    return nullptr;
  }
  
  // 获取连接
  auto channel = available_connections_.front();
  available_connections_.pop();
  in_use_count_++;

  // 检查健康状态
  if (!CheckHealth(channel)) {
    // 检查该端点连接数是否已达上限
    size_t endpoint_count = 0;
    auto ep_it = endpoint_connections_.find(channel->GetAddress());
    if (ep_it != endpoint_connections_.end()) {
      endpoint_count = ep_it->second.size();
    }
    if (endpoint_count >= config_.max_connections_per_endpoint) {
      in_use_count_--;
      return nullptr;
    }
    // 连接不健康，创建新连接替换
    auto old_address = channel->GetAddress();
    auto old_channel = channel;
    lock.unlock();
    channel = CreateConnection(old_address);
    lock.lock();

    if (!channel) {
      in_use_count_--;
      return nullptr;
    }
    // 从 endpoint_connections_ 中移除旧的不健康连接
    auto ep_it2 = endpoint_connections_.find(old_address);
    if (ep_it2 != endpoint_connections_.end()) {
      auto& conns = ep_it2->second;
      conns.erase(std::remove(conns.begin(), conns.end(), old_channel), conns.end());
      if (conns.empty()) {
        endpoint_connections_.erase(ep_it2);
      }
    }
    endpoint_connections_[old_address].push_back(channel);
  }
  
  channel->IncrementUseCount();
  
  // 统计
  total_acquisitions_++;
  auto wait_time = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now() - start).count();
  total_wait_time_us_ += wait_time;
  
  return channel;
}

std::shared_ptr<PooledChannel> GrpcConnectionPool::AcquireForEndpoint(
    const std::string& endpoint) {
  
  std::unique_lock<std::mutex> lock(mutex_);
  
  // 查找该端点的连接
  auto it = endpoint_connections_.find(endpoint);
  if (it != endpoint_connections_.end()) {
    for (auto& conn : it->second) {
      // 检查是否在可用队列中
      // 简化实现：直接返回第一个健康连接
      if (conn->GetUseCount() == 0 && CheckHealth(conn)) {
        in_use_count_++;
        conn->IncrementUseCount();
        total_acquisitions_++;
        return conn;
      }
    }
    // 已达上限且没有健康连接，拒绝创建
    if (it->second.size() >= config_.max_connections_per_endpoint) {
      return nullptr;
    }
  }

  lock.unlock();

  // 创建新连接
  auto conn = CreateConnection(endpoint);
  if (conn) {
    std::lock_guard<std::mutex> lock(mutex_);
    in_use_count_++;
    endpoint_connections_[endpoint].push_back(conn);
    total_acquisitions_++;
  }
  
  return conn;
}

void GrpcConnectionPool::Release(std::shared_ptr<PooledChannel> channel) {
  if (!channel) return;
  
  std::lock_guard<std::mutex> lock(mutex_);
  
  channel->DecrementUseCount();
  available_connections_.push(channel);
  in_use_count_--;
  total_releases_++;
  
  cv_.notify_one();
}

Status GrpcConnectionPool::WarmupConnections() {
  std::cerr << "[ConnectionPool] Warming up connections..." << std::endl;
  
  // 对每个端点执行一次简单调用以建立连接
  for (const auto& endpoint : endpoints_) {
    auto conn = AcquireForEndpoint(endpoint);
    if (conn) {
      // 这里可以执行一个简单的健康检查 RPC
      // 简化实现：只检查连接状态
      Release(conn);
    }
  }
  
  std::cerr << "[ConnectionPool] Warmup completed" << std::endl;
  return Status::OK();
}

GrpcConnectionPool::Stats GrpcConnectionPool::GetStats() const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  Stats stats;
  stats.total_connections = available_connections_.size() + in_use_count_.load();
  stats.available_connections = available_connections_.size();
  stats.in_use_connections = in_use_count_.load();
  stats.total_acquisitions = total_acquisitions_.load();
  stats.total_releases = total_releases_.load();
  
  // 计算平均等待时间
  if (total_acquisitions_.load() > 0) {
    stats.avg_wait_time_us = (double)total_wait_time_us_.load() / total_acquisitions_.load();
  } else {
    stats.avg_wait_time_us = 0.0;
  }
  
  // 计算不健康连接数
  stats.unhealthy_connections = 0;
  auto temp_queue = available_connections_;
  while (!temp_queue.empty()) {
    auto conn = temp_queue.front();
    temp_queue.pop();
    if (!conn->IsHealthy()) {
      stats.unhealthy_connections++;
    }
  }
  
  return stats;
}

void GrpcConnectionPool::Shutdown() noexcept {
  std::cerr << "[ConnectionPool] Shutting down..." << std::endl;
  
  shutdown_ = true;
  cv_.notify_all();
  
  std::lock_guard<std::mutex> join_lock(shutdown_mutex_);
  try {
    if (health_check_thread_.joinable()) {
      health_check_thread_.join();
    }
  } catch (...) {
    std::cerr << "[GrpcConnectionPool] Thread join exception" << std::endl;
  }
  
  try {
    if (cleanup_thread_.joinable()) {
      cleanup_thread_.join();
    }
  } catch (...) {
    std::cerr << "[GrpcConnectionPool] Thread join exception" << std::endl;
  }
  
  // 清空连接池
  std::lock_guard<std::mutex> lock(mutex_);
  while (!available_connections_.empty()) {
    available_connections_.pop();
  }
  endpoint_connections_.clear();
  
  std::cerr << "[ConnectionPool] Shutdown completed" << std::endl;
}

void GrpcConnectionPool::HealthCheckLoop() {
  while (!shutdown_) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait_for(lock, config_.health_check_interval, [this]() { return shutdown_.load(); });
    }
    
    if (shutdown_) break;
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    // 检查所有连接的健康状态
    auto temp_queue = available_connections_;
    std::queue<std::shared_ptr<PooledChannel>> healthy_queue;
    
    while (!temp_queue.empty()) {
      auto conn = temp_queue.front();
      temp_queue.pop();
      
      if (CheckHealth(conn)) {
        healthy_queue.push(conn);
      } else {
        // 不健康的连接会被丢弃，后续会创建新连接
        std::cerr << "[ConnectionPool] Removing unhealthy connection to " 
                  << conn->GetAddress() << std::endl;
      }
    }
    
    available_connections_ = std::move(healthy_queue);
  }
}

void GrpcConnectionPool::IdleCleanupLoop() {
  while (!shutdown_) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait_for(lock, std::chrono::seconds(60), [this]() { return shutdown_.load(); });
    }
    
    if (shutdown_) break;
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto now = std::chrono::steady_clock::now();
    std::queue<std::shared_ptr<PooledChannel>> keep_queue;
    size_t removed = 0;
    
    // 清理空闲时间过长的连接，但保留最小连接数
    size_t kept = 0;
    while (!available_connections_.empty()) {
      auto conn = available_connections_.front();
      available_connections_.pop();
      
      auto idle_time = std::chrono::duration_cast<std::chrono::seconds>(
          now - conn->GetCreateTime()).count();
      
      // 保留条件：空闲时间未超过上限，或未达到每个端点的最小连接数
      if (idle_time < config_.max_idle_time.count() || 
          kept < config_.min_connections_per_endpoint * endpoints_.size()) {
        keep_queue.push(conn);
        kept++;
      } else {
        removed++;
      }
    }
    
    available_connections_ = std::move(keep_queue);
    
    if (removed > 0) {
      std::cerr << "[ConnectionPool] Cleaned up " << removed 
                << " idle connections" << std::endl;
    }
  }
}

std::shared_ptr<PooledChannel> GrpcConnectionPool::CreateConnection(
    const std::string& endpoint) {
  
  // 创建 gRPC 通道选项
  grpc::ChannelArguments args;
  args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 10000);      // 10s keepalive
  args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 5000);    // 5s timeout
  args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
  args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 5000);
  
  // 创建通道
  auto client_creds = cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentialsFromEnv();
  if (!client_creds.ok()) {
    std::cerr << "[ConnectionPool] TLS error: " << client_creds.status().ToString() << std::endl;
    return nullptr;
  }
  auto channel = grpc::CreateCustomChannel(
      endpoint,
      client_creds.ValueOrDie(),
      args);

  // 等待连接就绪（避免将未连接好的通道放入连接池）
  auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(2);
  if (!channel->WaitForConnected(deadline)) {
    std::cerr << "[ConnectionPool] Channel to " << endpoint
              << " did not connect within 2s" << std::endl;
  }

  return std::make_shared<PooledChannel>(endpoint, channel);
}

bool GrpcConnectionPool::CheckHealth(const std::shared_ptr<PooledChannel>& channel) {
  if (!channel) return false;
  return channel->IsHealthy();
}

// =============================================================================
// ConnectionPoolManager Implementation
// =============================================================================

ConnectionPoolManager& ConnectionPoolManager::Instance() {
  static ConnectionPoolManager instance;
  return instance;
}

std::shared_ptr<GrpcConnectionPool> ConnectionPoolManager::GetPool(
    const std::string& pool_name) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto it = pools_.find(pool_name);
  if (it != pools_.end()) {
    return it->second;
  }
  
  // 创建新连接池
  auto pool = std::make_shared<GrpcConnectionPool>();
  pools_[pool_name] = pool;
  return pool;
}

void ConnectionPoolManager::DestroyPool(const std::string& pool_name) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto it = pools_.find(pool_name);
  if (it != pools_.end()) {
    it->second->Shutdown();
    pools_.erase(it);
  }
}

void ConnectionPoolManager::DestroyAllPools() {
  std::lock_guard<std::mutex> lock(mutex_);
  
  for (auto& [name, pool] : pools_) {
    pool->Shutdown();
  }
  pools_.clear();
}

// =============================================================================
// ScopedConnection Implementation
// =============================================================================

ScopedConnection::ScopedConnection(GrpcConnectionPool& pool) 
    : pool_(pool) {
  channel_ = pool_.Acquire();
}

ScopedConnection::~ScopedConnection() {
  if (channel_) {
    pool_.Release(channel_);
  }
}

}  // namespace dtx
}  // namespace cedar
