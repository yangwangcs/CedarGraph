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
// gRPC Connection Pool
// gRPC 连接池 - 复用连接减少建立开销，提升 20-30% 性能
// =============================================================================

#ifndef CEDAR_DTX_GRPC_CONNECTION_POOL_H_
#define CEDAR_DTX_GRPC_CONNECTION_POOL_H_

#include <grpcpp/grpcpp.h>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>
#include <atomic>
#include <condition_variable>
#include <thread>
#include <chrono>

#include "cedar/core/status.h"

namespace cedar {
namespace dtx {

// =============================================================================
// Pooled Channel - 可复用的 gRPC 通道包装
// =============================================================================

class PooledChannel {
 public:
  PooledChannel(const std::string& address, std::shared_ptr<grpc::Channel> channel);
  ~PooledChannel();
  
  // 获取底层 gRPC 通道
  std::shared_ptr<grpc::Channel> GetChannel() const { return channel_; }
  
  // 健康检查
  bool IsHealthy() const;
  
  // 获取地址
  const std::string& GetAddress() const { return address_; }
  
  // 获取创建时间
  std::chrono::steady_clock::time_point GetCreateTime() const { return create_time_; }
  
  // 获取使用次数
  uint64_t GetUseCount() const { return use_count_.load(); }
  void IncrementUseCount() { use_count_++; }
  
 private:
  std::string address_;
  std::shared_ptr<grpc::Channel> channel_;
  std::chrono::steady_clock::time_point create_time_;
  std::atomic<uint64_t> use_count_;
};

// =============================================================================
// gRPC Connection Pool
// =============================================================================

class GrpcConnectionPool {
 public:
  struct Config {
    size_t min_connections_per_endpoint;
    size_t max_connections_per_endpoint;
    std::chrono::seconds max_idle_time;
    std::chrono::seconds health_check_interval;
    bool enable_connection_warmup;
    
    Config() 
        : min_connections_per_endpoint(2),
          max_connections_per_endpoint(10),
          max_idle_time(std::chrono::seconds(300)),
          health_check_interval(std::chrono::seconds(30)),
          enable_connection_warmup(true) {}
  };
  
  explicit GrpcConnectionPool(const Config& config);
  GrpcConnectionPool();
  ~GrpcConnectionPool();
  
  // 初始化连接池
  Status Initialize(const std::vector<std::string>& endpoints);
  
  // 获取连接（轮询）
  std::shared_ptr<PooledChannel> Acquire();
  
  // 获取特定端点的连接
  std::shared_ptr<PooledChannel> AcquireForEndpoint(const std::string& endpoint);
  
  // 归还连接
  void Release(std::shared_ptr<PooledChannel> channel);
  
  // 预热连接
  Status WarmupConnections();
  
  // 获取统计信息
  struct Stats {
    size_t total_connections;
    size_t available_connections;
    size_t in_use_connections;
    size_t unhealthy_connections;
    uint64_t total_acquisitions;
    uint64_t total_releases;
    double avg_wait_time_us;
    
    Stats() : total_connections(0), available_connections(0), 
              in_use_connections(0), unhealthy_connections(0),
              total_acquisitions(0), total_releases(0), avg_wait_time_us(0.0) {}
  };
  Stats GetStats() const;
  
  // 关闭连接池
  void Shutdown();
  
 private:
  // 后台任务
  void HealthCheckLoop();
  void IdleCleanupLoop();
  
  // 创建新连接
  std::shared_ptr<PooledChannel> CreateConnection(const std::string& endpoint);
  
  // 检查连接健康
  bool CheckHealth(const std::shared_ptr<PooledChannel>& channel);
  
  Config config_;
  std::vector<std::string> endpoints_;
  
  // 连接池
  std::queue<std::shared_ptr<PooledChannel>> available_connections_;
  std::unordered_map<std::string, std::vector<std::shared_ptr<PooledChannel>>> endpoint_connections_;
  std::atomic<size_t> in_use_count_;
  
  // 同步
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  
  // 统计
  std::atomic<uint64_t> total_acquisitions_;
  std::atomic<uint64_t> total_releases_;
  std::atomic<uint64_t> total_wait_time_us_;
  
  // 后台线程
  std::atomic<bool> shutdown_;
  std::thread health_check_thread_;
  std::thread cleanup_thread_;
};

// =============================================================================
// Connection Pool Manager (Singleton)
// =============================================================================

class ConnectionPoolManager {
 public:
  static ConnectionPoolManager& Instance();
  
  // 获取或创建连接池
  std::shared_ptr<GrpcConnectionPool> GetPool(const std::string& pool_name);
  
  // 销毁连接池
  void DestroyPool(const std::string& pool_name);
  
  // 销毁所有连接池
  void DestroyAllPools();
  
 private:
  ConnectionPoolManager() = default;
  ~ConnectionPoolManager() = default;
  
  std::unordered_map<std::string, std::shared_ptr<GrpcConnectionPool>> pools_;
  mutable std::mutex mutex_;
};

// =============================================================================
// Scoped Connection - RAII 风格的连接获取
// =============================================================================

class ScopedConnection {
 public:
  ScopedConnection(GrpcConnectionPool& pool);
  ~ScopedConnection();
  
  // 获取底层通道
  std::shared_ptr<PooledChannel> GetChannel() const { return channel_; }
  
  // 检查是否有效
  bool IsValid() const { return channel_ != nullptr; }
  
 private:
  GrpcConnectionPool& pool_;
  std::shared_ptr<PooledChannel> channel_;
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_GRPC_CONNECTION_POOL_H_
