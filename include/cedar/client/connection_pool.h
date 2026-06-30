// Copyright 2025 The Cedar Authors
//
// Connection Pool for gRPC connections
// Manages connections to MetaD, GraphD, and StorageD services

#ifndef CEDAR_CLIENT_CONNECTION_POOL_H_
#define CEDAR_CLIENT_CONNECTION_POOL_H_

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>

namespace cedar {
namespace client {

// Connection configuration
struct ConnectionConfig {
  std::string host;
  int port;
  int max_connections = 10;
  int timeout_ms = 30000;  // 30 seconds idle timeout
  bool enable_tls = false;
  bool mtls_enabled = false;
  std::string ca_cert_path;
  std::string client_cert_path;
  std::string client_key_path;
};

// Internal connection wrapper
struct ConnectionWrapper {
  std::shared_ptr<grpc::Channel> channel;
  std::chrono::steady_clock::time_point last_used;
  bool available;
};

// Connection pool for a single service
class ConnectionPool {
 public:
  ConnectionPool(const ConnectionConfig& config);
  ~ConnectionPool();

  // Get a connection from the pool
  std::shared_ptr<grpc::Channel> GetConnection();

  // Return a connection to the pool
  void ReturnConnection(std::shared_ptr<grpc::Channel> connection);

  // Health check - replace unhealthy connections
  void HealthCheck();

  // Get pool statistics
  int GetActiveConnections() const;
  int GetAvailableConnections() const;
  int GetTotalConnections() const;

  // Health check
  bool IsHealthy() const;

 private:
  ConnectionConfig config_;
  std::vector<ConnectionWrapper> connections_;
  mutable std::mutex mutex_;
  int active_connections_ = 0;
  int temp_connections_ = 0;
  
  // Create a new channel
  std::shared_ptr<grpc::Channel> CreateChannel() const;
  
  // Check if a channel is healthy
  bool IsChannelHealthy(const std::shared_ptr<grpc::Channel>& channel) const;
};

// Connection pool manager for all services
class ConnectionPoolManager {
 public:
  ConnectionPoolManager();
  ~ConnectionPoolManager();

  // Initialize connection pools for all services
  void Initialize(const ConnectionConfig& metad_config,
                  const ConnectionConfig& graphd_config,
                  const ConnectionConfig& storaged_config);

  // Get connection pool for a specific service
  ConnectionPool* GetMetaDConnectionPool();
  ConnectionPool* GetGraphDConnectionPool();
  ConnectionPool* GetStorageDConnectionPool();

  // Get connections directly
  std::shared_ptr<grpc::Channel> GetMetaDConnection();
  std::shared_ptr<grpc::Channel> GetGraphDConnection();
  std::shared_ptr<grpc::Channel> GetStorageDConnection();

  // Health check for all services
  bool IsHealthy() const;

 private:
  std::unique_ptr<ConnectionPool> metad_pool_;
  std::unique_ptr<ConnectionPool> graphd_pool_;
  std::unique_ptr<ConnectionPool> storaged_pool_;
  
  // Health check thread
  std::thread health_check_thread_;
  std::atomic<bool> running_{false};
  std::condition_variable condition_;
  std::mutex mutex_;
};

}  // namespace client
}  // namespace cedar

#endif  // CEDAR_CLIENT_CONNECTION_POOL_H_
