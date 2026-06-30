// Copyright 2025 The Cedar Authors
//
// Connection Pool implementation with health checking and reconnection

#include "cedar/client/connection_pool.h"

#include <chrono>
#include <iostream>

#include "cedar/dtx/raft/grpc_tls.h"

namespace cedar {
namespace client {

// ============================================================================
// ConnectionPool
// ============================================================================

ConnectionPool::ConnectionPool(const ConnectionConfig& config) : config_(config) {
  // Don't pre-create connections - create them lazily on demand
  // This avoids timeout delays when services are not running
  // Connections will be created when first needed
}

ConnectionPool::~ConnectionPool() {
  // Clear all connections
  std::lock_guard<std::mutex> lock(mutex_);
  connections_.clear();
}

std::shared_ptr<grpc::Channel> ConnectionPool::CreateChannel() const {
  std::string target = config_.host + ":" + std::to_string(config_.port);
  
  if (config_.enable_tls || cedar::dtx::raft::TlsCredentialFactory::EnvTlsEnabled()) {
    cedar::dtx::raft::TlsConfig tls;
    tls.enabled = true;
    tls.ca_cert_file = config_.ca_cert_path;
    tls.mtls_enabled = config_.mtls_enabled;
    tls.client_cert_file = config_.client_cert_path;
    tls.client_key_file = config_.client_key_path;

    auto creds = config_.enable_tls
        ? cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentials(tls)
        : cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentialsFromEnvStrict();
    if (!creds.ok() && cedar::dtx::raft::TlsCredentialFactory::EnvTlsEnabled()) {
      creds = cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentialsFromEnvStrict();
    }
    if (!creds.ok()) {
      std::cerr << "[ConnectionPool] Failed to create TLS credentials for "
                << target << ": " << creds.status().ToString() << std::endl;
      return nullptr;
    }
    return grpc::CreateChannel(target, creds.ValueOrDie());
  }

  return grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
}

std::shared_ptr<grpc::Channel> ConnectionPool::GetConnection() {
  std::lock_guard<std::mutex> lock(mutex_);
  
  // Try to find an available healthy connection
  for (auto it = connections_.begin(); it != connections_.end(); ++it) {
    if (it->available && IsChannelHealthy(it->channel)) {
      it->available = false;
      it->last_used = std::chrono::steady_clock::now();
      active_connections_++;
      return it->channel;
    }
  }
  
  // No available connection, try to create a new one
  if (GetTotalConnections() < config_.max_connections) {
    auto channel = CreateChannel();
    if (channel && IsChannelHealthy(channel)) {
      connections_.push_back({channel, std::chrono::steady_clock::now(), false});
      active_connections_++;
      return channel;
    }
  }
  
  // Pool is full, wait for a connection to become available
  // For now, create a temporary connection
  auto channel = CreateChannel();
  if (channel) {
    temp_connections_++;
    return channel;
  }
  
  return nullptr;
}

void ConnectionPool::ReturnConnection(std::shared_ptr<grpc::Channel> connection) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  // Find the connection in the pool
  for (auto it = connections_.begin(); it != connections_.end(); ++it) {
    if (it->channel == connection) {
      it->available = true;
      it->last_used = std::chrono::steady_clock::now();
      active_connections_--;
      return;
    }
  }
  
  // Connection not found in pool (might be a temp connection)
  if (temp_connections_ > 0) {
    temp_connections_--;
  }
}

bool ConnectionPool::IsChannelHealthy(const std::shared_ptr<grpc::Channel>& channel) const {
  if (!channel) {
    return false;
  }
  
  // Check channel state (non-blocking)
  auto state = channel->GetState(false);
  return state == GRPC_CHANNEL_READY || state == GRPC_CHANNEL_IDLE;
}

void ConnectionPool::HealthCheck() {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto now = std::chrono::steady_clock::now();
  
  // Check each connection
  for (auto it = connections_.begin(); it != connections_.end(); ++it) {
    if (!it->available) {
      continue;  // Skip active connections
    }
    
    // Check if connection has been idle too long
    auto idle_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        now - it->last_used).count();
    
    if (idle_time > config_.timeout_ms) {
      // Connection idle too long, replace it
      auto new_channel = CreateChannel();
      if (new_channel && IsChannelHealthy(new_channel)) {
        it->channel = new_channel;
        it->last_used = now;
      }
    } else if (!IsChannelHealthy(it->channel)) {
      // Connection unhealthy, replace it
      auto new_channel = CreateChannel();
      if (new_channel && IsChannelHealthy(new_channel)) {
        it->channel = new_channel;
        it->last_used = now;
      }
    }
  }
}

int ConnectionPool::GetActiveConnections() const {
  return active_connections_;
}

int ConnectionPool::GetAvailableConnections() const {
  std::lock_guard<std::mutex> lock(mutex_);
  int count = 0;
  for (const auto& conn : connections_) {
    if (conn.available) {
      count++;
    }
  }
  return count;
}

int ConnectionPool::GetTotalConnections() const {
  // Note: No lock here - caller must hold mutex_ if needed
  return connections_.size();
}

bool ConnectionPool::IsHealthy() const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  // Pool is healthy if we have at least one available connection
  // or can create new connections
  for (const auto& conn : connections_) {
    if (conn.available && IsChannelHealthy(conn.channel)) {
      return true;
    }
  }
  
  return GetTotalConnections() < config_.max_connections;
}

// ============================================================================
// ConnectionPoolManager
// ============================================================================

ConnectionPoolManager::ConnectionPoolManager() = default;

ConnectionPoolManager::~ConnectionPoolManager() {
  // Stop health check thread
  running_ = false;
  condition_.notify_all();
  if (health_check_thread_.joinable()) {
    health_check_thread_.join();
  }
}

void ConnectionPoolManager::Initialize(const ConnectionConfig& metad_config,
                                        const ConnectionConfig& graphd_config,
                                        const ConnectionConfig& storaged_config) {
  metad_pool_ = std::make_unique<ConnectionPool>(metad_config);
  graphd_pool_ = std::make_unique<ConnectionPool>(graphd_config);
  storaged_pool_ = std::make_unique<ConnectionPool>(storaged_config);
  
  // Start health check thread
  running_ = true;
  health_check_thread_ = std::thread([this]() {
    while (running_) {
      std::unique_lock<std::mutex> lock(mutex_);
      condition_.wait_for(lock, std::chrono::seconds(10), [this] { return !running_; });
      
      if (!running_) {
        break;
      }
      
      if (metad_pool_) metad_pool_->HealthCheck();
      if (graphd_pool_) graphd_pool_->HealthCheck();
      if (storaged_pool_) storaged_pool_->HealthCheck();
    }
  });
}

ConnectionPool* ConnectionPoolManager::GetMetaDConnectionPool() {
  return metad_pool_.get();
}

ConnectionPool* ConnectionPoolManager::GetGraphDConnectionPool() {
  return graphd_pool_.get();
}

ConnectionPool* ConnectionPoolManager::GetStorageDConnectionPool() {
  return storaged_pool_.get();
}

std::shared_ptr<grpc::Channel> ConnectionPoolManager::GetMetaDConnection() {
  if (!metad_pool_) {
    return nullptr;
  }
  return metad_pool_->GetConnection();
}

std::shared_ptr<grpc::Channel> ConnectionPoolManager::GetGraphDConnection() {
  if (!graphd_pool_) {
    return nullptr;
  }
  return graphd_pool_->GetConnection();
}

std::shared_ptr<grpc::Channel> ConnectionPoolManager::GetStorageDConnection() {
  if (!storaged_pool_) {
    return nullptr;
  }
  return storaged_pool_->GetConnection();
}

bool ConnectionPoolManager::IsHealthy() const {
  return metad_pool_ && metad_pool_->IsHealthy() &&
         graphd_pool_ && graphd_pool_->IsHealthy() &&
         storaged_pool_ && storaged_pool_->IsHealthy();
}

}  // namespace client
}  // namespace cedar
