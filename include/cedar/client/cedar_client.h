// Copyright 2025 The Cedar Authors
//
// Unified CedarGraph Client
// Single entry point for all operations (query, DDL, admin)

#ifndef CEDAR_CLIENT_CEDAR_CLIENT_H_
#define CEDAR_CLIENT_CEDAR_CLIENT_H_

#include <memory>
#include <string>
#include <vector>

#include "cedar/client/connection_pool.h"
#include "cedar/client/service_discovery.h"
#include "cedar/client/load_balancer.h"
#include "cedar/client/query_router.h"
#include "cedar/client/session_manager.h"
#include "cedar/client/retry_handler.h"
#include "cedar/client/metrics_collector.h"
#include "cedar/client/jwt_manager.h"
#include "cedar/client/logger.h"
#include "cedar/client/config_loader.h"
#include "cedar/client/config_watcher.h"
#include "cedar/client/async_query.h"
#include "cedar/client/cluster_manager.h"
#include "cedar/client/cluster_monitor.h"
#include "cedar/client/auto_scaler.h"
#include "cedar/client/cluster_backup.h"
#include "cedar/client/types.h"
#include "cedar/core/status.h"

namespace cedar {
namespace client {

// Client configuration
struct CedarClientConfig {
  // MetaD configuration
  std::string metad_host = "localhost";
  int metad_port = 9559;
  
  // GraphD configuration
  std::string graphd_host = "localhost";
  int graphd_port = 9669;
  
  // StorageD configuration
  std::string storaged_host = "localhost";
  int storaged_port = 9779;
  
  // Connection pool settings
  int max_connections = 10;
  int timeout_ms = 5000;
  
  // Service discovery settings
  bool enable_service_discovery = true;
  int refresh_interval_ms = 10000;
  
  // TLS settings
  bool enable_tls = false;
  std::string ca_cert_path;
  std::string client_cert_path;
  std::string client_key_path;
  
  // JWT authentication settings
  bool enable_jwt = false;
  std::string jwt_secret_key;
  std::string jwt_issuer = "cedar-client";
  std::string jwt_audience = "cedar-server";
  int jwt_expiration_seconds = 3600;
  std::string jwt_token;  // Pre-configured token
  
  // Logging settings
  bool enable_logging = true;
  LogLevel log_level = LogLevel::INFO;
  std::string log_file_path;
  bool enable_console_logging = true;
  bool enable_file_logging = false;
  
  // Config hot-reload settings
  bool enable_config_watcher = false;
  std::string config_file_path;
  int config_poll_interval_ms = 1000;
};

// Main client class
class CedarClient {
 public:
  CedarClient(const CedarClientConfig& config = CedarClientConfig());
  ~CedarClient();

  // Initialize client
  bool Initialize();

  // Query operations
  QueryResult ExecuteQuery(const std::string& query, const std::string& space = "default");
  QueryResult ExecuteQueryWithParams(const std::string& query,
                                      const std::unordered_map<std::string, std::string>& params,
                                      const std::string& space = "default");

  // DDL operations
  DDLResult CreateSpace(const std::string& space_name, int partition_count = 16, int replica_count = 1);
  DDLResult CreateTag(const std::string& space_name, const std::string& tag_name,
                      const std::unordered_map<std::string, std::string>& properties);
  DDLResult CreateEdge(const std::string& space_name, const std::string& edge_name,
                        const std::unordered_map<std::string, std::string>& properties);
  DDLResult CreateIndex(const std::string& space_name, const std::string& index_name,
                         const std::string& tag_name, const std::vector<std::string>& properties);

  // Admin operations
  bool DropSpace(const std::string& space_name);
  bool DropIndex(const std::string& space_name, const std::string& index_name);
  
  // Information queries
  std::vector<std::string> ListSpaces();
  std::vector<std::string> ListTags(const std::string& space_name);
  std::vector<std::string> ListEdges(const std::string& space_name);
  std::vector<std::string> ListIndexes(const std::string& space_name);

  // Session management
  std::string CreateSession(const std::string& user_name);
  void SetSessionSpace(const std::string& session_id, const std::string& space_name);
  std::string GetSessionSpace(const std::string& session_id) const;
  std::string StartTransaction(const std::string& session_id);
  void EndTransaction(const std::string& session_id);

  // Statistics
  ClientStats GetStats() const;
  void ResetStats();

  // Metrics
  QueryMetrics GetQueryMetrics() const;
  ConnectionMetrics GetConnectionMetrics() const;
  std::unordered_map<std::string, NodeMetrics> GetNodeMetrics() const;

  // Health check
  bool IsHealthy() const;

  // Get internal components
  ConnectionPoolManager* GetConnectionPoolManager();
  ServiceDiscovery* GetServiceDiscovery();
  SessionManager* GetSessionManager();
  MetricsCollector* GetMetricsCollector();
  JWTManager* GetJWTManager();

  // JWT authentication
  std::string Authenticate(const std::string& user_id, const std::string& user_name);
  void SetToken(const std::string& token);
  std::string GetToken() const;
  bool IsAuthenticated() const;

  // Config hot-reload
  void EnableConfigWatcher(const std::string& file_path, int poll_interval_ms = 1000);
  void DisableConfigWatcher();
  bool IsConfigWatcherEnabled() const;
  void ReloadConfig();

  // Async query execution
  std::shared_ptr<AsyncQuery> ExecuteQueryAsync(const std::string& query, 
                                                  const std::string& space = "default");
  std::shared_ptr<AsyncQuery> ExecuteQueryAsyncWithCallback(const std::string& query,
                                                              const std::string& space,
                                                              AsyncQueryCallback callback);
  
  // Wait for all async queries
  void WaitForAllAsyncQueries();
  
  // Cancel all async queries
  void CancelAllAsyncQueries();
  
  // Get active async query count
  int GetActiveAsyncQueryCount() const;

  // Cluster management
  bool StartCluster(const std::string& config_file, 
                    DeploymentMode mode = DeploymentMode::DOCKER_COMPOSE);
  bool StopCluster();
  bool RestartCluster();
  ClusterStatusInfo GetClusterStatus();
  bool ScaleComponent(const std::string& component, int replicas);
  bool ScaleUp(const std::string& component);
  bool ScaleDown(const std::string& component);
  std::vector<NodeInfo> GetClusterNodes();
  std::string GetClusterLogs(const std::string& component, int lines = 100);
  bool IsClusterHealthy();

  // Cluster monitoring
  bool StartClusterMonitoring(const std::string& prometheus_url = "http://localhost:9090",
                               const std::string& grafana_url = "http://localhost:3000");
  void StopClusterMonitoring();
  ClusterMetrics GetClusterMetrics();
  std::string ExportPrometheusMetrics();

  // Auto-scaling
  bool StartAutoScaling(int interval_seconds = 60);
  void StopAutoScaling();
  void AddScalingRule(const ScalingRule& rule);
  std::vector<ScalingEvent> GetScalingEvents();

  // Cluster backup
  bool InitializeBackup(const BackupConfig& config);
  BackupInfo CreateBackup(const std::string& component, BackupType type = BackupType::FULL);
  bool RestoreBackup(const std::string& backup_id);
  bool RestoreLatestBackup(const std::string& component);
  std::vector<BackupInfo> ListBackups();

 private:
  CedarClientConfig config_;
  std::unique_ptr<ConnectionPoolManager> connection_pool_manager_;
  std::unique_ptr<ServiceDiscovery> service_discovery_;
  std::shared_ptr<LoadBalancer> load_balancer_;
  std::unique_ptr<QueryRouter> query_router_;
  std::unique_ptr<SessionManager> session_manager_;
  std::unique_ptr<RetryHandler> retry_handler_;
  std::unique_ptr<MetricsCollector> metrics_collector_;
  std::unique_ptr<JWTManager> jwt_manager_;
  std::unique_ptr<ConfigWatcher> config_watcher_;
  std::unique_ptr<AsyncQueryPool> async_query_pool_;
  std::unique_ptr<ClusterManager> cluster_manager_;
  std::unique_ptr<ClusterMonitor> cluster_monitor_;
  std::unique_ptr<AutoScaler> auto_scaler_;
  std::unique_ptr<ClusterBackup> cluster_backup_;
  
  // Statistics
  mutable std::mutex stats_mutex_;
  ClientStats stats_;
  
  // Helper methods
  std::shared_ptr<grpc::Channel> GetGraphDConnection();
  std::shared_ptr<grpc::Channel> GetStorageDConnection();
  std::shared_ptr<grpc::Channel> GetMetaDConnection();
  
  // Load balancing
  ServiceNode SelectGraphDNode();
  ServiceNode SelectStorageDNode(const std::string& space_name, int partition_id);
};

}  // namespace client
}  // namespace cedar

#endif  // CEDAR_CLIENT_CEDAR_CLIENT_H_
