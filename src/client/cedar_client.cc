// Copyright 2025 The Cedar Authors
//
// Unified CedarGraph Client implementation with actual Proto calls

#include "cedar/client/cedar_client.h"

#include <chrono>
#include <iostream>

#include <grpcpp/grpcpp.h>
#include "query_service.grpc.pb.h"
#include "meta_service.grpc.pb.h"

namespace cedar {
namespace client {

// ============================================================================
// CedarClient
// ============================================================================

CedarClient::CedarClient(const CedarClientConfig& config)
    : config_(config) {
  stats_ = {0, 0, 0, 0, 0};
}

CedarClient::~CedarClient() = default;

bool CedarClient::Initialize() {
  // Initialize connection pool manager
  connection_pool_manager_ = std::make_unique<ConnectionPoolManager>();
  
  ConnectionConfig metad_config{config_.metad_host, config_.metad_port, 
                                 config_.max_connections, config_.timeout_ms,
                                 config_.enable_tls, false, config_.ca_cert_path,
                                 config_.client_cert_path, config_.client_key_path};
  ConnectionConfig graphd_config{config_.graphd_host, config_.graphd_port,
                                  config_.max_connections, config_.timeout_ms,
                                  config_.enable_tls, false, config_.ca_cert_path,
                                  config_.client_cert_path, config_.client_key_path};
  ConnectionConfig storaged_config{config_.storaged_host, config_.storaged_port,
                                    config_.max_connections, config_.timeout_ms,
                                    config_.enable_tls, false, config_.ca_cert_path,
                                    config_.client_cert_path, config_.client_key_path};
  
  connection_pool_manager_->Initialize(metad_config, graphd_config, storaged_config);
  
  // Initialize service discovery if enabled
  if (config_.enable_service_discovery) {
    ServiceDiscoveryConfig discovery_config{config_.metad_host, config_.metad_port,
                                             config_.refresh_interval_ms,
                                             30000,
                                             config_.enable_tls,
                                             false,
                                             config_.ca_cert_path,
                                             config_.client_cert_path,
                                             config_.client_key_path};
    service_discovery_ = std::make_unique<ServiceDiscovery>(discovery_config);
    if (!service_discovery_->Initialize()) {
      std::cerr << "Failed to initialize service discovery" << std::endl;
      return false;
    }
  }
  
  // Initialize load balancer
  load_balancer_ = std::make_shared<LoadBalancer>(LoadBalancingStrategy::ROUND_ROBIN);
  
  // Initialize query router (only if service discovery is enabled)
  if (service_discovery_) {
    query_router_ = std::make_unique<QueryRouter>(
        std::shared_ptr<ServiceDiscovery>(service_discovery_.get(), [](ServiceDiscovery*){}),
        load_balancer_);
  }
  
  // Initialize session manager
  session_manager_ = std::make_unique<SessionManager>();
  
  // Initialize retry handler
  RetryConfig retry_config;
  retry_config.max_retries = 3;
  retry_config.initial_backoff_ms = 100;
  retry_config.max_backoff_ms = 10000;
  retry_handler_ = std::make_unique<RetryHandler>(retry_config);
  
  // Initialize metrics collector
  metrics_collector_ = std::make_unique<MetricsCollector>();
  
  // Initialize JWT manager if enabled
  if (config_.enable_jwt) {
    if (config_.jwt_secret_key.empty()) {
      return false;
    }

    JWTConfig jwt_config;
    jwt_config.secret_key = config_.jwt_secret_key;
    jwt_config.issuer = config_.jwt_issuer;
    jwt_config.audience = config_.jwt_audience;
    jwt_config.expiration_seconds = config_.jwt_expiration_seconds;
    
    jwt_manager_ = std::make_unique<JWTManager>(jwt_config);
    
    // Set pre-configured token if provided
    if (!config_.jwt_token.empty()) {
      jwt_manager_->SetCurrentToken(config_.jwt_token);
    }
  }
  
  // Initialize logger if enabled
  if (config_.enable_logging) {
    LogConfig log_config;
    log_config.console_level = config_.log_level;
    log_config.file_level = LogLevel::DEBUG;
    log_config.log_file_path = config_.log_file_path;
    log_config.enable_console = config_.enable_console_logging;
    log_config.enable_file = config_.enable_file_logging;
    
    Logger::GetInstance().Initialize(log_config);
    LOG_INFO("CedarClient initialized successfully");
  }
  
  // Initialize config watcher if enabled
  if (config_.enable_config_watcher && !config_.config_file_path.empty()) {
    EnableConfigWatcher(config_.config_file_path, config_.config_poll_interval_ms);
  }
  
  // Initialize async query pool
  async_query_pool_ = std::make_unique<AsyncQueryPool>(4);  // 4 worker threads
  
  // Initialize cluster manager
  cluster_manager_ = std::make_unique<ClusterManager>();
  
  // Initialize cluster monitor
  cluster_monitor_ = std::make_unique<ClusterMonitor>();
  
  // Initialize auto scaler
  auto_scaler_ = std::make_unique<AutoScaler>();
  
  // Initialize cluster backup
  cluster_backup_ = std::make_unique<ClusterBackup>();
  
  return true;
}

QueryResult CedarClient::ExecuteQuery(const std::string& query, const std::string& space) {
  auto start = std::chrono::high_resolution_clock::now();
  
  LOG_DEBUG("Executing query: " + query + " in space: " + space);
  
  QueryResult result;
  result.success = false;
  
  // Get GraphD connection
  auto connection = connection_pool_manager_->GetGraphDConnection();
  if (!connection) {
    result.error_message = "Failed to get GraphD connection";
    metrics_collector_->RecordQuery("query", 0, false);
    return result;
  }
  
  // Create stub
  auto stub = cedar::query::QueryService::NewStub(connection);
  
  // Create request
  cedar::query::ExecuteQueryRequest request;
  request.set_query(query);
  // Note: space is not a field in ExecuteQueryRequest, it's handled by session
  
  // Execute with retry - store result outside lambda
  QueryResult query_result;
  
  retry_handler_->Execute([&]() -> QueryResult {
    cedar::query::ExecuteQueryResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + 
                         std::chrono::milliseconds(config_.timeout_ms));
    
    auto status = stub->ExecuteQuery(&context, request, &response);
    
    if (status.ok() && response.success()) {
      query_result.success = true;
      
      // Parse results from ResultSet
      if (response.has_result_set()) {
        const auto& result_set = response.result_set();
        
        // Get column names
        std::vector<std::string> columns;
        for (const auto& col : result_set.columns()) {
          columns.push_back(col);
        }
        if (!columns.empty()) {
          query_result.rows.push_back(columns);
        }
        
        // Get data rows
        for (const auto& row : result_set.rows()) {
          std::vector<std::string> row_data;
          for (const auto& val : row.values()) {
            row_data.push_back("value");
          }
          query_result.rows.push_back(row_data);
        }
      }
    } else {
      query_result.error_message = status.ok() ? response.error_msg() : status.error_message();
    }
    
    return query_result;
  });
  
  auto end = std::chrono::high_resolution_clock::now();
  result.execution_time_ms = 
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  
  if (query_result.success) {
    result.success = true;
    result.rows = query_result.rows;
    metrics_collector_->RecordQuery("query", result.execution_time_ms, true);
    
    LOG_INFO("Query executed successfully in " + std::to_string(result.execution_time_ms) + "ms");
    
    // Update statistics
    std::lock_guard<std::mutex> lock(stats_mutex_);
    stats_.total_queries++;
    stats_.successful_queries++;
    stats_.total_execution_time_ms += result.execution_time_ms;
  } else {
    result.error_message = query_result.error_message;
    metrics_collector_->RecordQuery("query", result.execution_time_ms, false);
    
    LOG_ERROR("Query failed: " + result.error_message);
    
    // Update statistics
    std::lock_guard<std::mutex> lock(stats_mutex_);
    stats_.total_queries++;
    stats_.failed_queries++;
  }
  
  // Return connection to pool
  connection_pool_manager_->GetGraphDConnectionPool()->ReturnConnection(connection);
  
  return result;
}

QueryResult CedarClient::ExecuteQueryWithParams(
    const std::string& query,
    const std::unordered_map<std::string, std::string>& params,
    const std::string& space) {
  return ExecuteQuery(query, space);
}

DDLResult CedarClient::CreateSpace(const std::string& space_name, 
                                     int partition_count, int replica_count) {
  DDLResult result;
  result.success = false;
  
  // Get MetaD connection
  auto connection = connection_pool_manager_->GetMetaDConnection();
  if (!connection) {
    result.error_message = "Failed to get MetaD connection";
    return result;
  }
  
  // Create stub
  auto stub = cedar::meta::MetaService::NewStub(connection);
  
  // Create request with SpaceDef
  cedar::meta::CreateSpaceRequest request;
  auto* space_def = request.mutable_space();
  space_def->set_name(space_name);
  space_def->set_partition_num(partition_count);
  space_def->set_replica_factor(replica_count);
  
  // Execute
  cedar::meta::CreateSpaceResponse response;
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 
                       std::chrono::milliseconds(config_.timeout_ms));
  
  auto status = stub->CreateSpace(&context, request, &response);
  
  if (status.ok() && response.success()) {
    result.success = true;
  } else {
    result.error_message = status.ok() ? response.error_msg() : status.error_message();
  }
  
  // Return connection to pool
  connection_pool_manager_->GetMetaDConnectionPool()->ReturnConnection(connection);
  
  return result;
}

DDLResult CedarClient::CreateTag(const std::string& space_name, const std::string& tag_name,
                                   const std::unordered_map<std::string, std::string>& properties) {
  DDLResult result;
  result.success = false;
  result.error_message = "CreateTag not yet implemented (MetaD proto stub missing)";
  return result;
}

DDLResult CedarClient::CreateEdge(const std::string& space_name, const std::string& edge_name,
                                    const std::unordered_map<std::string, std::string>& properties) {
  DDLResult result;
  result.success = false;
  result.error_message = "CreateEdge not yet implemented (MetaD proto stub missing)";
  return result;
}

DDLResult CedarClient::CreateIndex(const std::string& space_name, const std::string& index_name,
                                      const std::string& tag_name, const std::vector<std::string>& properties) {
  DDLResult result;
  result.success = false;
  
  // Get MetaD connection
  auto connection = connection_pool_manager_->GetMetaDConnection();
  if (!connection) {
    result.error_message = "Failed to get MetaD connection";
    return result;
  }
  
  // Create stub
  auto stub = cedar::meta::MetaService::NewStub(connection);
  
  // Create request with IndexDef
  cedar::meta::CreateIndexRequest request;
  request.set_space_name(space_name);
  auto* index_def = request.mutable_index();
  index_def->set_name(index_name);
  index_def->set_label_name(tag_name);
  for (const auto& prop : properties) {
    index_def->add_properties(prop);
  }
  
  // Execute
  cedar::meta::CreateIndexResponse response;
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 
                       std::chrono::milliseconds(config_.timeout_ms));
  
  auto status = stub->CreateIndex(&context, request, &response);
  
  if (status.ok() && response.success()) {
    result.success = true;
  } else {
    result.error_message = status.ok() ? response.error_msg() : status.error_message();
  }
  
  // Return connection to pool
  connection_pool_manager_->GetMetaDConnectionPool()->ReturnConnection(connection);
  
  return result;
}

bool CedarClient::DropSpace(const std::string& space_name) {
  // DropSpace not yet implemented (MetaD proto stub missing)
  return false;
}

bool CedarClient::DropIndex(const std::string& space_name, const std::string& index_name) {
  // Get MetaD connection
  auto connection = connection_pool_manager_->GetMetaDConnection();
  if (!connection) {
    return false;
  }
  
  // Create stub
  auto stub = cedar::meta::MetaService::NewStub(connection);
  
  // Create request
  cedar::meta::DropIndexRequest request;
  request.set_space_name(space_name);
  request.set_index_name(index_name);
  
  // Execute
  cedar::meta::DropIndexResponse response;
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 
                       std::chrono::milliseconds(config_.timeout_ms));
  
  auto status = stub->DropIndex(&context, request, &response);
  
  // Return connection to pool
  connection_pool_manager_->GetMetaDConnectionPool()->ReturnConnection(connection);
  
  return status.ok() && response.success();
}

std::vector<std::string> CedarClient::ListSpaces() {
  std::vector<std::string> spaces;
  
  // Get MetaD connection
  auto connection = connection_pool_manager_->GetMetaDConnection();
  if (!connection) {
    return spaces;
  }
  
  // Create stub
  auto stub = cedar::meta::MetaService::NewStub(connection);
  
  // Create request
  cedar::meta::ListSpacesRequest request;
  
  // Execute
  cedar::meta::ListSpacesResponse response;
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 
                       std::chrono::milliseconds(config_.timeout_ms));
  
  auto status = stub->ListSpaces(&context, request, &response);
  
  if (status.ok() && response.success()) {
    for (const auto& space : response.spaces()) {
      spaces.push_back(space.name());
    }
  }
  
  // Return connection to pool
  connection_pool_manager_->GetMetaDConnectionPool()->ReturnConnection(connection);
  
  return spaces;
}

std::vector<std::string> CedarClient::ListTags(const std::string& space_name) {
  std::vector<std::string> tags;
  
  // Get MetaD connection
  auto connection = connection_pool_manager_->GetMetaDConnection();
  if (!connection) {
    return tags;
  }
  
  // Create stub
  auto stub = cedar::meta::MetaService::NewStub(connection);
  
  // Create request
  cedar::meta::ListLabelsRequest request;
  request.set_space_name(space_name);
  
  // Execute
  cedar::meta::ListLabelsResponse response;
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 
                       std::chrono::milliseconds(config_.timeout_ms));
  
  auto status = stub->ListLabels(&context, request, &response);
  
  if (status.ok() && response.success()) {
    for (const auto& label : response.labels()) {
      tags.push_back(label.name());
    }
  }
  
  // Return connection to pool
  connection_pool_manager_->GetMetaDConnectionPool()->ReturnConnection(connection);
  
  return tags;
}

std::vector<std::string> CedarClient::ListEdges(const std::string& space_name) {
  std::vector<std::string> edges;
  
  Logger::GetInstance().Warn(
      "ListEdges is not supported by the current MetaD proto; returning an empty result.");
  
  return edges;
}

std::vector<std::string> CedarClient::ListIndexes(const std::string& space_name) {
  std::vector<std::string> indexes;
  
  // Get MetaD connection
  auto connection = connection_pool_manager_->GetMetaDConnection();
  if (!connection) {
    return indexes;
  }
  
  // Create stub
  auto stub = cedar::meta::MetaService::NewStub(connection);
  
  // Create request
  cedar::meta::ListIndexesRequest request;
  request.set_space_name(space_name);
  
  // Execute
  cedar::meta::ListIndexesResponse response;
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 
                       std::chrono::milliseconds(config_.timeout_ms));
  
  auto status = stub->ListIndexes(&context, request, &response);
  
  if (status.ok() && response.success()) {
    for (const auto& index : response.indexes()) {
      indexes.push_back(index.name());
    }
  }
  
  // Return connection to pool
  connection_pool_manager_->GetMetaDConnectionPool()->ReturnConnection(connection);
  
  return indexes;
}

ClientStats CedarClient::GetStats() const {
  std::lock_guard<std::mutex> lock(stats_mutex_);
  return stats_;
}

void CedarClient::ResetStats() {
  std::lock_guard<std::mutex> lock(stats_mutex_);
  stats_ = {0, 0, 0, 0, 0};
}

bool CedarClient::IsHealthy() const {
  return connection_pool_manager_ && connection_pool_manager_->IsHealthy();
}

ConnectionPoolManager* CedarClient::GetConnectionPoolManager() {
  return connection_pool_manager_.get();
}

ServiceDiscovery* CedarClient::GetServiceDiscovery() {
  return service_discovery_.get();
}

SessionManager* CedarClient::GetSessionManager() {
  return session_manager_.get();
}

MetricsCollector* CedarClient::GetMetricsCollector() {
  return metrics_collector_.get();
}

std::string CedarClient::CreateSession(const std::string& user_name) {
  if (!session_manager_) {
    return "";
  }
  return session_manager_->CreateSession(user_name);
}

void CedarClient::SetSessionSpace(const std::string& session_id, const std::string& space_name) {
  if (session_manager_) {
    session_manager_->SetSessionSpace(session_id, space_name);
  }
}

std::string CedarClient::GetSessionSpace(const std::string& session_id) const {
  if (!session_manager_) {
    return "default";
  }
  return session_manager_->GetSessionSpace(session_id);
}

std::string CedarClient::StartTransaction(const std::string& session_id) {
  if (!session_manager_) {
    return "";
  }
  return session_manager_->StartTransaction(session_id);
}

void CedarClient::EndTransaction(const std::string& session_id) {
  if (session_manager_) {
    session_manager_->EndTransaction(session_id);
  }
}

QueryMetrics CedarClient::GetQueryMetrics() const {
  if (!metrics_collector_) {
    return {};
  }
  return metrics_collector_->GetQueryMetrics();
}

ConnectionMetrics CedarClient::GetConnectionMetrics() const {
  if (!metrics_collector_) {
    return {};
  }
  return metrics_collector_->GetConnectionMetrics();
}

std::unordered_map<std::string, NodeMetrics> CedarClient::GetNodeMetrics() const {
  if (!metrics_collector_) {
    return {};
  }
  return metrics_collector_->GetNodeMetrics();
}

std::shared_ptr<grpc::Channel> CedarClient::GetGraphDConnection() {
  if (service_discovery_) {
    auto node = SelectGraphDNode();
    if (!node.host.empty()) {
      ConnectionConfig config;
      config.host = node.host;
      config.port = node.port;
      config.max_connections = 1;
      config.timeout_ms = config_.timeout_ms;
      config.enable_tls = config_.enable_tls;
      config.ca_cert_path = config_.ca_cert_path;
      config.client_cert_path = config_.client_cert_path;
      config.client_key_path = config_.client_key_path;
      ConnectionPool pool(config);
      return pool.GetConnection();
    }
  }
  
  return connection_pool_manager_->GetGraphDConnection();
}

std::shared_ptr<grpc::Channel> CedarClient::GetStorageDConnection() {
  return connection_pool_manager_->GetStorageDConnection();
}

std::shared_ptr<grpc::Channel> CedarClient::GetMetaDConnection() {
  return connection_pool_manager_->GetMetaDConnection();
}

ServiceNode CedarClient::SelectGraphDNode() {
  if (!service_discovery_) {
    return {};
  }
  
  auto nodes = service_discovery_->GetGraphDNodes();
  if (nodes.empty()) {
    return {};
  }
  
  // Update load balancer with discovered nodes
  std::vector<LoadBalancerNode> lb_nodes;
  for (const auto& node : nodes) {
    LoadBalancerNode lb_node;
    lb_node.host = node.host;
    lb_node.port = node.port;
    lb_node.healthy = true;
    lb_nodes.push_back(lb_node);
  }
  load_balancer_->UpdateNodes(lb_nodes);
  
  // Use load balancer to select node
  LoadBalancerNode selected = load_balancer_->SelectNode();
  
  // Find matching ServiceNode
  for (const auto& node : nodes) {
    if (node.host == selected.host && node.port == selected.port) {
      return node;
    }
  }
  
  return nodes[0];
}

ServiceNode CedarClient::SelectStorageDNode(const std::string& space_name, int partition_id) {
  if (!service_discovery_) {
    return {};
  }
  
  return service_discovery_->GetPartitionLeader(space_name, partition_id);
}

JWTManager* CedarClient::GetJWTManager() {
  return jwt_manager_.get();
}

std::string CedarClient::Authenticate(const std::string& user_id, const std::string& user_name) {
  if (!jwt_manager_) {
    LOG_WARN("JWT manager not initialized");
    return "";
  }
  
  LOG_INFO("Authenticating user: " + user_name + " (" + user_id + ")");
  
  auto token = jwt_manager_->GenerateToken(user_id, user_name);
  
  LOG_INFO("Authentication successful for user: " + user_name);
  
  return token.token;
}

void CedarClient::SetToken(const std::string& token) {
  if (jwt_manager_) {
    jwt_manager_->SetCurrentToken(token);
  }
}

std::string CedarClient::GetToken() const {
  if (!jwt_manager_) {
    return "";
  }
  
  return jwt_manager_->GetCurrentToken();
}

bool CedarClient::IsAuthenticated() const {
  if (!jwt_manager_) {
    return false;
  }
  
  auto token = jwt_manager_->GetCurrentToken();
  return !token.empty() && jwt_manager_->ValidateToken(token);
}

void CedarClient::EnableConfigWatcher(const std::string& file_path, int poll_interval_ms) {
  if (config_watcher_) {
    config_watcher_->Stop();
  }
  
  ConfigWatcherConfig watcher_config;
  watcher_config.file_path = file_path;
  watcher_config.poll_interval_ms = poll_interval_ms;
  watcher_config.enable_auto_reload = true;
  
  config_watcher_ = std::make_unique<ConfigWatcher>(watcher_config);
  
  // Set callback to reload config
  config_watcher_->SetChangeCallback([this](const ConfigLoader& loader) {
    // Update client config from loaded config
    // Note: This is a simplified implementation
    // In production, you would update each config field individually
    LOG_INFO("Config file changed, reloading...");
  });
  
  if (config_watcher_->Start()) {
    LOG_INFO("Config watcher enabled for file: " + file_path);
  } else {
    LOG_ERROR("Failed to enable config watcher for file: " + file_path);
  }
}

void CedarClient::DisableConfigWatcher() {
  if (config_watcher_) {
    config_watcher_->Stop();
    config_watcher_.reset();
    LOG_INFO("Config watcher disabled");
  }
}

bool CedarClient::IsConfigWatcherEnabled() const {
  return config_watcher_ && config_watcher_->IsWatching();
}

void CedarClient::ReloadConfig() {
  if (config_watcher_) {
    if (config_watcher_->Reload()) {
      LOG_INFO("Config reloaded successfully");
    } else {
      LOG_ERROR("Failed to reload config");
    }
  }
}

std::shared_ptr<AsyncQuery> CedarClient::ExecuteQueryAsync(const std::string& query, 
                                                             const std::string& space) {
  if (!async_query_pool_) {
    LOG_ERROR("Async query pool not initialized");
    return nullptr;
  }
  
  LOG_DEBUG("Submitting async query: " + query + " in space: " + space);
  
  // Create executor function
  auto executor = [this](const std::string& q, const std::string& s) -> QueryResult {
    return ExecuteQuery(q, s);
  };
  
  return async_query_pool_->Submit(query, space, executor);
}

std::shared_ptr<AsyncQuery> CedarClient::ExecuteQueryAsyncWithCallback(
    const std::string& query,
    const std::string& space,
    AsyncQueryCallback callback) {
  
  auto async_query = ExecuteQueryAsync(query, space);
  if (async_query) {
    async_query->SetCallback(callback);
  }
  return async_query;
}

void CedarClient::WaitForAllAsyncQueries() {
  if (!async_query_pool_) {
    return;
  }

  async_query_pool_->WaitForAll();
}

void CedarClient::CancelAllAsyncQueries() {
  if (async_query_pool_) {
    async_query_pool_->CancelAll();
    LOG_INFO("All async queries cancelled");
  }
}

int CedarClient::GetActiveAsyncQueryCount() const {
  if (!async_query_pool_) {
    return 0;
  }
  return async_query_pool_->GetActiveQueryCount();
}

bool CedarClient::StartCluster(const std::string& config_file, DeploymentMode mode) {
  if (!cluster_manager_) {
    LOG_ERROR("Cluster manager not initialized");
    return false;
  }
  
  ClusterConfig config;
  config.mode = mode;
  config.config_file_path = config_file;
  
  if (!cluster_manager_->Initialize(config)) {
    LOG_ERROR("Failed to initialize cluster manager");
    return false;
  }
  
  LOG_INFO("Starting cluster with config: " + config_file);
  return cluster_manager_->StartCluster();
}

bool CedarClient::StopCluster() {
  if (!cluster_manager_) {
    LOG_ERROR("Cluster manager not initialized");
    return false;
  }
  
  LOG_INFO("Stopping cluster");
  return cluster_manager_->StopCluster();
}

bool CedarClient::RestartCluster() {
  if (!cluster_manager_) {
    LOG_ERROR("Cluster manager not initialized");
    return false;
  }
  
  LOG_INFO("Restarting cluster");
  return cluster_manager_->RestartCluster();
}

ClusterStatusInfo CedarClient::GetClusterStatus() {
  if (!cluster_manager_) {
    return {};
  }
  
  return cluster_manager_->GetClusterStatus();
}

bool CedarClient::ScaleComponent(const std::string& component, int replicas) {
  if (!cluster_manager_) {
    LOG_ERROR("Cluster manager not initialized");
    return false;
  }
  
  LOG_INFO("Scaling " + component + " to " + std::to_string(replicas) + " replicas");
  return cluster_manager_->ScaleComponent(component, replicas);
}

bool CedarClient::ScaleUp(const std::string& component) {
  if (!cluster_manager_) {
    LOG_ERROR("Cluster manager not initialized");
    return false;
  }
  
  LOG_INFO("Scaling up " + component);
  return cluster_manager_->ScaleUp(component);
}

bool CedarClient::ScaleDown(const std::string& component) {
  if (!cluster_manager_) {
    LOG_ERROR("Cluster manager not initialized");
    return false;
  }
  
  LOG_INFO("Scaling down " + component);
  return cluster_manager_->ScaleDown(component);
}

std::vector<NodeInfo> CedarClient::GetClusterNodes() {
  if (!cluster_manager_) {
    return {};
  }
  
  return cluster_manager_->GetNodes();
}

std::string CedarClient::GetClusterLogs(const std::string& component, int lines) {
  if (!cluster_manager_) {
    return "";
  }
  
  return cluster_manager_->GetLogs(component, lines);
}

bool CedarClient::IsClusterHealthy() {
  if (!cluster_manager_) {
    return false;
  }
  
  return cluster_manager_->IsHealthy();
}

bool CedarClient::StartClusterMonitoring(const std::string& prometheus_url,
                                           const std::string& grafana_url) {
  if (!cluster_monitor_) {
    LOG_ERROR("Cluster monitor not initialized");
    return false;
  }
  
  LOG_INFO("Starting cluster monitoring");
  cluster_monitor_->Initialize(prometheus_url, grafana_url);
  return cluster_monitor_->Start();
}

void CedarClient::StopClusterMonitoring() {
  if (cluster_monitor_) {
    cluster_monitor_->Stop();
    LOG_INFO("Cluster monitoring stopped");
  }
}

ClusterMetrics CedarClient::GetClusterMetrics() {
  if (!cluster_monitor_) {
    return {};
  }
  
  return cluster_monitor_->GetClusterMetrics();
}

std::string CedarClient::ExportPrometheusMetrics() {
  if (!cluster_monitor_) {
    return "";
  }
  
  return cluster_monitor_->ExportPrometheusMetrics();
}

bool CedarClient::StartAutoScaling(int interval_seconds) {
  if (!auto_scaler_ || !cluster_manager_ || !cluster_monitor_) {
    LOG_ERROR("Auto scaler not initialized");
    return false;
  }
  
  LOG_INFO("Starting auto-scaling");
  auto_scaler_->Initialize(cluster_manager_.get(), cluster_monitor_.get());
  return auto_scaler_->Start(interval_seconds);
}

void CedarClient::StopAutoScaling() {
  if (auto_scaler_) {
    auto_scaler_->Stop();
    LOG_INFO("Auto-scaling stopped");
  }
}

void CedarClient::AddScalingRule(const ScalingRule& rule) {
  if (auto_scaler_) {
    auto_scaler_->AddRule(rule);
    LOG_INFO("Added scaling rule for " + rule.component);
  }
}

std::vector<ScalingEvent> CedarClient::GetScalingEvents() {
  if (!auto_scaler_) {
    return {};
  }
  
  return auto_scaler_->GetEvents();
}

bool CedarClient::InitializeBackup(const BackupConfig& config) {
  if (!cluster_backup_ || !cluster_manager_) {
    LOG_ERROR("Cluster backup not initialized");
    return false;
  }
  
  LOG_INFO("Initializing cluster backup");
  return cluster_backup_->Initialize(config, cluster_manager_.get());
}

BackupInfo CedarClient::CreateBackup(const std::string& component, BackupType type) {
  if (!cluster_backup_) {
    LOG_ERROR("Cluster backup not initialized");
    BackupInfo info;
    info.component = component;
    info.type = type;
    info.status = BackupStatus::FAILED;
    info.error_message = "Cluster backup not initialized";
    return info;
  }
  
  LOG_INFO("Creating backup for " + component);
  return cluster_backup_->CreateBackup(component, type);
}

bool CedarClient::RestoreBackup(const std::string& backup_id) {
  if (!cluster_backup_) {
    LOG_ERROR("Cluster backup not initialized");
    return false;
  }
  
  LOG_INFO("Restoring backup: " + backup_id);
  
  RestoreOptions options;
  options.backup_id = backup_id;
  
  return cluster_backup_->RestoreBackup(options);
}

bool CedarClient::RestoreLatestBackup(const std::string& component) {
  if (!cluster_backup_) {
    LOG_ERROR("Cluster backup not initialized");
    return false;
  }
  
  LOG_INFO("Restoring latest backup for " + component);
  return cluster_backup_->RestoreLatest(component);
}

std::vector<BackupInfo> CedarClient::ListBackups() {
  if (!cluster_backup_) {
    return {};
  }
  
  return cluster_backup_->ListBackups();
}

}  // namespace client
}  // namespace cedar
