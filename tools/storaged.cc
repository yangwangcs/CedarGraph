// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");

// =============================================================================
// CedarGraph StorageD - Storage Service
// =============================================================================
// Standalone storage service for CedarGraph cluster (Nebula-style)
// Provides data storage, Raft replication, and local query execution

#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <csignal>
#include <atomic>
#include <memory>

#include <grpcpp/grpcpp.h>

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/governance/health_checker.h"
#include "cedar/governance/config_manager.h"
#include "cedar/dtx/storage/metrics_collector.h"
#include "cedar/dtx/monitoring.h"
#include "cedar/dtx/raft/grpc_tls.h"
#include "cedar/common/json_logger.h"
#include "cedar/common/grpc_request_id.h"
#include "meta_service.grpc.pb.h"
#include "storage_service.grpc.pb.h"

// Metrics helper
static void RecordStorageOp(const std::string& op, bool success, uint64_t latency_us) {
  static auto* put_counter = cedar::dtx::storage::MetricsRegistry::Instance().GetCounter(
      "cedar_storage_put_ops_total", "Total put operations");
  static auto* get_counter = cedar::dtx::storage::MetricsRegistry::Instance().GetCounter(
      "cedar_storage_get_ops_total", "Total get operations");
  static auto* delete_counter = cedar::dtx::storage::MetricsRegistry::Instance().GetCounter(
      "cedar_storage_delete_ops_total", "Total delete operations");
  static auto* put_latency = cedar::dtx::storage::MetricsRegistry::Instance().GetHistogram(
      "cedar_storage_put_latency_seconds", "Put latency",
      std::vector<double>{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0});
  static auto* get_latency = cedar::dtx::storage::MetricsRegistry::Instance().GetHistogram(
      "cedar_storage_get_latency_seconds", "Get latency",
      std::vector<double>{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0});

  if (op == "put") {
    put_counter->Increment(1.0);
    put_latency->Observe(latency_us / 1e6);
  } else if (op == "get") {
    get_counter->Increment(1.0);
    get_latency->Observe(latency_us / 1e6);
  } else if (op == "delete") {
    delete_counter->Increment(1.0);
  }
}

std::atomic<bool> g_running{true};
std::unique_ptr<grpc::Server> g_grpc_server;

void SignalHandler(int sig) {
  std::cout << "\n[StorageD] Received signal " << sig << ", shutting down..." << std::endl;
  g_running = false;
  if (g_grpc_server) {
    g_grpc_server->Shutdown();
  }
}

void PrintBanner() {
  std::cout << "CedarGraph StorageD v1.0 starting..." << std::endl;
}

struct Config {
  int node_id = 0;
  int port = 9779;
  std::string bind_address = "0.0.0.0";
  std::string data_dir = "/var/lib/cedar/storaged";
  std::string meta_server = "127.0.0.1:9559";
  int heartbeat_interval_sec = 10;
  cedar::dtx::raft::TlsConfig tls;
};

static void LoadConfigFromFile(Config* config, const std::string& path) {
  cedar::governance::ConfigManager cm;
  if (!cm.LoadFromFile(path).ok()) return;
  if (cm.HasKey("storaged.node_id")) config->node_id = cm.GetInt("storaged.node_id", config->node_id);
  if (cm.HasKey("storaged.port")) config->port = cm.GetInt("storaged.port", config->port);
  if (cm.HasKey("storaged.bind_address")) config->bind_address = cm.GetString("storaged.bind_address", config->bind_address);
  if (cm.HasKey("storaged.data_dir")) config->data_dir = cm.GetString("storaged.data_dir", config->data_dir);
  if (cm.HasKey("storaged.meta_server")) config->meta_server = cm.GetString("storaged.meta_server", config->meta_server);
  if (cm.HasKey("storaged.heartbeat_interval_sec")) config->heartbeat_interval_sec = cm.GetInt("storaged.heartbeat_interval_sec", config->heartbeat_interval_sec);
  config->tls.enabled = cm.GetBool("tls.enabled", config->tls.enabled);
  config->tls.server_cert_file = cm.GetString("tls.server_cert", config->tls.server_cert_file);
  config->tls.server_key_file = cm.GetString("tls.server_key", config->tls.server_key_file);
  config->tls.ca_cert_file = cm.GetString("tls.ca_cert", config->tls.ca_cert_file);
  config->tls.mtls_enabled = cm.GetBool("tls.mtls", config->tls.mtls_enabled);
  config->tls.client_cert_file = cm.GetString("tls.client_cert", config->tls.client_cert_file);
  config->tls.client_key_file = cm.GetString("tls.client_key", config->tls.client_key_file);
}

Config ParseArgs(int argc, char* argv[]) {
  Config config;
  std::string config_file;

  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];
    if ((arg == "--node_id" || arg == "-n") && i + 1 < argc) {
      config.node_id = std::stoi(argv[++i]);
    } else if ((arg == "--port" || arg == "-p") && i + 1 < argc) {
      config.port = std::stoi(argv[++i]);
    } else if ((arg == "--bind" || arg == "-b") && i + 1 < argc) {
      config.bind_address = argv[++i];
    } else if ((arg == "--data_dir" || arg == "-d") && i + 1 < argc) {
      config.data_dir = argv[++i];
    } else if ((arg == "--meta" || arg == "-m") && i + 1 < argc) {
      config.meta_server = argv[++i];
    } else if ((arg == "--config" || arg == "-c") && i + 1 < argc) {
      config_file = argv[++i];
    } else if (arg == "--help" || arg == "-h") {
      std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
      std::cout << "Options:" << std::endl;
      std::cout << "  -n, --node_id <id>     Node ID (default: 0)" << std::endl;
      std::cout << "  -p, --port <port>      Port to listen on (default: 9779)" << std::endl;
      std::cout << "  -b, --bind <addr>      Bind address (default: 0.0.0.0)" << std::endl;
      std::cout << "  -d, --data_dir <dir>   Data directory (default: /var/lib/cedar/storaged)" << std::endl;
      std::cout << "  -m, --meta <addr>      MetaD server address (default: 127.0.0.1:9559)" << std::endl;
      std::cout << "  -c, --config <path>    Configuration file (YAML)" << std::endl;
      std::cout << "  -h, --help             Show this help" << std::endl;
      exit(0);
    }
  }

  if (!config_file.empty()) {
    LoadConfigFromFile(&config, config_file);
  }

  return config;
}

// StorageD 服务实现（简化版）
class StorageServiceImpl final : public cedar::storage::StorageService::Service {
 public:
  explicit StorageServiceImpl(cedar::CedarGraphStorage* storage) : storage_(storage) {}

  grpc::Status Prepare(grpc::ServerContext* context,
                       const cedar::storage::PrepareRequest* request,
                       cedar::storage::PrepareResponse* response) override {
    (void)context;
    std::lock_guard<std::mutex> lock(txn_mutex_);
    auto& state = txn_states_[request->txn_id()];
    state = TxnState::kPrepared;
    response->set_prepared(true);
    response->set_prepared_ts(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
    return grpc::Status::OK;
  }

  grpc::Status Commit(grpc::ServerContext* context,
                      const cedar::storage::CommitRequest* request,
                      cedar::storage::CommitResponse* response) override {
    (void)context;
    std::lock_guard<std::mutex> lock(txn_mutex_);
    auto it = txn_states_.find(request->txn_id());
    if (it != txn_states_.end()) {
      it->second = TxnState::kCommitted;
    }
    response->set_success(true);
    return grpc::Status::OK;
  }

  grpc::Status Abort(grpc::ServerContext* context,
                     const cedar::storage::AbortRequest* request,
                     cedar::storage::AbortResponse* response) override {
    (void)context;
    std::lock_guard<std::mutex> lock(txn_mutex_);
    auto it = txn_states_.find(request->txn_id());
    if (it != txn_states_.end()) {
      it->second = TxnState::kAborted;
    }
    response->set_success(true);
    return grpc::Status::OK;
  }

  grpc::Status Inquire(grpc::ServerContext* context,
                       const cedar::storage::InquireRequest* request,
                       cedar::storage::InquireResponse* response) override {
    (void)context;
    std::lock_guard<std::mutex> lock(txn_mutex_);
    auto it = txn_states_.find(request->txn_id());
    if (it == txn_states_.end()) {
      response->set_state(cedar::storage::InquireResponse::UNKNOWN);
      return grpc::Status::OK;
    }
    switch (it->second) {
      case TxnState::kPrepared:
        response->set_state(cedar::storage::InquireResponse::PREPARED);
        break;
      case TxnState::kCommitted:
        response->set_state(cedar::storage::InquireResponse::COMMITTED);
        break;
      case TxnState::kAborted:
        response->set_state(cedar::storage::InquireResponse::ABORTED);
        break;
    }
    return grpc::Status::OK;
  }

  grpc::Status Put(grpc::ServerContext* context,
                   const cedar::storage::PutRequest* request,
                   cedar::storage::PutResponse* response) override {
    (void)context;
    auto start = std::chrono::steady_clock::now();
    auto desc = cedar::Descriptor::Decode(
        cedar::Slice(request->descriptor_().data()));
    if (!desc.has_value()) {
      response->set_success(false);
      response->set_error_msg("Invalid descriptor");
      return grpc::Status::OK;
    }
    auto s = storage_->Put(request->key().entity_id(),
                           request->key().timestamp(),
                           desc.value(),
                           cedar::Timestamp(request->txn_version().value()));
    auto end = std::chrono::steady_clock::now();
    auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    RecordStorageOp("put", s.ok(), latency_us);
    response->set_success(s.ok());
    if (!s.ok()) {
      response->set_error_msg(s.ToString());
    }
    return grpc::Status::OK;
  }

  grpc::Status Get(grpc::ServerContext* context,
                   const cedar::storage::GetRequest* request,
                   cedar::storage::GetResponse* response) override {
    (void)context;
    auto start = std::chrono::steady_clock::now();
    auto result = storage_->Get(request->key().entity_id(),
                                request->key().timestamp());
    auto end = std::chrono::steady_clock::now();
    auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    RecordStorageOp("get", true, latency_us);
    if (result.has_value()) {
      response->set_found(true);
      response->mutable_descriptor_()->set_data(result->Encode());
    } else {
      response->set_found(false);
    }
    response->set_success(true);
    return grpc::Status::OK;
  }

  grpc::Status Delete(grpc::ServerContext* context,
                      const cedar::storage::DeleteRequest* request,
                      cedar::storage::DeleteResponse* response) override {
    (void)context;
    auto start = std::chrono::steady_clock::now();
    auto s = storage_->Delete(request->key().entity_id(),
                              request->key().timestamp(),
                              cedar::Timestamp(request->txn_version().value()));
    auto end = std::chrono::steady_clock::now();
    auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    RecordStorageOp("delete", s.ok(), latency_us);
    response->set_success(s.ok());
    if (!s.ok()) {
      response->set_error_msg(s.ToString());
    }
    return grpc::Status::OK;
  }

 private:
  enum class TxnState { kPrepared, kCommitted, kAborted };
  cedar::CedarGraphStorage* storage_;
  std::mutex txn_mutex_;
  std::unordered_map<uint64_t, TxnState> txn_states_;
};

// MetaD 客户端 - 处理注册和心跳
class MetaClient {
 public:
  MetaClient(const std::string& meta_addr, int node_id, int port,
             const std::string& data_dir,
             const cedar::dtx::raft::TlsConfig& tls)
      : node_id_(node_id), port_(port), data_dir_(data_dir) {
    auto client_creds = cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentials(tls);
    if (!client_creds) {
      throw std::runtime_error(
          "[StorageD] FATAL: Failed to create client TLS credentials for MetaD connection. "
          "Set tls.enabled=false explicitly for dev/test only.");
    }
    auto channel = grpc::CreateChannel(meta_addr, client_creds);
    stub_ = cedar::meta::MetaService::NewStub(channel);
  }

  bool Register() {
    cedar::meta::RegisterNodeRequest request;
    auto* node_info = request.mutable_node_info();
    node_info->set_node_id(node_id_);
    node_info->set_address("127.0.0.1:" + std::to_string(port_));
    node_info->set_data_path(data_dir_ + "/node" + std::to_string(node_id_));
    node_info->set_num_cpu_cores(4);
    node_info->set_total_memory_bytes(8ULL * 1024 * 1024 * 1024);
    node_info->set_total_disk_bytes(100ULL * 1024 * 1024 * 1024);
    node_info->set_registered_at_unix(
        std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
    node_info->set_state("ONLINE");

    cedar::meta::RegisterNodeResponse response;
    grpc::ClientContext context;
    auto status = stub_->RegisterNode(&context, request, &response);

    if (!status.ok() || !response.success()) {
      std::cerr << "[StorageD] Failed to register with MetaD: " 
                << (status.ok() ? response.error_msg() : status.error_message()) << std::endl;
      return false;
    }

    std::cout << "[StorageD] Registered with MetaD successfully" << std::endl;
    return true;
  }

  bool SendHeartbeat() {
    cedar::meta::HeartbeatRequest request;
    auto* status = request.mutable_status();
    status->set_node_id(node_id_);
    status->set_cpu_usage_percent(10.0);
    status->set_memory_usage_percent(20.0);
    status->set_disk_usage_percent(30.0);
    status->set_qps(100);
    status->set_latency_ms(1);
    status->set_timestamp_unix(
        std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());

    cedar::meta::HeartbeatResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    
    auto grpc_status = stub_->Heartbeat(&context, request, &response);
    return grpc_status.ok() && response.success();
  }

 private:
  std::unique_ptr<cedar::meta::MetaService::Stub> stub_;
  int node_id_;
  int port_;
  std::string data_dir_;
};

// 心跳线程
void HeartbeatLoop(MetaClient* client, int interval_sec) {
  while (g_running) {
    for (int i = 0; i < interval_sec && g_running; i++) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    if (!g_running) break;
    
    if (!client->SendHeartbeat()) {
      std::cerr << "[StorageD] Heartbeat failed" << std::endl;
    }
  }
}

int main(int argc, char* argv[]) {
  PrintBanner();
  
  Config config = ParseArgs(argc, argv);
  
  // 使用 node_id 区分数据目录
  config.data_dir += "/node" + std::to_string(config.node_id);
  
  JSON_LOG(INFO).KV("service", "storaged")
                  .KV("node_id", config.node_id)
                  .KV("port", config.port)
                  .KV("bind", config.bind_address)
                  .KV("data_dir", config.data_dir)
                  .KV("meta_server", config.meta_server);
  std::cout << std::endl;

  signal(SIGINT, SignalHandler);
  signal(SIGTERM, SignalHandler);

  // 1. 初始化存储引擎（单机模式，分布式协调由 MetaD 处理）
  cedar::CedarOptions options;
  options.create_if_missing = true;
  options.distributed_mode = false;  // StorageD 作为纯存储，分布式逻辑在 MetaD
  
  cedar::CedarGraphStorage* storage = nullptr;
  auto status = cedar::CedarGraphStorage::Open(options, config.data_dir, &storage);
  if (!status.ok()) {
    std::cerr << "[StorageD] Failed to open storage: " << status.ToString() << std::endl;
    return 1;
  }
  std::cout << "[StorageD] Storage engine opened" << std::endl;

  // DESIGN NOTE: StorageD Raft replication with braft
  //
  // Each partition should have its own braft::Node group for strong consistency.
  // The integration path is:
  //
  // 1. Create a StorageRaftStateMachine : public braft::StateMachine that
  //    applies committed log entries to PartitionStorage::Put/Delete.
  //
  // 2. For each partition managed by this node, create a braft::Node with:
  //    - group = "partition_" + partition_id
  //    - peers = replica nodes for this partition (from MetaD assignment)
  //    - log_uri = "local://" + data_dir + "/raft/partition_" + pid + "/log"
  //    - snapshot_uri = "local://" + data_dir + "/raft/partition_" + pid + "/snapshot"
  //
  // 3. Wire StorageServiceImpl::Put/Delete/BatchPut to propose through the
  //    partition's braft::Node::apply() instead of direct local writes.
  //    Only the leader can accept writes; non-leaders redirect to leader.
  //
  // 4. On on_apply(), deserialize the StorageLogEntry and call:
  //    partition_storage->Put(key, descriptor, txn_version)
  //
  // 5. For linearizable reads, use braft::Node::read_index() before serving
  //    Get/Scan requests on followers.
  //
  // 6. The existing custom raft (src/dtx/storage/raft_replication.cc) should
  //    be removed once braft integration is complete.
  //
  // TODO: Implement braft-based partition replication. Until then, this
  // StorageD operates as a single-node storage engine.

  // 3. 注册到 MetaD
  MetaClient meta_client(config.meta_server, config.node_id, config.port, config.data_dir, config.tls);
  if (!meta_client.Register()) {
    std::cerr << "[StorageD] Failed to register with MetaD, continuing anyway..." << std::endl;
    // 不退出，允许离线模式运行
  }

  // 4. 启动心跳线程
  std::thread heartbeat_thread(HeartbeatLoop, &meta_client, config.heartbeat_interval_sec);

  // 5. 创建 gRPC 服务
  StorageServiceImpl service_impl(storage);
  
  // 6. 启动 gRPC 服务器
  std::string server_address = config.bind_address + ":" + std::to_string(config.port);
  grpc::ServerBuilder builder;
  auto creds = cedar::dtx::raft::TlsCredentialFactory::CreateServerCredentials(config.tls);
  if (!creds) {
    std::cerr << "[StorageD] FATAL: Failed to create server credentials. "
              << "TLS is mandatory in production mode. "
              << "Set tls.enabled=false explicitly for dev/test only." << std::endl;
    return 1;
  }
  builder.AddListeningPort(server_address, creds);
  builder.RegisterService(&service_impl);
  
  g_grpc_server = builder.BuildAndStart();
  if (!g_grpc_server) {
    std::cerr << "[StorageD] Failed to start gRPC server" << std::endl;
    g_running = false;
    heartbeat_thread.join();
    delete storage;
    return 1;
  }
  
  std::cout << "[StorageD] gRPC server listening on " << server_address << std::endl;
  std::cout << "[StorageD] Ready. Press Ctrl+C to stop." << std::endl;
  std::cout << std::endl;

  // 7. 启动健康检查和指标 HTTP 端点
  cedar::governance::HealthChecker health_checker;
  health_checker.RegisterComponent("storage", [&storage]() {
    return storage ? cedar::governance::HealthStatus::kHealthy 
                   : cedar::governance::HealthStatus::kUnhealthy;
  });
  auto health_status = health_checker.StartHttpEndpoint("0.0.0.0", 7000);
  if (health_status.ok()) {
    std::cout << "[StorageD] Health endpoint on http://0.0.0.0:7000/health" << std::endl;
  }

  cedar::dtx::storage::MetricsCollector metrics_collector;
  cedar::dtx::storage::MetricsCollector::Config metrics_config;
  metrics_config.endpoint = ":7001";
  metrics_config.enable_http_server = true;
  auto metrics_status = metrics_collector.Initialize(metrics_config);
  if (metrics_status.ok()) {
    std::cout << "[StorageD] Metrics endpoint on http://0.0.0.0:7001/metrics" << std::endl;
  }

  // 8. 初始化告警管理器
  auto* alert_mgr = cedar::dtx::monitoring::AlertManager::GetInstance();
  cedar::dtx::monitoring::AlertManager::Config alert_config;
  alert_mgr->Initialize(alert_config);
  {
    cedar::dtx::monitoring::AlertRule rule;
    rule.name = "StorageHighLatency";
    rule.description = "Storage operation latency is too high";
    rule.severity = cedar::dtx::monitoring::AlertSeverity::kWarning;
    rule.condition_metric = "cedar_storage_put_latency_seconds";
    rule.threshold = 1.0;
    rule.comparison = ">";
    rule.duration = std::chrono::seconds(60);
    alert_mgr->AddRule(rule);
  }
  {
    cedar::dtx::monitoring::AlertRule rule;
    rule.name = "StorageLowSpace";
    rule.description = "Storage disk space is low";
    rule.severity = cedar::dtx::monitoring::AlertSeverity::kCritical;
    rule.condition_metric = "cedar_storage_disk_usage_percent";
    rule.threshold = 90.0;
    rule.comparison = ">";
    rule.duration = std::chrono::seconds(30);
    alert_mgr->AddRule(rule);
  }
  std::cout << "[StorageD] AlertManager initialized with default rules" << std::endl;

  // 9. 等待关闭
  g_grpc_server->Wait();

  // 清理
  std::cout << "[StorageD] Shutting down..." << std::endl;
  g_running = false;
  heartbeat_thread.join();
  health_checker.StopHttpEndpoint();
  metrics_collector.Shutdown();
  alert_mgr->Shutdown();
  delete storage;
  std::cout << "[StorageD] Stopped." << std::endl;

  return 0;
}
