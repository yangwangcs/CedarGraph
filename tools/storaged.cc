// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");

// =============================================================================
// CedarGraph StorageD - Storage Service
// =============================================================================
// Standalone storage service for CedarGraph cluster (Nebula-style)
// Provides data storage, Raft replication, and local query execution

#include <iostream>
#include <algorithm>
#include <string>
#include <thread>
#include <chrono>
#include <csignal>
#include <atomic>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <memory>
#include <filesystem>
#include <execinfo.h>
#include <sstream>
#include <unordered_map>
#include <vector>
#include <unistd.h>

#include <grpcpp/grpcpp.h>

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/governance/health_checker.h"
#include "cedar/governance/config_manager.h"
#include "cedar/dtx/storage/metrics_collector.h"
#include "cedar/dtx/monitoring.h"
#include "cedar/dtx/raft/grpc_tls.h"
#include "cedar/dtx/storage/storaged_raft_state_machine.h"
#include "cedar/dtx/storage/partition_raft_manager.h"
#include "cedar/cdc/partition_change_log.h"
#include "cedar/cdc/rpc_limits.h"
#include "cedar/core/crc32c.h"
#include "cedar/common/json_logger.h"
#include "cedar/common/grpc_request_id.h"
#include "cedar/cypher/cypher_engine.h"
#include "cedar/cypher/value.h"
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
static volatile sig_atomic_t g_shutdown_requested = 0;

static bool DeadlineExpired(grpc::ServerContext* context) {
  return context->deadline() <= std::chrono::system_clock::now();
}

static bool CdcLimitsValid(uint32_t limit_records, uint64_t limit_bytes) {
  return limit_records != 0 &&
         limit_records <= cedar::cdc::kMaxCdcRpcRecords &&
         limit_bytes != 0 &&
         limit_bytes <= cedar::cdc::kMaxCdcRpcBytes;
}

static uint32_t ComputeSnapshotChecksum(
    const cedar::storage::ComputeSnapshotBatch& batch) {
  std::string encoded;
  if (!batch.SerializeToString(&encoded)) {
    return 0;
  }
  uint32_t checksum = cedar::crc32c::Value(encoded.data(), encoded.size());
  return checksum == 0 ? 1 : checksum;
}

void SignalHandler(int sig) {
  (void)sig;
  g_shutdown_requested = 1;
}

// Signal handler for crash detection
void CrashSignalHandler(int sig) {
  // Print backtrace
  void* array[100];
  size_t size = backtrace(array, 100);
  
  std::cerr << "\n=== CRASH DETECTED ===" << std::endl;
  std::cerr << "Signal: " << sig << " (" << 
    (sig == SIGSEGV ? "SIGSEGV" : 
     sig == SIGABRT ? "SIGABRT" : 
     sig == SIGBUS ? "SIGBUS" : "UNKNOWN") << ")" << std::endl;
  std::cerr << "Backtrace:" << std::endl;
  backtrace_symbols_fd(array, size, STDERR_FILENO);
  std::cerr << "========================\n" << std::endl;
  
  // Re-raise the signal to get core dump
  signal(sig, SIG_DFL);
  raise(sig);
}

void PrintBanner() {
  std::cout << "CedarGraph StorageD v1.0 starting..." << std::endl;
}

std::string GetEnvOrEmpty(const char* name) {
  const char* value = std::getenv(name);
  return value ? value : "";
}

bool IsTruthyEnv(const std::string& value) {
  return value == "1" || value == "true" || value == "TRUE" ||
         value == "yes" || value == "YES";
}

bool IsFalseyEnv(const std::string& value) {
  return value == "0" || value == "false" || value == "FALSE" ||
         value == "no" || value == "NO";
}

void ApplyTlsEnvOverrides(cedar::dtx::raft::TlsConfig* config) {
  const std::string tls_enabled = GetEnvOrEmpty("CEDAR_GRPC_TLS_ENABLED");
  if (!tls_enabled.empty()) {
    if (IsTruthyEnv(tls_enabled)) {
      config->enabled = true;
    } else if (IsFalseyEnv(tls_enabled)) {
      config->enabled = false;
    }
  }

  const std::string mtls_enabled = GetEnvOrEmpty("CEDAR_GRPC_MTLS_ENABLED");
  if (!mtls_enabled.empty()) {
    config->mtls_enabled = IsTruthyEnv(mtls_enabled);
  }

  const std::string server_cert = GetEnvOrEmpty("CEDAR_GRPC_SERVER_CERT");
  if (!server_cert.empty()) config->server_cert_file = server_cert;
  const std::string server_key = GetEnvOrEmpty("CEDAR_GRPC_SERVER_KEY");
  if (!server_key.empty()) config->server_key_file = server_key;
  const std::string ca_cert = GetEnvOrEmpty("CEDAR_GRPC_CA_CERT");
  if (!ca_cert.empty()) config->ca_cert_file = ca_cert;
  const std::string client_cert = GetEnvOrEmpty("CEDAR_GRPC_CLIENT_CERT");
  if (!client_cert.empty()) config->client_cert_file = client_cert;
  const std::string client_key = GetEnvOrEmpty("CEDAR_GRPC_CLIENT_KEY");
  if (!client_key.empty()) config->client_key_file = client_key;
}

cedar::StatusOr<std::shared_ptr<grpc::ServerCredentials>> CreateServerCredentialsWithEnvFallback(
    const cedar::dtx::raft::TlsConfig& config) {
  const bool needs_env_fallback =
      config.enabled &&
      (config.server_cert_file.empty() || config.server_key_file.empty() ||
       (config.mtls_enabled && config.ca_cert_file.empty()));
  if (needs_env_fallback) {
    auto env_creds = cedar::dtx::raft::TlsCredentialFactory::CreateServerCredentialsFromEnvStrict();
    if (env_creds.ok()) {
      return env_creds;
    }
  }

  auto creds = cedar::dtx::raft::TlsCredentialFactory::CreateServerCredentials(config);
  if (creds.ok() || !config.enabled) {
    return creds;
  }
  return cedar::dtx::raft::TlsCredentialFactory::CreateServerCredentialsFromEnvStrict();
}

cedar::StatusOr<std::shared_ptr<grpc::ChannelCredentials>> CreateClientCredentialsWithEnvFallback(
    const cedar::dtx::raft::TlsConfig& config) {
  const bool needs_env_fallback =
      config.enabled &&
      (config.ca_cert_file.empty() ||
       (config.mtls_enabled &&
        (config.client_cert_file.empty() || config.client_key_file.empty())));
  if (needs_env_fallback) {
    auto env_creds = cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentialsFromEnvStrict();
    if (env_creds.ok()) {
      return env_creds;
    }
  }

  auto creds = cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentials(config);
  if (creds.ok() || !config.enabled) {
    return creds;
  }
  return cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentialsFromEnvStrict();
}

struct Config {
  int node_id = 0;
  int port = 9779;
  std::string bind_address = "0.0.0.0";
  std::string advertise_address;  // Empty = auto-detect; used for MetaD registration
  std::string data_dir = "./data/storaged";
  std::string meta_server = "127.0.0.1:10559";
  int heartbeat_interval_sec = 10;
  int health_port = 7000;
  int metrics_port = 7001;
  cedar::dtx::raft::TlsConfig tls;

  Config() {
    tls.enabled = true;  // DEFAULT: TLS enabled for production
  }
};

static void LoadConfigFromFile(Config* config, const std::string& path) {
  cedar::governance::ConfigManager cm;
  if (!cm.LoadFromFile(path).ok()) return;
  if (cm.HasKey("storaged.node_id")) config->node_id = cm.GetInt("storaged.node_id", config->node_id);
  if (cm.HasKey("storaged.port")) config->port = cm.GetInt("storaged.port", config->port);
  if (cm.HasKey("storaged.bind_address")) config->bind_address = cm.GetString("storaged.bind_address", config->bind_address);
  if (cm.HasKey("storaged.advertise_address")) config->advertise_address = cm.GetString("storaged.advertise_address", config->advertise_address);
  if (cm.HasKey("storaged.data_dir")) config->data_dir = cm.GetString("storaged.data_dir", config->data_dir);
  if (cm.HasKey("storaged.meta_server")) config->meta_server = cm.GetString("storaged.meta_server", config->meta_server);
  if (cm.HasKey("storaged.heartbeat_interval_sec")) config->heartbeat_interval_sec = cm.GetInt("storaged.heartbeat_interval_sec", config->heartbeat_interval_sec);
  if (cm.HasKey("storaged.health_port")) config->health_port = cm.GetInt("storaged.health_port", config->health_port);
  if (cm.HasKey("storaged.metrics_port")) config->metrics_port = cm.GetInt("storaged.metrics_port", config->metrics_port);
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
    } else if ((arg == "--advertise_address" || arg == "-a") && i + 1 < argc) {
      config.advertise_address = argv[++i];
    } else if ((arg == "--data_dir" || arg == "-d") && i + 1 < argc) {
      config.data_dir = argv[++i];
    } else if ((arg == "--meta" || arg == "-m") && i + 1 < argc) {
      config.meta_server = argv[++i];
    } else if (arg == "--health_port" && i + 1 < argc) {
      config.health_port = std::stoi(argv[++i]);
    } else if (arg == "--metrics_port" && i + 1 < argc) {
      config.metrics_port = std::stoi(argv[++i]);
    } else if (arg.rfind("--config=", 0) == 0) {
      config_file = arg.substr(std::string("--config=").size());
    } else if ((arg == "--config" || arg == "-c") && i + 1 < argc) {
      config_file = argv[++i];
    } else if (arg == "--tls" && i + 1 < argc) {
      std::string val = argv[++i];
      config.tls.enabled = (val == "true" || val == "1" || val == "yes");
    } else if (arg == "--test_mode") {
      config.tls.enabled = false;  // Disable TLS in test mode for convenience
    } else if (arg == "--help" || arg == "-h") {
      std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
      std::cout << "Options:" << std::endl;
      std::cout << "  -n, --node_id <id>     Node ID (default: 0)" << std::endl;
      std::cout << "  -p, --port <port>      Port to listen on (default: 9779)" << std::endl;
      std::cout << "  -b, --bind <addr>      Bind address (default: 0.0.0.0)" << std::endl;
      std::cout << "  -a, --advertise_address <addr>  Address advertised to MetaD (default: auto-detect)" << std::endl;
      std::cout << "  -d, --data_dir <dir>   Data directory (default: ./data/storaged)" << std::endl;
      std::cout << "  -m, --meta <addr>      MetaD gRPC server address (default: 127.0.0.1:10559)" << std::endl;
      std::cout << "  --health_port <port>   Health HTTP port (default: 7000)" << std::endl;
      std::cout << "  --metrics_port <port>  Metrics HTTP port (default: 7001)" << std::endl;
      std::cout << "  -c, --config <path>    Configuration file (YAML)" << std::endl;
      std::cout << "  --tls <true|false>     Enable/disable TLS (default: true)" << std::endl;
      std::cout << "  --test_mode            Test mode (disables TLS)" << std::endl;
      std::cout << "  -h, --help             Show this help" << std::endl;
      exit(0);
    }
  }

  if (!config_file.empty()) {
    LoadConfigFromFile(&config, config_file);
  }

  return config;
}

// StorageD 服务实现（完整版）
class StorageServiceImpl final : public cedar::storage::StorageService::Service {
 public:
  explicit StorageServiceImpl(cedar::CedarGraphStorage* storage,
                               cedar::dtx::PartitionRaftManager* raft_manager = nullptr,
                               const std::string& data_dir = "") 
      : storage_(storage), raft_manager_(raft_manager), data_dir_(data_dir) {
    if (storage_) {
      cypher_engine_ = std::make_unique<cedar::cypher::CypherEngine>(storage_);
      if (data_dir_.empty()) {
        data_dir_ = storage_->GetDbPath();
      }
      RecoverCdcIntents();
    }
  }

  ~StorageServiceImpl() override {
    // Ensure proper cleanup
    storage_ = nullptr;
    raft_manager_ = nullptr;
    cypher_engine_.reset();
  }

  grpc::Status Prepare(grpc::ServerContext* context,
                       const cedar::storage::PrepareRequest* request,
                       cedar::storage::PrepareResponse* response) override {
    (void)context;
    std::lock_guard<std::mutex> lock(txn_mutex_);
    auto& ctx = txn_states_[request->txn_id()];
    ctx.state = TxnState::kPrepared;
    ctx.commit_ts = cedar::Timestamp(request->commit_ts());
    
    for (const auto& key : request->read_set()) {
      ctx.read_set.emplace_back(
          key.entity_id(), static_cast<cedar::EntityType>(key.entity_type()),
          static_cast<uint16_t>(key.column_id()),
          cedar::Timestamp(key.timestamp()),
          static_cast<uint16_t>(key.sequence()),
          key.target_id(), static_cast<uint8_t>(key.type_flags()),
          static_cast<uint16_t>(key.partition_id()));
    }
    for (const auto& key : request->write_set()) {
      ctx.write_set.emplace_back(
          key.entity_id(), static_cast<cedar::EntityType>(key.entity_type()),
          static_cast<uint16_t>(key.column_id()),
          cedar::Timestamp(key.timestamp()),
          static_cast<uint16_t>(key.sequence()),
          key.target_id(), static_cast<uint8_t>(key.type_flags()),
          static_cast<uint16_t>(key.partition_id()));
    }
    for (const auto& kv : request->write_descriptors()) {
      const std::string& data = kv.second.data();
      if (data.size() >= sizeof(uint64_t)) {
        uint64_t raw;
        std::memcpy(&raw, data.data(), sizeof(uint64_t));
        ctx.write_descriptors[kv.first] = cedar::Descriptor(raw);
      }
    }
    
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
    if (it == txn_states_.end()) {
      response->set_success(false);
      response->set_error_msg("Transaction not found");
      return grpc::Status::OK;
    }
    auto& ctx = it->second;
    if (ctx.state != TxnState::kPrepared && ctx.state != TxnState::kCommitting) {
      response->set_success(false);
      response->set_error_msg("Transaction not in committable state");
      return grpc::Status::OK;
    }
    ctx.state = TxnState::kCommitting;
    
    // 如果有 Raft Manager，通过 Raft Propose 提交
    if (raft_manager_) {
      // 构造 Raft 日志条目 (Commit 类型)
      cedar::dtx::StorageLogEntry entry;
      entry.type = cedar::dtx::StorageLogEntry::Type::kCommit;
      entry.txn_id = request->txn_id();
      entry.commit_ts = ctx.commit_ts;
      
      // 选择第一个 write_set 的分区作为 Raft Group
      uint32_t partition_id = 0;
      if (!ctx.write_set.empty()) {
        partition_id = GetPartitionId(ctx.write_set[0].entity_id());
      }
      
      auto* raft_group = raft_manager_->GetRaftGroup(partition_id);
      if (raft_group && raft_group->IsLeader()) {
        auto raft_status = raft_group->Propose(entry);
        if (raft_status.ok()) {
          ctx.state = TxnState::kCommitted;
          response->set_success(true);
        } else {
          response->set_success(false);
          response->set_error_msg("Raft commit failed: " + raft_status.ToString());
        }
        return grpc::Status::OK;
      }
    }
    
    // Fallback: 直接提交 (无 Raft 模式)
    bool all_ok = true;
    cedar::WriteOptions write_options;
    write_options.sync = true;
    struct CommitWrite {
      cedar::CedarKey key;
      cedar::Descriptor desc;
    };
    std::vector<CommitWrite> commit_writes;
    std::unordered_map<uint32_t, std::vector<std::pair<uint64_t, cedar::cdc::ChangeRecord>>> cdc_batches;
    for (const auto& key : ctx.write_set) {
      uint64_t key_hash = static_cast<uint64_t>(
          std::hash<std::string>{}(std::string(reinterpret_cast<const char*>(&key), sizeof(key))));
      auto desc_it = ctx.write_descriptors.find(key_hash);
      cedar::Descriptor desc = (desc_it != ctx.write_descriptors.end()) ? desc_it->second : cedar::Descriptor(0);
      commit_writes.push_back({key, desc});
      cedar::cdc::ChangeRecord record;
      record.set_txn_id(request->txn_id());
      record.set_entity_id(key.entity_id());
      record.set_target_id(key.target_id());
      record.set_entity_type(static_cast<uint32_t>(key.entity_type()));
      record.set_edge_type(key.IsEdge() ? key.column_id() : 0);
      record.set_column_id(key.column_id());
      record.set_operation(desc.IsTombstone()
                               ? cedar::cdc::CHANGE_OPERATION_DELETE
                               : cedar::cdc::CHANGE_OPERATION_UPDATE);
      record.set_valid_from(ctx.commit_ts.value());
      uint64_t raw_descriptor = desc.AsRaw();
      record.set_payload(
          std::string(reinterpret_cast<const char*>(&raw_descriptor),
                      sizeof(raw_descriptor)));
      cdc_batches[key.part_id()].push_back({key.timestamp().value(),
                                            std::move(record)});
    }
    for (const auto& [partition_id, records] : cdc_batches) {
      auto s = PersistCdcIntent(partition_id, request->txn_id(),
                                ctx.commit_ts.value(), records);
      if (!s.ok()) {
        all_ok = false;
        std::cerr << "[StorageD] CDC intent persist failed: "
                  << s.ToString() << std::endl;
        break;
      }
    }
    if (all_ok) {
      for (const auto& entry : commit_writes) {
        auto s = storage_->Put(write_options, entry.key.entity_id(),
                               entry.key.timestamp().value(), entry.desc,
                               ctx.commit_ts);
        if (!s.ok()) {
          all_ok = false;
          std::cerr << "[StorageD] Commit Put failed: " << s.ToString()
                    << std::endl;
          break;
        }
      }
    }
    if (all_ok) {
      for (auto& [partition_id, intent_records] : cdc_batches) {
        auto* log = GetOrOpenChangeLog(partition_id);
        if (!log) {
          all_ok = false;
          std::cerr << "[StorageD] CDC log unavailable for partition "
                    << partition_id << std::endl;
          break;
        }
        std::vector<cedar::cdc::ChangeRecord> records;
        records.reserve(intent_records.size());
        for (auto& [storage_timestamp, record] : intent_records) {
          (void)storage_timestamp;
          records.push_back(std::move(record));
        }
        auto s = log->AppendCommittedBatch(ctx.commit_ts.value(),
                                           std::move(records));
        if (!s.ok()) {
          all_ok = false;
          std::cerr << "[StorageD] CDC append failed: " << s.ToString()
                    << std::endl;
          break;
        }
        auto cleanup = DeleteCdcIntent(partition_id, request->txn_id());
        if (!cleanup.ok()) {
          std::cerr << "[StorageD] CDC intent cleanup warning: "
                    << cleanup.ToString() << std::endl;
        }
      }
    }
    
    if (all_ok) {
      ctx.state = TxnState::kCommitted;
      response->set_success(true);
    } else {
      ctx.state = TxnState::kCommitting;
      response->set_success(false);
      response->set_error_msg("Commit incomplete; retry will continue from committing state");
    }
    return grpc::Status::OK;
  }

  grpc::Status Abort(grpc::ServerContext* context,
                     const cedar::storage::AbortRequest* request,
                     cedar::storage::AbortResponse* response) override {
    (void)context;
    std::lock_guard<std::mutex> lock(txn_mutex_);
    auto it = txn_states_.find(request->txn_id());
    if (it != txn_states_.end()) {
      it->second.state = TxnState::kAborted;
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
    switch (it->second.state) {
      case TxnState::kPrepared:
        response->set_state(cedar::storage::InquireResponse::PREPARED);
        break;
      case TxnState::kCommitting:
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
        cedar::Slice(request->value_descriptor().data()));
    if (!desc.has_value()) {
      response->set_success(false);
      response->set_error_msg("Invalid descriptor");
      return grpc::Status::OK;
    }
    
    // 构造 CedarKey
    cedar::CedarKey key(
        request->key().entity_id(),
        static_cast<cedar::EntityType>(request->key().entity_type()),
        static_cast<uint16_t>(request->key().column_id()),
        cedar::Timestamp(request->key().timestamp()),
        static_cast<uint16_t>(request->key().sequence()),
        request->key().target_id(),
        static_cast<uint8_t>(request->key().type_flags()),
        static_cast<uint16_t>(request->key().partition_id()));
    
    // 如果有 Raft Manager，通过 Raft Propose 写入
    if (raft_manager_) {
      uint32_t partition_id = request->key().partition_id();
      if (partition_id == 0) {
        partition_id = GetPartitionId(request->key().entity_id());
      }
      
      auto raft_status = ProposeWrite(partition_id, key, desc.value(),
                                       cedar::Timestamp(request->txn_version().value()));
      auto end = std::chrono::steady_clock::now();
      auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      RecordStorageOp("put", raft_status.ok(), latency_us);
      
      if (raft_status.ok()) {
        response->set_success(true);
      } else {
        response->set_success(false);
        response->set_error_msg(raft_status.error_message());
      }
      return grpc::Status::OK;
    }
    
    // Fallback: 直接写入 (无 Raft 模式)
    cedar::WriteOptions write_options;
    write_options.sync = (request->txn_id() != 0);
    auto s = storage_->Put(write_options, request->key().entity_id(),
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
    
    // 如果有 Raft Manager，检查 Lease 有效性 (线性一致性读)
    if (raft_manager_) {
      uint32_t partition_id = request->key().partition_id();
      if (partition_id == 0) {
        partition_id = GetPartitionId(request->key().entity_id());
      }
      
      auto* raft_group = raft_manager_->GetRaftGroup(partition_id);
      if (raft_group) {
        // 如果不是 Leader 且 Lease 无效，拒绝读取
        if (!raft_group->IsLeader() && !raft_group->IsLeaseValid()) {
          auto leader_addr = raft_group->GetLeaderAddress();
          std::string hint = leader_addr.value_or("unknown");
          return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Not leader, redirect to: " + hint);
        }
      }
    }
    
    auto result = storage_->Get(request->key().entity_id(),
                                request->key().timestamp());
    auto end = std::chrono::steady_clock::now();
    auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    RecordStorageOp("get", true, latency_us);
    if (result.has_value()) {
      response->set_found(true);
      response->mutable_value_descriptor()->set_data(result->Encode());
    } else {
      response->set_found(false);
    }
    response->set_success(true);
    return grpc::Status::OK;
  }

  grpc::Status BatchGet(grpc::ServerContext* context,
                        const cedar::storage::BatchGetRequest* request,
                        cedar::storage::BatchGetResponse* response) override {
    (void)context;
    auto start = std::chrono::steady_clock::now();

    for (const auto& proto_key : request->keys()) {
      auto result = storage_->Get(proto_key.entity_id(), proto_key.timestamp());
      if (result.has_value()) {
        response->add_descriptors()->set_data(result->Encode());
        response->add_found(true);
      } else {
        response->add_descriptors();
        response->add_found(false);
      }
    }

    auto end = std::chrono::steady_clock::now();
    auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
        end - start).count();
    RecordStorageOp("batch_get", true, latency_us);
    response->set_success(true);
    return grpc::Status::OK;
  }

  grpc::Status Delete(grpc::ServerContext* context,
                      const cedar::storage::DeleteRequest* request,
                      cedar::storage::DeleteResponse* response) override {
    (void)context;
    auto start = std::chrono::steady_clock::now();
    
    // 如果有 Raft Manager，通过 Raft Propose 删除
    if (raft_manager_) {
      cedar::CedarKey key(
          request->key().entity_id(),
          static_cast<cedar::EntityType>(request->key().entity_type()),
          static_cast<uint16_t>(request->key().column_id()),
          cedar::Timestamp(request->key().timestamp()),
          static_cast<uint16_t>(request->key().sequence()),
          request->key().target_id(),
          static_cast<uint8_t>(request->key().type_flags()),
          static_cast<uint16_t>(request->key().partition_id()));
      
      uint32_t partition_id = request->key().partition_id();
      if (partition_id == 0) {
        partition_id = GetPartitionId(request->key().entity_id());
      }
      
      auto* raft_group = raft_manager_->GetRaftGroup(partition_id);
      if (raft_group && raft_group->IsLeader()) {
        cedar::dtx::StorageLogEntry entry;
        entry.type = cedar::dtx::StorageLogEntry::Type::kDelete;
        entry.key = key;
        entry.txn_version = cedar::Timestamp(request->txn_version().value());
        
        auto raft_status = raft_group->Propose(entry);
        auto end = std::chrono::steady_clock::now();
        auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        RecordStorageOp("delete", raft_status.ok(), latency_us);
        
        response->set_success(raft_status.ok());
        if (!raft_status.ok()) {
          response->set_error_msg(raft_status.ToString());
        }
        return grpc::Status::OK;
      }
    }
    
    // Fallback: 直接删除
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

  grpc::Status Scan(grpc::ServerContext* context,
                     const cedar::storage::ScanRequest* request,
                     cedar::storage::ScanResponse* response) override {
    (void)context;
    auto results = storage_->Scan(
        request->entity_id(),
        cedar::Timestamp(request->start_time()),
        cedar::Timestamp(request->end_time()));
    for (const auto& [ts, desc] : results) {
      auto* item = response->add_items();
      item->set_timestamp(ts.value());
      item->mutable_value_descriptor()->set_data(desc.Encode());
    }
    response->set_success(true);
    return grpc::Status::OK;
  }

  grpc::Status ScanNodeV2(grpc::ServerContext* context,
                          const cedar::storage::ScanNodeRequestV2* request,
                          cedar::storage::ScanResponse* response) override {
    (void)context;
    std::vector<std::pair<cedar::Timestamp, cedar::Descriptor>> results;
    auto s = storage_->ScanNode(request->node_id(),
                                cedar::Timestamp(request->end_time()),
                                &results);
    if (!s.ok()) {
      response->set_success(false);
      response->set_error_msg(s.ToString());
      return grpc::Status::OK;
    }
    for (const auto& [ts, desc] : results) {
      auto* item = response->add_items();
      item->set_timestamp(ts.value());
      item->mutable_value_descriptor()->set_data(desc.Encode());
    }
    response->set_success(true);
    return grpc::Status::OK;
  }

  grpc::Status ScanEdgeV2(grpc::ServerContext* context,
                          const cedar::storage::ScanEdgeRequestV2* request,
                          cedar::storage::ScanResponse* response) override {
    (void)context;
    cedar::EntityType edge_dir = cedar::EntityType::EdgeOut;
    if (request->direction() == cedar::storage::Direction::INCOMING) {
      edge_dir = cedar::EntityType::EdgeIn;
    } else if (request->direction() == cedar::storage::Direction::BOTH) {
      // Scan both directions
      auto edges_out = storage_->ScanEdgesWithFolding(
          request->node_id(), cedar::EntityType::EdgeOut,
          static_cast<uint16_t>(request->edge_type()),
          cedar::Timestamp(request->end_time()));
      auto edges_in = storage_->ScanEdgesWithFolding(
          request->node_id(), cedar::EntityType::EdgeIn,
          static_cast<uint16_t>(request->edge_type()),
          cedar::Timestamp(request->end_time()));
      response->set_success(true);
      for (const auto& e : edges_out) {
        auto* item = response->add_items();
        item->set_timestamp(e.timestamp.value());
        item->mutable_value_descriptor()->set_data(e.descriptor.Encode());
      }
      for (const auto& e : edges_in) {
        auto* item = response->add_items();
        item->set_timestamp(e.timestamp.value());
        item->mutable_value_descriptor()->set_data(e.descriptor.Encode());
      }
      return grpc::Status::OK;
    }

    auto edges = storage_->ScanEdgesWithFolding(
        request->node_id(), edge_dir,
        static_cast<uint16_t>(request->edge_type()),
        cedar::Timestamp(request->end_time()));
    response->set_success(true);
    for (const auto& e : edges) {
      auto* item = response->add_items();
      item->set_timestamp(e.timestamp.value());
      item->mutable_value_descriptor()->set_data(e.descriptor.Encode());
    }
    return grpc::Status::OK;
  }

  grpc::Status ExecuteSubQuery(grpc::ServerContext* context,
                               const cedar::storage::ExecuteSubQueryRequest* request,
                               grpc::ServerWriter<cedar::storage::SubQueryResultBatch>* writer) override {
    // Check if client is still connected
    if (context->IsCancelled()) {
      return grpc::Status(grpc::StatusCode::CANCELLED, "Client disconnected");
    }
    
    cedar::storage::SubQueryResultBatch batch;
    if (!cypher_engine_) {
      batch.set_is_last(true);
      writer->Write(batch);
      return grpc::Status::OK;
    }

    // Convert proto parameters to cypher::Value map
    std::map<std::string, cedar::cypher::Value> params;
    for (const auto& kv : request->parameters()) {
      const auto& qv = kv.second;
      switch (qv.value_type_case()) {
        case cedar::storage::QueryValue::kBoolVal:
          params[kv.first] = cedar::cypher::Value(qv.bool_val());
          break;
        case cedar::storage::QueryValue::kIntVal:
          params[kv.first] = cedar::cypher::Value(qv.int_val());
          break;
        case cedar::storage::QueryValue::kFloatVal:
          params[kv.first] = cedar::cypher::Value(qv.float_val());
          break;
        case cedar::storage::QueryValue::kStringVal:
          params[kv.first] = cedar::cypher::Value(qv.string_val());
          break;
        default:
          params[kv.first] = cedar::cypher::Value::Null();
          break;
      }
    }

    cedar::cypher::ResultSet result;
    {
      std::lock_guard<std::mutex> lock(cypher_mutex_);
      result = cypher_engine_->Execute(request->query_fragment(), params);
    }

    if (result.HasError()) {
      batch.set_is_last(true);
      writer->Write(batch);
      return grpc::Status::OK;
    }

    for (const auto& col : result.columns) {
      batch.add_columns(col);
    }
    for (const auto& record : result.records) {
      auto* row = batch.add_records();
      for (const auto& col : result.columns) {
        auto it = record.values.find(col);
        if (it != record.values.end()) {
          cedar::storage::QueryValue qv;
          switch (it->second.Type()) {
            case cedar::cypher::ValueType::kBool:
              qv.set_bool_val(it->second.GetBool());
              break;
            case cedar::cypher::ValueType::kInt:
              qv.set_int_val(it->second.GetInt());
              break;
            case cedar::cypher::ValueType::kFloat:
              qv.set_float_val(it->second.GetFloat());
              break;
            case cedar::cypher::ValueType::kString:
              qv.set_string_val(it->second.GetString());
              break;
            default:
              qv.set_string_val(it->second.ToString());
              break;
          }
          *row->add_values() = std::move(qv);
        } else {
          row->add_values();  // null
        }
      }
    }

    batch.set_is_last(true);
    writer->Write(batch);
    return grpc::Status::OK;
  }

  // ============================================================================
  // 备份恢复
  // ============================================================================

  grpc::Status CreateBackup(grpc::ServerContext* context,
                            const cedar::storage::CreateBackupRequest* request,
                            cedar::storage::CreateBackupResponse* response) override {
    if (!storage_) {
      response->set_success(false);
      response->set_error_msg("Storage not initialized");
      return grpc::Status::OK;
    }

    // Determine backup path
    std::string backup_path = request->backup_path();
    if (backup_path.empty()) {
      auto now = std::chrono::system_clock::now();
      auto ts = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
      backup_path = data_dir_ + "/backups/backup_" + std::to_string(ts);
    }

    // Flush memtable if requested
    if (request->flush_first()) {
      auto* engine = storage_->GetLsmEngine();
      if (engine) {
        auto flush_status = engine->ForceFlush();
        if (!flush_status.ok()) {
          std::cerr << "[StorageD] Backup flush warning: " << flush_status.ToString() << std::endl;
        }
      }
    }

    // Create backup directory
    try {
      std::filesystem::create_directories(backup_path);
    } catch (const std::exception& e) {
      response->set_success(false);
      response->set_error_msg(std::string("Failed to create backup dir: ") + e.what());
      return grpc::Status::OK;
    }

    // Copy data directory to backup
    std::string data_path = storage_->GetDbPath();
    uint64_t total_size = 0;
    try {
      for (const auto& entry : std::filesystem::recursive_directory_iterator(data_path)) {
        if (entry.is_regular_file()) {
          std::string relative = std::filesystem::relative(entry.path(), data_path).string();
          std::string dst = backup_path + "/" + relative;
          std::filesystem::create_directories(std::filesystem::path(dst).parent_path());
          std::filesystem::copy_file(entry.path(), dst,
                                     std::filesystem::copy_options::overwrite_existing);
          total_size += entry.file_size();
        }
      }
    } catch (const std::exception& e) {
      response->set_success(false);
      response->set_error_msg(std::string("Backup copy failed: ") + e.what());
      return grpc::Status::OK;
    }

    response->set_success(true);
    response->set_backup_path(backup_path);
    response->set_backup_size(total_size);
    response->set_timestamp_unix(
        std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());

    std::cout << "[StorageD] Backup created: " << backup_path 
              << " (" << total_size << " bytes)" << std::endl;
    return grpc::Status::OK;
  }

  grpc::Status RestoreFromBackup(grpc::ServerContext* context,
                                 const cedar::storage::RestoreFromBackupRequest* request,
                                 cedar::storage::RestoreFromBackupResponse* response) override {
    if (!storage_) {
      response->set_success(false);
      response->set_error_msg("Storage not initialized");
      return grpc::Status::OK;
    }

    auto status = storage_->RestoreFromSnapshot(request->backup_path());
    if (!status.ok()) {
      response->set_success(false);
      response->set_error_msg(status.ToString());
    } else {
      response->set_success(true);
      std::cout << "[StorageD] Restored from backup: " << request->backup_path() << std::endl;
    }
    return grpc::Status::OK;
  }

  grpc::Status ListBackups(grpc::ServerContext* context,
                           const cedar::storage::ListBackupsRequest* request,
                           cedar::storage::ListBackupsResponse* response) override {
    std::string backup_dir = request->backup_dir();
    if (backup_dir.empty()) {
      backup_dir = data_dir_ + "/backups";
    }

    if (!std::filesystem::exists(backup_dir)) {
      response->set_success(true);
      return grpc::Status::OK;
    }

    try {
      for (const auto& entry : std::filesystem::directory_iterator(backup_dir)) {
        if (entry.is_directory()) {
          auto* info = response->add_backups();
          info->set_path(entry.path().string());
          
          // Calculate directory size
          uint64_t size = 0;
          for (const auto& f : std::filesystem::recursive_directory_iterator(entry)) {
            if (f.is_regular_file()) {
              size += f.file_size();
            }
          }
          info->set_size(size);
          
          // Get creation time (use last_write_time as proxy)
          auto ftime = entry.last_write_time();
          auto sctp = std::chrono::time_point_cast<std::chrono::seconds>(
              ftime - std::filesystem::file_time_type::clock::now() + 
              std::chrono::system_clock::now());
          info->set_created_at_unix(sctp.time_since_epoch().count());
          
          info->set_status("complete");
        }
      }
    } catch (const std::exception& e) {
      response->set_success(false);
      response->set_error_msg(std::string("Failed to list backups: ") + e.what());
      return grpc::Status::OK;
    }

    response->set_success(true);
    return grpc::Status::OK;
  }

  // ============================================================================
  // 热点检测
  // ============================================================================

  grpc::Status GetHotSpots(grpc::ServerContext* context,
                           const cedar::storage::GetHotSpotsRequest* request,
                           cedar::storage::GetHotSpotsResponse* response) override {
    if (!storage_) {
      response->set_success(false);
      response->set_error_msg("Storage not initialized");
      return grpc::Status::OK;
    }

    auto* engine = storage_->GetLsmEngine();
    if (!engine) {
      response->set_success(false);
      response->set_error_msg("LSM engine not available");
      return grpc::Status::OK;
    }

    size_t top_n = request->top_n() > 0 ? request->top_n() : 20;
    auto hot_spots = engine->GetHotSpots(top_n);

    for (const auto& spot : hot_spots) {
      auto* info = response->add_hot_spots();
      info->set_entity_id(spot.entity_id);
      info->set_column_id(spot.column_id);
      info->set_query_count(spot.query_count);
      // Convert steady_clock to approximate unix timestamp
      auto now = std::chrono::system_clock::now();
      auto elapsed = std::chrono::steady_clock::now() - spot.last_query;
      auto last_query_sys = now - std::chrono::duration_cast<std::chrono::system_clock::duration>(elapsed);
      info->set_last_query_unix(
          std::chrono::duration_cast<std::chrono::seconds>(last_query_sys.time_since_epoch()).count());
    }

    response->set_success(true);
    return grpc::Status::OK;
  }

  grpc::Status ResetHotSpotStats(grpc::ServerContext* context,
                                 const cedar::storage::ResetHotSpotStatsRequest* request,
                                 cedar::storage::ResetHotSpotStatsResponse* response) override {
    if (!storage_) {
      response->set_success(false);
      return grpc::Status::OK;
    }

    auto* engine = storage_->GetLsmEngine();
    if (engine) {
      engine->ResetQueryPatterns();
    }

    response->set_success(true);
    std::cout << "[StorageD] Hot spot statistics reset" << std::endl;
    return grpc::Status::OK;
  }

  // ============================================================================
  // 存储容量监控
  // ============================================================================

  grpc::Status GetStorageCapacity(grpc::ServerContext* context,
                                  const cedar::storage::GetStorageCapacityRequest* request,
                                  cedar::storage::GetStorageCapacityResponse* response) override {
    if (!storage_) {
      response->set_success(false);
      response->set_error_msg("Storage not initialized");
      return grpc::Status::OK;
    }

    auto* engine = storage_->GetLsmEngine();
    if (!engine) {
      response->set_success(false);
      response->set_error_msg("LSM engine not available");
      return grpc::Status::OK;
    }

    auto capacity = engine->GetCapacityInfo();
    auto stats = engine->GetStats();

    response->set_success(true);
    response->set_total_disk_bytes(capacity.total_bytes);
    response->set_used_disk_bytes(capacity.used_bytes);
    response->set_available_disk_bytes(capacity.available_bytes);
    response->set_db_size_bytes(capacity.db_size_bytes);
    response->set_disk_usage_percent(capacity.usage_percent);
    response->set_is_warning(capacity.is_warning);
    response->set_is_critical(capacity.is_critical);
    response->set_memtable_size(stats.memtable_size);
    response->set_sst_size(stats.sst_size);
    response->set_sst_count(stats.sst_count);

    return grpc::Status::OK;
  }

  grpc::Status ScanLabel(grpc::ServerContext* context,
                         const cedar::storage::ScanLabelRequest* request,
                         cedar::storage::ScanLabelResponse* response) override {
    (void)context;
    auto* engine = storage_->GetLsmEngine();
    if (!engine) {
      response->set_success(false);
      response->set_error_message("LSM engine not available");
      return grpc::Status::OK;
    }

    // Note: space_name is currently ignored because LookupLabelIndex is global
    // per engine. Reserved for future multi-space support.
    const auto& entity_ids = engine->LookupLabelIndex(request->label());

    response->set_success(true);
    auto it = std::lower_bound(entity_ids.begin(), entity_ids.end(), request->min_id());
    uint64_t count = 0;
    for (; it != entity_ids.end() && count < request->limit(); ++it) {
      if (*it > request->max_id()) break;
      response->add_entity_ids(*it);
      count++;
    }

    return grpc::Status::OK;
  }

  grpc::Status GetChangeLogState(
      grpc::ServerContext* context,
      const cedar::storage::GetChangeLogStateRequest* request,
      cedar::storage::GetChangeLogStateResponse* response) override {
    if (DeadlineExpired(context)) {
      return grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED,
                          "deadline expired before CDC state read");
    }
    if (context->IsCancelled()) {
      return grpc::Status::CANCELLED;
    }
    auto leader_status = CheckCdcReadLeader(request->partition_id());
    if (!leader_status.ok()) {
      response->set_success(false);
      response->set_error_code(cedar::storage::CDC_UNAVAILABLE);
      response->set_error_msg(leader_status.error_message());
      return grpc::Status::OK;
    }
    auto* log = GetOrOpenChangeLog(request->partition_id());
    if (!log) {
      response->set_success(false);
      response->set_error_code(cedar::storage::CDC_PARTITION_NOT_FOUND);
      response->set_error_msg("CDC log not found");
      return grpc::Status::OK;
    }
    auto state = log->GetState();
    if (request->expected_epoch() != 0 &&
        request->expected_epoch() != state.partition_epoch) {
      FillCdcState(response, request->partition_id(), state);
      response->set_success(false);
      response->set_error_code(cedar::storage::CDC_STALE_EPOCH);
      response->set_error_msg("CDC partition epoch mismatch");
      return grpc::Status::OK;
    }
    FillCdcState(response, request->partition_id(), state);
    return grpc::Status::OK;
  }

  grpc::Status FetchChanges(
      grpc::ServerContext* context,
      const cedar::storage::FetchChangesRequest* request,
      cedar::storage::FetchChangesResponse* response) override {
    if (DeadlineExpired(context)) {
      return grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED,
                          "deadline expired before CDC fetch");
    }
    if (context->IsCancelled()) {
      return grpc::Status::CANCELLED;
    }
    if (!CdcLimitsValid(request->limit_records(), request->limit_bytes())) {
      response->set_success(false);
      response->set_error_code(cedar::storage::CDC_INVALID_LIMIT);
      response->set_error_msg("Invalid CDC fetch limits");
      return grpc::Status::OK;
    }
    auto leader_status = CheckCdcReadLeader(request->partition_id());
    if (!leader_status.ok()) {
      response->set_success(false);
      response->set_error_code(cedar::storage::CDC_UNAVAILABLE);
      response->set_error_msg(leader_status.error_message());
      return grpc::Status::OK;
    }
    auto* log = GetOrOpenChangeLog(request->partition_id());
    if (!log) {
      response->set_success(false);
      response->set_error_code(cedar::storage::CDC_PARTITION_NOT_FOUND);
      response->set_error_msg("CDC log not found");
      return grpc::Status::OK;
    }
    auto state = log->GetState();
    if (request->expected_epoch() != 0 &&
        request->expected_epoch() != state.partition_epoch) {
      FillCdcState(response, request->partition_id(), state);
      response->set_success(false);
      response->set_error_code(cedar::storage::CDC_STALE_EPOCH);
      response->set_error_msg("CDC partition epoch mismatch");
      return grpc::Status::OK;
    }
    auto records = log->ReadAfter(request->after_offset(),
                                  request->limit_records(),
                                  request->limit_bytes());
    if (!records.ok()) {
      FillCdcState(response, request->partition_id(), state);
      response->set_success(false);
      response->set_error_code(cedar::storage::CDC_UNAVAILABLE);
      response->set_error_msg(records.status().ToString());
      return grpc::Status::OK;
    }
    FillCdcState(response, request->partition_id(), state);
    uint64_t next_offset = request->after_offset();
    for (const auto& record : records.ValueOrDie()) {
      *response->add_records() = record;
      next_offset = record.offset();
    }
    response->set_next_offset(next_offset);
    response->set_has_more(next_offset < state.high_watermark);
    return grpc::Status::OK;
  }

  grpc::Status GetComputeSnapshot(
      grpc::ServerContext* context,
      const cedar::storage::GetComputeSnapshotRequest* request,
      grpc::ServerWriter<cedar::storage::ComputeSnapshotBatch>* writer) override {
    if (DeadlineExpired(context)) {
      return grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED,
                          "deadline expired before CDC snapshot");
    }
    if (context->IsCancelled()) {
      return grpc::Status::CANCELLED;
    }
    if (!CdcLimitsValid(request->limit_records(), request->limit_bytes())) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                          "Invalid CDC snapshot limits");
    }
    auto leader_status = CheckCdcReadLeader(request->partition_id());
    if (!leader_status.ok()) {
      return leader_status;
    }
    auto* log = GetOrOpenChangeLog(request->partition_id());
    if (!log) {
      return grpc::Status(grpc::StatusCode::NOT_FOUND,
                          "CDC log not found");
    }
    auto state = log->GetState();
    if (request->expected_epoch() != 0 &&
        request->expected_epoch() != state.partition_epoch) {
      return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                          "CDC partition epoch mismatch");
    }
    if (DeadlineExpired(context)) {
      return grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED,
                          "deadline expired before CDC snapshot read");
    }
    const uint64_t snapshot_version = request->snapshot_version() == 0
                                          ? state.committed_version
                                          : request->snapshot_version();
    uint64_t cursor = request->resume_offset();
    uint64_t sequence = 0;
    bool wrote_batch = false;

    while (true) {
      if (DeadlineExpired(context)) {
        return grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED,
                            "deadline expired during CDC snapshot read");
      }
      if (context->IsCancelled()) {
        return grpc::Status::CANCELLED;
      }

      auto records = log->ReadAfter(cursor,
                                    request->limit_records(),
                                    request->limit_bytes());
      if (!records.ok()) {
        return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                            records.status().ToString());
      }

      const auto& batch_records = records.ValueOrDie();
      if (batch_records.empty() && cursor < state.high_watermark) {
        return grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED,
                            "next CDC record exceeds snapshot byte limit");
      }

      uint64_t max_seen_offset = cursor;
      cedar::storage::ComputeSnapshotBatch batch;
      batch.set_partition_id(request->partition_id());
      batch.set_partition_epoch(state.partition_epoch);
      batch.set_snapshot_version(snapshot_version);
      batch.set_resume_offset(cursor);
      batch.set_sequence(sequence);
      for (const auto& record : batch_records) {
        max_seen_offset = std::max(max_seen_offset, record.offset());
        if (record.commit_version() <= snapshot_version) {
          *batch.add_records() = record;
        }
      }

      const bool reached_log_end = max_seen_offset >= state.high_watermark ||
                                   batch_records.empty();
      if (batch.records_size() == 0 && wrote_batch && !reached_log_end) {
        cursor = max_seen_offset;
        continue;
      }
      if (batch.records_size() == 0 && wrote_batch && reached_log_end) {
        batch.set_resume_offset(cursor);
      }
      batch.set_final(reached_log_end);
      batch.set_checksum(ComputeSnapshotChecksum(batch));

      if (DeadlineExpired(context)) {
        return grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED,
                            "deadline expired before CDC snapshot write");
      }
      if (!writer->Write(batch)) {
        return grpc::Status(grpc::StatusCode::CANCELLED,
                            "client closed CDC snapshot stream");
      }

      wrote_batch = true;
      ++sequence;
      cursor = max_seen_offset;
      if (reached_log_end) {
        return grpc::Status::OK;
      }
    }
  }

 private:
  template <typename Response>
  void FillCdcState(Response* response,
                    uint32_t partition_id,
                    const cedar::cdc::ChangeLogState& state) {
    response->set_success(true);
    response->set_error_code(cedar::storage::CDC_OK);
    response->set_partition_id(partition_id);
    response->set_partition_epoch(state.partition_epoch);
    response->set_earliest_offset(state.earliest_offset);
    response->set_high_watermark(state.high_watermark);
    response->set_committed_version(state.committed_version);
  }

  grpc::Status CheckCdcReadLeader(uint32_t partition_id) {
    if (!raft_manager_) {
      return grpc::Status::OK;
    }
    auto* raft_group = raft_manager_->GetRaftGroup(partition_id);
    if (!raft_group) {
      return grpc::Status::OK;
    }
    if (raft_group->IsLeader() && raft_group->IsLeaseValid()) {
      return grpc::Status::OK;
    }
    auto leader_addr = raft_group->GetLeaderAddress();
    std::string hint = leader_addr.value_or("unknown");
    return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                        "Not leader, redirect to: " + hint);
  }

  // 根据 entity_id 计算分区 ID (简化: 取模)
  uint32_t GetPartitionId(uint64_t entity_id) const {
    return static_cast<uint32_t>(entity_id % 32768);
  }

  cedar::cdc::PartitionChangeLog* GetOrOpenChangeLog(uint32_t partition_id) {
    auto it = change_logs_.find(partition_id);
    if (it != change_logs_.end()) {
      return it->second.get();
    }
    if (data_dir_.empty()) {
      return nullptr;
    }
    cedar::cdc::PartitionChangeLog::Options options;
    options.directory = data_dir_ + "/cdc/partition_" +
                        std::to_string(partition_id);
    options.partition_id = partition_id;
    options.partition_epoch = 1;
    auto opened = cedar::cdc::PartitionChangeLog::Open(options);
    if (!opened.ok()) {
      std::cerr << "[StorageD] Failed to open CDC log for partition "
                << partition_id << ": " << opened.status().ToString()
                << std::endl;
      return nullptr;
    }
    auto log = std::move(opened.ValueOrDie());
    auto* raw = log.get();
    change_logs_[partition_id] = std::move(log);
    return raw;
  }

  std::filesystem::path CdcIntentDir(uint32_t partition_id) const {
    return std::filesystem::path(data_dir_) / "cdc_intents" /
           ("partition_" + std::to_string(partition_id));
  }

  std::filesystem::path CdcIntentPath(uint32_t partition_id,
                                      uint64_t txn_id) const {
    return CdcIntentDir(partition_id) /
           ("txn_" + std::to_string(txn_id) + ".intent");
  }

  cedar::Status PersistCdcIntent(
      uint32_t partition_id, uint64_t txn_id, uint64_t commit_version,
      const std::vector<std::pair<uint64_t, cedar::cdc::ChangeRecord>>& records) const {
    if (records.empty()) {
      return cedar::Status::OK();
    }
    auto dir = CdcIntentDir(partition_id);
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    if (ec) {
      return cedar::Status::IOError("PersistCdcIntent", ec.message());
    }
    auto final_path = CdcIntentPath(partition_id, txn_id);
    auto tmp_path = final_path;
    tmp_path += ".tmp";
    int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
      return cedar::Status::IOError("PersistCdcIntent", "open failed");
    }
    auto write_all = [&](const void* data, size_t size) -> cedar::Status {
      const char* cursor = static_cast<const char*>(data);
      while (size > 0) {
        ssize_t written = ::write(fd, cursor, size);
        if (written <= 0) {
          return cedar::Status::IOError("PersistCdcIntent", "write failed");
        }
        cursor += written;
        size -= static_cast<size_t>(written);
      }
      return cedar::Status::OK();
    };

    const std::string magic = "CEDAR_CDC_INTENT_V1";
    uint32_t magic_size = static_cast<uint32_t>(magic.size());
    uint32_t record_count = static_cast<uint32_t>(records.size());
    cedar::Status s = write_all(&magic_size, sizeof(magic_size));
    if (s.ok()) s = write_all(magic.data(), magic.size());
    if (s.ok()) s = write_all(&txn_id, sizeof(txn_id));
    if (s.ok()) s = write_all(&commit_version, sizeof(commit_version));
    if (s.ok()) s = write_all(&record_count, sizeof(record_count));
    for (const auto& [storage_timestamp, record] : records) {
      if (!s.ok()) break;
      std::string serialized;
      if (!record.SerializeToString(&serialized)) {
        s = cedar::Status::Corruption("PersistCdcIntent",
                                      "serialize failed");
        break;
      }
      uint32_t size = static_cast<uint32_t>(serialized.size());
      s = write_all(&storage_timestamp, sizeof(storage_timestamp));
      if (!s.ok()) break;
      s = write_all(&size, sizeof(size));
      if (s.ok()) s = write_all(serialized.data(), serialized.size());
    }
    if (s.ok() && ::fsync(fd) < 0) {
      s = cedar::Status::IOError("PersistCdcIntent", "fsync failed");
    }
    ::close(fd);
    if (!s.ok()) {
      std::filesystem::remove(tmp_path, ec);
      return s;
    }
    std::filesystem::rename(tmp_path, final_path, ec);
    if (ec) {
      return cedar::Status::IOError("PersistCdcIntent", ec.message());
    }
    int dir_fd = ::open(dir.c_str(), O_RDONLY);
    if (dir_fd >= 0) {
      ::fsync(dir_fd);
      ::close(dir_fd);
    }
    return cedar::Status::OK();
  }

  cedar::Status DeleteCdcIntent(uint32_t partition_id, uint64_t txn_id) const {
    std::error_code ec;
    std::filesystem::remove(CdcIntentPath(partition_id, txn_id), ec);
    if (ec) {
      return cedar::Status::IOError("DeleteCdcIntent", ec.message());
    }
    return cedar::Status::OK();
  }

  struct CdcIntentBatch {
    uint64_t txn_id = 0;
    uint64_t commit_version = 0;
    std::vector<std::pair<uint64_t, cedar::cdc::ChangeRecord>> records;
  };

  cedar::StatusOr<CdcIntentBatch> ReadCdcIntent(
      const std::filesystem::path& path) const {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
      return cedar::Status::IOError("ReadCdcIntent", "open failed");
    }
    uint32_t magic_size = 0;
    in.read(reinterpret_cast<char*>(&magic_size), sizeof(magic_size));
    if (!in || magic_size == 0 || magic_size > 1024) {
      return cedar::Status::Corruption("ReadCdcIntent", "bad magic size");
    }
    std::string magic(magic_size, '\0');
    in.read(magic.data(), magic.size());
    if (!in || magic != "CEDAR_CDC_INTENT_V1") {
      return cedar::Status::Corruption("ReadCdcIntent", "bad magic");
    }
    CdcIntentBatch batch;
    uint32_t record_count = 0;
    in.read(reinterpret_cast<char*>(&batch.txn_id), sizeof(batch.txn_id));
    in.read(reinterpret_cast<char*>(&batch.commit_version),
            sizeof(batch.commit_version));
    in.read(reinterpret_cast<char*>(&record_count), sizeof(record_count));
    if (!in || record_count > 1000000) {
      return cedar::Status::Corruption("ReadCdcIntent", "bad record count");
    }
    batch.records.reserve(record_count);
    for (uint32_t i = 0; i < record_count; ++i) {
      uint64_t storage_timestamp = 0;
      in.read(reinterpret_cast<char*>(&storage_timestamp),
              sizeof(storage_timestamp));
      if (!in) {
        return cedar::Status::Corruption("ReadCdcIntent",
                                         "bad storage timestamp");
      }
      uint32_t size = 0;
      in.read(reinterpret_cast<char*>(&size), sizeof(size));
      if (!in || size == 0 || size > 16 * 1024 * 1024) {
        return cedar::Status::Corruption("ReadCdcIntent", "bad record size");
      }
      std::string serialized(size, '\0');
      in.read(serialized.data(), serialized.size());
      cedar::cdc::ChangeRecord record;
      if (!in || !record.ParseFromString(serialized)) {
        return cedar::Status::Corruption("ReadCdcIntent", "bad record");
      }
      batch.records.push_back({storage_timestamp, std::move(record)});
    }
    return batch;
  }

  bool ChangeLogContainsBatch(cedar::cdc::PartitionChangeLog* log,
                              const CdcIntentBatch& batch) const {
    uint64_t offset = 0;
    std::vector<bool> matched_indexes(batch.records.size(), false);
    while (true) {
      auto page = log->ReadAfter(offset, 4096, 4 * 1024 * 1024);
      if (!page.ok() || page.ValueOrDie().empty()) {
        return false;
      }
      for (const auto& record : page.ValueOrDie()) {
        offset = std::max(offset, record.offset());
        if (record.txn_id() == batch.txn_id &&
            record.commit_version() == batch.commit_version &&
            record.batch_size() == batch.records.size() &&
            record.batch_index() < batch.records.size()) {
          const auto& expected = batch.records[record.batch_index()].second;
          if (record.entity_id() == expected.entity_id() &&
              record.target_id() == expected.target_id() &&
              record.entity_type() == expected.entity_type() &&
              record.column_id() == expected.column_id() &&
              record.operation() == expected.operation() &&
              record.payload() == expected.payload()) {
            matched_indexes[record.batch_index()] = true;
          }
        }
      }
      if (!matched_indexes.empty() &&
          std::all_of(matched_indexes.begin(), matched_indexes.end(),
                      [](bool matched) { return matched; })) {
        return true;
      }
    }
  }

  void RecoverCdcIntents() {
    auto root = std::filesystem::path(data_dir_) / "cdc_intents";
    std::error_code ec;
    if (!std::filesystem::exists(root, ec)) {
      return;
    }
    for (const auto& partition_dir : std::filesystem::directory_iterator(root, ec)) {
      if (ec || !partition_dir.is_directory()) {
        continue;
      }
      const std::string name = partition_dir.path().filename().string();
      if (name.rfind("partition_", 0) != 0) {
        continue;
      }
      uint32_t partition_id = static_cast<uint32_t>(
          std::stoul(name.substr(std::string("partition_").size())));
      auto* log = GetOrOpenChangeLog(partition_id);
      if (!log) {
        continue;
      }
      for (const auto& intent : std::filesystem::directory_iterator(partition_dir.path(), ec)) {
        if (ec || !intent.is_regular_file() ||
            intent.path().extension() != ".intent") {
          continue;
        }
        auto batch_or = ReadCdcIntent(intent.path());
        if (!batch_or.ok()) {
          std::cerr << "[StorageD] CDC intent recovery read failed: "
                    << batch_or.status().ToString() << std::endl;
          continue;
        }
        auto batch = std::move(batch_or.ValueOrDie());
        if (!ChangeLogContainsBatch(log, batch)) {
          std::vector<cedar::cdc::ChangeRecord> records;
          records.reserve(batch.records.size());
          for (const auto& [storage_timestamp, record] : batch.records) {
            cedar::Descriptor desc;
            if (record.payload().size() >= sizeof(uint64_t)) {
              uint64_t raw_descriptor = 0;
              std::memcpy(&raw_descriptor, record.payload().data(),
                          sizeof(raw_descriptor));
              desc = cedar::Descriptor(raw_descriptor);
            }
            auto put_status = storage_->Put(record.entity_id(),
                                           storage_timestamp,
                                           desc,
                                           cedar::Timestamp(batch.commit_version));
            if (!put_status.ok()) {
              std::cerr << "[StorageD] CDC intent recovery storage replay failed: "
                        << put_status.ToString() << std::endl;
              records.clear();
              break;
            }
            records.push_back(record);
          }
          if (records.empty() && !batch.records.empty()) {
            continue;
          }
          auto s = log->AppendCommittedBatch(batch.commit_version,
                                             std::move(records));
          if (!s.ok()) {
            std::cerr << "[StorageD] CDC intent recovery append failed: "
                      << s.ToString() << std::endl;
            continue;
          }
        }
        std::filesystem::remove(intent.path(), ec);
      }
    }
  }
  
  // 通过 Raft Propose 写入
  grpc::Status ProposeWrite(uint32_t partition_id,
                            const cedar::CedarKey& key,
                            const cedar::Descriptor& desc,
                            cedar::Timestamp txn_version) {
    if (!raft_manager_) {
      return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Raft manager not initialized");
    }
    
    auto* raft_group = raft_manager_->GetRaftGroup(partition_id);
    if (!raft_group) {
      return grpc::Status(grpc::StatusCode::NOT_FOUND,
                          "No Raft group for partition " + std::to_string(partition_id));
    }
    
    // 检查是否是 Leader
    if (!raft_group->IsLeader()) {
      auto leader_addr = raft_group->GetLeaderAddress();
      std::string hint = leader_addr.value_or("unknown");
      return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Not leader, redirect to: " + hint);
    }
    
    // 构造 Raft 日志条目
    cedar::dtx::StorageLogEntry entry;
    entry.type = cedar::dtx::StorageLogEntry::Type::kPut;
    entry.key = key;
    entry.descriptor = desc;
    entry.txn_version = txn_version;
    
    // Propose 到 Raft
    auto status = raft_group->Propose(entry);
    if (!status.ok()) {
      return grpc::Status(grpc::StatusCode::INTERNAL, status.ToString());
    }
    
    return grpc::Status::OK;
  }
  
  enum class TxnState { kPrepared, kCommitting, kCommitted, kAborted };
  struct TxnContext {
    TxnState state = TxnState::kPrepared;
    std::vector<cedar::CedarKey> read_set;
    std::vector<cedar::CedarKey> write_set;
    std::unordered_map<uint64_t, cedar::Descriptor> write_descriptors;
    cedar::Timestamp commit_ts;
  };
  cedar::CedarGraphStorage* storage_;
  cedar::dtx::PartitionRaftManager* raft_manager_;
  std::unique_ptr<cedar::cypher::CypherEngine> cypher_engine_;
  std::string data_dir_;
  std::mutex txn_mutex_;
  std::mutex cypher_mutex_;  // Protect CypherEngine execution
  std::unordered_map<uint64_t, TxnContext> txn_states_;
  std::unordered_map<uint32_t, std::unique_ptr<cedar::cdc::PartitionChangeLog>> change_logs_;
};

// MetaD 客户端 - 处理注册和心跳
class MetaClient {
 public:
  MetaClient(const std::string& meta_addr, int node_id, int port,
             const std::string& advertise_address,
             const std::string& data_dir,
             const cedar::dtx::raft::TlsConfig& tls)
      : node_id_(node_id),
        port_(port),
        advertise_address_(advertise_address),
        data_dir_(data_dir) {
    auto client_creds_result = CreateClientCredentialsWithEnvFallback(tls);
    if (!client_creds_result.ok()) {
      throw std::runtime_error(
          "[StorageD] FATAL: TLS credentials required. Provide valid certs or "
          "explicitly set tls.enabled=false for development only. Error: " +
          client_creds_result.status().ToString());
    }
    meta_addrs_ = ParseMetaAddrs(meta_addr);
    if (meta_addrs_.empty()) {
      meta_addrs_.push_back(meta_addr);
    }
    credentials_ = client_creds_result.ValueOrDie();
    ResetStub(meta_addrs_[current_meta_index_]);
  }

  bool Register() {
    constexpr int kMaxAttempts = 30;
    constexpr auto kRetryDelay = std::chrono::seconds(1);

    for (int attempt = 1; attempt <= kMaxAttempts && g_running.load(); ++attempt) {
      if (RegisterOnce()) {
        std::cout << "[StorageD] Registered with MetaD successfully" << std::endl;
        return true;
      }
      std::cerr << "[StorageD] MetaD registration attempt " << attempt
                << "/" << kMaxAttempts << " failed; retrying" << std::endl;
      AdvanceMetaEndpoint();
      std::this_thread::sleep_for(kRetryDelay);
    }

    return false;
  }

  bool RegisterOnce() {
    cedar::meta::RegisterNodeRequest request;
    auto* node_info = request.mutable_node_info();
    node_info->set_node_id(node_id_);
    node_info->set_address(advertise_address_ + ":" + std::to_string(port_));
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
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    auto status = stub_->RegisterNode(&context, request, &response);

    // Handle leader redirect
    if (status.ok() && !response.success() && response.leader_address().size() > 0) {
      const std::string leader_address = NormalizeMetaAddress(response.leader_address());
      std::cerr << "[StorageD] MetaD leader is currently " << leader_address
                << "; retrying registration through leader" << std::endl;
      if (SwitchToLeaderHint(leader_address)) {
        return false;
      }
      AdvanceMetaEndpoint();
      return false;
    } else if (!status.ok() || !response.success()) {
      std::cerr << "[StorageD] Registration failed: "
                << (status.ok() ? response.error_msg() : status.error_message()) << std::endl;
    }

    if (!status.ok() || !response.success()) {
      std::cerr << "[StorageD] Failed to register with MetaD: "
                << (status.ok() ? response.error_msg() : status.error_message()) << std::endl;
      return false;
    }

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
    if (grpc_status.ok() && response.success()) {
      return true;
    }
    if (grpc_status.ok() && response.error_msg().find("Not leader") != std::string::npos) {
      AdvanceMetaEndpoint();
    }
    return false;
  }

 private:
  static std::string Trim(std::string value) {
    const auto begin = value.find_first_not_of(" \t\n\r");
    if (begin == std::string::npos) {
      return "";
    }
    const auto end = value.find_last_not_of(" \t\n\r");
    return value.substr(begin, end - begin + 1);
  }

  static std::string NormalizeMetaAddress(std::string address) {
    address = Trim(address);
    constexpr const char* kBraftPrefix = "braft://";
    if (address.rfind(kBraftPrefix, 0) == 0) {
      address = address.substr(std::string(kBraftPrefix).size());
    }
    const auto slash = address.find('/');
    if (slash != std::string::npos) {
      address = address.substr(0, slash);
    }
    return Trim(address);
  }

  static std::string AddressHost(const std::string& address) {
    const std::string normalized = NormalizeMetaAddress(address);
    const auto colon = normalized.rfind(':');
    if (colon == std::string::npos) {
      return normalized;
    }
    return normalized.substr(0, colon);
  }

  static std::vector<std::string> ParseMetaAddrs(const std::string& meta_addrs) {
    std::vector<std::string> result;
    std::stringstream ss(meta_addrs);
    std::string item;
    while (std::getline(ss, item, ',')) {
      std::string address = NormalizeMetaAddress(item);
      if (address.empty()) {
        continue;
      }
      result.push_back(address);
    }
    return result;
  }

  bool SwitchToLeaderHint(const std::string& leader_hint) {
    const std::string leader_host = AddressHost(leader_hint);
    if (leader_host.empty()) {
      return false;
    }
    for (size_t i = 0; i < meta_addrs_.size(); ++i) {
      if (AddressHost(meta_addrs_[i]) == leader_host) {
        current_meta_index_ = i;
        ResetStub(meta_addrs_[current_meta_index_]);
        return true;
      }
    }
    return false;
  }

  void ResetStub(const std::string& address) {
    auto channel = grpc::CreateChannel(address, credentials_);
    stub_ = cedar::meta::MetaService::NewStub(channel);
  }

  void AdvanceMetaEndpoint() {
    if (meta_addrs_.empty()) {
      return;
    }
    current_meta_index_ = (current_meta_index_ + 1) % meta_addrs_.size();
    ResetStub(meta_addrs_[current_meta_index_]);
  }

  std::vector<std::string> meta_addrs_;
  size_t current_meta_index_{0};
  std::unique_ptr<cedar::meta::MetaService::Stub> stub_;
  int node_id_;
  int port_;
  std::string advertise_address_;
  std::string data_dir_;
  std::shared_ptr<grpc::ChannelCredentials> credentials_;
};

// 心跳线程
void HeartbeatLoop(MetaClient* client, int interval_sec) {
  // Send initial heartbeat immediately so MetaD marks node as ONLINE
  if (!client->SendHeartbeat()) {
    std::cerr << "[StorageD] Initial heartbeat failed" << std::endl;
  }
  
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
  ApplyTlsEnvOverrides(&config.tls);
  
  // 使用 node_id 区分数据目录
  config.data_dir += "/node" + std::to_string(config.node_id);
  
  struct sigaction sa;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = 0;
  sa.sa_handler = SignalHandler;
  sigaction(SIGINT, &sa, nullptr);
  sigaction(SIGTERM, &sa, nullptr);
  
  // Register crash signal handlers
  signal(SIGSEGV, CrashSignalHandler);
  signal(SIGABRT, CrashSignalHandler);
  signal(SIGBUS, CrashSignalHandler);

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

  // Initialize Raft manager for partition replication
  cedar::dtx::PartitionRaftManager raft_manager;
  auto raft_init_status = raft_manager.Initialize(
      config.node_id, config.data_dir + "/raft", config.bind_address + ":" + std::to_string(config.port + 100));
  if (raft_init_status.ok()) {
    std::cout << "[StorageD] Raft manager initialized on port " << (config.port + 100) << std::endl;
  } else {
    std::cerr << "[StorageD] WARNING: Raft manager init failed: " << raft_init_status.ToString() << std::endl;
    std::cerr << "[StorageD] Running without Raft replication" << std::endl;
  }

  // 3. 注册到 MetaD
  // Determine advertise address: explicit > env > bind_address (if not 0.0.0.0) > 127.0.0.1 fallback
  if (config.advertise_address.empty()) {
    const char* env_adv = std::getenv("CEDAR_ADVERTISE_ADDRESS");
    if (env_adv && env_adv[0] != '\0') {
      config.advertise_address = env_adv;
    } else if (config.bind_address != "0.0.0.0" && config.bind_address != "::") {
      config.advertise_address = config.bind_address;
    } else {
      config.advertise_address = "127.0.0.1";
      std::cerr << "[StorageD] WARNING: advertise_address not set; falling back to 127.0.0.1. "
                << "Use --advertise_address or CEDAR_ADVERTISE_ADDRESS for cluster visibility." << std::endl;
    }
  }

  JSON_LOG(INFO).KV("service", "storaged")
                  .KV("node_id", config.node_id)
                  .KV("port", config.port)
                  .KV("bind", config.bind_address)
                  .KV("advertise_address", config.advertise_address)
                  .KV("data_dir", config.data_dir)
                  .KV("meta_server", config.meta_server);
  std::cout << std::endl;

  MetaClient meta_client(config.meta_server, config.node_id, config.port,
                         config.advertise_address, config.data_dir, config.tls);
  if (!meta_client.Register()) {
    const char* env_offline_mode = std::getenv("CEDAR_STORAGED_OFFLINE_MODE");
    bool offline_mode = (env_offline_mode != nullptr && std::string(env_offline_mode) == "1");
    if (!offline_mode) {
      std::cerr << "[StorageD] FATAL: Failed to register with MetaD. "
                << "Set CEDAR_STORAGED_OFFLINE_MODE=1 to run without MetaD." << std::endl;
      delete storage;
      return 1;
    }
    std::cerr << "[StorageD] Running in offline mode (no MetaD)" << std::endl;
  }

  // 4. 启动心跳线程
  std::thread heartbeat_thread(HeartbeatLoop, &meta_client, config.heartbeat_interval_sec);

  // 5. 创建 gRPC 服务 (heap allocation to avoid lifetime issues)
  auto service_impl = std::make_unique<StorageServiceImpl>(storage, &raft_manager, config.data_dir);
  
  // 6. 启动 gRPC 服务器
  std::string server_address = config.bind_address + ":" + std::to_string(config.port);
  grpc::ServerBuilder builder;
  auto creds_result = CreateServerCredentialsWithEnvFallback(config.tls);
  if (!creds_result.ok()) {
    std::cerr << "[StorageD] FATAL: Failed to create server credentials: "
              << creds_result.status().ToString() << std::endl;
    return 1;
  }
  builder.AddListeningPort(server_address, creds_result.ValueOrDie());
  builder.RegisterService(service_impl.get());
  
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
  
  // Capacity monitoring component
  health_checker.RegisterComponent("disk_capacity", [&storage]() {
    if (!storage) return cedar::governance::HealthStatus::kUnknown;
    auto* engine = storage->GetLsmEngine();
    if (!engine) return cedar::governance::HealthStatus::kUnknown;
    
    auto capacity = engine->GetCapacityInfo();
    if (capacity.is_critical) {
      std::cerr << "[StorageD] CRITICAL: Disk usage at " << capacity.usage_percent << "%" << std::endl;
      return cedar::governance::HealthStatus::kUnhealthy;
    }
    if (capacity.is_warning) {
      std::cerr << "[StorageD] WARNING: Disk usage at " << capacity.usage_percent << "%" << std::endl;
      return cedar::governance::HealthStatus::kDegraded;
    }
    return cedar::governance::HealthStatus::kHealthy;
  });
  
  auto health_status = health_checker.StartHttpEndpoint("0.0.0.0", config.health_port);
  if (health_status.ok()) {
    std::cout << "[StorageD] Health endpoint on http://0.0.0.0:"
              << config.health_port << "/health" << std::endl;
  } else {
    std::cerr << "[StorageD] Health endpoint disabled: " << health_status.ToString() << std::endl;
  }

  cedar::dtx::storage::MetricsCollector metrics_collector;
  cedar::dtx::storage::MetricsCollector::Config metrics_config;
  metrics_config.endpoint = ":" + std::to_string(config.metrics_port);
  metrics_config.enable_http_server = true;
  auto metrics_status = metrics_collector.Initialize(metrics_config);
  if (metrics_status.ok()) {
    std::cout << "[StorageD] Metrics endpoint on http://0.0.0.0:"
              << config.metrics_port << "/metrics" << std::endl;
  } else {
    std::cerr << "[StorageD] Metrics endpoint disabled: " << metrics_status.ToString() << std::endl;
  }

  // 8. 启动配置热加载
  cedar::governance::ConfigManager config_mgr;
  std::string config_file = config.data_dir + "/storaged.yaml";
  if (std::filesystem::exists(config_file)) {
    auto load_status = config_mgr.LoadFromFile(config_file);
    if (load_status.ok()) {
      auto reload_status = config_mgr.EnableHotReload(config_file, 5000);
      if (reload_status.ok()) {
        std::cout << "[StorageD] Config hot reload enabled: " << config_file << std::endl;
      }
    }
  }

  // 9. 初始化告警管理器
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
    rule.description = "Storage free disk space is low";
    rule.severity = cedar::dtx::monitoring::AlertSeverity::kCritical;
    rule.condition_metric = "cedar_disk_free_bytes";
    rule.threshold = 10737418240.0;  // 10 GiB.
    rule.comparison = "<";
    rule.duration = std::chrono::seconds(30);
    alert_mgr->AddRule(rule);
  }
  std::cout << "[StorageD] AlertManager initialized with default rules" << std::endl;

  // 9. 等待关闭（在主线程中轮询信号）
  std::thread shutdown_monitor([]() {
    while (!g_shutdown_requested && g_running) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    if (g_shutdown_requested) {
      std::cout << "\n[StorageD] Shutdown requested, stopping..." << std::endl;
      g_running = false;
      if (g_grpc_server) {
        g_grpc_server->Shutdown();
      }
    }
  });
  g_grpc_server->Wait();
  shutdown_monitor.join();

  // 清理
  std::cout << "[StorageD] Shutting down..." << std::endl;
  g_running = false;
  heartbeat_thread.join();
  health_checker.StopHttpEndpoint();
  metrics_collector.Shutdown();
  alert_mgr->Shutdown();
  service_impl.reset();  // Destroy service before storage
  delete storage;
  std::cout << "[StorageD] Stopped." << std::endl;

  return 0;
}
