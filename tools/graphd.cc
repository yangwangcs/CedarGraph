// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

// =============================================================================
// CedarGraph GraphD - Graph Query Service (Full Router Version)
// =============================================================================
// Standalone query service with Cypher routing to StorageD

#include <iostream>
#include <string>
#include <algorithm>
#include <cctype>
#include <csignal>
#include <atomic>
#include <thread>
#include <chrono>
#include <cstdlib>
#include <grpcpp/grpcpp.h>

#include "cedar/service/graph_service_router.h"
#include "cedar/service/graphd_registrar.h"
#include "cedar/governance/health_checker.h"
#include "cedar/governance/config_manager.h"
#include "cedar/dtx/storage/metrics_collector.h"
#include "cedar/dtx/monitoring.h"
#include "cedar/dtx/raft/grpc_tls.h"
#include "cedar/common/json_logger.h"
#include "cedar/queryd/distributed_executor.h"
#include "cedar/queryd/query_storage_client.h"
#include "cedar/queryd/meta_client.h"
#include "gcn_service.grpc.pb.h"

std::atomic<bool> g_running{true};
std::atomic<bool> g_shutdown_requested{false};
std::unique_ptr<grpc::Server> g_grpc_server;

void SignalHandler(int sig) {
  (void)sig;
  g_shutdown_requested.store(true);
}

void PrintBanner() {
  std::cout << "CedarGraph GraphD v2.0 starting..." << std::endl;
}

bool IsPartitionMapBootstrapNotFound(const cedar::Status& status) {
  if (status.IsNotFound()) {
    return true;
  }

  const std::string message = status.ToString();
  return message.find("NotFound: Space partition map not found") != std::string::npos ||
         message.find("Space partition map not found") != std::string::npos;
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

std::string Trim(std::string value) {
  const auto begin = std::find_if_not(value.begin(), value.end(), [](unsigned char ch) {
    return std::isspace(ch);
  });
  const auto end = std::find_if_not(value.rbegin(), value.rend(), [](unsigned char ch) {
    return std::isspace(ch);
  }).base();
  if (begin >= end) {
    return "";
  }
  return std::string(begin, end);
}

std::string SelectPrimaryMetaServer(const std::string& meta_servers) {
  const auto comma = meta_servers.find(',');
  const std::string first = comma == std::string::npos
      ? meta_servers
      : meta_servers.substr(0, comma);
  return Trim(first);
}

void ApplySecurityEnvOverrides(cedar::dtx::security::SecurityManager::Config* config) {
  const std::string jwt_secret = GetEnvOrEmpty("CEDAR_GRAPHD_AUTH_JWT_SECRET");
  if (!jwt_secret.empty()) {
    config->auth.jwt_secret = jwt_secret;
  }

  const std::string username = GetEnvOrEmpty("CEDAR_GRAPHD_AUTH_USER");
  const std::string password = GetEnvOrEmpty("CEDAR_GRAPHD_AUTH_PASSWORD");
  if (!username.empty() || !password.empty()) {
    if (username.empty() || password.empty()) {
      std::cerr << "[GraphD] Ignoring incomplete auth account environment; "
                << "set both CEDAR_GRAPHD_AUTH_USER and CEDAR_GRAPHD_AUTH_PASSWORD"
                << std::endl;
      return;
    }

    cedar::dtx::security::Authenticator::Account account;
    account.username = username;
    account.password = password;
    const std::string role = GetEnvOrEmpty("CEDAR_GRAPHD_AUTH_ROLE");
    account.roles.push_back(role.empty() ? "admin" : role);
    config->auth.accounts.push_back(account);
  }
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

struct Config {
  int port = 9669;
  std::string bind_address = "0.0.0.0";
  std::string meta_server = "127.0.0.1:10559";
  std::string gcn_server = "127.0.0.1:9780";
  int health_port = 9668;
  int metrics_port = 9667;
  cedar::dtx::raft::TlsConfig tls;
  bool test_mode = false;  // Test mode: disable auth
};

static void LoadConfigFromFile(Config* config, const std::string& path) {
  cedar::governance::ConfigManager cm;
  if (!cm.LoadFromFile(path).ok()) return;
  if (cm.HasKey("graphd.port")) config->port = cm.GetInt("graphd.port", config->port);
  if (cm.HasKey("graphd.bind_address")) config->bind_address = cm.GetString("graphd.bind_address", config->bind_address);
  if (cm.HasKey("graphd.meta_server")) config->meta_server = cm.GetString("graphd.meta_server", config->meta_server);
  if (cm.HasKey("graphd.gcn_server")) config->gcn_server = cm.GetString("graphd.gcn_server", config->gcn_server);
  if (cm.HasKey("graphd.health_port")) config->health_port = cm.GetInt("graphd.health_port", config->health_port);
  if (cm.HasKey("graphd.metrics_port")) config->metrics_port = cm.GetInt("graphd.metrics_port", config->metrics_port);
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
    if ((arg == "--port" || arg == "-p") && i + 1 < argc) {
      config.port = std::stoi(argv[++i]);
    } else if ((arg == "--bind" || arg == "-b") && i + 1 < argc) {
      config.bind_address = argv[++i];
    } else if ((arg == "--meta" || arg == "-m") && i + 1 < argc) {
      config.meta_server = argv[++i];
    } else if ((arg == "--gcn" || arg == "-g") && i + 1 < argc) {
      config.gcn_server = argv[++i];
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
      config.test_mode = true;     // Disable auth in test mode
    } else if (arg == "--help" || arg == "-h") {
      std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
      std::cout << "Options:" << std::endl;
      std::cout << "  -p, --port <port>      Port to listen on (default: 9669)" << std::endl;
      std::cout << "  -b, --bind <addr>      Bind address (default: 0.0.0.0)" << std::endl;
      std::cout << "  -m, --meta <addr>      MetaD gRPC server address (default: 127.0.0.1:10559)" << std::endl;
      std::cout << "  -g, --gcn <addr>       GCN server address (default: 127.0.0.1:9780)" << std::endl;
      std::cout << "  --health_port <port>   Health HTTP port (default: 9668)" << std::endl;
      std::cout << "  --metrics_port <port>  Metrics HTTP port (default: 9667)" << std::endl;
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

int main(int argc, char* argv[]) {
  PrintBanner();
  
  Config config = ParseArgs(argc, argv);

  // Environment variable overrides for containerized / cloud deployments
  const char* env_meta = std::getenv("CEDAR_METAD_ENDPOINT");
  if (env_meta && env_meta[0] != '\0') {
    config.meta_server = env_meta;
    std::cout << "[GraphD] MetaD address overridden by CEDAR_METAD_ENDPOINT: " << config.meta_server << std::endl;
  }
  const char* env_gcn = std::getenv("CEDAR_GCN_ENDPOINT");
  if (env_gcn && env_gcn[0] != '\0') {
    config.gcn_server = env_gcn;
    std::cout << "[GraphD] GCN address overridden by CEDAR_GCN_ENDPOINT: " << config.gcn_server << std::endl;
  }
  ApplyTlsEnvOverrides(&config.tls);

  const std::string raw_meta_server = config.meta_server;
  config.meta_server = SelectPrimaryMetaServer(config.meta_server);
  if (config.meta_server.empty()) {
    std::cerr << "[GraphD] FATAL: MetaD address is empty" << std::endl;
    return 1;
  }
  if (config.meta_server != raw_meta_server) {
    std::cout << "[GraphD] Using primary MetaD address: " << config.meta_server
              << " (from " << raw_meta_server << ")" << std::endl;
  }
  
  JSON_LOG(INFO).KV("service", "graphd")
                  .KV("port", config.port)
                  .KV("bind", config.bind_address)
                  .KV("meta_server", config.meta_server)
                  .KV("gcn_server", config.gcn_server);

  signal(SIGINT, SignalHandler);
  signal(SIGTERM, SignalHandler);

  // Initialize QueryD components for merged execution
  cedar::queryd::QueryStorageClient::Options storage_options;
  auto query_storage_client = std::make_unique<cedar::queryd::QueryStorageClient>(storage_options);

  cedar::queryd::QueryMetaClient::Options meta_options;
  meta_options.meta_service_address = config.meta_server;
  auto query_meta_client = std::make_unique<cedar::queryd::QueryMetaClient>(meta_options);
  auto meta_init_status = query_meta_client->Init();
  if (!meta_init_status.ok()) {
    if (IsPartitionMapBootstrapNotFound(meta_init_status)) {
      std::cerr << "[GraphD] QueryMetaClient init deferred until default space bootstrap: "
                << meta_init_status.ToString() << std::endl;
    } else {
      std::cerr << "[GraphD] QueryMetaClient init failed: "
                << meta_init_status.ToString() << std::endl;
    }
  }

  auto distributed_executor = std::make_unique<cedar::queryd::DistributedExecutor>(
      query_storage_client.get(), query_meta_client.get());
  std::cout << "[GraphD] DistributedExecutor initialized (merged from QueryD)" << std::endl;

  // Create router service
  auto router = std::make_unique<cedar::service::GraphServiceRouter>();
  router->SetDistributedExecutor(distributed_executor.get());
  router->SetTlsConfig(config.tls);

  // Initialize SecurityManager (disable auth in test mode)
  {
    cedar::dtx::security::SecurityManager::Config sm_config;
    sm_config.enable_encryption = config.tls.enabled;
    sm_config.enable_auth = !config.test_mode;
    sm_config.enable_audit = false;
    ApplySecurityEnvOverrides(&sm_config);
    auto* sm = cedar::dtx::security::SecurityManager::GetInstance();
    auto sm_status = sm->Initialize(sm_config);
    if (!sm_status.ok()) {
      std::cerr << "[GraphD] SecurityManager init failed: " << sm_status.ToString() << std::endl;
      if (!config.test_mode) {
        return 1;
      }
    } else {
      std::cout << "[GraphD] SecurityManager initialized (auth=" 
                << (config.test_mode ? "disabled" : "enabled") << ")" << std::endl;
    }
  }

  // Initialize router
  auto status = router->Initialize(config.meta_server, config.gcn_server);
  if (!status.ok()) {
    std::cerr << "[GraphD] Failed to initialize router: " << status.ToString() << std::endl;
    return 1;
  }
  std::cout << "[GraphD] Router initialized" << std::endl;
  
  // Start background tasks
  status = router->Start();
  if (!status.ok()) {
    std::cerr << "[GraphD] Failed to start router: " << status.ToString() << std::endl;
    return 1;
  }
  std::cout << "[GraphD] Background tasks started" << std::endl;
  
  // Build and start gRPC server
  std::string server_address = config.bind_address + ":" + std::to_string(config.port);
  grpc::ServerBuilder builder;
  auto creds_result = CreateServerCredentialsWithEnvFallback(config.tls);
  if (!creds_result.ok()) {
    std::cerr << "[GraphD] FATAL: Failed to create server credentials: "
              << creds_result.status().ToString() << std::endl;
    return 1;
  }
  builder.AddListeningPort(server_address, creds_result.ValueOrDie());
  builder.RegisterService(static_cast<cedar::query::QueryService::Service*>(router.get()));
  builder.RegisterService(static_cast<cedargrpc::CedarGraphService::Service*>(router.get()));
  
  g_grpc_server = builder.BuildAndStart();
  if (!g_grpc_server) {
    std::cerr << "[GraphD] Failed to start gRPC server" << std::endl;
    return 1;
  }
  
  std::cout << "[GraphD] gRPC server listening on " << server_address << std::endl;
  
  // Register with MetaD for load balancing
  cedar::service::GraphDRegistrar::Config registrar_config;
  registrar_config.meta_address = config.meta_server;
  registrar_config.graphd_address = config.bind_address;
  registrar_config.graphd_port = config.port;
  registrar_config.heartbeat_interval_seconds = 10;
  registrar_config.max_qps = 10000;
  
  cedar::service::GraphDRegistrar registrar(registrar_config);
  if (registrar.Start()) {
    std::cout << "[GraphD] Registered with MetaD as " << registrar.GetNodeId() << std::endl;
  } else {
    std::cerr << "[GraphD] Warning: Failed to register with MetaD (load balancing disabled)" << std::endl;
  }
  
  std::cout << "[GraphD] Ready for queries. Press Ctrl+C to stop." << std::endl;
  std::cout << std::endl;

  // Start health check and metrics HTTP endpoints
  cedar::governance::HealthChecker health_checker;
  health_checker.RegisterComponent("graphd", [&router]() {
    return router ? cedar::governance::HealthStatus::kHealthy 
                  : cedar::governance::HealthStatus::kUnhealthy;
  });
  auto health_status = health_checker.StartHttpEndpoint("0.0.0.0", config.health_port);
  if (health_status.ok()) {
    std::cout << "[GraphD] Health endpoint on http://0.0.0.0:"
              << config.health_port << "/health" << std::endl;
  } else {
    std::cerr << "[GraphD] Health endpoint disabled: " << health_status.ToString() << std::endl;
  }

  cedar::dtx::storage::MetricsCollector metrics_collector;
  cedar::dtx::storage::MetricsCollector::Config metrics_config;
  metrics_config.endpoint = ":" + std::to_string(config.metrics_port);
  metrics_config.enable_http_server = true;
  auto metrics_status = metrics_collector.Initialize(metrics_config);
  if (metrics_status.ok()) {
    std::cout << "[GraphD] Metrics endpoint on http://0.0.0.0:"
              << config.metrics_port << "/metrics" << std::endl;
  } else {
    std::cerr << "[GraphD] Metrics endpoint disabled: " << metrics_status.ToString() << std::endl;
  }

  auto* alert_mgr = cedar::dtx::monitoring::AlertManager::GetInstance();
  cedar::dtx::monitoring::AlertManager::Config alert_config;
  alert_mgr->Initialize(alert_config);
  {
    cedar::dtx::monitoring::AlertRule rule;
    rule.name = "GraphDHighMemory";
    rule.description = "GraphD process memory is too high";
    rule.severity = cedar::dtx::monitoring::AlertSeverity::kWarning;
    rule.condition_metric = "cedar_process_memory_bytes";
    rule.threshold = 8589934592.0;  // 8 GiB.
    rule.comparison = ">";
    rule.duration = std::chrono::seconds(60);
    alert_mgr->AddRule(rule);
  }
  std::cout << "[GraphD] AlertManager initialized with default rules" << std::endl;

  std::thread shutdown_monitor([]() {
    while (!g_shutdown_requested.load() && g_running.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    if (g_shutdown_requested.exchange(false)) {
      std::cout << "\n[GraphD] Shutdown requested, stopping..." << std::endl;
      g_running.store(false);
      if (g_grpc_server) {
        g_grpc_server->Shutdown();
      }
    }
  });

  // Wait for shutdown
  g_grpc_server->Wait();
  g_running.store(false);
  if (shutdown_monitor.joinable()) {
    shutdown_monitor.join();
  }

  // Cleanup
  std::cout << "[GraphD] Shutting down..." << std::endl;
  registrar.Stop();
  health_checker.StopHttpEndpoint();
  metrics_collector.Shutdown();
  alert_mgr->Shutdown();
  router->Stop();
  std::cout << "[GraphD] Stopped." << std::endl;

  return 0;
}
