// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

// =============================================================================
// CedarGraph GraphD - Graph Query Service (Full Router Version)
// =============================================================================
// Standalone query service with Cypher routing to StorageD

#include <iostream>
#include <string>
#include <csignal>
#include <atomic>
#include <thread>
#include <grpcpp/grpcpp.h>

#include "cedar/service/graph_service_router.h"
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
std::unique_ptr<grpc::Server> g_grpc_server;

void SignalHandler(int sig) {
  std::cout << "\n[GraphD] Received signal " << sig << ", shutting down..." << std::endl;
  g_running = false;
  if (g_grpc_server) {
    g_grpc_server->Shutdown();
  }
}

void PrintBanner() {
  std::cout << "CedarGraph GraphD v2.0 starting..." << std::endl;
}

struct Config {
  int port = 9669;
  std::string bind_address = "0.0.0.0";
  std::string meta_server = "127.0.0.1:9559";
  std::string gcn_server = "127.0.0.1:9780";
  cedar::dtx::raft::TlsConfig tls;
};

static void LoadConfigFromFile(Config* config, const std::string& path) {
  cedar::governance::ConfigManager cm;
  if (!cm.LoadFromFile(path).ok()) return;
  if (cm.HasKey("graphd.port")) config->port = cm.GetInt("graphd.port", config->port);
  if (cm.HasKey("graphd.bind_address")) config->bind_address = cm.GetString("graphd.bind_address", config->bind_address);
  if (cm.HasKey("graphd.meta_server")) config->meta_server = cm.GetString("graphd.meta_server", config->meta_server);
  if (cm.HasKey("graphd.gcn_server")) config->gcn_server = cm.GetString("graphd.gcn_server", config->gcn_server);
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
      std::cout << "  -p, --port <port>      Port to listen on (default: 9669)" << std::endl;
      std::cout << "  -b, --bind <addr>      Bind address (default: 0.0.0.0)" << std::endl;
      std::cout << "  -m, --meta <addr>      MetaD server address (default: 127.0.0.1:9559)" << std::endl;
      std::cout << "  -g, --gcn <addr>       GCN server address (default: 127.0.0.1:9780)" << std::endl;
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
    std::cerr << "[GraphD] QueryMetaClient init failed: " << meta_init_status.ToString() << std::endl;
  }

  auto distributed_executor = std::make_unique<cedar::queryd::DistributedExecutor>(
      query_storage_client.get(), query_meta_client.get());
  std::cout << "[GraphD] DistributedExecutor initialized (merged from QueryD)" << std::endl;

  // Create router service
  auto router = std::make_unique<cedar::service::GraphServiceRouter>();
  router->SetDistributedExecutor(distributed_executor.get());
  router->SetTlsConfig(config.tls);

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
  auto creds_result = cedar::dtx::raft::TlsCredentialFactory::CreateServerCredentials(config.tls);
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
  std::cout << "[GraphD] Ready for queries. Press Ctrl+C to stop." << std::endl;
  std::cout << std::endl;

  // Start health check and metrics HTTP endpoints
  cedar::governance::HealthChecker health_checker;
  health_checker.RegisterComponent("graphd", [&router]() {
    return router ? cedar::governance::HealthStatus::kHealthy 
                  : cedar::governance::HealthStatus::kUnhealthy;
  });
  auto health_status = health_checker.StartHttpEndpoint("0.0.0.0", 9668);
  if (health_status.ok()) {
    std::cout << "[GraphD] Health endpoint on http://0.0.0.0:9668/health" << std::endl;
  }

  cedar::dtx::storage::MetricsCollector metrics_collector;
  cedar::dtx::storage::MetricsCollector::Config metrics_config;
  metrics_config.endpoint = ":9667";
  metrics_config.enable_http_server = true;
  auto metrics_status = metrics_collector.Initialize(metrics_config);
  if (metrics_status.ok()) {
    std::cout << "[GraphD] Metrics endpoint on http://0.0.0.0:9667/metrics" << std::endl;
  }

  auto* alert_mgr = cedar::dtx::monitoring::AlertManager::GetInstance();
  cedar::dtx::monitoring::AlertManager::Config alert_config;
  alert_mgr->Initialize(alert_config);
  {
    cedar::dtx::monitoring::AlertRule rule;
    rule.name = "GraphDHighLatency";
    rule.description = "GraphD query latency is too high";
    rule.severity = cedar::dtx::monitoring::AlertSeverity::kWarning;
    rule.condition_metric = "cedar_graphd_query_latency_seconds";
    rule.threshold = 2.0;
    rule.comparison = ">";
    rule.duration = std::chrono::seconds(60);
    alert_mgr->AddRule(rule);
  }
  std::cout << "[GraphD] AlertManager initialized with default rules" << std::endl;

  // Wait for shutdown
  g_grpc_server->Wait();

  // Cleanup
  std::cout << "[GraphD] Shutting down..." << std::endl;
  health_checker.StopHttpEndpoint();
  metrics_collector.Shutdown();
  alert_mgr->Shutdown();
  router->Stop();
  std::cout << "[GraphD] Stopped." << std::endl;

  return 0;
}
