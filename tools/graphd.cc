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

#include "src/service/graph_service_router.h"

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
  std::cout << R"(
   ____                 _     _              
  / ___|_ __ __ _ _ __ | |__ (_)_ __   __ _  
 | |  _| '__/ _` | '_ \| '_ \| | '_ \ / _` | 
 | |_| | | | (_| | |_) | | | | | | | | (_| | 
  \____|_|  \__,_| .__/|_| |_|_|_| |_|\__, | 
                 |_|                  |___/  
         Graph Query Router Service v2.0
)" << std::endl;
}

struct Config {
  int port = 9669;
  std::string bind_address = "0.0.0.0";
  std::string meta_server = "127.0.0.1:9559";
};

Config ParseArgs(int argc, char* argv[]) {
  Config config;
  
  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];
    if ((arg == "--port" || arg == "-p") && i + 1 < argc) {
      config.port = std::stoi(argv[++i]);
    } else if ((arg == "--bind" || arg == "-b") && i + 1 < argc) {
      config.bind_address = argv[++i];
    } else if ((arg == "--meta" || arg == "-m") && i + 1 < argc) {
      config.meta_server = argv[++i];
    } else if (arg == "--help" || arg == "-h") {
      std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
      std::cout << "Options:" << std::endl;
      std::cout << "  -p, --port <port>      Port to listen on (default: 9669)" << std::endl;
      std::cout << "  -b, --bind <addr>      Bind address (default: 0.0.0.0)" << std::endl;
      std::cout << "  -m, --meta <addr>      MetaD server address (default: 127.0.0.1:9559)" << std::endl;
      std::cout << "  -h, --help             Show this help" << std::endl;
      exit(0);
    }
  }
  
  return config;
}

int main(int argc, char* argv[]) {
  PrintBanner();
  
  Config config = ParseArgs(argc, argv);
  
  std::cout << "Configuration:" << std::endl;
  std::cout << "  Port:      " << config.port << std::endl;
  std::cout << "  Bind:      " << config.bind_address << std::endl;
  std::cout << "  MetaD:     " << config.meta_server << std::endl;
  std::cout << std::endl;

  signal(SIGINT, SignalHandler);
  signal(SIGTERM, SignalHandler);

  // Create router service
  auto router = std::make_unique<cedar::service::GraphServiceRouter>();
  
  // Initialize router
  auto status = router->Initialize(config.meta_server);
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
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(router.get());
  
  g_grpc_server = builder.BuildAndStart();
  if (!g_grpc_server) {
    std::cerr << "[GraphD] Failed to start gRPC server" << std::endl;
    return 1;
  }
  
  std::cout << "[GraphD] gRPC server listening on " << server_address << std::endl;
  std::cout << "[GraphD] Ready for queries. Press Ctrl+C to stop." << std::endl;
  std::cout << std::endl;

  // Wait for shutdown
  g_grpc_server->Wait();

  // Cleanup
  std::cout << "[GraphD] Shutting down..." << std::endl;
  router->Stop();
  std::cout << "[GraphD] Stopped." << std::endl;

  return 0;
}
