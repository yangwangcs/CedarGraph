// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");

// =============================================================================
// CedarGraph MetaD - Metadata Service
// =============================================================================
// Standalone metadata service for CedarGraph cluster
// Provides partition topology, node registration, and cluster state management

#include <iostream>
#include <string>
#include <csignal>
#include <atomic>
#include <chrono>
#include <thread>
#include <grpcpp/grpcpp.h>

#include "src/service/meta_service_handler.h"

std::atomic<bool> g_running{true};
std::unique_ptr<grpc::Server> g_grpc_server;

void SignalHandler(int sig) {
  std::cout << "\n[MetaD] Received signal " << sig << ", shutting down..." << std::endl;
  g_running = false;
  if (g_grpc_server) {
    g_grpc_server->Shutdown();
  }
}

void PrintBanner() {
  std::cout << R"(
   __  __       _        _                 _ 
  |  \/  | __ _| |_ _ __| |__   __ _ _ __ | |_
  | |\/| |/ _` | __| '__| '_ \ / _` | '_ \| __|
  | |  | | (_| | |_| |  | |_) | (_| | | | | |_ 
  |_|  |_|\__,_|\__|_|  |_.__/ \__,_|_| |_|\__|
              Meta Data Service v1.0
)" << std::endl;
}

struct Config {
  int port = 9559;
  std::string data_dir = "/tmp/cedar/metad";
  std::string bind_address = "0.0.0.0";
};

Config ParseArgs(int argc, char* argv[]) {
  Config config;
  
  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];
    if ((arg == "--port" || arg == "-p") && i + 1 < argc) {
      config.port = std::stoi(argv[++i]);
    } else if ((arg == "--data_dir" || arg == "-d") && i + 1 < argc) {
      config.data_dir = argv[++i];
    } else if ((arg == "--bind" || arg == "-b") && i + 1 < argc) {
      config.bind_address = argv[++i];
    } else if (arg == "--help" || arg == "-h") {
      std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
      std::cout << "Options:" << std::endl;
      std::cout << "  -p, --port <port>      Port to listen on (default: 9559)" << std::endl;
      std::cout << "  -d, --data_dir <dir>   Data directory (default: /tmp/cedar/metad)" << std::endl;
      std::cout << "  -b, --bind <addr>      Bind address (default: 0.0.0.0)" << std::endl;
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
  std::cout << "  Data Dir:  " << config.data_dir << std::endl;
  std::cout << std::endl;
  
  // Setup signal handlers
  signal(SIGINT, SignalHandler);
  signal(SIGTERM, SignalHandler);
  
  // Create service handler
  auto service_handler = std::make_unique<cedar::service::MetaServiceHandler>();
  
  // Initialize
  auto status = service_handler->Initialize(config.data_dir);
  if (!status.ok()) {
    std::cerr << "[MetaD] Failed to initialize: " << status.ToString() << std::endl;
    return 1;
  }
  std::cout << "[MetaD] Service handler initialized" << std::endl;
  
  // Start background tasks
  status = service_handler->Start();
  if (!status.ok()) {
    std::cerr << "[MetaD] Failed to start: " << status.ToString() << std::endl;
    return 1;
  }
  std::cout << "[MetaD] Background tasks started" << std::endl;
  
  // Build and start gRPC server
  std::string server_address = config.bind_address + ":" + std::to_string(config.port);
  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(service_handler.get());
  
  g_grpc_server = builder.BuildAndStart();
  if (!g_grpc_server) {
    std::cerr << "[MetaD] Failed to start gRPC server" << std::endl;
    return 1;
  }
  
  std::cout << "[MetaD] gRPC server listening on " << server_address << std::endl;
  std::cout << "[MetaD] Ready for connections. Press Ctrl+C to stop." << std::endl;
  std::cout << std::endl;
  
  // Wait for shutdown
  g_grpc_server->Wait();
  
  // Cleanup
  std::cout << "[MetaD] Shutting down..." << std::endl;
  service_handler->Stop();
  std::cout << "[MetaD] Stopped." << std::endl;
  
  return 0;
}
