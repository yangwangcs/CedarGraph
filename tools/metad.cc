// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");

// =============================================================================
// CedarGraph MetaD - Metadata Service
// =============================================================================
// Production metadata service using braft for consensus and gRPC for client
// communication.
//
// Dependencies:
//   - braft (third_party/braft) for Raft consensus
//   - brpc (third_party/brpc) for Raft internal communication (via braft)
//   - gRPC for MetaService client API
// =============================================================================

#include <csignal>
#include <iostream>
#include <string>
#include <thread>
#include <atomic>

#ifdef __APPLE__
#include <execinfo.h>
#include <cxxabi.h>
#endif

#include "cedar/dtx/meta_service.h"
#include "cedar/dtx/meta_service_grpc.h"
#include "cedar/dtx/raft/grpc_tls.h"

namespace cedar {
namespace dtx {

std::atomic<bool> g_running{true};
std::unique_ptr<MetaServiceGrpcServer> g_grpc_server;
std::unique_ptr<MetadataService> g_meta_service;

void CrashHandler(int sig) {
    void* callstack[64];
    int frames = backtrace(callstack, 64);
    std::cerr << "\n[FATAL] Signal " << sig << " (SIGSEGV) received. Stack trace:\n";
    char** symbols = backtrace_symbols(callstack, frames);
    for (int i = 0; i < frames; i++) {
        std::cerr << "  " << symbols[i] << "\n";
    }
    free(symbols);
    _exit(128 + sig);
}

void SignalHandler(int signal) {
  if (signal == SIGINT || signal == SIGTERM) {
    std::cout << "[MetaD] Received signal " << signal << ", shutting down..." << std::endl;
    g_running = false;
    if (g_grpc_server) {
      g_grpc_server->Stop();
    }
  }
}

struct MetadConfig {
  uint32_t node_id = 0;
  std::string listen_address = "0.0.0.0:9559";
  std::string advertise_address;
  std::string grpc_address;  // Separate gRPC port for client API
  std::string data_dir = "./meta_data";
  std::vector<std::pair<uint32_t, std::string>> peers;
  
  // Raft settings
  uint64_t election_timeout_ms = 1000;
  uint64_t heartbeat_interval_ms = 100;
  
  // Heartbeat check
  uint64_t heartbeat_timeout_sec = 10;
  uint64_t heartbeat_check_interval_sec = 5;
  
  // Test mode: skip braft initialization
  bool test_mode = false;
};

MetadConfig ParseArgs(int argc, char* argv[]) {
  MetadConfig config;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--node_id" && i + 1 < argc) {
      config.node_id = std::stoul(argv[++i]);
    } else if (arg == "--listen" && i + 1 < argc) {
      config.listen_address = argv[++i];
    } else if (arg == "--advertise" && i + 1 < argc) {
      config.advertise_address = argv[++i];
    } else if (arg == "--data_dir" && i + 1 < argc) {
      config.data_dir = argv[++i];
    } else if (arg == "--peer" && i + 1 < argc) {
      std::string peer = argv[++i];
      size_t colon = peer.find(':');
      if (colon != std::string::npos) {
        try {
          uint32_t id = std::stoul(peer.substr(0, colon));
          std::string addr = peer.substr(colon + 1);
          config.peers.push_back({id, addr});
        } catch (...) {
          std::cerr << "[MetaD] Invalid peer format: " << peer << std::endl;
        }
      }
    } else if (arg == "--election_timeout" && i + 1 < argc) {
      config.election_timeout_ms = std::stoul(argv[++i]);
    } else if (arg == "--heartbeat_interval" && i + 1 < argc) {
      config.heartbeat_interval_ms = std::stoul(argv[++i]);
    } else if (arg == "--grpc_port" && i + 1 < argc) {
      config.grpc_address = "0.0.0.0:" + std::string(argv[++i]);
    } else if (arg == "--test_mode") {
      config.test_mode = true;
    } else if (arg == "--help" || arg == "-h") {
      std::cout << "CedarGraph Metadata Server (MetaD)\n"
                << "Usage: " << argv[0] << " [options]\n"
                << "\n"
                << "Options:\n"
                << "  --node_id <id>              Node ID (default: 0)\n"
                << "  --listen <addr>             Listen address (default: 0.0.0.0:9559)\n"
                << "  --advertise <addr>          Advertise address for Raft\n"
                << "  --data_dir <path>           Data directory (default: ./meta_data)\n"
                << "  --peer <id:address>         Add a peer node (repeatable)\n"
                << "  --election_timeout <ms>     Raft election timeout (default: 1000)\n"
                << "  --heartbeat_interval <ms>   Raft heartbeat interval (default: 100)\n"
                << "  --grpc_port <port>          gRPC client API port (default: listen_port + 1000)\n"
                << "  -h, --help                  Show this help\n";
      exit(0);
    }
  }
  return config;
}

}  // namespace dtx
}  // namespace cedar

int main(int argc, char* argv[]) {
  using namespace cedar::dtx;
  
  MetadConfig config = ParseArgs(argc, argv);
  
  // Compute default gRPC address: 0.0.0.0 on port + 1000
  if (config.grpc_address.empty()) {
    auto colon = config.listen_address.rfind(':');
    if (colon != std::string::npos) {
      int raft_port = std::stoi(config.listen_address.substr(colon + 1));
      config.grpc_address = "0.0.0.0:" + std::to_string(raft_port + 1000);
    } else {
      config.grpc_address = "0.0.0.0:10559";
    }
  }
  
  std::cout << "[MetaD] CedarGraph Metadata Server starting..." << std::endl;
  std::cout << "[MetaD] Node ID: " << config.node_id << std::endl;
  std::cout << "[MetaD] Raft Listen: " << config.listen_address << std::endl;
  std::cout << "[MetaD] gRPC Listen: " << config.grpc_address << std::endl;
  std::cout << "[MetaD] Data dir: " << config.data_dir << std::endl;
  std::cout << "[MetaD] Peers: " << config.peers.size() << std::endl;
  std::cout << "[MetaD] Test mode: " << (config.test_mode ? "yes" : "no") << std::endl;
  
  // Setup signal handlers
  std::signal(SIGINT, SignalHandler);
  std::signal(SIGTERM, SignalHandler);
  std::signal(SIGSEGV, CrashHandler);
  std::signal(SIGBUS, CrashHandler);
  
  // Create metadata service with braft consensus
  g_meta_service = std::make_unique<MetadataService>();
  
  MetaServiceConfig meta_config;
  meta_config.node_id = config.node_id;
  meta_config.listen_address = config.listen_address;
  meta_config.advertise_address = config.advertise_address;
  meta_config.data_dir = config.data_dir;
  meta_config.peers = config.peers;
  meta_config.election_timeout_ms = config.election_timeout_ms;
  meta_config.heartbeat_timeout_sec = config.heartbeat_timeout_sec;
  meta_config.heartbeat_check_interval_sec = config.heartbeat_check_interval_sec;
  meta_config.test_mode = config.test_mode;
  
  auto init_status = g_meta_service->Initialize(meta_config);
  if (!init_status.ok()) {
    std::cerr << "[MetaD] Failed to initialize metadata service: " 
              << init_status.ToString() << std::endl;
    return 1;
  }
  std::cout << "[MetaD] Metadata service initialized with braft consensus" << std::endl;
  
  // Start gRPC server for client API (on separate port from braft)
  g_grpc_server = std::make_unique<MetaServiceGrpcServer>();
  auto grpc_status = g_grpc_server->Start(config.grpc_address, g_meta_service.get());
  if (!grpc_status.ok()) {
    std::cerr << "[MetaD] Failed to start gRPC server: " 
              << grpc_status.ToString() << std::endl;
    g_meta_service->Shutdown();
    return 1;
  }
  std::cout << "[MetaD] gRPC server listening on " << config.grpc_address << std::endl;
  
  // Wait for shutdown signal
  std::cout << "[MetaD] Server running. Press Ctrl+C to stop." << std::endl;
  while (g_running) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  
  // Graceful shutdown
  std::cout << "[MetaD] Shutting down..." << std::endl;
  g_grpc_server->Stop();
  g_meta_service->Shutdown();
  std::cout << "[MetaD] Shutdown complete." << std::endl;
  
  return 0;
}
