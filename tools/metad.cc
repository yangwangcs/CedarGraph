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

#include "cedar/dtx/meta_service.h"
#include "cedar/dtx/meta_service_grpc.h"
#include "cedar/dtx/raft/grpc_tls.h"

namespace cedar {
namespace dtx {

std::atomic<bool> g_running{true};
std::unique_ptr<MetaServiceGrpcServer> g_grpc_server;
std::unique_ptr<MetadataService> g_meta_service;

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
  std::string data_dir = "./meta_data";
  std::vector<std::pair<uint32_t, std::string>> peers;
  
  // Raft settings
  uint64_t election_timeout_ms = 1000;
  uint64_t heartbeat_interval_ms = 100;
  
  // Heartbeat check
  uint64_t heartbeat_timeout_sec = 10;
  uint64_t heartbeat_check_interval_sec = 5;
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
  
  std::cout << "[MetaD] CedarGraph Metadata Server starting..." << std::endl;
  std::cout << "[MetaD] Node ID: " << config.node_id << std::endl;
  std::cout << "[MetaD] Listen: " << config.listen_address << std::endl;
  std::cout << "[MetaD] Data dir: " << config.data_dir << std::endl;
  std::cout << "[MetaD] Peers: " << config.peers.size() << std::endl;
  
  // Setup signal handlers
  std::signal(SIGINT, SignalHandler);
  std::signal(SIGTERM, SignalHandler);
  
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
  
  auto init_status = g_meta_service->Initialize(meta_config);
  if (!init_status.ok()) {
    std::cerr << "[MetaD] Failed to initialize metadata service: " 
              << init_status.ToString() << std::endl;
    return 1;
  }
  std::cout << "[MetaD] Metadata service initialized with braft consensus" << std::endl;
  
  // Start gRPC server for client API
  g_grpc_server = std::make_unique<MetaServiceGrpcServer>();
  auto grpc_status = g_grpc_server->Start(config.listen_address, g_meta_service.get());
  if (!grpc_status.ok()) {
    std::cerr << "[MetaD] Failed to start gRPC server: " 
              << grpc_status.ToString() << std::endl;
    g_meta_service->Shutdown();
    return 1;
  }
  std::cout << "[MetaD] gRPC server listening on " << config.listen_address << std::endl;
  
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
