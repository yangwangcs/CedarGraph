// tools/cedar_storage_node.cc
// CedarGraph Storage Node - Independent Process for Cluster Deployment

#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <signal.h>

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/dtx/storage_service_impl.h"
#include "cedar/core/env.h"

using namespace cedar;

std::atomic<bool> g_running{true};

void SignalHandler(int sig) {
  std::cout << "Received signal " << sig << ", shutting down..." << std::endl;
  g_running = false;
}

int main(int argc, char* argv[]) {
  if (argc < 4) {
    std::cerr << "Usage: " << argv[0] 
              << " <node_id> <listen_port> <data_dir> [peer1:port] [peer2:port]" << std::endl;
    return 1;
  }

  std::string node_id = argv[1];
  int port = std::stoi(argv[2]);
  std::string data_dir = argv[3];
  
  std::cout << "Starting CedarGraph Storage Node" << std::endl;
  std::cout << "  Node ID: " << node_id << std::endl;
  std::cout << "  Port: " << port << std::endl;
  std::cout << "  Data Dir: " << data_dir << std::endl;

  signal(SIGINT, SignalHandler);
  signal(SIGTERM, SignalHandler);

  // 1. Open storage engine
  CedarOptions options;
  options.create_if_missing = true;
  options.distributed_mode = true;
  
  CedarGraphStorage* storage = nullptr;
  Status s = CedarGraphStorage::Open(options, data_dir, &storage);
  if (!s.ok()) {
    std::cerr << "Failed to open storage: " << s.ToString() << std::endl;
    return 1;
  }
  std::cout << "Storage engine opened successfully" << std::endl;

  // Note: braft partition replication is fully implemented in cedar-storaged.
  // This tool is a minimal standalone example. For production use, run:
  //   ./cedar-storaged --node_id <id> --port <port> --data_dir <dir> --meta <metad_addr>

  std::cout << "Storage node " << node_id << " is running on port " << port << std::endl;
  std::cout << "Press Ctrl+C to stop" << std::endl;

  // Main loop
  while (g_running) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  std::cout << "Shutting down node " << node_id << "..." << std::endl;
  delete storage;
  std::cout << "Node stopped." << std::endl;
  
  return 0;
}
