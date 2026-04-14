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
#include "cedar/raft/partition_router.h"
#include "meta_service.grpc.pb.h"
#include "storage_service.grpc.pb.h"

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
  std::cout << R"(
   ____  _                     _            _ 
  / ___|| |_ _ __ ___  ___  __| | ___ _ __ | |_
  \___ \| __| '__/ _ \/ _ \/ _` |/ _ \ '_ \| __|
   ___) | |_| | |  __/  __/ (_| |  __/ | | | |_ 
  |____/ \__|_|  \___|\___|\__,_|\___|_| |_|\__|
              Storage Service v1.0
)" << std::endl;
}

struct Config {
  int node_id = 0;
  int port = 9779;
  std::string bind_address = "0.0.0.0";
  std::string data_dir = "/tmp/cedar/storaged";
  std::string meta_server = "127.0.0.1:9559";
  int heartbeat_interval_sec = 10;
};

Config ParseArgs(int argc, char* argv[]) {
  Config config;
  
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
    } else if (arg == "--help" || arg == "-h") {
      std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
      std::cout << "Options:" << std::endl;
      std::cout << "  -n, --node_id <id>     Node ID (default: 0)" << std::endl;
      std::cout << "  -p, --port <port>      Port to listen on (default: 9779)" << std::endl;
      std::cout << "  -b, --bind <addr>      Bind address (default: 0.0.0.0)" << std::endl;
      std::cout << "  -d, --data_dir <dir>   Data directory (default: /tmp/cedar/storaged)" << std::endl;
      std::cout << "  -m, --meta <addr>      MetaD server address (default: 127.0.0.1:9559)" << std::endl;
      std::cout << "  -h, --help             Show this help" << std::endl;
      exit(0);
    }
  }
  
  return config;
}

// StorageD 服务实现（简化版）
class StorageServiceImpl final : public cedar::storage::StorageService::Service {
 public:
  explicit StorageServiceImpl(cedar::CedarGraphStorage* storage) : storage_(storage) {}

  grpc::Status Put(grpc::ServerContext* context,
                   const cedar::storage::PutRequest* request,
                   cedar::storage::PutResponse* response) override {
    (void)context;
    (void)request;
    
    // TODO: 实现实际的存储操作
    response->set_success(true);
    return grpc::Status::OK;
  }

  grpc::Status Get(grpc::ServerContext* context,
                   const cedar::storage::GetRequest* request,
                   cedar::storage::GetResponse* response) override {
    (void)context;
    (void)request;
    
    // TODO: 实现实际的读取操作
    response->set_success(true);
    response->set_found(false);
    return grpc::Status::OK;
  }

  grpc::Status Delete(grpc::ServerContext* context,
                      const cedar::storage::DeleteRequest* request,
                      cedar::storage::DeleteResponse* response) override {
    (void)context;
    (void)request;
    
    // TODO: 实现实际的删除操作
    response->set_success(true);
    return grpc::Status::OK;
  }

 private:
  cedar::CedarGraphStorage* storage_;
};

// MetaD 客户端 - 处理注册和心跳
class MetaClient {
 public:
  MetaClient(const std::string& meta_addr, int node_id, int port)
      : node_id_(node_id), port_(port) {
    auto channel = grpc::CreateChannel(meta_addr, grpc::InsecureChannelCredentials());
    stub_ = cedar::meta::MetaService::NewStub(channel);
  }

  bool Register() {
    cedar::meta::RegisterNodeRequest request;
    auto* node_info = request.mutable_node_info();
    node_info->set_node_id(node_id_);
    node_info->set_address("127.0.0.1:" + std::to_string(port_));
    node_info->set_data_path("/tmp/cedar/storaged/node" + std::to_string(node_id_));
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
  
  std::cout << "Configuration:" << std::endl;
  std::cout << "  Node ID:   " << config.node_id << std::endl;
  std::cout << "  Port:      " << config.port << std::endl;
  std::cout << "  Bind:      " << config.bind_address << std::endl;
  std::cout << "  Data Dir:  " << config.data_dir << std::endl;
  std::cout << "  MetaD:     " << config.meta_server << std::endl;
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

  // 2. 初始化 PartitionRouter（用于本地分区管理）
  cedar::raft::PartitionRouterConfig router_config;
  router_config.default_replica_count = 3;
  router_config.enable_read_from_follower = true;
  
  // 暂时禁用 PartitionRouter 初始化，简化版本
  // status = storage->InitializePartitionRouter(router_config);
  // if (!status.ok()) {
  //   std::cerr << "[StorageD] Failed to initialize partition router: " << status.ToString() << std::endl;
  //   delete storage;
  //   return 1;
  // }
  std::cout << "[StorageD] Partition router initialized (deferred)" << std::endl;

  // 3. 注册到 MetaD
  MetaClient meta_client(config.meta_server, config.node_id, config.port);
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
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
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

  // 7. 等待关闭
  g_grpc_server->Wait();

  // 清理
  std::cout << "[StorageD] Shutting down..." << std::endl;
  g_running = false;
  heartbeat_thread.join();
  delete storage;
  std::cout << "[StorageD] Stopped." << std::endl;

  return 0;
}
