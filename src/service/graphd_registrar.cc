// Copyright 2025 The Cedar Authors. All rights reserved.
// GraphD Registrar Implementation

#include "cedar/service/graphd_registrar.h"

#include "cedar/dtx/raft/grpc_tls.h"

#include <iostream>
#include <chrono>

namespace cedar {
namespace service {

GraphDRegistrar::GraphDRegistrar(const Config& config) : config_(config) {}

GraphDRegistrar::~GraphDRegistrar() {
  Stop();
}

bool GraphDRegistrar::Start() {
  // Create gRPC channel to MetaD
  auto creds = cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentialsFromEnvStrict();
  if (!creds.ok()) {
    std::cerr << "[GraphD] Failed to create MetaD client credentials: "
              << creds.status().ToString() << std::endl;
    return false;
  }
  auto channel = grpc::CreateChannel(config_.meta_address, creds.ValueOrDie());
  stub_ = cedar::meta::MetaService::NewStub(channel);

  // Register with MetaD
  if (!Register()) {
    std::cerr << "[GraphD] Failed to register with MetaD" << std::endl;
    return false;
  }

  // Start heartbeat thread
  running_ = true;
  heartbeat_thread_ = std::make_unique<std::thread>(&GraphDRegistrar::HeartbeatLoop, this);

  std::cout << "[GraphD] Registered with MetaD as " << node_id_ << std::endl;
  return true;
}

void GraphDRegistrar::Stop() {
  running_ = false;
  heartbeat_cv_.notify_all();
  
  if (heartbeat_thread_ && heartbeat_thread_->joinable()) {
    heartbeat_thread_->join();
  }

  if (registered_) {
    Unregister();
  }
}

bool GraphDRegistrar::Register() {
  cedar::meta::RegisterGraphDRequest request;
  
  auto* node_info = request.mutable_node_info();
  node_info->set_address(config_.graphd_address);
  node_info->set_port(config_.graphd_port);
  node_info->set_version("1.0.0");
  node_info->set_max_qps(config_.max_qps);
  node_info->set_current_qps(0);
  node_info->set_active_queries(0);
  node_info->set_queued_queries(0);
  node_info->set_cpu_usage(0.0);
  node_info->set_memory_usage(0.0);
  node_info->set_state("ONLINE");

  cedar::meta::RegisterGraphDResponse response;
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

  grpc::Status status = stub_->RegisterGraphD(&context, request, &response);
  if (!status.ok()) {
    std::cerr << "[GraphD] RegisterGraphD RPC failed: " << status.error_message() << std::endl;
    return false;
  }

  if (!response.success()) {
    std::cerr << "[GraphD] RegisterGraphD failed: " << response.error_msg() << std::endl;
    return false;
  }

  node_id_ = response.node_id();
  registered_ = true;
  return true;
}

bool GraphDRegistrar::Unregister() {
  if (!stub_ || node_id_.empty()) {
    return false;
  }

  cedar::meta::UnregisterGraphDRequest request;
  request.set_node_id(node_id_);

  cedar::meta::UnregisterGraphDResponse response;
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));

  grpc::Status status = stub_->UnregisterGraphD(&context, request, &response);
  if (!status.ok() || !response.success()) {
    std::cerr << "[GraphD] UnregisterGraphD failed" << std::endl;
    return false;
  }

  registered_ = false;
  std::cout << "[GraphD] Unregistered from MetaD: " << node_id_ << std::endl;
  return true;
}

void GraphDRegistrar::HeartbeatLoop() {
  while (running_) {
    {
      std::unique_lock<std::mutex> lock(heartbeat_cv_mutex_);
      heartbeat_cv_.wait_for(lock, std::chrono::seconds(config_.heartbeat_interval_seconds),
                             [this]() { return !running_.load(); });
      if (!running_) break;
    }

    cedar::meta::GraphDHeartbeatRequest request;
    request.set_node_id(node_id_);
    request.set_current_qps(0);  // TODO: Get actual QPS
    request.set_active_queries(0);  // TODO: Get actual active queries
    request.set_queued_queries(0);
    request.set_cpu_usage(0.0);  // TODO: Get actual CPU usage
    request.set_memory_usage(0.0);  // TODO: Get actual memory usage

    cedar::meta::GraphDHeartbeatResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

    grpc::Status status = stub_->GraphDHeartbeat(&context, request, &response);
    if (!status.ok()) {
      std::cerr << "[GraphD] Heartbeat failed: " << status.error_message() << std::endl;
    } else if (!response.success()) {
      std::cerr << "[GraphD] Heartbeat rejected: " << response.error_msg() << std::endl;
    }
  }
}

}  // namespace service
}  // namespace cedar
