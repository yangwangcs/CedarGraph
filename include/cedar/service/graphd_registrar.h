// Copyright 2025 The Cedar Authors. All rights reserved.
// GraphD Registrar - Handles GraphD registration and heartbeat with MetaD

#pragma once

#include <string>
#include <thread>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <memory>
#include <grpcpp/grpcpp.h>
#include "meta_service.grpc.pb.h"

namespace cedar {
namespace service {

class GraphDRegistrar {
 public:
  struct Config {
    std::string meta_address = "127.0.0.1:10559";
    std::string graphd_address = "0.0.0.0";
    int graphd_port = 9669;
    int heartbeat_interval_seconds = 10;
    int max_qps = 10000;
  };

  explicit GraphDRegistrar(const Config& config);
  ~GraphDRegistrar();

  // Register with MetaD and start heartbeat thread
  bool Start();

  // Unregister and stop heartbeat thread
  void Stop();

  // Get assigned node ID
  std::string GetNodeId() const { return node_id_; }

  // Check if registered
  bool IsRegistered() const { return registered_.load(); }

 private:
  void HeartbeatLoop();
  bool Register();
  bool Unregister();

  Config config_;
  std::string node_id_;
  std::atomic<bool> registered_{false};
  std::atomic<bool> running_{false};
  std::unique_ptr<std::thread> heartbeat_thread_;
  std::condition_variable heartbeat_cv_;
  std::mutex heartbeat_cv_mutex_;
  std::unique_ptr<cedar::meta::MetaService::Stub> stub_;
};

}  // namespace service
}  // namespace cedar
