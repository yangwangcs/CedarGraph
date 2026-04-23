// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef CEDAR_GCN_GCN_NODE_H_
#define CEDAR_GCN_GCN_NODE_H_

#include <atomic>
#include <memory>
#include <thread>

#include <grpcpp/grpcpp.h>

#include "cedar/core/status.h"
#include "cedar/gcn/event_applier.h"
#include "cedar/gcn/gcn_service.h"
#include "cedar/gcn/tmv_engine.h"

namespace cedar {

// GcnNode orchestrates the entire Graph Compute Node (GCN) process lifecycle.
// It owns the TMVEngine, the gRPC service implementation, and all background
// threads (garbage collection, CDC listener).
class GcnNode {
 public:
  GcnNode();
  ~GcnNode();

  // Non-copyable, non-movable
  GcnNode(const GcnNode&) = delete;
  GcnNode& operator=(const GcnNode&) = delete;

  // Reads parsed gflags, creates TMVEngine, builds and starts the gRPC server,
  // and registers GcnServiceImpl.
  cedar::Status Initialize();

  // Starts background threads (GC, CDC listener).
  cedar::Status Start();

  // Signals the gRPC server to shut down, joins all background threads,
  // and releases owned resources.
  cedar::Status Stop();

 private:
  void GarbageCollectLoop();
  void CdcListenerLoop();

  std::unique_ptr<gcn::TMVEngine> engine_;
  std::unique_ptr<gcn::EventApplier> event_applier_;
  std::unique_ptr<gcn::GcnServiceImpl> service_impl_;
  std::unique_ptr<grpc::Server> grpc_server_;

  std::atomic<bool> running_{false};
  std::thread gc_thread_;
  std::thread cdc_thread_;
};

}  // namespace cedar

#endif  // CEDAR_GCN_GCN_NODE_H_
