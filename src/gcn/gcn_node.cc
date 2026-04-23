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

#include "cedar/gcn/gcn_node.h"

#include <chrono>
#include <iostream>
#include <limits>
#include <sstream>

#include <gflags/gflags.h>
#include <grpcpp/grpcpp.h>

DEFINE_int32(gcn_port, 9780, "GCN service port");
DEFINE_string(gcn_bind_address, "0.0.0.0", "GCN bind address");
DEFINE_string(gcn_coordinator, "127.0.0.1:9559", "Coordinator endpoint");
DEFINE_int64(gcn_tmv_max_chunks, 256, "Maximum TMV chunks per engine");

namespace cedar {

GcnNode::GcnNode() = default;

GcnNode::~GcnNode() {
  if (running_.load()) {
    Stop().IgnoreError();
  }
}

 cedar::Status GcnNode::Initialize() {
  // Create TMVEngine
  engine_ = std::make_unique<gcn::TMVEngine>(static_cast<size_t>(FLAGS_gcn_tmv_max_chunks));

  // Create EventApplier
  event_applier_ = std::make_unique<gcn::EventApplier>(engine_.get());

  // Create GcnServiceImpl with callback forwarding to EventApplier
  auto callback = [this](const cedar::gcn::CDCEvent& proto_event) {
    gcn::GraphCDCEvent event;
    event.commit_version = proto_event.version();
    event.entity_id = proto_event.entity_id();
    event.target_id = proto_event.entity_id() + 1;
    event.valid_from = static_cast<uint32_t>(proto_event.timestamp());
    event.valid_to = std::numeric_limits<uint32_t>::max();
    event.edge_type = 1;
    event.op = gcn::CDCEventOp::kCreate;
    this->event_applier_->ApplyUnordered(event);
  };
  service_impl_ = std::make_unique<gcn::GcnServiceImpl>(std::move(callback));

  // Build and start gRPC server
  std::ostringstream address;
  address << FLAGS_gcn_bind_address << ":" << FLAGS_gcn_port;
  std::string server_address = address.str();

  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(service_impl_.get());

  grpc_server_ = builder.BuildAndStart();
  if (!grpc_server_) {
    return cedar::Status::IOError("Failed to start gRPC server on " + server_address);
  }

  std::cout << "GCN listening on " << server_address << std::endl;

  return cedar::Status::OK();
}

 cedar::Status GcnNode::Start() {
  if (!grpc_server_) {
    return cedar::Status::InvalidArgument("GcnNode not initialized");
  }

  running_ = true;

  // Start garbage collection thread
  gc_thread_ = std::thread(&GcnNode::GarbageCollectLoop, this);

  // Start CDC listener thread
  cdc_thread_ = std::thread(&GcnNode::CdcListenerLoop, this);

  return cedar::Status::OK();
}

 cedar::Status GcnNode::Stop() {
  running_ = false;

  if (grpc_server_) {
    grpc_server_->Shutdown();
  }

  if (gc_thread_.joinable()) {
    gc_thread_.join();
  }
  if (cdc_thread_.joinable()) {
    cdc_thread_.join();
  }

  // Release resources
  grpc_server_.reset();
  service_impl_.reset();
  engine_.reset();

  return cedar::Status::OK();
}

void GcnNode::GarbageCollectLoop() {
  while (running_.load()) {
    // TODO: Implement actual GC logic (e.g., DropBelowWatermark) in Task 3.3+
    std::this_thread::sleep_for(std::chrono::seconds(10));
  }
}

void GcnNode::CdcListenerLoop() {
  while (running_.load()) {
    // TODO: Implement actual CDC listener logic in Task 3.3+
    std::this_thread::sleep_for(std::chrono::seconds(5));
  }
}

}  // namespace cedar
