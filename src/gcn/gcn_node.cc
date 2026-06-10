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
#include <cstring>
#include <iostream>
#include <limits>
#include <sstream>

#include "cedar/dtx/raft/grpc_tls.h"
#include "cedar/gcn/scatter_gather_router.h"
#include "cedar/core/logging.h"
#include "cedar/storage/cedar_graph_storage.h"

#include <gflags/gflags.h>
#include <grpcpp/grpcpp.h>

DEFINE_int32(gcn_port, 9780, "GCN service port");
DEFINE_string(gcn_bind_address, "127.0.0.1", "GCN bind address (local-only for internal GraphD access)");
DEFINE_string(gcn_coordinator, "127.0.0.1:9559", "Coordinator endpoint");
DEFINE_int64(gcn_tmv_max_chunks, 256, "Maximum TMV chunks per engine");
DEFINE_bool(gcn_backfill_enabled, false, "Enable storage to TMV backfill on startup");
DEFINE_uint64(gcn_backfill_start_id, 1, "Start entity ID for backfill");
DEFINE_uint64(gcn_backfill_end_id, 1000, "End entity ID for backfill");
DEFINE_int32(gcn_heartbeat_interval_ms, 5000, "GCN heartbeat interval in milliseconds");

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

  // Storage backfill (optional, or lazy on-demand)
  if (storage_) {
    backfill_service_ = std::make_unique<gcn::StorageBackfillService>(engine_.get(), storage_);
    if (FLAGS_gcn_backfill_enabled) {
      backfill_service_->BackfillRange(FLAGS_gcn_backfill_start_id, FLAGS_gcn_backfill_end_id);
    }
  }

  // Create WatermarkGc and start it (watermark = 0 means no GC yet)
  constexpr uint64_t kWatermarkGcIntervalMs = 5000;
  watermark_gc_ = std::make_unique<gcn::WatermarkGc>(engine_.get());
  watermark_gc_->Start(kWatermarkGcIntervalMs);

  // Create EventApplier
  event_applier_ = std::make_unique<gcn::EventApplier>(engine_.get());

  // Create GcnServiceImpl with callback forwarding to EventApplier
  auto callback = [this](const cedar::gcn::CDCEvent& proto_event) {
    gcn::GraphCDCEvent event;
    event.commit_version = proto_event.version();
    event.entity_id = proto_event.entity_id();

    // Parse target_id and edge_type from payload
    const std::string& payload = proto_event.payload();
    if (payload.size() >= sizeof(uint64_t)) {
      std::memcpy(&event.target_id, payload.data(), sizeof(uint64_t));
      if (payload.size() >= sizeof(uint64_t) + sizeof(uint32_t)) {
        std::memcpy(&event.edge_type, payload.data() + sizeof(uint64_t), sizeof(uint32_t));
      } else {
        event.edge_type = 1;
      }
    } else {
      CEDAR_LOG_WARN() << "CDCEvent missing payload for target_id, entity_id="
                       << proto_event.entity_id() << "\n";
      event.target_id = proto_event.entity_id();
      event.edge_type = 1;
    }

    event.valid_from = static_cast<uint32_t>(proto_event.timestamp());

    // Map event_type to op and valid_to
    if (proto_event.event_type() == "DELETE") {
      event.valid_to = event.valid_from;
      event.op = gcn::CDCEventOp::kDelete;
    } else {
      event.valid_to = std::numeric_limits<uint32_t>::max();
      event.op = gcn::CDCEventOp::kCreate;
    }

    cedar::Status s = this->event_applier_->ApplyUnordered(event);
    if (!s.ok()) {
      CEDAR_LOG_ERROR() << "Failed to apply CDC event: " << s.ToString() << "\n";
    }
  };
  service_impl_ = std::make_unique<gcn::GcnServiceImpl>(
      engine_.get(), backfill_service_.get(), std::move(callback));

  // Register peers in ScatterGatherRouter for multi-GCN routing
  auto router = std::make_shared<gcn::ScatterGatherRouter>();
  for (const auto& addr : peer_addresses_) {
    auto client_creds = cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentialsFromEnv();
    if (!client_creds.ok()) {
      std::cerr << "GCN TLS error: " << client_creds.status().ToString() << std::endl;
      continue;
    }
    auto channel = grpc::CreateChannel(addr, client_creds.ValueOrDie());
    router->RegisterPeer(addr, channel);
  }
  service_impl_->SetScatterGatherRouter(router);

  // Create CoordinatorClient connection to metad
  auto coordinator_creds = cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentialsFromEnv();
  if (!coordinator_creds.ok()) {
    return cedar::Status::IOError("Failed to create coordinator TLS credentials: " + coordinator_creds.status().ToString());
  }
  auto coordinator_channel = grpc::CreateChannel(
      FLAGS_gcn_coordinator, coordinator_creds.ValueOrDie());
  coordinator_client_ =
      std::make_unique<gcn::CoordinatorClient>(coordinator_channel);
  coordinator_client_->SetGcnNodeId(
      static_cast<uint32_t>(FLAGS_gcn_port));

  // Build and start gRPC server
  std::ostringstream address;
  address << FLAGS_gcn_bind_address << ":" << FLAGS_gcn_port;
  std::string server_address = address.str();

  grpc::ServerBuilder builder;
  auto server_creds = cedar::dtx::raft::TlsCredentialFactory::CreateServerCredentialsFromEnv();
  if (!server_creds.ok()) {
    return cedar::Status::IOError("Failed to create server TLS credentials: " + server_creds.status().ToString());
  }
  builder.AddListeningPort(server_address, server_creds.ValueOrDie());
  builder.RegisterService(service_impl_.get());

  grpc_server_ = builder.BuildAndStart();
  if (!grpc_server_) {
    return cedar::Status::IOError("Failed to start gRPC server on " + server_address);
  }

  std::cerr << "GCN listening on " << server_address << std::endl;

  return cedar::Status::OK();
}

 cedar::Status GcnNode::Start() {
  if (!grpc_server_) {
    return cedar::Status::InvalidArgument("GcnNode not initialized");
  }

  running_ = true;

  // Start CDC listener thread
  cdc_thread_ = std::thread(&GcnNode::CdcListenerLoop, this);

  // Start heartbeat thread to report liveness to metad
  heartbeat_thread_ = std::thread(&GcnNode::HeartbeatLoop, this);

  return cedar::Status::OK();
}

 cedar::Status GcnNode::Stop() {
  running_ = false;

  if (grpc_server_) {
    grpc_server_->Shutdown();
  }

  if (watermark_gc_) {
    watermark_gc_->Stop();
  }
  if (cdc_thread_.joinable()) {
    cdc_thread_.join();
  }
  if (heartbeat_thread_.joinable()) {
    heartbeat_thread_.join();
  }

  // Release resources
  grpc_server_.reset();
  service_impl_.reset();
  coordinator_client_.reset();
  backfill_service_.reset();
  event_applier_.reset();
  watermark_gc_.reset();
  engine_.reset();

  return cedar::Status::OK();
}

void GcnNode::CdcListenerLoop() {
  uint64_t last_polled_version = 0;
  while (running_.load()) {
    // TODO(#C-CDC-001): Full CDC streaming integration.
    // For now, we poll storage directly if co-located, or rely on
    // coordinator heartbeats for watermark advancement.
    if (storage_) {
      // Simple polling: scan for active entities and apply as CDC events
      // This is a best-effort fallback for development/demo scenarios.
      auto entities = storage_->GetActiveEntities(
          cedar::EntityType::Vertex,
          cedar::Timestamp(std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::system_clock::now().time_since_epoch()).count()));
      for (uint64_t entity_id : entities) {
        gcn::GraphCDCEvent event;
        event.entity_id = entity_id;
        event.target_id = entity_id;
        event.edge_type = 0;
        event.valid_from = static_cast<uint32_t>(
            std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count());
        event.valid_to = std::numeric_limits<uint32_t>::max();
        event.op = gcn::CDCEventOp::kCreate;
        auto s = event_applier_->ApplyUnordered(event);
        if (!s.ok()) {
          CEDAR_LOG_WARN() << "CDC apply failed for entity " << entity_id << ": " << s.ToString() << "\n";
        }
      }
    }
    std::this_thread::sleep_for(std::chrono::seconds(5));
  }
}

void GcnNode::HeartbeatLoop() {
  while (running_.load()) {
    if (coordinator_client_) {
      // TODO(#C-GCN-003): When TMVEngine supports enumerating cached vertices,
      // construct real CacheWindow vectors from the engine and pass them here.
      // For now we send an empty heartbeat so MetaD knows this GCN is alive.
      std::vector<coordinator::CacheWindow> windows;
      auto status = coordinator_client_->Heartbeat(windows);
      if (!status.ok()) {
        CEDAR_LOG_WARN() << "GCN heartbeat failed: " << status.ToString() << "\n";
      }
    }
    // Advance watermark based on a safe time-based heuristic.
    // (Production-grade watermark should come from min active query time or CDC commit pointer.)
    if (watermark_gc_) {
      auto now_sec = std::chrono::duration_cast<std::chrono::seconds>(
          std::chrono::system_clock::now().time_since_epoch()).count();
      constexpr int64_t kWatermarkLagSeconds = 60;
      watermark_gc_->UpdateWatermark(static_cast<uint64_t>(now_sec - kWatermarkLagSeconds));
    }
    std::this_thread::sleep_for(
        std::chrono::milliseconds(FLAGS_gcn_heartbeat_interval_ms));
  }
}

}  // namespace cedar
