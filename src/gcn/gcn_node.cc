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

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <limits>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

#include "cedar/dtx/raft/grpc_tls.h"
#include "cedar/gcn/scatter_gather_router.h"
#include "cedar/core/logging.h"
#include "cedar/storage/cedar_graph_storage.h"

#include <gflags/gflags.h>
#include <grpcpp/grpcpp.h>

DEFINE_int32(gcn_port, 9780, "GCN service port");
DEFINE_string(gcn_bind_address, "0.0.0.0", "GCN bind address (default 0.0.0.0 for cluster visibility)");
DEFINE_string(gcn_coordinator, "127.0.0.1:10559", "Coordinator MetaD gRPC endpoint");
DEFINE_int64(gcn_tmv_max_chunks, 256, "Maximum TMV chunks per engine");
DEFINE_bool(gcn_backfill_enabled, false, "Enable storage to TMV backfill on startup");
DEFINE_uint64(gcn_backfill_start_id, 1, "Start entity ID for backfill");
DEFINE_uint64(gcn_backfill_end_id, 1000, "End entity ID for backfill");
DEFINE_int32(gcn_heartbeat_interval_ms, 5000, "GCN heartbeat interval in milliseconds");
DEFINE_string(gcn_checkpoint_dir, "/tmp/cedar-gcn-checkpoints",
              "Directory for GCN partition checkpoints");

namespace cedar {

GcnNode::GcnNode() = default;

GcnNode::GcnNode(Options options) : options_(std::move(options)) {}

GcnNode::~GcnNode() {
  if (running_.load()) {
    Stop().IgnoreError();
  }
}

 cedar::Status GcnNode::Initialize() {
  // Environment overrides for containerized / cloud deployments
  const char* env_bind = std::getenv("CEDAR_GCN_BIND_ADDRESS");
  if (env_bind && env_bind[0] != '\0') {
    FLAGS_gcn_bind_address = env_bind;
    std::cerr << "[GCN] Bind address overridden by CEDAR_GCN_BIND_ADDRESS: " << FLAGS_gcn_bind_address << std::endl;
  }
  const char* env_coord = std::getenv("CEDAR_GCN_COORDINATOR");
  if (env_coord && env_coord[0] != '\0') {
    FLAGS_gcn_coordinator = env_coord;
    std::cerr << "[GCN] Coordinator overridden by CEDAR_GCN_COORDINATOR: " << FLAGS_gcn_coordinator << std::endl;
  }

  const size_t tmv_max_chunks =
      options_.tmv_max_chunks == 0
          ? static_cast<size_t>(FLAGS_gcn_tmv_max_chunks)
          : options_.tmv_max_chunks;
  const std::string checkpoint_dir =
      options_.checkpoint_directory.empty() ? FLAGS_gcn_checkpoint_dir
                                            : options_.checkpoint_directory;
  const std::string bind_address =
      options_.bind_address.empty() ? FLAGS_gcn_bind_address
                                    : options_.bind_address;
  const int port = options_.port == 0 ? FLAGS_gcn_port : options_.port;
  const std::string coordinator_endpoint =
      options_.coordinator_endpoint.empty() ? FLAGS_gcn_coordinator
                                            : options_.coordinator_endpoint;

  // Create TMVEngine
  engine_ = std::make_unique<gcn::TMVEngine>(tmv_max_chunks);

  // Storage backfill (optional, or lazy on-demand)
  if (storage_) {
    backfill_service_ = std::make_unique<gcn::StorageBackfillService>(engine_.get(), storage_);
    if (FLAGS_gcn_backfill_enabled) {
      backfill_service_->BackfillRange(FLAGS_gcn_backfill_start_id, FLAGS_gcn_backfill_end_id);
    }
  }

  // Create WatermarkGc and start it (watermark = 0 means no GC yet)
  constexpr uint64_t kWatermarkGcIntervalMs = 5000;
  if (options_.enable_watermark_gc) {
    watermark_gc_ = std::make_unique<gcn::WatermarkGc>(engine_.get());
    watermark_gc_->Start(kWatermarkGcIntervalMs);
  }

  // Create EventApplier
  event_applier_ = std::make_unique<gcn::EventApplier>(engine_.get());
  checkpoint_store_ = std::make_unique<gcn::CheckpointStore>(checkpoint_dir);
  tmv_snapshot_store_ =
      std::make_unique<gcn::TmvSnapshotStore>(checkpoint_dir + "/tmv_snapshots");

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
  service_impl_->SetNodeReadiness(false, "GCN node not started");

  // Register peers in ScatterGatherRouter for multi-GCN routing
  auto router = std::make_shared<gcn::ScatterGatherRouter>();
  for (const auto& addr : peer_addresses_) {
    auto client_creds = cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentialsFromEnvStrict();
    if (!client_creds.ok()) {
      std::cerr << "GCN TLS error: " << client_creds.status().ToString() << std::endl;
      continue;
    }
    auto channel = grpc::CreateChannel(addr, client_creds.ValueOrDie());
    router->RegisterPeer(addr, channel);
  }
  service_impl_->SetScatterGatherRouter(router);

  // Create CoordinatorClient connection to metad
  if (options_.enable_coordinator) {
    StatusOr<std::shared_ptr<grpc::ChannelCredentials>> coordinator_creds =
        options_.use_insecure_coordinator
            ? StatusOr<std::shared_ptr<grpc::ChannelCredentials>>(
                  grpc::InsecureChannelCredentials())
            : cedar::dtx::raft::TlsCredentialFactory::
                  CreateClientCredentialsFromEnvStrict();
    if (!coordinator_creds.ok()) {
      return cedar::Status::IOError("Failed to create coordinator TLS credentials: " + coordinator_creds.status().ToString());
    }
    auto coordinator_channel = grpc::CreateChannel(
        coordinator_endpoint, coordinator_creds.ValueOrDie());
    coordinator_client_ =
        std::make_unique<gcn::CoordinatorClient>(coordinator_channel);
    coordinator_client_->SetGcnNodeId(static_cast<uint32_t>(port));
  }

  if (!options_.enable_grpc_server) {
    return cedar::Status::OK();
  }

  // Build and start gRPC server
  std::ostringstream address;
  address << bind_address << ":" << port;
  std::string server_address = address.str();

  grpc::ServerBuilder builder;
  auto server_creds = cedar::dtx::raft::TlsCredentialFactory::CreateServerCredentialsFromEnvStrict();
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
  if (options_.enable_grpc_server && !grpc_server_) {
    return cedar::Status::InvalidArgument("GcnNode not initialized");
  }
  if (!checkpoint_store_ || !event_applier_ || !engine_ || !service_impl_) {
    return cedar::Status::InvalidArgument("GcnNode not initialized");
  }
  if (options_.cdc_poll_interval.count() <= 0) {
    service_impl_->SetNodeReadiness(false, "invalid CDC poll interval");
    return cedar::Status::InvalidArgument("CDC poll interval must be positive");
  }

  const bool dynamic_leases =
      options_.enable_coordinator && options_.use_metad_leases;
  const int node_port = options_.port == 0 ? FLAGS_gcn_port : options_.port;
  const std::string bind_address =
      options_.bind_address.empty() ? FLAGS_gcn_bind_address
                                    : options_.bind_address;
  const std::string default_endpoint =
      bind_address + ":" + std::to_string(node_port);
  if (!dynamic_leases) {
    std::unordered_set<uint32_t> seen_partitions;
    for (const auto& lease : options_.partition_leases) {
      if (!seen_partitions.insert(lease.partition_id).second) {
        service_impl_->SetNodeReadiness(false, "duplicate partition lease");
        return cedar::Status::InvalidArgument(
            "duplicate partition lease " + std::to_string(lease.partition_id));
      }
      auto source_it = options_.storage_cdc_sources.find(lease.partition_id);
      if (source_it == options_.storage_cdc_sources.end() || !source_it->second) {
        service_impl_->SetNodeReadiness(false, "missing StorageD CDC source");
        return cedar::Status::InvalidArgument(
            "missing StorageD CDC source for partition " +
            std::to_string(lease.partition_id));
      }
    }
  }

  gcn::PartitionCheckpoint probe_checkpoint;
  probe_checkpoint.partition_id = std::numeric_limits<uint32_t>::max();
  auto checkpoint_status = checkpoint_store_->Save(probe_checkpoint);
  if (checkpoint_status.ok()) {
    checkpoint_status = checkpoint_store_->Remove(probe_checkpoint.partition_id);
  }
  if (!checkpoint_status.ok()) {
    service_impl_->SetNodeReadiness(false, "checkpoint store is not writable");
    return checkpoint_status;
  }

  running_ = true;
  service_impl_->SetNodeReadiness(false, "partition consumers starting");

  {
    std::lock_guard<std::mutex> lock(consumers_mutex_);
    consumers_.clear();
    stopped_progress_.clear();
  }
  if (dynamic_leases) {
    if (!coordinator_client_) {
      running_ = false;
      return cedar::Status::InvalidArgument("coordinator client is not initialized");
    }
    runtime_gcn_id_ =
        options_.gcn_id == 0 ? static_cast<uint64_t>(node_port) : options_.gcn_id;
    runtime_gcn_incarnation_ =
        options_.gcn_incarnation == 0
            ? static_cast<uint64_t>(
                  std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::system_clock::now().time_since_epoch())
                      .count())
            : options_.gcn_incarnation;
    const std::string endpoint =
        options_.advertised_endpoint.empty() ? default_endpoint
                                             : options_.advertised_endpoint;
    auto register_status =
        coordinator_client_->RegisterGcn(runtime_gcn_id_, endpoint,
                                         runtime_gcn_incarnation_);
    if (!register_status.ok()) {
      running_ = false;
      service_impl_->SetNodeReadiness(false, register_status.ToString());
      return register_status;
    }
  } else {
    for (const auto& lease : options_.partition_leases) {
      auto status = StartConsumerForPartitionLease(lease);
      if (!status.ok()) {
        std::lock_guard<std::mutex> lock(consumers_mutex_);
        for (auto& started : consumers_) {
          if (started) {
            started->Stop(std::chrono::milliseconds(1000)).IgnoreError();
          }
        }
        consumers_.clear();
        running_ = false;
        service_impl_->SetNodeReadiness(false, status.ToString());
        return status;
      }
    }
  }

  if (!dynamic_leases && consumers_.empty()) {
    service_impl_->SetNodeReadiness(false, "no partition leases configured");
  }

  // Start CDC progress publication thread.
  cdc_thread_ = std::thread(&GcnNode::CdcListenerLoop, this);

  // Start heartbeat thread to report liveness to metad
  heartbeat_thread_ = std::thread(&GcnNode::HeartbeatLoop, this);

  return cedar::Status::OK();
}

 cedar::Status GcnNode::Stop() {
  running_ = false;
  stop_cv_.notify_all();

  {
    std::lock_guard<std::mutex> lock(consumers_mutex_);
    for (auto& consumer : consumers_) {
      if (consumer) {
        auto progress = consumer->GetProgress();
        consumer->Stop(std::chrono::milliseconds(1000)).IgnoreError();
        progress.state = gcn::PartitionConsumerState::kStopped;
        progress.query_ready = false;
        stopped_progress_[progress.partition_id] = progress;
      }
    }
  }

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
  consumers_.clear();
  checkpoint_store_.reset();
  tmv_snapshot_store_.reset();
  event_applier_.reset();
  watermark_gc_.reset();
  engine_.reset();

  return cedar::Status::OK();
}

void GcnNode::CdcListenerLoop() {
  while (running_.load()) {
    PublishConsumerProgress();
    std::unique_lock<std::mutex> lock(stop_mutex_);
    stop_cv_.wait_for(lock, options_.cdc_poll_interval, [this]() {
      return !running_.load();
    });
  }
}

void GcnNode::PublishConsumerProgress() {
  if (!service_impl_) {
    return;
  }
  std::lock_guard<std::mutex> lock(consumers_mutex_);
  bool all_ready = !consumers_.empty();
  std::string not_ready_reason = consumers_.empty()
                                     ? "no partition leases configured"
                                     : "partition consumers catching up";
  for (const auto& consumer : consumers_) {
    if (!consumer) {
      all_ready = false;
      continue;
    }
    auto progress = consumer->GetProgress();
    service_impl_->SetPartitionProgress(
        progress.partition_id, progress.partition_epoch,
        progress.applied_version, progress.query_ready);
    if (!progress.query_ready) {
      all_ready = false;
      if (!progress.error.empty()) {
        not_ready_reason = progress.error;
      }
    }
  }
  service_impl_->SetNodeReadiness(all_ready,
                                  all_ready ? "ready" : not_ready_reason);
}

gcn::PartitionConsumerProgress GcnNode::GetPartitionProgress(
    uint32_t partition_id) const {
  std::lock_guard<std::mutex> lock(consumers_mutex_);
  for (const auto& consumer : consumers_) {
    if (!consumer) {
      continue;
    }
    auto progress = consumer->GetProgress();
    if (progress.partition_id == partition_id) {
      return progress;
    }
  }
  auto stopped_it = stopped_progress_.find(partition_id);
  if (stopped_it != stopped_progress_.end()) {
    return stopped_it->second;
  }
  return gcn::PartitionConsumerProgress{};
}

std::vector<dtx::GcnPartitionProgress> GcnNode::CollectLeaseProgress() const {
  std::lock_guard<std::mutex> lock(consumers_mutex_);
  std::vector<dtx::GcnPartitionProgress> progress;
  progress.reserve(consumers_.size());
  for (const auto& consumer : consumers_) {
    if (!consumer) {
      continue;
    }
    auto item = consumer->GetProgress();
    // MetaD progress field 2 is the GCN lease epoch/generation, despite the
    // historical proto/internal member name partition_epoch. StorageD CDC
    // partition_epoch remains local consumer progress and must not be used for
    // MetaD lease ownership validation.
    progress.push_back(dtx::GcnPartitionProgress{
        item.partition_id,
        item.lease_epoch,
        item.applied_offset,
        item.applied_version,
        item.query_ready,
    });
  }
  return progress;
}

cedar::Status GcnNode::StartConsumerForLease(const dtx::GcnLease& lease) {
  gcn::PartitionLease partition_lease;
  partition_lease.partition_id = lease.partition_id;
  partition_lease.partition_epoch = 0;
  partition_lease.lease_epoch = lease.lease_epoch;
  return StartConsumerForPartitionLease(partition_lease);
}

cedar::Status GcnNode::StartConsumerForPartitionLease(
    const gcn::PartitionLease& partition_lease) {
  auto source_it =
      options_.storage_cdc_sources.find(partition_lease.partition_id);
  if (source_it == options_.storage_cdc_sources.end() || !source_it->second) {
    return cedar::Status::InvalidArgument(
        "missing StorageD CDC source for partition " +
        std::to_string(partition_lease.partition_id));
  }
  gcn::PartitionConsumer::Options consumer_options;
  consumer_options.poll_interval = options_.cdc_poll_interval;
  auto consumer = std::make_unique<gcn::PartitionConsumer>(
      source_it->second.get(), checkpoint_store_.get(), event_applier_.get(),
      engine_.get(), consumer_options, tmv_snapshot_store_.get());
  auto status = consumer->Start(partition_lease);
  if (!status.ok()) {
    return status;
  }
  std::lock_guard<std::mutex> lock(consumers_mutex_);
  stopped_progress_.erase(partition_lease.partition_id);
  consumers_.push_back(std::move(consumer));
  return cedar::Status::OK();
}

void GcnNode::ReconcileLeases(const std::vector<dtx::GcnLease>& leases) {
  std::unordered_map<uint32_t, dtx::GcnLease> desired;
  const uint64_t now_ms = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());
  for (const auto& lease : leases) {
    if (lease.gcn_id != runtime_gcn_id_) {
      continue;
    }
    if (lease.expires_at_ms <= now_ms) {
      continue;
    }
    if (options_.storage_cdc_sources.find(lease.partition_id) !=
        options_.storage_cdc_sources.end()) {
      desired[lease.partition_id] = lease;
    }
  }

  std::vector<dtx::GcnLease> start_leases;
  {
    std::lock_guard<std::mutex> lock(consumers_mutex_);
    for (auto it = consumers_.begin(); it != consumers_.end();) {
      if (!*it) {
        it = consumers_.erase(it);
        continue;
      }
      auto progress = (*it)->GetProgress();
      auto desired_it = desired.find(progress.partition_id);
      if (desired_it == desired.end() ||
          desired_it->second.lease_epoch != progress.lease_epoch) {
        (*it)->Stop(std::chrono::milliseconds(1000)).IgnoreError();
        progress.state = gcn::PartitionConsumerState::kStopped;
        progress.query_ready = false;
        stopped_progress_[progress.partition_id] = progress;
        it = consumers_.erase(it);
      } else {
        ++it;
      }
    }
    for (const auto& [partition_id, lease] : desired) {
      bool exists = false;
      for (const auto& consumer : consumers_) {
        if (consumer && consumer->GetProgress().partition_id == partition_id) {
          exists = true;
          break;
        }
      }
      if (!exists) {
        start_leases.push_back(lease);
      }
    }
  }
  for (const auto& lease : start_leases) {
    auto status = StartConsumerForLease(lease);
    if (!status.ok()) {
      CEDAR_LOG_WARN() << "Failed to start GCN partition consumer for "
                       << lease.partition_id << ": " << status.ToString()
                       << "\n";
    }
  }
}

void GcnNode::HeartbeatLoop() {
  while (running_.load()) {
    if (coordinator_client_) {
      if (options_.use_metad_leases) {
        auto leases = coordinator_client_->RenewGcnLeases(
            runtime_gcn_id_, runtime_gcn_incarnation_, CollectLeaseProgress());
        if (leases.ok()) {
          ReconcileLeases(leases.ValueOrDie());
        } else {
          CEDAR_LOG_WARN() << "GCN lease renewal failed: "
                           << leases.status().ToString() << "\n";
          ReconcileLeases({});
        }
      } else {
        std::vector<coordinator::CacheWindow> windows;
        auto status = coordinator_client_->Heartbeat(windows);
        if (!status.ok()) {
          CEDAR_LOG_WARN() << "GCN heartbeat failed: " << status.ToString() << "\n";
        }
      }
    }
    if (watermark_gc_) {
      uint64_t minimum_applied_version = std::numeric_limits<uint64_t>::max();
      bool all_partitions_have_applied_state = false;
      {
        std::lock_guard<std::mutex> lock(consumers_mutex_);
        all_partitions_have_applied_state = !consumers_.empty();
        for (const auto& consumer : consumers_) {
          if (!consumer) {
            all_partitions_have_applied_state = false;
            continue;
          }
          auto progress = consumer->GetProgress();
          if (progress.applied_version == 0) {
            all_partitions_have_applied_state = false;
          } else {
            minimum_applied_version =
                std::min(minimum_applied_version, progress.applied_version);
          }
        }
      }
      if (!all_partitions_have_applied_state ||
          minimum_applied_version == std::numeric_limits<uint64_t>::max()) {
        watermark_gc_->UpdateWatermark(0);
      } else {
        watermark_gc_->UpdateWatermark(gcn::ComputeSafeWatermark(
            gcn::WatermarkInputs{
                .minimum_applied_version = minimum_applied_version,
                .minimum_active_query_version =
                    service_impl_
                        ? service_impl_->MinimumActiveQueryVersion()
                        : std::numeric_limits<uint64_t>::max(),
                .retention_floor_version = minimum_applied_version}));
      }
    }
    std::unique_lock<std::mutex> lock(stop_mutex_);
    stop_cv_.wait_for(
        lock,
        options_.use_metad_leases
            ? options_.lease_renew_interval
            : std::chrono::milliseconds(FLAGS_gcn_heartbeat_interval_ms),
        [this]() { return !running_.load(); });
  }
}

}  // namespace cedar
