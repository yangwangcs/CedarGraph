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
#include <chrono>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "cedar/core/status.h"
#include "cedar/gcn/coordinator_client.h"
#include "cedar/gcn/checkpoint_store.h"
#include "cedar/gcn/event_applier.h"
#include "cedar/gcn/gcn_service.h"
#include "cedar/gcn/partition_consumer.h"
#include "cedar/gcn/storage_backfill_service.h"
#include "cedar/gcn/storage_cdc_client.h"
#include "cedar/gcn/tmv_engine.h"
#include "cedar/gcn/tmv_snapshot_store.h"
#include "cedar/gcn/watermark_gc.h"

namespace cedar {

class CedarGraphStorage;

// GcnNode orchestrates the entire Graph Compute Node (GCN) process lifecycle.
// It owns the TMVEngine, the gRPC service implementation, and all background
// threads (garbage collection, CDC listener).
class GcnNode {
 public:
  struct Options {
    bool enable_grpc_server = true;
    bool enable_coordinator = true;
    bool enable_watermark_gc = true;
    std::string bind_address;
    int port = 0;
    std::string coordinator_endpoint;
    size_t tmv_max_chunks = 0;
    std::string checkpoint_directory;
    std::vector<gcn::PartitionLease> partition_leases;
    std::map<uint32_t, std::shared_ptr<gcn::StorageCdcSource>>
        storage_cdc_sources;
    std::chrono::milliseconds cdc_poll_interval{50};
    std::chrono::milliseconds lease_renew_interval{5000};
    uint64_t gcn_id{0};
    uint64_t gcn_incarnation{0};
    std::string advertised_endpoint;
    bool use_insecure_coordinator = false;
    bool use_metad_leases = false;
  };

  GcnNode();
  explicit GcnNode(Options options);
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

  // Inject storage for optional backfill on startup
  void SetStorage(CedarGraphStorage* storage) { storage_ = storage; }

  // Inject peer GCN addresses for scatter-gather routing
  void SetPeerAddresses(const std::vector<std::string>& addresses) {
    peer_addresses_ = addresses;
  }

  gcn::PartitionConsumerProgress GetPartitionProgress(
      uint32_t partition_id) const;

 private:
  void CdcListenerLoop();
  void HeartbeatLoop();
  void PublishConsumerProgress();
  std::vector<dtx::GcnPartitionProgress> CollectLeaseProgress() const;
  void ReconcileLeases(const std::vector<dtx::GcnLease>& leases);
  cedar::Status StartConsumerForLease(const dtx::GcnLease& lease);
  cedar::Status StartConsumerForPartitionLease(
      const gcn::PartitionLease& lease);

  Options options_;
  std::unique_ptr<gcn::TMVEngine> engine_;
  std::unique_ptr<gcn::EventApplier> event_applier_;
  std::unique_ptr<gcn::CheckpointStore> checkpoint_store_;
  std::unique_ptr<gcn::TmvSnapshotStore> tmv_snapshot_store_;
  std::unique_ptr<gcn::StorageBackfillService> backfill_service_;
  std::unique_ptr<gcn::GcnServiceImpl> service_impl_;
  std::unique_ptr<grpc::Server> grpc_server_;
  std::unique_ptr<gcn::CoordinatorClient> coordinator_client_;

  CedarGraphStorage* storage_ = nullptr;

  std::unique_ptr<gcn::WatermarkGc> watermark_gc_;
  std::vector<std::unique_ptr<gcn::PartitionConsumer>> consumers_;
  mutable std::mutex consumers_mutex_;
  std::unordered_map<uint32_t, gcn::PartitionConsumerProgress> stopped_progress_;
  uint64_t runtime_gcn_id_ = 0;
  uint64_t runtime_gcn_incarnation_ = 0;

  std::vector<std::string> peer_addresses_;

  std::atomic<bool> running_{false};
  std::condition_variable stop_cv_;
  std::mutex stop_mutex_;
  std::thread cdc_thread_;
  std::thread heartbeat_thread_;
};

}  // namespace cedar

#endif  // CEDAR_GCN_GCN_NODE_H_
