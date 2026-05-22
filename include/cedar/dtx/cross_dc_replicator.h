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

#ifndef CEDAR_DTX_CROSS_DC_REPLICATOR_H_
#define CEDAR_DTX_CROSS_DC_REPLICATOR_H_

#include <atomic>
#include <chrono>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include "cedar/types/cedar_types.h"

#include <grpcpp/grpcpp.h>

namespace cedar {
namespace dtx {

enum class ReplicationMode {
  kAsync,
  kSemiSync,
  kSync
};

struct DCReplicationConfig {
  ReplicationMode mode = ReplicationMode::kAsync;
  std::chrono::milliseconds replication_timeout{5000};
  uint32_t max_retry_attempts = 3;
  std::chrono::milliseconds retry_delay{1000};
  bool enable_compression = true;
  uint32_t batch_size = 100;
  std::map<std::string, std::string> remote_dc_endpoints;
  bool tls_enabled{false};
  struct {
    std::string ca_cert_file;
    std::string client_cert_file;
    std::string client_key_file;
  } tls_config;
  bool allow_insecure{false};
};

struct ReplicationLog {
  uint64_t sequence_num;
  CedarKey key;
  Descriptor value;
  Timestamp timestamp;
  std::string source_dc;
  std::vector<std::string> target_dcs;
  std::chrono::system_clock::time_point created_at;
};

struct ReplicationStatus {
  uint64_t last_sequence = 0;
  uint64_t replicated_count = 0;
  uint64_t failed_count = 0;
  std::chrono::milliseconds replication_lag{0};
  bool is_healthy = true;
};

class CrossDCReplicator {
 public:
  using ReplicationCallback = std::function<void(
      const ReplicationLog& log, 
      Status status)>;

  CrossDCReplicator();
  ~CrossDCReplicator();

  CrossDCReplicator(const CrossDCReplicator&) = delete;
  CrossDCReplicator& operator=(const CrossDCReplicator&) = delete;

  Status Initialize(const DCReplicationConfig& config,
                    const std::string& local_dc_id,
                    const std::vector<std::string>& peer_dcs);

  Status Start();
  void Stop();

  Status Replicate(const std::string& key,
                   const Descriptor& value,
                   Timestamp timestamp);

  Status Replicate(const CedarKey& key,
                   const Descriptor& value,
                   Timestamp timestamp);

  Status ReplicateBatch(const std::vector<ReplicationLog>& logs);

  Status ReceiveReplication(const ReplicationLog& log);

  ReplicationStatus GetStatus(const std::string& dc_id) const;
  std::map<std::string, ReplicationStatus> GetAllStatuses() const;

  Status SyncWithDC(const std::string& dc_id);

  Status ResolveConflict(const std::string& key,
                         const std::vector<ReplicationLog>& conflicting_logs);

  void SetStorage(cedar::CedarGraphStorage* storage) { storage_ = storage; }

  void SetReplicationCallback(ReplicationCallback callback);

 private:
  void ReplicationLoop();
  void ProcessReplicationQueue();
  Status ReplicateToDC(const ReplicationLog& log, const std::string& dc_id);
  Status SendToRemoteDC(const ReplicationLog& log, const std::string& dc_id);
  Status SendToRemoteDCWithRetry(const ReplicationLog& log,
                                  const std::string& dc_id);
  void DrainPendingQueue();
  void UpdateLag(const std::string& dc_id);
  ReplicationLog CreateTimestampBasedResolution(
      const std::vector<ReplicationLog>& logs);

  struct PendingLog {
    ReplicationLog log;
    std::string dc_id;
    uint32_t retry_count{0};
    std::chrono::steady_clock::time_point next_attempt;
  };
  std::deque<PendingLog> pending_queue_;
  std::mutex pending_mutex_;

  DCReplicationConfig config_;
  std::string local_dc_id_;
  std::vector<std::string> peer_dcs_;
  
  std::atomic<uint64_t> sequence_counter_{0};
  
  mutable std::mutex queue_mutex_;
  std::queue<ReplicationLog> replication_queue_;
  
  mutable std::mutex status_mutex_;
  std::map<std::string, ReplicationStatus> dc_statuses_;
  
  std::atomic<bool> running_{false};
  std::thread replication_thread_;
  
  std::map<std::string, std::shared_ptr<grpc::Channel>> dc_channels_;
  cedar::CedarGraphStorage* storage_ = nullptr;

  mutable std::mutex callback_mutex_;
  ReplicationCallback replication_callback_;
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_CROSS_DC_REPLICATOR_H_
