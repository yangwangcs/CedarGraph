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

// =============================================================================
// DTX RPC Client - Real gRPC Implementation for 2PC Protocol
// =============================================================================

#ifndef CEDAR_DTX_DTX_RPC_CLIENT_H_
#define CEDAR_DTX_DTX_RPC_CLIENT_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <mutex>
#include <atomic>
#include <functional>
#include <future>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "cedar/core/status.h"
#include "cedar/core/threading.h"
#include "cedar/dtx/raft/grpc_tls.h"
#include "cedar/dtx/types.h"
#include "dtx_protocol.grpc.pb.h"

namespace cedar {
namespace dtx {

// Helper: create gRPC channel credentials from environment
StatusOr<std::shared_ptr<grpc::ChannelCredentials>> CreateClientCredentialsFromEnv();

// =============================================================================
// DTX RPC Client Configuration
// =============================================================================

struct DTXRpcConfig {
  // RPC timeout in milliseconds
  uint64_t rpc_timeout_ms = 5000;

  // Connection pool size per endpoint
  size_t max_channels_per_endpoint = 4;

  // Thread pool size for parallel RPCs
  size_t max_rpc_threads = 64;

  // Retry configuration
  bool enable_retries = true;
  int max_retry_attempts = 3;

  // Health check interval
  uint64_t health_check_interval_ms = 1000;

  // TLS configuration for DTX replication
  raft::TlsConfig tls_config;

  // Development-only escape hatch. Production deployments should keep TLS on.
  bool allow_insecure = false;
};

// =============================================================================
// Participant Information
// =============================================================================

struct ParticipantInfo {
  NodeID id;
  std::string endpoint;
  bool available{true};

  // Health tracking
  std::chrono::steady_clock::time_point last_health_check;
  int consecutive_failures{0};

  // Connection state
  std::shared_ptr<grpc::Channel> channel;
  std::shared_ptr<cedar::dtx::DTXService::Stub> stub;
};

// =============================================================================
// DTX RPC Client
// =============================================================================

class DTXRpcClient {
 public:
  DTXRpcClient(const DTXRpcConfig& config);
  ~DTXRpcClient();

  // ===========================================================================
  // Connection Management
  // ===========================================================================

  Status AddParticipant(NodeID id, const std::string& endpoint);
  Status RemoveParticipant(NodeID id);
  void UpdateEndpoint(NodeID id, const std::string& new_endpoint);

  // ===========================================================================
  // 2PC Protocol RPCs (using proto types directly)
  // ===========================================================================

  // Phase 1: Prepare
  Status Prepare(NodeID participant_id,
                 const std::string& txn_id,
                 const std::string& coordinator_id,
                 uint64_t prepare_version,
                 const std::vector<cedar::dtx::CedarKey>& reads,
                 const std::vector<cedar::dtx::CedarKey>& writes,
                 int32_t isolation_level,
                 int64_t timeout_ms,
                 cedar::dtx::PrepareResponse* response);

  // Phase 2: Commit
  Status Commit(NodeID participant_id,
                const std::string& txn_id,
                const std::string& coordinator_id,
                uint64_t commit_version,
                cedar::dtx::CommitResponse* response);

  // Phase 2: Abort
  Status Abort(NodeID participant_id,
               const std::string& txn_id,
               const std::string& coordinator_id,
               const std::string& reason,
               cedar::dtx::AbortResponse* response);

  // State Inquiry
  Status Inquire(NodeID participant_id,
                 const std::string& txn_id,
                 cedar::dtx::InquireResponse* response);

  // ===========================================================================
  // Parallel Execution Support
  // ===========================================================================

  std::vector<std::pair<NodeID, cedar::dtx::PrepareResponse>> PrepareAll(
      const std::vector<NodeID>& participant_ids,
      const std::string& txn_id,
      const std::string& coordinator_id,
      uint64_t prepare_version,
      const std::vector<cedar::dtx::CedarKey>& reads,
      const std::vector<cedar::dtx::CedarKey>& writes,
      int32_t isolation_level,
      int64_t timeout_ms);

  std::vector<std::pair<NodeID, cedar::dtx::CommitResponse>> CommitAll(
      const std::vector<NodeID>& participant_ids,
      const std::string& txn_id,
      const std::string& coordinator_id,
      uint64_t commit_version);

  std::vector<std::pair<NodeID, cedar::dtx::AbortResponse>> AbortAll(
      const std::vector<NodeID>& participant_ids,
      const std::string& txn_id,
      const std::string& coordinator_id,
      const std::string& reason);

  // ===========================================================================
  // Health Management
  // ===========================================================================

  bool IsParticipantHealthy(NodeID id) const;
  void MarkParticipantUnhealthy(NodeID id);
  void MarkParticipantHealthy(NodeID id);

  std::vector<NodeID> GetHealthyParticipants() const;
  size_t GetParticipantCount() const;

 private:
  std::shared_ptr<DTXService::Stub> GetStub(NodeID id);

  DTXRpcConfig config_;
  mutable std::mutex participants_mutex_;
  std::unordered_map<NodeID, std::shared_ptr<ParticipantInfo>> participants_;
  std::shared_ptr<grpc::ChannelCredentials> credentials_;
  mutable std::atomic<size_t> round_robin_index_{0};
  std::unique_ptr<cedar::ThreadPool> thread_pool_;
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_DTX_RPC_CLIENT_H_
