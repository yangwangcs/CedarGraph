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

#ifndef CEDAR_DTX_SERVICE_IMPL_H_
#define CEDAR_DTX_SERVICE_IMPL_H_

#include "cedar/dtx/cross_dc_replicator.h"
#include "dtx_protocol.grpc.pb.h"
#include "cedar/storage/cedar_graph_storage.h"
#include <grpcpp/grpcpp.h>

// NEW includes for participant registry
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace cedar {
namespace dtx {

// Forward declaration to avoid circular include
class StorageServiceImpl;

// NEW: participant record for in-memory registry
struct ParticipantRecord {
  std::string participant_id;
  std::string endpoint;
  cedar::dtx::RegisterRequest::Role role;
};

class DTXServiceImpl final : public cedar::dtx::DTXService::Service {
 public:
  explicit DTXServiceImpl(cedar::CedarGraphStorage* storage,
                         cedar::dtx::StorageServiceImpl* storage_service = nullptr);

  void SetCrossDCReplicator(CrossDCReplicator* replicator) {
    cross_dc_replicator_ = replicator;
  }

  // NEW: set the directory where participant registration logs are persisted
  void SetParticipantLogPath(const std::string& path) {
    participant_log_path_ = path;
  }

  // Test-only accessor for verifying registry state
  size_t ParticipantCountForTest(const std::string& txn_id) const;

  ::grpc::Status Prepare(::grpc::ServerContext* context,
                         const cedar::dtx::PrepareRequest* request,
                         cedar::dtx::PrepareResponse* response) override;
  ::grpc::Status Commit(::grpc::ServerContext* context,
                        const cedar::dtx::CommitRequest* request,
                        cedar::dtx::CommitResponse* response) override;
  ::grpc::Status Abort(::grpc::ServerContext* context,
                       const cedar::dtx::AbortRequest* request,
                       cedar::dtx::AbortResponse* response) override;
  ::grpc::Status Inquire(::grpc::ServerContext* context,
                         const cedar::dtx::InquireRequest* request,
                         cedar::dtx::InquireResponse* response) override;
  ::grpc::Status RegisterParticipant(::grpc::ServerContext* context,
                                     const cedar::dtx::RegisterRequest* request,
                                     cedar::dtx::RegisterResponse* response) override;
  ::grpc::Status Replicate(::grpc::ServerContext* context,
                           const cedar::dtx::ReplicateRequest* request,
                           cedar::dtx::ReplicateResponse* response) override;
  ::grpc::Status ApplyReplication(
      ::grpc::ServerContext* context,
      ::grpc::ServerReader<cedar::dtx::ApplyReplicationRequest>* reader,
      cedar::dtx::ApplyReplicationResponse* response) override;

 private:
  cedar::CedarGraphStorage* storage_;
  cedar::dtx::StorageServiceImpl* storage_service_ = nullptr;
  CrossDCReplicator* cross_dc_replicator_ = nullptr;

  Status ApplySingleLog(const cedar::dtx::ReplicationLogEntry& log_entry);

  // NEW: participant registry
  mutable std::mutex participants_mutex_;
  std::unordered_map<std::string, std::vector<ParticipantRecord>> participants_;
  std::string participant_log_path_;

  Status PersistParticipantRegistration(const cedar::dtx::RegisterRequest& request);

  // Remove all participant records for a completed transaction
  void CleanupParticipants(const std::string& txn_id);
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_SERVICE_IMPL_H_
