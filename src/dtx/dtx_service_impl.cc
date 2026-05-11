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

#include "cedar/dtx/dtx_service_impl.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

namespace cedar {
namespace dtx {

DTXServiceImpl::DTXServiceImpl(cedar::CedarGraphStorage* storage)
    : storage_(storage) {}

::grpc::Status DTXServiceImpl::Replicate(
    ::grpc::ServerContext* context,
    const cedar::dtx::ReplicateRequest* request,
    cedar::dtx::ReplicateResponse* response) {
  (void)context;
  uint64_t last_sequence = 0;
  for (const auto& log : request->logs()) {
    auto s = ApplySingleLog(log);
    if (!s.ok()) {
      response->set_success(false);
      response->set_error_msg(s.ToString());
      return ::grpc::Status::OK;
    }
    last_sequence = log.sequence_num();
  }
  response->set_success(true);
  response->set_last_applied_sequence(last_sequence);
  return ::grpc::Status::OK;
}

::grpc::Status DTXServiceImpl::ApplyReplication(
    ::grpc::ServerContext* context,
    ::grpc::ServerReader<cedar::dtx::ApplyReplicationRequest>* reader,
    cedar::dtx::ApplyReplicationResponse* response) {
  (void)context;
  cedar::dtx::ApplyReplicationRequest request;
  uint64_t last_sequence = 0;
  while (reader->Read(&request)) {
    for (const auto& log : request.logs()) {
      auto s = ApplySingleLog(log);
      if (!s.ok()) {
        response->set_success(false);
        response->set_error_msg(s.ToString());
        return ::grpc::Status::OK;
      }
      last_sequence = log.sequence_num();
    }
  }
  response->set_success(true);
  response->set_last_applied_sequence(last_sequence);
  return ::grpc::Status::OK;
}

Status DTXServiceImpl::ApplySingleLog(const cedar::dtx::ReplicationLogEntry& log_entry) {
  if (!storage_) {
    return Status::IOError("Storage not available");
  }

  // Deserialize CedarKey from bytes
  if (log_entry.key().size() < sizeof(::cedar::CedarKey)) {
    return Status::InvalidArgument("Invalid key size in replication log");
  }
  auto key_opt = ::cedar::CedarKey::Decode(log_entry.key());
  if (!key_opt.has_value()) {
    return Status::InvalidArgument("Failed to decode CedarKey");
  }
  ::cedar::CedarKey key = key_opt.value();

  // Deserialize Descriptor from bytes (8 bytes = uint64_t)
  if (log_entry.value().size() != sizeof(uint64_t)) {
    return Status::InvalidArgument("Invalid descriptor size in replication log");
  }
  uint64_t desc_raw;
  std::memcpy(&desc_raw, log_entry.value().data(), sizeof(uint64_t));
  Descriptor desc(desc_raw);

  Timestamp ts(log_entry.timestamp());

  // Apply to local storage
  auto s = storage_->Put(key.entity_id(), key.timestamp().value(), desc, ts);
  if (!s.ok()) {
    return Status::IOError("Storage Put failed: " + s.ToString());
  }
  return Status::OK();
}

// Stub implementations for 2PC methods
::grpc::Status DTXServiceImpl::Prepare(::grpc::ServerContext* context,
                                       const cedar::dtx::PrepareRequest* request,
                                       cedar::dtx::PrepareResponse* response) {
  (void)context; (void)request;
  response->set_success(false);
  response->set_error_msg("Prepare not implemented in DTXService; use StorageService");
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "Use StorageService");
}

::grpc::Status DTXServiceImpl::Commit(::grpc::ServerContext* context,
                                      const cedar::dtx::CommitRequest* request,
                                      cedar::dtx::CommitResponse* response) {
  (void)context; (void)request;
  response->set_success(false);
  response->set_error_msg("Commit not implemented in DTXService; use StorageService");
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "Use StorageService");
}

::grpc::Status DTXServiceImpl::Abort(::grpc::ServerContext* context,
                                     const cedar::dtx::AbortRequest* request,
                                     cedar::dtx::AbortResponse* response) {
  (void)context; (void)request;
  response->set_success(false);
  response->set_error_msg("Abort not implemented in DTXService; use StorageService");
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "Use StorageService");
}

::grpc::Status DTXServiceImpl::Inquire(::grpc::ServerContext* context,
                                       const cedar::dtx::InquireRequest* request,
                                       cedar::dtx::InquireResponse* response) {
  (void)context; (void)request;
  response->set_state(cedar::dtx::InquireResponse_TxnState_UNKNOWN);
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "Use StorageService");
}

::grpc::Status DTXServiceImpl::RegisterParticipant(::grpc::ServerContext* context,
                                                   const cedar::dtx::RegisterRequest* request,
                                                   cedar::dtx::RegisterResponse* response) {
  (void)context; (void)request;
  response->set_success(false);
  response->set_error_msg("RegisterParticipant not implemented");
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "Not implemented");
}

}  // namespace dtx
}  // namespace cedar
