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

#include "cedar/dtx/storage_service_impl.h"
#include "cedar/dtx/dtx_service_impl.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include <iostream>
#include "storage_service.pb.h"

#include <functional>
#include <string>

// NEW includes for participant persistence
#include <chrono>
#include <filesystem>
#include <fstream>
#include <mutex>

namespace cedar {
namespace dtx {

namespace {

uint64_t ConvertTxnId(const std::string& txn_id_str) {
  try {
    return std::stoull(txn_id_str);
  } catch (...) {
    std::cerr << "[DtxService] Failed to parse txn_id: " << txn_id_str << std::endl;
    return std::hash<std::string>{}(txn_id_str);
  }
}

void CopyCedarKey(const cedar::dtx::CedarKey& src, cedar::storage::CedarKey* dst) {
  dst->set_entity_id(src.entity_id());
  dst->set_timestamp(src.timestamp());
}

uint64_t GetCommitTs(const cedar::dtx::PrepareRequest& dtx_req) {
  if (dtx_req.has_timestamp_info()) {
    return dtx_req.timestamp_info().wall_time();
  }
  return dtx_req.prepare_version();
}

uint64_t GetCommitTs(const cedar::dtx::CommitRequest& dtx_req) {
  if (dtx_req.has_timestamp_info()) {
    return dtx_req.timestamp_info().wall_time();
  }
  return dtx_req.commit_version();
}

}  // namespace

DTXServiceImpl::DTXServiceImpl(cedar::CedarGraphStorage* storage,
                               cedar::dtx::StorageServiceImpl* storage_service)
    : storage_(storage), storage_service_(storage_service) {}

::grpc::Status DTXServiceImpl::Replicate(
    ::grpc::ServerContext* context,
    const cedar::dtx::ReplicateRequest* request,
    cedar::dtx::ReplicateResponse* response) {
  (void)context;
  uint64_t last_sequence = 0;
  for (const auto& log : request->logs()) {
    Status s;
    if (cross_dc_replicator_) {
      ReplicationLog replication_log;
      replication_log.sequence_num = log.sequence_num();
      auto key_opt = ::cedar::CedarKey::Decode(log.key());
      if (!key_opt.has_value()) {
        s = Status::InvalidArgument("Failed to decode CedarKey");
      } else {
        replication_log.key = key_opt.value();
        if (log.value().size() != sizeof(uint64_t)) {
          s = Status::InvalidArgument("Invalid descriptor size in replication log");
        } else {
          uint64_t desc_raw;
          std::memcpy(&desc_raw, log.value().data(), sizeof(uint64_t));
          replication_log.value = Descriptor(desc_raw);
          replication_log.timestamp = Timestamp(log.timestamp());
          replication_log.source_dc = log.source_dc();
          for (const auto& target : log.target_dcs()) {
            replication_log.target_dcs.push_back(target);
          }
          s = cross_dc_replicator_->ReceiveReplication(replication_log);
        }
      }
    } else {
      s = ApplySingleLog(log);
    }
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
      Status s;
      if (cross_dc_replicator_) {
        ReplicationLog replication_log;
        replication_log.sequence_num = log.sequence_num();
        auto key_opt = ::cedar::CedarKey::Decode(log.key());
        if (!key_opt.has_value()) {
          s = Status::InvalidArgument("Failed to decode CedarKey");
        } else {
          replication_log.key = key_opt.value();
          if (log.value().size() != sizeof(uint64_t)) {
            s = Status::InvalidArgument("Invalid descriptor size in replication log");
          } else {
            uint64_t desc_raw;
            std::memcpy(&desc_raw, log.value().data(), sizeof(uint64_t));
            replication_log.value = Descriptor(desc_raw);
            replication_log.timestamp = Timestamp(log.timestamp());
            replication_log.source_dc = log.source_dc();
            for (const auto& target : log.target_dcs()) {
              replication_log.target_dcs.push_back(target);
            }
            s = cross_dc_replicator_->ReceiveReplication(replication_log);
          }
        }
      } else {
        s = ApplySingleLog(log);
      }
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

::grpc::Status DTXServiceImpl::Prepare(::grpc::ServerContext* context,
                                       const cedar::dtx::PrepareRequest* request,
                                       cedar::dtx::PrepareResponse* response) {
  if (!storage_service_) {
    response->set_success(false);
    response->set_error_msg("Prepare not implemented in DTXService; use StorageService");
    return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "Use StorageService");
  }

  cedar::storage::PrepareRequest storage_req;
  storage_req.set_txn_id(ConvertTxnId(request->txn_id()));
  storage_req.set_commit_ts(GetCommitTs(*request));

  for (const auto& key : request->reads()) {
    CopyCedarKey(key, storage_req.add_read_set());
  }
  for (const auto& key : request->writes()) {
    CopyCedarKey(key, storage_req.add_write_set());
  }

  cedar::storage::PrepareResponse storage_resp;
  auto grpc_status = storage_service_->Prepare(context, &storage_req, &storage_resp);
  if (!grpc_status.ok()) {
    response->set_success(false);
    response->set_error_msg(grpc_status.error_message());
    return grpc_status;
  }

  response->set_success(storage_resp.prepared());
  response->set_vote_commit(storage_resp.prepared());
  response->set_can_prepare(storage_resp.prepared());
  response->set_error_msg(storage_resp.error_msg());
  return ::grpc::Status::OK;
}

::grpc::Status DTXServiceImpl::Commit(::grpc::ServerContext* context,
                                      const cedar::dtx::CommitRequest* request,
                                      cedar::dtx::CommitResponse* response) {
  if (!storage_service_) {
    response->set_success(false);
    response->set_error_msg("Commit not implemented in DTXService; use StorageService");
    return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "Use StorageService");
  }

  cedar::storage::CommitRequest storage_req;
  storage_req.set_txn_id(ConvertTxnId(request->txn_id()));
  storage_req.set_commit_ts(GetCommitTs(*request));

  cedar::storage::CommitResponse storage_resp;
  auto grpc_status = storage_service_->Commit(context, &storage_req, &storage_resp);
  if (!grpc_status.ok()) {
    response->set_success(false);
    response->set_error_msg(grpc_status.error_message());
    return grpc_status;
  }

  response->set_success(storage_resp.success());
  response->set_error_msg(storage_resp.error_msg());
  return ::grpc::Status::OK;
}

::grpc::Status DTXServiceImpl::Abort(::grpc::ServerContext* context,
                                     const cedar::dtx::AbortRequest* request,
                                     cedar::dtx::AbortResponse* response) {
  if (!storage_service_) {
    response->set_success(false);
    response->set_error_msg("Abort not implemented in DTXService; use StorageService");
    return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "Use StorageService");
  }

  cedar::storage::AbortRequest storage_req;
  storage_req.set_txn_id(ConvertTxnId(request->txn_id()));

  cedar::storage::AbortResponse storage_resp;
  auto grpc_status = storage_service_->Abort(context, &storage_req, &storage_resp);
  if (!grpc_status.ok()) {
    response->set_success(false);
    response->set_error_msg(grpc_status.error_message());
    return grpc_status;
  }

  response->set_success(storage_resp.success());
  response->set_error_msg(storage_resp.error_msg());
  return ::grpc::Status::OK;
}

::grpc::Status DTXServiceImpl::Inquire(::grpc::ServerContext* context,
                                       const cedar::dtx::InquireRequest* request,
                                       cedar::dtx::InquireResponse* response) {
  if (!storage_service_) {
    response->set_state(cedar::dtx::InquireResponse_TxnState_UNKNOWN);
    return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "Use StorageService");
  }

  cedar::storage::InquireRequest storage_req;
  storage_req.set_txn_id(ConvertTxnId(request->txn_id()));

  cedar::storage::InquireResponse storage_resp;
  auto grpc_status = storage_service_->Inquire(context, &storage_req, &storage_resp);
  if (!grpc_status.ok()) {
    response->set_state(cedar::dtx::InquireResponse_TxnState_UNKNOWN);
    response->set_details(grpc_status.error_message());
    return grpc_status;
  }

  cedar::dtx::InquireResponse_TxnState dtx_state;
  switch (storage_resp.state()) {
    case cedar::storage::InquireResponse_TxnState_PREPARED:
      dtx_state = cedar::dtx::InquireResponse_TxnState_PREPARED;
      break;
    case cedar::storage::InquireResponse_TxnState_COMMITTED:
      dtx_state = cedar::dtx::InquireResponse_TxnState_COMMITTED;
      break;
    case cedar::storage::InquireResponse_TxnState_ABORTED:
      dtx_state = cedar::dtx::InquireResponse_TxnState_ABORTED;
      break;
    case cedar::storage::InquireResponse_TxnState_INCONSISTENT:
    case cedar::storage::InquireResponse_TxnState_UNKNOWN:
    default:
      dtx_state = cedar::dtx::InquireResponse_TxnState_UNKNOWN;
      break;
  }
  response->set_txn_id(request->txn_id());
  response->set_state(dtx_state);
  response->set_details(storage_resp.error_msg());
  return ::grpc::Status::OK;
}

::grpc::Status DTXServiceImpl::RegisterParticipant(::grpc::ServerContext* context,
                                                   const cedar::dtx::RegisterRequest* request,
                                                   cedar::dtx::RegisterResponse* response) {
  (void)context;

  // Validate required fields
  if (request->txn_id().empty()) {
    response->set_success(false);
    response->set_error_msg("txn_id is required");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "txn_id is required");
  }
  if (request->participant_id().empty()) {
    response->set_success(false);
    response->set_error_msg("participant_id is required");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "participant_id is required");
  }

  // Build record
  ParticipantRecord record;
  record.participant_id = request->participant_id();
  record.endpoint = request->endpoint();
  record.role = request->role();

  // Add to in-memory registry
  {
    std::lock_guard<std::mutex> lock(participants_mutex_);
    participants_[request->txn_id()].push_back(record);
  }

  // Persist to decision log (best-effort; do not fail RPC on log write failure)
  auto persist_status = PersistParticipantRegistration(*request);
  if (!persist_status.ok()) {
    std::cerr << "[DTXServiceImpl] Failed to persist participant registration for txn="
              << request->txn_id() << ": " << persist_status.ToString() << std::endl;
  }

  response->set_success(true);
  response->set_assigned_id(request->participant_id());
  return ::grpc::Status::OK;
}

Status DTXServiceImpl::PersistParticipantRegistration(
    const cedar::dtx::RegisterRequest& request) {
  if (participant_log_path_.empty()) {
    return Status::OK();  // persistence disabled
  }

  std::string log_file = participant_log_path_ + "/participant_registry.log";
  std::ofstream ofs(log_file, std::ios::app);
  if (!ofs) {
    return Status::IOError("Cannot open participant log: " + log_file);
  }

  // Format: txn_id|participant_id|endpoint|role|timestamp_us
  auto now_us = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count();
  ofs << request.txn_id() << "|"
      << request.participant_id() << "|"
      << request.endpoint() << "|"
      << static_cast<int>(request.role()) << "|"
      << now_us << "\n";
  ofs.flush();
  if (!ofs) {
    return Status::IOError("Failed to write participant log");
  }
  return Status::OK();
}

}  // namespace dtx
}  // namespace cedar
