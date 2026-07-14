// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#include "cedar/gcn/storage_cdc_client.h"

#include <string>

namespace cedar::gcn {

StorageCdcClient::StorageCdcClient(std::shared_ptr<grpc::Channel> channel,
                                   Options options)
    : options_(options),
      stub_(cedar::storage::StorageService::NewStub(std::move(channel))) {}

StatusOr<cedar::storage::GetChangeLogStateResponse>
StorageCdcClient::GetState(uint32_t partition_id, uint64_t expected_epoch) {
  grpc::ClientContext context;
  ContextRegistration registration(this, &context);
  ConfigureContext(&context);

  cedar::storage::GetChangeLogStateRequest request;
  request.set_partition_id(partition_id);
  request.set_expected_epoch(expected_epoch);

  cedar::storage::GetChangeLogStateResponse response;
  grpc::Status rpc_status = stub_->GetChangeLogState(&context, request,
                                                      &response);
  if (!rpc_status.ok()) {
    return MapGrpcStatus(rpc_status);
  }
  if (!response.success() || response.error_code() != cedar::storage::CDC_OK) {
    return MapCdcError(response.error_code(), response.error_msg());
  }
  return response;
}

StatusOr<cedar::storage::FetchChangesResponse> StorageCdcClient::Fetch(
    uint32_t partition_id, uint64_t after_offset, uint64_t expected_epoch) {
  grpc::ClientContext context;
  ContextRegistration registration(this, &context);
  ConfigureContext(&context);

  cedar::storage::FetchChangesRequest request;
  request.set_partition_id(partition_id);
  request.set_after_offset(after_offset);
  request.set_expected_epoch(expected_epoch);
  request.set_limit_records(options_.max_records);
  request.set_limit_bytes(options_.max_bytes);

  cedar::storage::FetchChangesResponse response;
  grpc::Status rpc_status = stub_->FetchChanges(&context, request, &response);
  if (!rpc_status.ok()) {
    return MapGrpcStatus(rpc_status);
  }
  if (!response.success() || response.error_code() != cedar::storage::CDC_OK) {
    return MapCdcError(response.error_code(), response.error_msg());
  }
  Status validation = ValidateFetchResponse(response);
  if (!validation.ok()) {
    return validation;
  }
  return response;
}

Status StorageCdcClient::StreamSnapshot(
    uint32_t partition_id, uint64_t snapshot_version,
    const std::function<Status(
        const cedar::storage::ComputeSnapshotBatch&)>& on_batch) {
  if (!on_batch) {
    return Status::InvalidArgument("snapshot callback is empty");
  }

  grpc::ClientContext context;
  ContextRegistration registration(this, &context);
  ConfigureContext(&context);

  cedar::storage::GetComputeSnapshotRequest request;
  request.set_partition_id(partition_id);
  request.set_snapshot_version(snapshot_version);
  request.set_resume_offset(0);
  request.set_limit_records(options_.max_records);
  request.set_limit_bytes(options_.max_bytes);
  request.set_expected_epoch(0);

  auto reader = stub_->GetComputeSnapshot(&context, request);
  cedar::storage::ComputeSnapshotBatch batch;
  while (reader->Read(&batch)) {
    CEDAR_RETURN_IF_ERROR(ValidateSnapshotBatch(batch));
    CEDAR_RETURN_IF_ERROR(on_batch(batch));
  }
  return MapGrpcStatus(reader->Finish());
}

void StorageCdcClient::Cancel() {
  std::lock_guard<std::mutex> lock(contexts_mutex_);
  for (grpc::ClientContext* context : active_contexts_) {
    if (context) {
      context->TryCancel();
    }
  }
}

StorageCdcClient::ContextRegistration::ContextRegistration(
    StorageCdcClient* client, grpc::ClientContext* context)
    : client_(client), context_(context) {
  client_->RegisterContext(context_);
}

StorageCdcClient::ContextRegistration::~ContextRegistration() {
  client_->UnregisterContext(context_);
}

void StorageCdcClient::RegisterContext(grpc::ClientContext* context) {
  std::lock_guard<std::mutex> lock(contexts_mutex_);
  active_contexts_.insert(context);
}

void StorageCdcClient::UnregisterContext(grpc::ClientContext* context) {
  std::lock_guard<std::mutex> lock(contexts_mutex_);
  active_contexts_.erase(context);
}

void StorageCdcClient::ConfigureContext(grpc::ClientContext* context) const {
  context->set_deadline(std::chrono::system_clock::now() +
                        options_.rpc_timeout);
  if (options_.auth_token_provider) {
    const std::string token = options_.auth_token_provider();
    if (!token.empty()) {
      context->AddMetadata("authorization", "Bearer " + token);
    }
  }
}

Status StorageCdcClient::MapGrpcStatus(const grpc::Status& status) const {
  if (status.ok()) {
    return Status::OK();
  }
  const std::string message = status.error_message().empty()
                                  ? "StorageD CDC RPC failed"
                                  : status.error_message();
  switch (status.error_code()) {
    case grpc::StatusCode::DEADLINE_EXCEEDED:
    case grpc::StatusCode::UNAVAILABLE:
      return Status::Unavailable(message);
    case grpc::StatusCode::CANCELLED:
      return Status::Cancelled(message);
    case grpc::StatusCode::INVALID_ARGUMENT:
      return Status::InvalidArgument(message);
    case grpc::StatusCode::NOT_FOUND:
      return Status::NotFound(message);
    case grpc::StatusCode::RESOURCE_EXHAUSTED:
      return Status::ResourceExhausted(message);
    default:
      return Status::IOError(message);
  }
}

Status StorageCdcClient::MapCdcError(cedar::storage::CdcErrorCode code,
                                     const std::string& message) const {
  const std::string detail =
      message.empty() ? "StorageD CDC request failed" : message;
  switch (code) {
    case cedar::storage::CDC_OK:
      return Status::Corruption("StorageD CDC response marked failure with CDC_OK");
    case cedar::storage::CDC_STALE_EPOCH:
      return Status::Conflict(detail);
    case cedar::storage::CDC_INVALID_LIMIT:
      return Status::InvalidArgument(detail);
    case cedar::storage::CDC_PARTITION_NOT_FOUND:
      return Status::NotFound(detail);
    case cedar::storage::CDC_UNAVAILABLE:
      return Status::Unavailable(detail);
    default:
      return Status::IOError(detail);
  }
}

Status StorageCdcClient::ValidateFetchResponse(
    const cedar::storage::FetchChangesResponse& response) const {
  if (static_cast<uint32_t>(response.records_size()) > options_.max_records) {
    return Status::ResourceExhausted(
        "StorageD CDC response exceeded max_records");
  }
  uint64_t bytes = 0;
  for (const auto& record : response.records()) {
    bytes += static_cast<uint64_t>(record.ByteSizeLong());
    if (bytes > options_.max_bytes) {
      return Status::ResourceExhausted(
          "StorageD CDC response exceeded max_bytes");
    }
  }
  return Status::OK();
}

Status StorageCdcClient::ValidateSnapshotBatch(
    const cedar::storage::ComputeSnapshotBatch& batch) const {
  if (static_cast<uint32_t>(batch.records_size()) > options_.max_records) {
    return Status::ResourceExhausted(
        "StorageD snapshot batch exceeded max_records");
  }
  uint64_t bytes = 0;
  for (const auto& record : batch.records()) {
    bytes += static_cast<uint64_t>(record.ByteSizeLong());
    if (bytes > options_.max_bytes) {
      return Status::ResourceExhausted(
          "StorageD snapshot batch exceeded max_bytes");
    }
  }
  return Status::OK();
}

}  // namespace cedar::gcn
