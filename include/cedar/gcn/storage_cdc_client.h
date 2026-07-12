// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#ifndef CEDAR_GCN_STORAGE_CDC_CLIENT_H_
#define CEDAR_GCN_STORAGE_CDC_CLIENT_H_

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>

#include <grpcpp/grpcpp.h>

#include "cedar/core/status.h"
#include "storage_service.grpc.pb.h"

namespace cedar::gcn {

class StorageCdcClient {
 public:
  struct Options {
    std::chrono::milliseconds rpc_timeout{3000};
    uint32_t max_records = 1024;
    uint64_t max_bytes = 4ULL * 1024ULL * 1024ULL;
    std::function<std::string()> auth_token_provider;
  };

  StorageCdcClient(std::shared_ptr<grpc::Channel> channel, Options options);

  StatusOr<cedar::storage::GetChangeLogStateResponse> GetState(
      uint32_t partition_id, uint64_t expected_epoch);
  StatusOr<cedar::storage::FetchChangesResponse> Fetch(
      uint32_t partition_id, uint64_t after_offset, uint64_t expected_epoch);
  Status StreamSnapshot(
      uint32_t partition_id, uint64_t snapshot_version,
      const std::function<Status(
          const cedar::storage::ComputeSnapshotBatch&)>& on_batch);

 private:
  void ConfigureContext(grpc::ClientContext* context) const;
  Status MapGrpcStatus(const grpc::Status& status) const;
  Status MapCdcError(cedar::storage::CdcErrorCode code,
                     const std::string& message) const;
  Status ValidateFetchResponse(
      const cedar::storage::FetchChangesResponse& response) const;
  Status ValidateSnapshotBatch(
      const cedar::storage::ComputeSnapshotBatch& batch) const;

  Options options_;
  std::unique_ptr<cedar::storage::StorageService::Stub> stub_;
};

}  // namespace cedar::gcn

#endif  // CEDAR_GCN_STORAGE_CDC_CLIENT_H_
