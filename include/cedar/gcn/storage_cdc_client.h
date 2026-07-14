// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#ifndef CEDAR_GCN_STORAGE_CDC_CLIENT_H_
#define CEDAR_GCN_STORAGE_CDC_CLIENT_H_

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <set>

#include <grpcpp/grpcpp.h>

#include "cedar/core/status.h"
#include "storage_service.grpc.pb.h"

namespace cedar::gcn {

class StorageCdcSource {
 public:
  virtual ~StorageCdcSource() = default;

  virtual StatusOr<cedar::storage::GetChangeLogStateResponse> GetState(
      uint32_t partition_id, uint64_t expected_epoch) = 0;
  virtual StatusOr<cedar::storage::FetchChangesResponse> Fetch(
      uint32_t partition_id, uint64_t after_offset,
      uint64_t expected_epoch) = 0;
  virtual Status StreamSnapshot(
      uint32_t partition_id, uint64_t snapshot_version,
      const std::function<Status(
          const cedar::storage::ComputeSnapshotBatch&)>& on_batch) = 0;
  virtual void Cancel() = 0;
};

class StorageCdcClient : public StorageCdcSource {
 public:
  struct Options {
    std::chrono::milliseconds rpc_timeout{3000};
    uint32_t max_records = 1024;
    uint64_t max_bytes = 4ULL * 1024ULL * 1024ULL;
    std::function<std::string()> auth_token_provider;
  };

  StorageCdcClient(std::shared_ptr<grpc::Channel> channel, Options options);

  StatusOr<cedar::storage::GetChangeLogStateResponse> GetState(
      uint32_t partition_id, uint64_t expected_epoch) override;
  StatusOr<cedar::storage::FetchChangesResponse> Fetch(
      uint32_t partition_id, uint64_t after_offset,
      uint64_t expected_epoch) override;
  Status StreamSnapshot(
      uint32_t partition_id, uint64_t snapshot_version,
      const std::function<Status(
          const cedar::storage::ComputeSnapshotBatch&)>& on_batch) override;
  void Cancel() override;

 private:
  class ContextRegistration {
   public:
    ContextRegistration(StorageCdcClient* client, grpc::ClientContext* context);
    ~ContextRegistration();

    ContextRegistration(const ContextRegistration&) = delete;
    ContextRegistration& operator=(const ContextRegistration&) = delete;

   private:
    StorageCdcClient* client_;
    grpc::ClientContext* context_;
  };

  void RegisterContext(grpc::ClientContext* context);
  void UnregisterContext(grpc::ClientContext* context);
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
  mutable std::mutex contexts_mutex_;
  std::set<grpc::ClientContext*> active_contexts_;
};

}  // namespace cedar::gcn

#endif  // CEDAR_GCN_STORAGE_CDC_CLIENT_H_
