// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#include "cedar/gcn/snapshot_loader.h"

#include <limits>
#include <sstream>
#include <string>
#include <vector>

#include "cedar/core/crc32c.h"

namespace cedar::gcn {
namespace {

uint32_t SnapshotChecksum(const cedar::storage::ComputeSnapshotBatch& batch) {
  cedar::storage::ComputeSnapshotBatch copy = batch;
  copy.clear_checksum();
  std::string encoded;
  if (!copy.SerializeToString(&encoded)) {
    return 0;
  }
  uint32_t checksum = cedar::crc32c::Value(encoded.data(), encoded.size());
  return checksum == 0 ? 1 : checksum;
}

bool ChecksumMatches(const cedar::storage::ComputeSnapshotBatch& batch) {
  if (batch.checksum() == 0) {
    return true;
  }
  return batch.checksum() == SnapshotChecksum(batch);
}

}  // namespace

SnapshotLoader::SnapshotLoader(StorageCdcSource* source, TMVEngine* live_engine,
                               EventApplier* live_applier, Options options)
    : source_(source),
      live_engine_(live_engine),
      live_applier_(live_applier),
      options_(options) {}

StatusOr<SnapshotLoadResult> SnapshotLoader::Load(uint32_t partition_id,
                                                  uint64_t expected_epoch,
                                                  uint64_t snapshot_version,
                                                  uint64_t high_watermark) {
  if (!source_ || !live_engine_ || !live_applier_) {
    return Status::InvalidArgument("snapshot loader dependencies are missing");
  }

  TMVEngine temp_engine(options_.temp_tmv_chunks);
  EventApplier temp_applier(&temp_engine);
  std::vector<cedar::cdc::ChangeRecord> verified_records;
  uint64_t expected_sequence = 0;
  bool saw_final = false;
  uint64_t observed_epoch = expected_epoch;

  Status stream_status = source_->StreamSnapshot(
      partition_id, snapshot_version,
      [&](const cedar::storage::ComputeSnapshotBatch& batch) -> Status {
        if (batch.partition_id() != partition_id) {
          return Status::Corruption("snapshot batch partition mismatch");
        }
        if (expected_epoch != 0 && batch.partition_epoch() != expected_epoch) {
          return Status::Conflict("snapshot batch epoch mismatch");
        }
        if (batch.snapshot_version() != snapshot_version) {
          return Status::Corruption("snapshot batch version mismatch");
        }
        if (batch.sequence() != expected_sequence) {
          return Status::Corruption("snapshot batch sequence gap");
        }
        if (!ChecksumMatches(batch)) {
          return Status::Corruption("snapshot batch checksum mismatch");
        }
        observed_epoch = batch.partition_epoch();
        for (const auto& record : batch.records()) {
          if (record.partition_id() != partition_id) {
            return Status::Corruption("snapshot record partition mismatch");
          }
          if (record.partition_epoch() != batch.partition_epoch()) {
            return Status::Conflict("snapshot record epoch mismatch");
          }
          CEDAR_RETURN_IF_ERROR(temp_applier.ApplyChangeRecord(record));
          verified_records.push_back(record);
        }
        ++expected_sequence;
        saw_final = batch.final();
        return Status::OK();
      });
  if (!stream_status.ok()) {
    return stream_status;
  }
  if (!saw_final) {
    return Status::Corruption("snapshot stream ended without final batch");
  }

  // Publish only after the full stream has validated in the temporary engine.
  CEDAR_RETURN_IF_ERROR(live_applier_->ApplySnapshotRecordsAtomically(
      partition_id, high_watermark, snapshot_version, verified_records));

  SnapshotLoadResult result;
  result.partition_id = partition_id;
  result.partition_epoch = observed_epoch;
  result.applied_offset = high_watermark;
  result.applied_version = snapshot_version;
  std::ostringstream snapshot_id;
  snapshot_id << "snapshot-" << partition_id << "-" << observed_epoch << "-"
              << snapshot_version << "-" << high_watermark;
  result.snapshot_id = snapshot_id.str();
  return result;
}

}  // namespace cedar::gcn
