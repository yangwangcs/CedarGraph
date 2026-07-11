#ifndef CEDAR_CDC_PARTITION_CHANGE_LOG_H_
#define CEDAR_CDC_PARTITION_CHANGE_LOG_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "cdc_service.pb.h"
#include "cedar/core/status.h"

namespace cedar::cdc {

struct ChangeLogState {
  uint64_t partition_epoch = 0;
  uint64_t earliest_offset = 1;
  uint64_t high_watermark = 0;
  uint64_t committed_version = 0;
  uint64_t segment_bytes = 0;
  uint64_t segment_count = 0;
  uint64_t active_segment_first_offset = 1;
  uint64_t oldest_closed_segment_age_hours = 0;
  uint64_t youngest_closed_segment_age_hours = 0;
};

class PartitionChangeLog {
 public:
  struct Options {
    std::string directory;
    uint32_t partition_id = 0;
    uint64_t partition_epoch = 0;
    size_t max_segment_bytes = 64 * 1024 * 1024;
    size_t max_fetch_records = 4096;
    size_t max_fetch_bytes = 4 * 1024 * 1024;
  };

  static StatusOr<std::unique_ptr<PartitionChangeLog>> Open(Options options);

  Status AppendCommittedBatch(uint64_t commit_version,
                              std::vector<ChangeRecord> records);
  StatusOr<std::vector<ChangeRecord>> ReadAfter(uint64_t offset,
                                                size_t limit_records,
                                                size_t limit_bytes) const;
  ChangeLogState GetState() const;
  Status Compact(uint64_t retain_from_offset);

 private:
  explicit PartitionChangeLog(Options options);

  Status Recover();
  Status AppendRecordFrame(const ChangeRecord& record);
  Status RewriteSegmentsLocked();
  Status PersistManifestLocked() const;

  Options options_;
  mutable std::mutex mu_;
  ChangeLogState state_;
  std::vector<ChangeRecord> records_;
  std::vector<std::string> manifest_segment_names_;
  uint64_t active_segment_first_offset_ = 1;
  size_t active_segment_size_ = 0;
};

}  // namespace cedar::cdc

#endif  // CEDAR_CDC_PARTITION_CHANGE_LOG_H_
