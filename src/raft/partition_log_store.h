#ifndef CEDAR_RAFT_PARTITION_LOG_STORE_H_
#define CEDAR_RAFT_PARTITION_LOG_STORE_H_

#include <cstdint>
#include <string>
#include <vector>
#include <mutex>
#include "cedar/core/status.h"
#include "raft_service.pb.h"

namespace cedar {
namespace raft {

using LogEntry = cedar::raft::internal::LogEntry;

class PartitionLogStore {
 public:
  explicit PartitionLogStore(uint32_t partition_id, const std::string& data_dir);
  ~PartitionLogStore();
  PartitionLogStore(const PartitionLogStore&) = delete;
  PartitionLogStore& operator=(const PartitionLogStore&) = delete;

  Status Initialize();
  Status Close();
  Status AppendEntry(const LogEntry& entry);
  Status AppendEntries(const std::vector<LogEntry>& entries);
  StatusOr<LogEntry> GetEntry(uint64_t index);
  std::vector<LogEntry> GetEntries(uint64_t start_index, uint64_t end_index);
  Status TruncateFrom(uint64_t from_index);
  uint64_t GetLastLogIndex() const;
  uint64_t GetLastLogTerm() const;
  uint64_t GetCommittedIndex() const { return committed_index_; }
  void SetCommittedIndex(uint64_t index) { committed_index_ = index; }
  Status SaveMetadata(uint64_t current_term, const std::string& voted_for);
  Status LoadMetadata(uint64_t* current_term, std::string* voted_for);

 private:
  uint32_t partition_id_;
  std::string data_dir_;
  std::string log_file_path_;
  std::string meta_file_path_;
  mutable std::mutex mutex_;
  std::vector<LogEntry> entries_;
  uint64_t committed_index_ = 0;
  int log_fd_ = -1;
  Status FlushToDisk();
};

}  // namespace raft
}  // namespace cedar

#endif
