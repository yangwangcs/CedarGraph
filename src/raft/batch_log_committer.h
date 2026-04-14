#ifndef CEDAR_RAFT_BATCH_LOG_COMMITTER_H_
#define CEDAR_RAFT_BATCH_LOG_COMMITTER_H_

#include <cstdint>
#include <vector>
#include <memory>
#include <functional>
#include <atomic>
#include <thread>
#include <condition_variable>
#include <mutex>
#include <chrono>
#include "cedar/core/status.h"
#include "raft_service.pb.h"

namespace cedar {
namespace raft {

using LogEntry = cedar::raft::internal::LogEntry;

class PartitionLogStore;

struct BatchCommitConfig {
  uint32_t max_batch_size = 100;
  uint32_t max_batch_bytes = 1024 * 1024;
  uint32_t max_wait_ms = 5;
  bool enable_pipeline = true;
};

class BatchLogCommitter {
 public:
  using CommitCallback = std::function<void(uint64_t log_index, Status)>;

  explicit BatchLogCommitter(uint32_t partition_id, const BatchCommitConfig& config);
  ~BatchLogCommitter();
  BatchLogCommitter(const BatchLogCommitter&) = delete;
  BatchLogCommitter& operator=(const BatchLogCommitter&) = delete;

  Status Start();
  void Stop();
  Status SubmitLog(const LogEntry& entry, CommitCallback callback);
  Status SubmitLogs(const std::vector<LogEntry>& entries, CommitCallback callback);
  Status ForceFlush();
  
  // Inject the underlying log store for actual persistence.
  void SetLogStore(PartitionLogStore* log_store);

  struct Stats {
    uint64_t total_submitted = 0;
    uint64_t total_committed = 0;
    uint64_t total_batches = 0;
    double avg_batch_size = 0.0;
    double avg_latency_us = 0.0;
  };
  Stats GetStats() const;

 private:
  uint32_t partition_id_;
  BatchCommitConfig config_;
  
  struct PendingEntry {
    LogEntry entry;
    CommitCallback callback;
    std::chrono::steady_clock::time_point submit_time;
  };
  
  std::vector<PendingEntry> pending_queue_;
  std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::atomic<bool> running_{false};
  std::thread worker_thread_;
  mutable std::mutex stats_mutex_;
  Stats stats_;
  
  PartitionLogStore* log_store_ = nullptr;
  
  Status DoCommitBatch(const std::vector<PendingEntry>& batch);
  void WorkerLoop();
};

}  // namespace raft
}  // namespace cedar

#endif
