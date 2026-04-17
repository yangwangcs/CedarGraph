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
#include <unordered_map>
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
  explicit BatchLogCommitter(PartitionLogStore* log_store,
                             const std::vector<std::string>& peers);
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
  void SetPeers(const std::vector<std::string>& peers);
  
  // Callback to replicate entries to followers. Called by DoCommitBatch for multi-node clusters.
  using SendEntriesCallback = std::function<void(const std::vector<LogEntry>&)>;
  void SetSendEntriesCallback(SendEntriesCallback callback);

  // Notify the committer that a follower has acknowledged entries up to
  // the given index. This is called by the Raft transport layer.
  void Acknowledge(const std::string& follower_id, uint64_t match_index);

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
  size_t quorum_size_ = 1;
  std::vector<std::string> peers_;
  SendEntriesCallback send_entries_callback_;

  struct InflightEntry {
    uint64_t index;
    uint64_t term;
    size_t ack_count;
    size_t nack_count;
    bool committed;
    std::chrono::steady_clock::time_point send_time;
  };

  std::unordered_map<uint64_t, InflightEntry> inflight_;
  mutable std::mutex inflight_mutex_;

  // Callbacks waiting for commit, keyed by log index.
  std::unordered_map<uint64_t, CommitCallback> pending_callbacks_;
  mutable std::mutex callback_mutex_;

  Status DoCommitBatch(const std::vector<PendingEntry>& batch);
  void WorkerLoop();
  void InvokeCallbacks(uint64_t new_commit_index,
                       const std::vector<PendingEntry>& batch);
  void ProcessCommitted(uint64_t up_to_index,
                        const std::chrono::steady_clock::time_point& commit_time);
};

}  // namespace raft
}  // namespace cedar

#endif
