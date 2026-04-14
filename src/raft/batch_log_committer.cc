#include "raft/batch_log_committer.h"

#include <algorithm>
#include <chrono>
#include "src/raft/partition_log_store.h"

namespace cedar {
namespace raft {

BatchLogCommitter::BatchLogCommitter(uint32_t partition_id, const BatchCommitConfig& config)
    : partition_id_(partition_id), config_(config) {
}

BatchLogCommitter::~BatchLogCommitter() {
  Stop();
}

Status BatchLogCommitter::Start() {
  if (running_.exchange(true)) {
    return Status::OK();  // Already running
  }
  
  worker_thread_ = std::thread(&BatchLogCommitter::WorkerLoop, this);
  return Status::OK();
}

void BatchLogCommitter::Stop() {
  if (!running_.exchange(false)) {
    return;  // Already stopped
  }
  
  queue_cv_.notify_all();
  
  if (worker_thread_.joinable()) {
    worker_thread_.join();
  }
  
  // Clear pending queue with error callbacks
  std::lock_guard<std::mutex> lock(queue_mutex_);
  for (auto& pending : pending_queue_) {
    if (pending.callback) {
      pending.callback(0, Status::NotSupported("BatchLogCommitter stopped"));
    }
  }
  pending_queue_.clear();
}

Status BatchLogCommitter::SubmitLog(const LogEntry& entry, CommitCallback callback) {
  if (!running_) {
    return Status::InvalidArgument("BatchLogCommitter not running");
  }
  
  std::lock_guard<std::mutex> lock(queue_mutex_);
  
  PendingEntry pending;
  pending.entry = entry;
  pending.callback = callback;
  pending.submit_time = std::chrono::steady_clock::now();
  
  pending_queue_.push_back(std::move(pending));
  
  {
    std::lock_guard<std::mutex> stats_lock(stats_mutex_);
    stats_.total_submitted++;
  }
  
  // Notify worker thread
  if (pending_queue_.size() >= config_.max_batch_size / 2) {
    queue_cv_.notify_one();
  }
  
  return Status::OK();
}

Status BatchLogCommitter::SubmitLogs(const std::vector<LogEntry>& entries, CommitCallback callback) {
  if (!running_) {
    return Status::InvalidArgument("BatchLogCommitter not running");
  }
  
  if (entries.empty()) {
    return Status::OK();
  }
  
  std::lock_guard<std::mutex> lock(queue_mutex_);
  
  for (const auto& entry : entries) {
    PendingEntry pending;
    pending.entry = entry;
    pending.callback = callback;
    pending.submit_time = std::chrono::steady_clock::now();
    
    pending_queue_.push_back(std::move(pending));
  }
  
  {
    std::lock_guard<std::mutex> stats_lock(stats_mutex_);
    stats_.total_submitted += entries.size();
  }
  
  // Notify worker thread
  if (pending_queue_.size() >= config_.max_batch_size / 2) {
    queue_cv_.notify_one();
  }
  
  return Status::OK();
}

Status BatchLogCommitter::ForceFlush() {
  if (!running_) {
    return Status::InvalidArgument("BatchLogCommitter not running");
  }
  
  std::unique_lock<std::mutex> lock(queue_mutex_);
  
  if (pending_queue_.empty()) {
    return Status::OK();
  }
  
  std::vector<PendingEntry> batch;
  batch.swap(pending_queue_);
  lock.unlock();
  
  return DoCommitBatch(batch);
}

void BatchLogCommitter::SetLogStore(PartitionLogStore* log_store) {
  log_store_ = log_store;
}

BatchLogCommitter::Stats BatchLogCommitter::GetStats() const {
  std::lock_guard<std::mutex> lock(stats_mutex_);
  return stats_;
}

void BatchLogCommitter::WorkerLoop() {
  while (running_) {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    
    // Wait until we have enough entries or timeout
    auto timeout = std::chrono::milliseconds(config_.max_wait_ms);
    queue_cv_.wait_for(lock, timeout, [this] {
      return !running_ || pending_queue_.size() >= config_.max_batch_size;
    });
    
    if (!running_) {
      break;
    }
    
    // Check if we should process now (batch full or timeout)
    if (pending_queue_.empty()) {
      continue;
    }
    
    // Calculate current batch size in bytes
    size_t current_bytes = 0;
    size_t batch_count = 0;
    auto now = std::chrono::steady_clock::now();
    
    for (const auto& pending : pending_queue_) {
      if (batch_count >= config_.max_batch_size || 
          (current_bytes > 0 && current_bytes >= config_.max_batch_bytes)) {
        break;
      }
      current_bytes += pending.entry.ByteSizeLong();
      batch_count++;
    }
    
    // If oldest entry has been waiting too long, commit what we have
    if (batch_count == 0 && !pending_queue_.empty()) {
      auto oldest_time = pending_queue_.front().submit_time;
      auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
          now - oldest_time).count();
      if (elapsed_ms >= static_cast<int64_t>(config_.max_wait_ms)) {
        batch_count = std::min(static_cast<size_t>(config_.max_batch_size), 
                               pending_queue_.size());
      }
    }
    
    if (batch_count == 0) {
      continue;
    }
    
    // Extract batch
    std::vector<PendingEntry> batch;
    batch.reserve(batch_count);
    for (size_t i = 0; i < batch_count && !pending_queue_.empty(); ++i) {
      batch.push_back(std::move(pending_queue_.front()));
      pending_queue_.erase(pending_queue_.begin());
    }
    
    lock.unlock();
    
    // Commit the batch
    DoCommitBatch(batch);
  }
  
  // Process any remaining entries on shutdown
  std::unique_lock<std::mutex> lock(queue_mutex_);
  if (!pending_queue_.empty()) {
    std::vector<PendingEntry> batch;
    batch.swap(pending_queue_);
    lock.unlock();
    DoCommitBatch(batch);
  }
}

Status BatchLogCommitter::DoCommitBatch(const std::vector<PendingEntry>& batch) {
  if (batch.empty()) {
    return Status::OK();
  }
  
  if (!log_store_) {
    // Without a log store we cannot safely acknowledge commits.
    std::lock_guard<std::mutex> stats_lock(stats_mutex_);
    for (const auto& pending : batch) {
      if (pending.callback) {
        pending.callback(0, Status::NotSupported("BatchLogCommitter", "No log store configured"));
      }
    }
    return Status::NotSupported("BatchLogCommitter", "No log store configured");
  }
  
  // 1. Append to local log store
  std::vector<LogEntry> entries;
  entries.reserve(batch.size());
  for (const auto& pending : batch) {
    entries.push_back(pending.entry);
  }
  
  auto append_status = log_store_->AppendEntries(entries);
  if (!append_status.ok()) {
    std::lock_guard<std::mutex> stats_lock(stats_mutex_);
    for (const auto& pending : batch) {
      if (pending.callback) {
        pending.callback(0, append_status);
      }
    }
    return append_status;
  }
  
  // 2. Update commit index (local commit)
  uint64_t new_commit_index = log_store_->GetLastLogIndex();
  log_store_->SetCommittedIndex(new_commit_index);
  
  uint64_t base_log_index = new_commit_index - batch.size() + 1;
  
  // 3. Notify callbacks and update stats
  std::lock_guard<std::mutex> stats_lock(stats_mutex_);
  
  double total_latency_us = 0.0;
  auto commit_end = std::chrono::steady_clock::now();
  
  for (size_t i = 0; i < batch.size(); ++i) {
    const auto& pending = batch[i];
    
    auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
        commit_end - pending.submit_time).count();
    total_latency_us += static_cast<double>(latency_us);
    
    if (pending.callback) {
      uint64_t log_index = base_log_index + i;
      pending.callback(log_index, Status::OK());
    }
  }
  
  stats_.total_committed += batch.size();
  stats_.total_batches++;
  
  if (stats_.total_batches > 0) {
    stats_.avg_batch_size = static_cast<double>(stats_.total_committed) / 
                            static_cast<double>(stats_.total_batches);
  }
  
  double batch_avg_latency = total_latency_us / static_cast<double>(batch.size());
  if (stats_.total_batches == 1) {
    stats_.avg_latency_us = batch_avg_latency;
  } else {
    stats_.avg_latency_us = 0.9 * stats_.avg_latency_us + 0.1 * batch_avg_latency;
  }
  
  return Status::OK();
}

}  // namespace raft
}  // namespace cedar
