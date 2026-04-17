#include "raft/batch_log_committer.h"

#include <algorithm>
#include <chrono>
#include "src/raft/partition_log_store.h"

namespace cedar {
namespace raft {

BatchLogCommitter::BatchLogCommitter(uint32_t partition_id, const BatchCommitConfig& config)
    : partition_id_(partition_id), config_(config) {
}

BatchLogCommitter::BatchLogCommitter(PartitionLogStore* log_store,
                                     const std::vector<std::string>& peers)
    : log_store_(log_store),
      quorum_size_((peers.size() + 1) / 2 + 1) {
  if (quorum_size_ < 1) quorum_size_ = 1;
}

BatchLogCommitter::~BatchLogCommitter() {
  Stop();
}

void BatchLogCommitter::SetPeers(const std::vector<std::string>& peers) {
  peers_ = peers;
  quorum_size_ = (peers.size() + 1) / 2 + 1;
  if (quorum_size_ < 1) quorum_size_ = 1;
}

void BatchLogCommitter::SetSendEntriesCallback(SendEntriesCallback callback) {
  send_entries_callback_ = std::move(callback);
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
  
  // 2. Register inflight entries (including self-ack) and save callbacks
  {
    std::lock_guard<std::mutex> lock(inflight_mutex_);
    uint64_t base_index = log_store_->GetLastLogIndex() - entries.size() + 1;
    for (size_t i = 0; i < entries.size(); ++i) {
      InflightEntry ie;
      ie.index = entries[i].index();
      ie.term = entries[i].term();
      ie.ack_count = 1;  // Leader counts as one ack
      ie.nack_count = 0;
      ie.committed = false;
      ie.send_time = std::chrono::steady_clock::now();
      inflight_[entries[i].index()] = ie;
    }
  }
  {
    std::lock_guard<std::mutex> lock(callback_mutex_);
    uint64_t base_index = log_store_->GetLastLogIndex() - entries.size() + 1;
    for (size_t i = 0; i < batch.size(); ++i) {
      if (batch[i].callback) {
        pending_callbacks_[base_index + i] = batch[i].callback;
      }
    }
  }

  // 3. If quorum is 1 (single node), we can commit immediately
  if (quorum_size_ <= 1) {
    uint64_t new_commit_index = log_store_->GetLastLogIndex();
    log_store_->SetCommittedIndex(new_commit_index);
    ProcessCommitted(new_commit_index, std::chrono::steady_clock::now());
  } else {
    // 4. For multi-node clusters, replicate entries to followers
    if (send_entries_callback_) {
      send_entries_callback_(entries);
    }
  }

  return Status::OK();
}

void BatchLogCommitter::Acknowledge(const std::string& follower_id,
                                    uint64_t match_index) {
  (void)follower_id;
  std::lock_guard<std::mutex> lock(inflight_mutex_);
  
  for (auto& [index, entry] : inflight_) {
    if (index <= match_index && !entry.committed) {
      entry.ack_count++;
      if (entry.ack_count >= quorum_size_) {
        entry.committed = true;
      }
    }
  }
  
  // Advance commit index to the highest contiguous committed index
  uint64_t new_commit = log_store_->GetCommittedIndex();
  for (uint64_t idx = new_commit + 1;; ++idx) {
    auto it = inflight_.find(idx);
    if (it == inflight_.end() || !it->second.committed) {
      break;
    }
    new_commit = idx;
  }
  
  if (new_commit > log_store_->GetCommittedIndex()) {
    log_store_->SetCommittedIndex(new_commit);
    ProcessCommitted(new_commit, std::chrono::steady_clock::now());
  }

  // Clean up old inflight entries
  auto now = std::chrono::steady_clock::now();
  for (auto it = inflight_.begin(); it != inflight_.end();) {
    if (it->second.committed ||
        std::chrono::duration_cast<std::chrono::seconds>(
            now - it->second.send_time).count() > 30) {
      it = inflight_.erase(it);
    } else {
      ++it;
    }
  }
}

void BatchLogCommitter::InvokeCallbacks(uint64_t new_commit_index,
                                        const std::vector<PendingEntry>& batch) {
  std::lock_guard<std::mutex> stats_lock(stats_mutex_);
  
  double total_latency_us = 0.0;
  auto commit_end = std::chrono::steady_clock::now();
  
  // If batch is provided, invoke callbacks for that specific batch
  if (!batch.empty()) {
    uint64_t base_log_index = new_commit_index - batch.size() + 1;
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
  }
}

void BatchLogCommitter::ProcessCommitted(
    uint64_t up_to_index,
    const std::chrono::steady_clock::time_point& commit_time) {
  std::lock_guard<std::mutex> lock(callback_mutex_);

  double total_latency_us = 0.0;
  size_t invoked = 0;

  for (auto it = pending_callbacks_.begin(); it != pending_callbacks_.end();) {
    if (it->first <= up_to_index) {
      if (it->second) {
        it->second(it->first, Status::OK());
      }
      // We don't have submit_time here, so latency tracking is skipped.
      // For accurate stats, the caller should also invoke InvokeCallbacks
      // when batch is known.
      it = pending_callbacks_.erase(it);
      invoked++;
    } else {
      ++it;
    }
  }

  if (invoked > 0) {
    std::lock_guard<std::mutex> stats_lock(stats_mutex_);
    stats_.total_committed += invoked;
    stats_.total_batches++;
    if (stats_.total_batches > 0) {
      stats_.avg_batch_size = static_cast<double>(stats_.total_committed) /
                              static_cast<double>(stats_.total_batches);
    }
  }
}

}  // namespace raft
}  // namespace cedar
