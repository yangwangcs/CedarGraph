// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "cedar/dtx/transaction_timeout_manager.h"

#include <algorithm>
#include <iostream>

namespace cedar {

TransactionTimeoutManager::TransactionTimeoutManager() = default;

TransactionTimeoutManager::~TransactionTimeoutManager() {
  Shutdown();
}

void TransactionTimeoutManager::Initialize(const TimeoutConfig& config, 
                                           TimeoutCallback* callback) {
  if (running_.exchange(true)) {
    return;  // 已初始化
  }
  config_ = config;
  {
    std::lock_guard<std::mutex> lock(callback_mutex_);
    callback_ = callback;
  }
  
  timeout_thread_ = std::thread(&TransactionTimeoutManager::TimeoutCheckLoop, this);
  retry_thread_ = std::thread(&TransactionTimeoutManager::RetryLoop, this);
}

void TransactionTimeoutManager::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }
  cv_.notify_all();
  
  try {
    if (timeout_thread_.joinable()) {
      timeout_thread_.join();
    }
  } catch (...) { std::cerr << "[TimeoutManager] Timeout thread join exception" << std::endl; }
  try {
    if (retry_thread_.joinable()) {
      retry_thread_.join();
    }
  } catch (...) { std::cerr << "[TimeoutManager] Retry thread join exception" << std::endl; }
  
  std::lock_guard<std::mutex> lock(mutex_);
  transactions_.clear();
  while (!retry_queue_.empty()) {
    retry_queue_.pop();
  }
}

void TransactionTimeoutManager::RegisterTransaction(
    dtx::TxnID txn_id, 
    const std::vector<dtx::PartitionID>& participants) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  TransactionTimeoutInfo info;
  info.txn_id = txn_id;
  info.start_time = std::chrono::steady_clock::now();
  info.last_activity = info.start_time;
  info.is_preparing = true;
  
  auto now = std::chrono::steady_clock::now();
  for (const auto& pid : participants) {
    info.participant_timeouts[pid] = now;
  }
  
  transactions_[txn_id] = std::move(info);
}

void TransactionTimeoutManager::UnregisterTransaction(dtx::TxnID txn_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  transactions_.erase(txn_id);
}

void TransactionTimeoutManager::UpdateTransactionState(dtx::TxnID txn_id, 
                                                       bool is_preparing) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto it = transactions_.find(txn_id);
  if (it != transactions_.end()) {
    it->second.is_preparing = is_preparing;
    it->second.last_activity = std::chrono::steady_clock::now();
  }
}

void TransactionTimeoutManager::ScheduleRetry(const PendingOperation& op) {
  std::lock_guard<std::mutex> lock(mutex_);
  retry_queue_.push(op);
  cv_.notify_one();
}

void TransactionTimeoutManager::CancelRetries(dtx::TxnID txn_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  // Note: We can't easily remove from priority_queue
  // Instead, we'll filter when processing
}

std::vector<PendingOperation> TransactionTimeoutManager::GetPendingRetries(size_t max_count) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::vector<PendingOperation> result;
  auto now = std::chrono::steady_clock::now();
  
  while (!retry_queue_.empty() && result.size() < max_count) {
    const auto& op = retry_queue_.top();
    if (op.next_attempt <= now) {
      result.push_back(op);
      retry_queue_.pop();
    } else {
      break;
    }
  }
  
  return result;
}

bool TransactionTimeoutManager::IsTimedOut(dtx::TxnID txn_id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto it = transactions_.find(txn_id);
  if (it == transactions_.end()) {
    return false;
  }
  
  auto now = std::chrono::steady_clock::now();
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      now - it->second.start_time);
  
  return elapsed > config_.max_transaction_duration;
}

void TransactionTimeoutManager::TimeoutCheckLoop() {
  while (running_) {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait_for(lock, std::chrono::milliseconds(100), [this] {
      return !running_;
    });
    
    if (!running_) break;
    
    auto now = std::chrono::steady_clock::now();
    
    // Check for transaction timeouts
    // Collect timed-out txns first to avoid UB of erasing during range-for
    std::vector<dtx::TxnID> timed_out_txns;
    std::vector<std::pair<dtx::TxnID, dtx::PartitionID>> participant_timeouts;
    
    for (const auto& [txn_id, info] : transactions_) {
      auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
          now - info.start_time);
      
      if (elapsed > config_.max_transaction_duration) {
        timed_out_txns.push_back(txn_id);
        continue;
      }
      
      // Check participant timeouts using last_activity (updated on state changes)
      auto phase_timeout = info.is_preparing ? config_.prepare_timeout : config_.commit_timeout;
      auto participant_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
          now - info.last_activity);
      
      if (participant_elapsed > phase_timeout) {
        for (const auto& [pid, last_update] : info.participant_timeouts) {
          (void)last_update;
          participant_timeouts.emplace_back(txn_id, pid);
        }
      }
    }
    
    // Process timeouts outside the iteration
    for (dtx::TxnID txn_id : timed_out_txns) {
      TimeoutCallback* cb = nullptr;
      {
        std::lock_guard<std::mutex> cb_lock(callback_mutex_);
        cb = callback_;
      }
      if (cb) {
        lock.unlock();
        try {
          cb->OnTransactionTimeout(txn_id);
        } catch (...) { std::cerr << "[TimeoutManager] OnTransactionTimeout exception for txn_id=" << txn_id << std::endl; }
        lock.lock();
      }
      transactions_.erase(txn_id);
    }
    
    for (const auto& [txn_id, pid] : participant_timeouts) {
      auto it = transactions_.find(txn_id);
      if (it != transactions_.end()) {
        it->second.last_activity = now;
      }
      TimeoutCallback* cb = nullptr;
      {
        std::lock_guard<std::mutex> cb_lock(callback_mutex_);
        cb = callback_;
      }
      if (cb) {
        lock.unlock();
        try {
          cb->OnParticipantTimeout(txn_id, pid);
        } catch (...) { std::cerr << "[TimeoutManager] OnParticipantTimeout exception for txn_id=" << txn_id << ", participant_id=" << pid << std::endl; }
        lock.lock();
      }
    }
  }
}

void TransactionTimeoutManager::RetryLoop() {
  while (running_) {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait_for(lock, std::chrono::milliseconds(100), [this] {
      return !running_ || !retry_queue_.empty();
    });
    
    if (!running_) break;
    
    auto now = std::chrono::steady_clock::now();
    std::vector<PendingOperation> ready_ops;
    
    // Collect operations ready for retry
    while (!retry_queue_.empty()) {
      const auto& op = retry_queue_.top();
      if (op.next_attempt <= now) {
        ready_ops.push_back(op);
        retry_queue_.pop();
      } else {
        break;
      }
    }
    
    lock.unlock();
    
    // Process ready operations
    for (const auto& op : ready_ops) {
      TimeoutCallback* cb = nullptr;
      {
        std::lock_guard<std::mutex> cb_lock(callback_mutex_);
        cb = callback_;
      }
      if (cb) {
        try {
          cb->OnOperationRetry(op);
        } catch (...) { std::cerr << "[TimeoutManager] OnOperationRetry exception for txn_id=" << op.txn_id << std::endl; }
      }
    }
  }
}

}  // namespace cedar
