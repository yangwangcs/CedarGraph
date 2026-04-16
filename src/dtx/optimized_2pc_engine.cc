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

#include "cedar/dtx/optimized_2pc_engine.h"
#include "cedar/dtx/storage_service_impl.h"
#include "cedar/dtx/transaction_state.h"

#include <algorithm>
#include <chrono>

namespace cedar {
namespace dtx {

// =============================================================================
// Constructor / Destructor
// =============================================================================

Optimized2PCEngine::Optimized2PCEngine(const TwoPCConfig& config)
    : config_(config) {
}

Optimized2PCEngine::~Optimized2PCEngine() {
  Shutdown();
}

// =============================================================================
// Lifecycle
// =============================================================================

Status Optimized2PCEngine::Initialize(
    const std::vector<std::shared_ptr<StorageClient>>& clients) {
  if (running_.exchange(true)) {
    return Status::InvalidArgument("Engine already initialized");
  }
  
  clients_ = clients;
  
  // Start worker threads for parallel execution
  int num_workers = config_.parallel_threads > 0 ? config_.parallel_threads : 4;
  for (int i = 0; i < num_workers; ++i) {
    worker_threads_.emplace_back([this]() {
      // Worker thread main loop
      while (!shutdown_.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    });
  }
  
  // Start pipeline thread if using pipelined strategy
  if (config_.strategy == TwoPCConfig::Strategy::kPipelined ||
      config_.strategy == TwoPCConfig::Strategy::kHybrid) {
    pipeline_thread_ = std::thread([this]() {
      PipelineWorkerLoop();
    });
  }
  
  // Start adaptive tuning thread
  if (config_.enable_adaptive_tuning) {
    tuning_thread_ = std::thread([this]() {
      AdaptiveTuningLoop();
    });
  }
  
  return Status::OK();
}

void Optimized2PCEngine::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }
  
  shutdown_ = true;
  
  // Wake up all waiting threads
  pipeline_cv_.notify_all();
  batch_cv_.notify_all();
  
  // Join worker threads
  for (auto& t : worker_threads_) {
    if (t.joinable()) {
      t.join();
    }
  }
  
  if (pipeline_thread_.joinable()) {
    pipeline_thread_.join();
  }
  
  if (tuning_thread_.joinable()) {
    tuning_thread_.join();
  }
}

// =============================================================================
// Transaction Execution APIs
// =============================================================================

Status Optimized2PCEngine::Execute2PC(TxnID txn_id,
                                       const std::vector<CedarKey>& read_set,
                                       const std::vector<CedarKey>& write_set,
                                       Timestamp commit_ts) {
  auto ctx = std::make_shared<TransactionContext>(
      txn_id, read_set, write_set, commit_ts);
  
  // Select execution strategy
  TwoPCConfig::Strategy strategy;
  {
    std::lock_guard<std::mutex> lock(config_mutex_);
    strategy = config_.strategy;
  }
  
  switch (strategy) {
    case TwoPCConfig::Strategy::kSequential:
      return ExecuteSequential2PC(ctx);
    case TwoPCConfig::Strategy::kParallel:
      return ExecuteParallel2PC(ctx);
    case TwoPCConfig::Strategy::kPipelined:
      return ExecutePipelined2PC(ctx);
    case TwoPCConfig::Strategy::kBatched:
      return ExecuteBatched2PC(ctx);
    case TwoPCConfig::Strategy::kHybrid:
      // Auto-select based on workload characteristics
      if (write_set.size() > 10) {
        return ExecuteBatched2PC(ctx);  // Large writes -> batch
      } else if (stats_.current_throughput.load() > 10000) {
        return ExecutePipelined2PC(ctx);  // High throughput -> pipeline
      } else {
        return ExecuteParallel2PC(ctx);  // Default -> parallel
      }
    default:
      return ExecuteParallel2PC(ctx);
  }
}

void Optimized2PCEngine::Execute2PCAsync(
    TxnID txn_id,
    const std::vector<CedarKey>& read_set,
    const std::vector<CedarKey>& write_set,
    Timestamp commit_ts,
    std::function<void(Status)> callback) {
  
  auto ctx = std::make_shared<TransactionContext>(
      txn_id, read_set, write_set, commit_ts);
  ctx->callback = callback;
  
  // Submit to worker thread pool
  std::thread([this, ctx]() {
    auto status = Execute2PC(ctx->txn_id, ctx->read_set, 
                             ctx->write_set, ctx->commit_ts);
    if (ctx->callback) {
      ctx->callback(status);
    }
  }).detach();
}

std::vector<Status> Optimized2PCEngine::Execute2PCBatch(
    const std::vector<std::tuple<TxnID, std::vector<CedarKey>,
                                 std::vector<CedarKey>, Timestamp>>& transactions) {
  
  std::vector<Status> results;
  results.reserve(transactions.size());
  
  // Execute in parallel using thread pool
  std::vector<std::future<Status>> futures;
  futures.reserve(transactions.size());
  
  for (const auto& txn : transactions) {
    TxnID txn_id = std::get<0>(txn);
    const auto& read_set = std::get<1>(txn);
    const auto& write_set = std::get<2>(txn);
    Timestamp commit_ts = std::get<3>(txn);
    futures.push_back(std::async(std::launch::async, [this, txn_id, &read_set, 
                                                      &write_set, commit_ts]() {
      return Execute2PC(txn_id, read_set, write_set, commit_ts);
    }));
  }
  
  // Collect results
  for (auto& f : futures) {
    results.push_back(f.get());
  }
  
  return results;
}

Status Optimized2PCEngine::SubmitPipelined(
    TxnID txn_id,
    const std::vector<CedarKey>& read_set,
    const std::vector<CedarKey>& write_set,
    Timestamp commit_ts) {
  
  auto ctx = std::make_shared<TransactionContext>(
      txn_id, read_set, write_set, commit_ts);
  
  // Add to pipeline queue
  {
    std::lock_guard<std::mutex> lock(pipeline_mutex_);
    pipeline_queue_.push(ctx);
  }
  pipeline_cv_.notify_one();
  
  // Wait for completion (synchronous for now)
  while (ctx->state.load() != TransactionContext::State::kCommitted &&
         ctx->state.load() != TransactionContext::State::kAborted) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
    
    // Check timeout
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        now - ctx->start_time).count();
    if (elapsed > config_.prepare_timeout_ms + config_.commit_timeout_ms) {
      ctx->state.store(TransactionContext::State::kAborted);
      stats_.timeout_transactions++;
      return Status::IOError("Transaction timeout");
    }
  }
  
  return ctx->state.load() == TransactionContext::State::kCommitted 
         ? Status::OK() 
         : Status::IOError("Transaction aborted");
}

// =============================================================================
// Strategy Implementations
// =============================================================================

Status Optimized2PCEngine::ExecuteSequential2PC(
    const std::shared_ptr<TransactionContext>& ctx) {
  
  auto participants = GetParticipants(ctx->write_set);
  
  // Phase 1: Prepare (sequential RPC)
  ctx->state.store(TransactionContext::State::kPreparing);
  
  for (auto& client : participants) {
    // Real Prepare RPC call
    auto result = client->Prepare(
        ctx->txn_id,
        ctx->read_set,
        ctx->write_set,
        ctx->commit_ts);
    
    if (result.ok() && result.ValueOrDie()) {
      ctx->prepare_acks.fetch_add(1);
    } else {
      ctx->prepare_nacks.fetch_add(1);
    }
  }
  
  if (ctx->prepare_acks.load() < static_cast<int>(participants.size())) {
    ctx->state.store(TransactionContext::State::kAborting);
    
    // Abort participants that prepared
    for (auto& client : participants) {
      client->Abort(ctx->txn_id);
    }
    
    stats_.aborted_transactions++;
    return Status::IOError("Prepare phase failed");
  }
  
  ctx->state.store(TransactionContext::State::kPrepared);
  ctx->prepare_complete_time = std::chrono::steady_clock::now();
  
  // Phase 2: Commit (sequential RPC)
  ctx->state.store(TransactionContext::State::kCommitting);
  
  for (auto& client : participants) {
    // Real Commit RPC call
    auto status = client->Commit(ctx->txn_id, ctx->commit_ts);
    
    if (status.ok()) {
      ctx->commit_acks.fetch_add(1);
    }
  }
  
  if (ctx->commit_acks.load() == static_cast<int>(participants.size())) {
    ctx->state.store(TransactionContext::State::kCommitted);
    stats_.committed_transactions++;
    
    auto end_time = std::chrono::steady_clock::now();
    auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
        end_time - ctx->start_time).count();
    stats_.RecordLatency(latency_us);
    
    return Status::OK();
  } else {
    ctx->state.store(TransactionContext::State::kAborted);
    stats_.aborted_transactions++;
    return Status::IOError("Commit phase failed");
  }
}

Status Optimized2PCEngine::ExecuteParallel2PC(
    const std::shared_ptr<TransactionContext>& ctx) {
  
  auto participants = GetParticipants(ctx->write_set);
  
  // Phase 1: Prepare (parallel RPC)
  ctx->state.store(TransactionContext::State::kPreparing);
  
  std::vector<std::future<bool>> prepare_futures;
  for (auto& client : participants) {
    prepare_futures.push_back(std::async(std::launch::async, [&client, &ctx]() {
      // Real Prepare RPC call
      auto result = client->Prepare(
          ctx->txn_id,
          ctx->read_set,
          ctx->write_set,
          ctx->commit_ts);
      
      if (result.ok() && result.ValueOrDie()) {
        ctx->prepare_acks.fetch_add(1);
        return true;
      } else {
        ctx->prepare_nacks.fetch_add(1);
        return false;
      }
    }));
  }
  
  // Wait for all prepares with timeout
  bool all_prepared = WaitForPrepareQuorum(ctx, prepare_futures);
  
  if (!all_prepared) {
    ctx->state.store(TransactionContext::State::kAborting);
    
    // Send Abort to participants that prepared successfully
    for (auto& client : participants) {
      std::thread([&client, &ctx]() {
        client->Abort(ctx->txn_id);
      }).detach();
    }
    
    stats_.aborted_transactions++;
    return Status::IOError("Prepare phase failed or timed out");
  }
  
  ctx->state.store(TransactionContext::State::kPrepared);
  ctx->prepare_complete_time = std::chrono::steady_clock::now();
  
  if (state_manager_) {
    state_manager_->UpdateState(ctx->txn_id, TxnState::kPrepared);
  }
  
  // Phase 2: Commit (parallel RPC)
  ctx->state.store(TransactionContext::State::kCommitting);
  
  std::vector<std::future<bool>> commit_futures;
  for (auto& client : participants) {
    commit_futures.push_back(std::async(std::launch::async, [&client, &ctx]() {
      // Real Commit RPC call
      auto status = client->Commit(ctx->txn_id, ctx->commit_ts);
      
      if (status.ok()) {
        ctx->commit_acks.fetch_add(1);
        return true;
      } else {
        return false;
      }
    }));
  }
  
  bool all_committed = WaitForCommitQuorum(ctx, commit_futures);
  
  if (all_committed) {
    ctx->state.store(TransactionContext::State::kCommitted);
    if (state_manager_) {
      state_manager_->UpdateState(ctx->txn_id, TxnState::kCommitted);
    }
    stats_.committed_transactions++;
    stats_.total_transactions++;
    
    auto end_time = std::chrono::steady_clock::now();
    auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
        end_time - ctx->start_time).count();
    stats_.RecordLatency(latency_us);
    
    return Status::OK();
  } else {
    ctx->state.store(TransactionContext::State::kAborted);
    stats_.aborted_transactions++;
    return Status::IOError("Commit phase failed or timed out");
  }
}

Status Optimized2PCEngine::ExecutePipelined2PC(
    const std::shared_ptr<TransactionContext>& ctx) {
  // Add to pipeline and wait for completion
  return SubmitPipelined(ctx->txn_id, ctx->read_set, 
                         ctx->write_set, ctx->commit_ts);
}

Status Optimized2PCEngine::ExecuteBatched2PC(
    const std::shared_ptr<TransactionContext>& ctx) {
  // Add to batch buffer
  {
    std::lock_guard<std::mutex> lock(batch_mutex_);
    batch_buffer_.push_back(ctx);
    
    // If batch is full or timeout, process it
    if (batch_buffer_.size() >= static_cast<size_t>(config_.batch_size)) {
      batch_cv_.notify_one();
    }
  }
  
  // Wait for completion
  while (ctx->state.load() != TransactionContext::State::kCommitted &&
         ctx->state.load() != TransactionContext::State::kAborted) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
    
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        now - ctx->start_time).count();
    if (elapsed > config_.prepare_timeout_ms + config_.commit_timeout_ms) {
      ctx->state.store(TransactionContext::State::kAborted);
      stats_.timeout_transactions++;
      return Status::IOError("Transaction timeout");
    }
  }
  
  return ctx->state.load() == TransactionContext::State::kCommitted 
         ? Status::OK() 
         : Status::IOError("Transaction aborted");
}

// =============================================================================
// Pipeline Worker
// =============================================================================

void Optimized2PCEngine::PipelineWorkerLoop() {
  while (!shutdown_.load()) {
    std::unique_lock<std::mutex> lock(pipeline_mutex_);
    
    // Wait for transactions or timeout
    pipeline_cv_.wait_for(lock, std::chrono::milliseconds(config_.pipeline_timeout_ms),
                          [this]() {
                            return !pipeline_queue_.empty() || shutdown_.load();
                          });
    
    if (shutdown_.load()) break;
    
    // Collect batch of transactions
    std::vector<std::shared_ptr<TransactionContext>> batch;
    int max_batch = config_.batch_size > 0 ? config_.batch_size : 4;
    
    while (!pipeline_queue_.empty() && batch.size() < static_cast<size_t>(max_batch)) {
      batch.push_back(pipeline_queue_.front());
      pipeline_queue_.pop();
    }
    
    lock.unlock();
    
    // Process batch in parallel
    if (!batch.empty()) {
      for (auto& ctx : batch) {
        auto participants = GetParticipants(ctx->read_set);
        auto write_participants = GetParticipants(ctx->write_set);
        participants.insert(participants.end(), write_participants.begin(), write_participants.end());
        // deduplicate
        std::sort(participants.begin(), participants.end());
        participants.erase(std::unique(participants.begin(), participants.end()), participants.end());
        
        if (participants.empty()) {
          ctx->state.store(TransactionContext::State::kAborted);
          continue;
        }
        
        // Phase 1: Prepare all in parallel
        ctx->state.store(TransactionContext::State::kPreparing);
        std::vector<std::future<bool>> prepare_results;
        for (auto& client : participants) {
          prepare_results.push_back(std::async(std::launch::async, [&client, &ctx]() {
            auto result = client->Prepare(ctx->txn_id, ctx->read_set, ctx->write_set, ctx->commit_ts);
            if (result.ok() && result.ValueOrDie()) {
              ctx->prepare_acks.fetch_add(1);
              return true;
            } else {
              ctx->prepare_nacks.fetch_add(1);
              return false;
            }
          }));
        }
        
        for (auto& f : prepare_results) {
          f.get();
        }
        
        bool all_prepared = (ctx->prepare_acks.load() == static_cast<int>(participants.size()));
        if (!all_prepared) {
          ctx->state.store(TransactionContext::State::kAborting);
          for (auto& client : participants) {
            client->Abort(ctx->txn_id);
          }
          ctx->state.store(TransactionContext::State::kAborted);
          stats_.aborted_transactions++;
          continue;
        }
        
        ctx->state.store(TransactionContext::State::kPrepared);
        if (state_manager_) {
          state_manager_->UpdateState(ctx->txn_id, TxnState::kPrepared);
        }
        
        // Phase 2: Commit all in parallel
        ctx->state.store(TransactionContext::State::kCommitting);
        std::vector<std::future<bool>> commit_results;
        for (auto& client : participants) {
          commit_results.push_back(std::async(std::launch::async, [&client, &ctx]() {
            auto status = client->Commit(ctx->txn_id, ctx->commit_ts);
            if (status.ok()) {
              ctx->commit_acks.fetch_add(1);
              return true;
            }
            return false;
          }));
        }
        
        for (auto& f : commit_results) {
          f.get();
        }
        
        bool all_committed = (ctx->commit_acks.load() == static_cast<int>(participants.size()));
        if (all_committed) {
          ctx->state.store(TransactionContext::State::kCommitted);
          if (state_manager_) {
            state_manager_->UpdateState(ctx->txn_id, TxnState::kCommitted);
          }
          stats_.committed_transactions++;
          stats_.total_transactions++;
          
          auto end_time = std::chrono::steady_clock::now();
          auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
              end_time - ctx->start_time).count();
          stats_.RecordLatency(latency_us);
        } else {
          ctx->state.store(TransactionContext::State::kAborted);
          stats_.aborted_transactions++;
        }
      }
    }
  }
}

// =============================================================================
// Adaptive Tuning
// =============================================================================

void Optimized2PCEngine::AdaptiveTuningLoop() {
  while (!shutdown_.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(config_.tuning_interval_sec));
    
    if (shutdown_.load()) break;
    
    TuneConfiguration();
  }
}

void Optimized2PCEngine::TuneConfiguration() {
  std::lock_guard<std::mutex> lock(tuning_mutex_);
  
  auto current_throughput = stats_.current_throughput.load();
  auto avg_latency = stats_.GetAverageLatency();
  
  // Simple heuristic tuning
  if (current_throughput > tuning_state_.last_throughput * 1.1) {
    // Throughput improved, keep direction
    tuning_state_.stable_rounds++;
  } else if (current_throughput < tuning_state_.last_throughput * 0.9) {
    // Throughput dropped, reverse direction
    tuning_state_.stable_rounds = 0;
    
    // Adjust batch size
    if (avg_latency > config_.latency_target_ms * 1000) {
      // Latency too high, decrease batch
      tuning_state_.current_batch_size = std::max(2, tuning_state_.current_batch_size / 2);
    } else {
      // Latency ok, can increase batch
      tuning_state_.current_batch_size = std::min(64, tuning_state_.current_batch_size + 4);
    }
    
    // Update config
    {
      std::lock_guard<std::mutex> config_lock(config_mutex_);
      config_.batch_size = tuning_state_.current_batch_size;
    }
  }
  
  tuning_state_.last_throughput = current_throughput;
  tuning_state_.last_latency = avg_latency;
  
  // Reset throughput counter
  stats_.current_throughput.store(stats_.ops_last_second.exchange(0));
}

void Optimized2PCEngine::EnableAdaptiveTuning(bool enable) {
  config_.enable_adaptive_tuning = enable;
}

void Optimized2PCEngine::UpdateConfiguration(const TwoPCConfig& config) {
  std::lock_guard<std::mutex> lock(config_mutex_);
  config_ = config;
}

TwoPCConfig Optimized2PCEngine::GetCurrentConfig() const {
  std::lock_guard<std::mutex> lock(config_mutex_);
  return config_;
}

// =============================================================================
// Statistics
// =============================================================================

void Optimized2PCEngine::ResetStats() {
  std::lock_guard<std::mutex> lock(stats_mutex_);
  stats_.total_transactions.store(0);
  stats_.committed_transactions.store(0);
  stats_.aborted_transactions.store(0);
  stats_.timeout_transactions.store(0);
  stats_.latency_sum_us.store(0);
  stats_.latency_count.store(0);
  stats_.ops_last_second.store(0);
  stats_.current_throughput.store(0.0);
}

// =============================================================================
// Helper Methods
// =============================================================================

std::vector<std::shared_ptr<StorageClient>> Optimized2PCEngine::GetParticipants(
    const std::vector<CedarKey>& keys) {
  // Simple hash-based participant selection
  std::vector<std::shared_ptr<StorageClient>> participants;
  
  if (clients_.empty()) {
    return participants;
  }
  
  // For simplicity, use all clients
  // In production, select based on key partitioning
  participants = clients_;
  
  return participants;
}

bool Optimized2PCEngine::WaitForPrepareQuorum(
    const std::shared_ptr<TransactionContext>& ctx,
    std::vector<std::future<bool>>& futures) {
  
  int timeout_ms = config_.prepare_timeout_ms;
  auto start = std::chrono::steady_clock::now();
  
  for (auto& f : futures) {
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - start).count();
    
    if (elapsed >= timeout_ms) {
      return false;  // Timeout
    }
    
    auto remaining = timeout_ms - elapsed;
    auto status = f.wait_for(std::chrono::milliseconds(remaining));
    
    if (status != std::future_status::ready) {
      return false;  // Timeout
    }
    
    if (!f.get()) {
      return false;  // Prepare failed
    }
  }
  
  return true;
}

bool Optimized2PCEngine::WaitForCommitQuorum(
    const std::shared_ptr<TransactionContext>& ctx,
    std::vector<std::future<bool>>& futures) {
  
  int timeout_ms = config_.commit_timeout_ms;
  auto start = std::chrono::steady_clock::now();
  
  for (auto& f : futures) {
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - start).count();
    
    if (elapsed >= timeout_ms) {
      return false;
    }
    
    auto remaining = timeout_ms - elapsed;
    auto status = f.wait_for(std::chrono::milliseconds(remaining));
    
    if (status != std::future_status::ready) {
      return false;
    }
    
    if (!f.get()) {
      return false;
    }
  }
  
  return true;
}

}  // namespace dtx
}  // namespace cedar
