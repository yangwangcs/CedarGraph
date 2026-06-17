#include "cedar/core/logging.h"
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
#include "cedar/dtx/transaction_recovery_manager.h"
#include "cedar/dtx/transaction_timeout_manager.h"
#include "cedar/dtx/transaction_metrics.h"
#include "cedar/dtx/dtx_rpc_client.h"

#include <algorithm>
#include <chrono>
#include <cerrno>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <unistd.h>

namespace cedar {
namespace dtx {

// =============================================================================
// Constructor / Destructor
// =============================================================================

Optimized2PCEngine::Optimized2PCEngine(const TwoPCConfig& config)
    : config_(config),
      thread_pool_(std::make_unique<ThreadPool>(config.parallel_threads)),
      atomic_batch_size_(config.batch_size),
      atomic_enable_adaptive_tuning_(config.enable_adaptive_tuning) {
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
  
  shutdown_.store(false);
  
  {
    std::lock_guard<std::mutex> lock(clients_mutex_);
    clients_ = clients;
  }
  
  // Wire RPC client to recovery manager if both are available
  if (recovery_manager_) {
    SyncRecoveryRpcClient();
  }
  
  // Initialize timeout manager if set
  if (timeout_manager_ && recovery_manager_) {
    cedar::TimeoutConfig timeout_config;
    timeout_config.prepare_timeout = std::chrono::milliseconds(config_.prepare_timeout_ms);
    timeout_config.commit_timeout = std::chrono::milliseconds(config_.commit_timeout_ms);
    timeout_config.max_transaction_duration = std::chrono::milliseconds(
        config_.prepare_timeout_ms + config_.commit_timeout_ms + 10000);
    timeout_manager_->Initialize(timeout_config, recovery_manager_);
  }
  
  // Start worker threads for parallel execution
  int num_workers = config_.parallel_threads > 0 ? config_.parallel_threads : 4;
  for (int i = 0; i < num_workers; ++i) {
    worker_threads_.emplace_back([this]() {
      while (!shutdown_.load()) {
        std::function<void()> task;
        {
          std::unique_lock<std::mutex> lock(task_mutex_);
          task_cv_.wait_for(lock, std::chrono::milliseconds(100),
                            [this]() { return !task_queue_.empty() || shutdown_.load(); });
          if (shutdown_.load()) break;
          if (task_queue_.empty()) continue;
          task = std::move(task_queue_.front());
          task_queue_.pop();
        }
        if (task) {
          task();
        }
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
  
  // Start batch thread if using batched strategy
  if (config_.strategy == TwoPCConfig::Strategy::kBatched ||
      config_.strategy == TwoPCConfig::Strategy::kHybrid) {
    batch_thread_ = std::thread([this]() {
      BatchWorkerLoop();
    });
  }
  
  // Start adaptive tuning thread
  if (atomic_enable_adaptive_tuning_.load()) {
    tuning_thread_ = std::thread([this]() {
      AdaptiveTuningLoop();
    });
  }
  
  return Status::OK();
}

void Optimized2PCEngine::SetRecoveryManager(
    TransactionRecoveryManager* recovery_manager) {
  recovery_manager_ = recovery_manager;
  SyncRecoveryRpcClient();
}

void Optimized2PCEngine::Shutdown() noexcept {
  if (!running_.exchange(false)) {
    return;
  }
  
  shutdown_.store(true);
  
  // Wake up all waiting threads FIRST
  pipeline_cv_.notify_all();
  batch_cv_.notify_all();
  task_cv_.notify_all();
  
  // Join worker threads BEFORE destroying thread pool
  for (auto& t : worker_threads_) {
    if (t.joinable()) {
      try { t.join(); } catch (...) { CEDAR_LOG_ERROR() << "[2PC] Worker thread join exception" << std::endl; }
    }
  }
  
  try {
    if (pipeline_thread_.joinable()) {
      pipeline_thread_.join();
    }
  } catch (...) { CEDAR_LOG_ERROR() << "[2PC] Pipeline thread join exception" << std::endl; }
  
  try {
    if (batch_thread_.joinable()) {
      batch_thread_.join();
    }
  } catch (...) { CEDAR_LOG_ERROR() << "[2PC] Batch thread join exception" << std::endl; }
  
  try {
    if (tuning_thread_.joinable()) {
      tuning_thread_.join();
    }
  } catch (...) { CEDAR_LOG_ERROR() << "[2PC] Tuning thread join exception" << std::endl; }
  
  // NOW safe to destroy thread pool
  if (thread_pool_) {
    thread_pool_->WaitForAll();
    thread_pool_.reset();
  }
  
  // Shutdown timeout and recovery managers
  try {
    if (timeout_manager_) {
      timeout_manager_->Shutdown();
    }
  } catch (...) { CEDAR_LOG_ERROR() << "[2PC] Timeout manager shutdown exception" << std::endl; }
  try {
    if (recovery_manager_) {
      recovery_manager_->SetDecisionLogLoader(nullptr);
      recovery_manager_->Shutdown();
    }
  } catch (...) { CEDAR_LOG_ERROR() << "[2PC] Recovery manager shutdown exception" << std::endl; }
}

// =============================================================================
// Transaction Execution APIs
// =============================================================================

Status Optimized2PCEngine::Execute2PC(TxnID txn_id,
                                       const std::vector<::cedar::CedarKey>& read_set,
                                       const std::vector<::cedar::CedarKey>& write_set,
                                       Timestamp commit_ts,
                                       Timestamp read_timestamp) {
  TXN_METRICS_START(txn_id, TxnType::kCrossTemporalRange);
  auto ctx = std::make_shared<TransactionContext>(
      txn_id, read_set, write_set, commit_ts, read_timestamp);
  
  // Register transaction with timeout manager
  struct TimeoutGuard {
    TransactionTimeoutManager* mgr;
    TxnID txn_id;
    bool active = true;
    ~TimeoutGuard() {
      if (active && mgr) {
        mgr->UnregisterTransaction(txn_id);
      }
    }
    void Dismiss() { active = false; }
  };
  TimeoutGuard timeout_guard{timeout_manager_, txn_id, false};
  if (timeout_manager_) {
    auto pids = GetPartitionIDs(write_set);
    timeout_manager_->RegisterTransaction(txn_id, pids);
    timeout_guard.active = true;
  }
  
  // Persist initial transaction state with correct participant list
  if (state_manager_) {
    auto pids = GetPartitionIDs(write_set);
    state_manager_->CreateTransaction(txn_id, pids);
  }
  
  // Select execution strategy
  TwoPCConfig::Strategy strategy;
  {
    std::lock_guard<std::mutex> lock(config_mutex_);
    strategy = config_.strategy;
  }
  
  Status result;
  switch (strategy) {
    case TwoPCConfig::Strategy::kSequential:
      result = ExecuteSequential2PC(ctx);
      break;
    case TwoPCConfig::Strategy::kParallel:
      result = ExecuteParallel2PC(ctx);
      break;
    case TwoPCConfig::Strategy::kPipelined:
      result = ExecutePipelined2PC(ctx);
      break;
    case TwoPCConfig::Strategy::kBatched:
      result = ExecuteBatched2PC(ctx);
      break;
    case TwoPCConfig::Strategy::kHybrid:
      // Auto-select based on workload characteristics
      if (write_set.size() > 10) {
        result = ExecuteBatched2PC(ctx);  // Large writes -> batch
      } else if (stats_.current_throughput.load() > 10000) {
        result = ExecutePipelined2PC(ctx);  // High throughput -> pipeline
      } else {
        result = ExecuteParallel2PC(ctx);  // Default -> parallel
      }
      break;
    default:
      result = ExecuteParallel2PC(ctx);
  }

  // Record final metrics
  auto end_time = std::chrono::steady_clock::now();
  auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
      end_time - ctx->start_time).count();
  if (result.ok()) {
    TXN_METRICS_COMMIT(txn_id, latency_us);
  } else {
    TXN_METRICS_ABORT(txn_id, latency_us, result.ToString());
  }

  // Unregister transaction from timeout manager
  if (timeout_manager_) {
    timeout_guard.Dismiss();
    timeout_manager_->UnregisterTransaction(txn_id);
  }

  return result;
}

void Optimized2PCEngine::Execute2PCAsync(
    TxnID txn_id,
    const std::vector<::cedar::CedarKey>& read_set,
    const std::vector<::cedar::CedarKey>& write_set,
    Timestamp commit_ts,
    std::function<void(Status)> callback) {
  
  auto ctx = std::make_shared<TransactionContext>(
      txn_id, read_set, write_set, commit_ts);
  ctx->callback = callback;
  
  // Submit to internal task queue (processed by worker_threads_)
  {
    std::lock_guard<std::mutex> lock(task_mutex_);
    task_queue_.push([this, ctx]() {
      auto status = Execute2PC(ctx->txn_id, ctx->read_set,
                               ctx->write_set, ctx->commit_ts);
      if (ctx->callback) {
        ctx->callback(status);
      }
    });
  }
  task_cv_.notify_one();
}

std::vector<Status> Optimized2PCEngine::Execute2PCBatch(
    const std::vector<std::tuple<TxnID, std::vector<::cedar::CedarKey>,
                                 std::vector<::cedar::CedarKey>, Timestamp>>& transactions) {
  if (transactions.empty()) {
    return {};
  }

  // 使用 shared_ptr 包装同步状态，避免 task_queue_.push 异常时栈变量被销毁导致 UAF
  struct BatchSync {
    std::vector<Status> results;
    std::atomic<size_t> completed{0};
    std::mutex done_mutex;
    std::condition_variable done_cv;
  };
  auto sync = std::make_shared<BatchSync>();
  sync->results.resize(transactions.size());

  // 复制事务数据到 lambda 中，避免捕获 transactions 引用
  std::vector<std::tuple<TxnID, std::vector<::cedar::CedarKey>,
                         std::vector<::cedar::CedarKey>, Timestamp>> txn_copies;
  txn_copies.reserve(transactions.size());
  for (const auto& txn : transactions) {
    txn_copies.push_back(txn);
  }

  // Dispatch to internal worker thread pool to avoid std::async thread explosion
  for (size_t i = 0; i < txn_copies.size(); ++i) {
    auto txn = std::move(txn_copies[i]);
    {
      std::lock_guard<std::mutex> lock(task_mutex_);
      task_queue_.push([this, sync, i, txn = std::move(txn)]() mutable {
        sync->results[i] = Execute2PC(
            std::get<0>(txn), std::get<1>(txn),
            std::get<2>(txn), std::get<3>(txn));
        size_t c = sync->completed.fetch_add(1) + 1;
        if (c >= sync->results.size()) {
          std::lock_guard<std::mutex> lock(sync->done_mutex);
          sync->done_cv.notify_one();
        }
      });
    }
    task_cv_.notify_one();
  }

  // Wait for all tasks to complete
  std::unique_lock<std::mutex> lock(sync->done_mutex);
  sync->done_cv.wait(lock, [&sync]() {
    return sync->completed.load() >= sync->results.size();
  });

  return sync->results;
}

Status Optimized2PCEngine::SubmitPipelined(
    TxnID txn_id,
    const std::vector<::cedar::CedarKey>& read_set,
    const std::vector<::cedar::CedarKey>& write_set,
    Timestamp commit_ts) {

  auto ctx = std::make_shared<TransactionContext>(
      txn_id, read_set, write_set, commit_ts);
  auto done_promise = std::make_shared<std::promise<void>>();
  ctx->done_promise = done_promise;
  auto future = done_promise->get_future();

  // Add to pipeline queue
  {
    std::lock_guard<std::mutex> lock(pipeline_mutex_);
    pipeline_queue_.push(ctx);
  }
  pipeline_cv_.notify_one();

  // Wait for completion with timeout
  auto timeout = std::chrono::milliseconds(
      config_.prepare_timeout_ms + config_.commit_timeout_ms);
  if (future.wait_for(timeout) == std::future_status::timeout) {
    // Signal the worker that this transaction should be abandoned.
    // Use compare-and-swap so we only abort if worker hasn't finished yet.
    auto expected = TransactionContext::State::kInit;
    if (ctx->state.compare_exchange_strong(
            expected, TransactionContext::State::kAborted)) {
      if (state_manager_) {
        state_manager_->UpdateState(ctx->txn_id, TxnState::kAborted);
      }
      stats_.timeout_transactions++;
      return Status::IOError("Transaction timeout");
    }
    // If CAS failed, worker already committed or aborted. Wait for it.
    future.wait();
  }

  return ctx->state.load() == TransactionContext::State::kCommitted
         ? Status::OK()
         : Status::IOError("Transaction aborted");
}

std::string Optimized2PCEngine::DecisionLogPath(TxnID txn_id) const {
  std::lock_guard<std::mutex> lock(config_mutex_);
  return config_.decision_log_dir + "/txn_" + std::to_string(txn_id) + ".decision";
}

Status Optimized2PCEngine::PersistCommitDecision(const CommitDecision& decision) {
  std::string decision_log_dir;
  {
    std::lock_guard<std::mutex> lock(config_mutex_);
    decision_log_dir = config_.decision_log_dir;
  }
  if (decision_log_dir.empty()) {
    return Status::OK();
  }
  if (!decision_log_dir.empty()) {
    std::filesystem::create_directories(decision_log_dir);
  }
  std::string path = decision_log_dir + "/txn_" + std::to_string(decision.txn_id) + ".decision";
  int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) {
    return Status::IOError("Cannot write decision log", path);
  }

  auto write_all = [&](const void* data, size_t len) -> bool {
    const char* ptr = static_cast<const char*>(data);
    size_t written = 0;
    while (written < len) {
      ssize_t n = ::write(fd, ptr + written, len - written);
      if (n < 0) {
        if (errno == EINTR) continue;
        return false;
      }
      written += static_cast<size_t>(n);
    }
    return true;
  };

  constexpr uint32_t kMagic = 0x44454301;
  constexpr uint32_t kVersion = 1;
  bool ok = write_all(&kMagic, sizeof(kMagic));
  ok = ok && write_all(&kVersion, sizeof(kVersion));
  ok = ok && write_all(&decision.txn_id, sizeof(decision.txn_id));
  ok = ok && write_all(&decision.commit_ts, sizeof(decision.commit_ts));
  uint32_t num_parts = static_cast<uint32_t>(decision.participants.size());
  ok = ok && write_all(&num_parts, sizeof(num_parts));
  for (PartitionID pid : decision.participants) {
    ok = ok && write_all(&pid, sizeof(pid));
  }

  if (!ok) {
    ::close(fd);
    return Status::IOError("Decision log write incomplete", path);
  }

  if (::fsync(fd) != 0) {
    ::close(fd);
    return Status::IOError("Decision log fsync failed", path);
  }

  if (::close(fd) != 0) {
    return Status::IOError("Decision log close failed", path);
  }
  
  // fsync parent directory to ensure directory entry is durable
  {
    std::string parent_dir = decision_log_dir;
    int dir_fd = ::open(parent_dir.c_str(), O_RDONLY);
    if (dir_fd >= 0) {
      ::fsync(dir_fd);
      ::close(dir_fd);
    }
  }
  
  if (replicated_decision_log_ != nullptr) {
    auto rep_status = replicated_decision_log_->Append(decision);
    if (!rep_status.ok()) {
      CEDAR_LOG_ERROR() << "[2PC] Decision log replication failed for txn "
                << decision.txn_id << ": " << rep_status.ToString() << std::endl;
    }
  }
  return Status::OK();
}

Status Optimized2PCEngine::LoadCommitDecision(TxnID txn_id, CommitDecision* out) {
  std::string decision_log_dir;
  {
    std::lock_guard<std::mutex> lock(config_mutex_);
    decision_log_dir = config_.decision_log_dir;
  }
  if (decision_log_dir.empty()) {
    return Status::NotFound("Decision logging disabled");
  }
  std::string path = decision_log_dir + "/txn_" + std::to_string(txn_id) + ".decision";
  std::ifstream ifs(path, std::ios::binary);
  if (!ifs) {
    return Status::NotFound("Decision log not found", path);
  }
  uint32_t magic = 0, version = 0;
  ifs.read(reinterpret_cast<char*>(&magic), sizeof(magic));
  ifs.read(reinterpret_cast<char*>(&version), sizeof(version));
  if (magic != 0x44454301 || version != 1) {
    return Status::IOError("Decision log format mismatch", path);
  }
  ifs.read(reinterpret_cast<char*>(&out->txn_id), sizeof(out->txn_id));
  ifs.read(reinterpret_cast<char*>(&out->commit_ts), sizeof(out->commit_ts));
  uint32_t num_parts = 0;
  ifs.read(reinterpret_cast<char*>(&num_parts), sizeof(num_parts));
  if (!ifs) {
    return Status::IOError("Decision log truncated: cannot read num_parts", path);
  }
  const uint32_t kMaxParticipants = 10000;  // Sanity limit
  if (num_parts > kMaxParticipants) {
    return Status::IOError("Decision log corrupt: num_parts too large", path);
  }
  out->participants.resize(num_parts);
  for (uint32_t i = 0; i < num_parts; ++i) {
    PartitionID pid;
    ifs.read(reinterpret_cast<char*>(&pid), sizeof(pid));
    if (!ifs) {
      return Status::IOError("Decision log truncated: cannot read participant", path);
    }
    out->participants[i] = pid;
  }
  out->decision_made = true;
  return Status::OK();
}

// =============================================================================
// Strategy Implementations
// =============================================================================

Status Optimized2PCEngine::ExecuteSequential2PC(
    const std::shared_ptr<TransactionContext>& ctx) {
  
  auto participants = GetParticipants(ctx->write_set);
  
  // Phase 1: Prepare (sequential RPC)
  ctx->state.store(TransactionContext::State::kPreparing);
  auto prepare_start = std::chrono::steady_clock::now();

  for (auto& client : participants) {
    // Real Prepare RPC call
    auto result = client->Prepare(
        ctx->txn_id,
        ctx->read_set,
        ctx->write_set,
        ctx->commit_ts,
        ctx->read_timestamp);

    if (result.ok() && result.ValueOrDie()) {
      ctx->prepare_acks.fetch_add(1);
    } else {
      ctx->prepare_nacks.fetch_add(1);
      break;  // Stop on first failure — no point preparing remaining participants
    }
  }

  auto prepare_end = std::chrono::steady_clock::now();
  auto prepare_latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
      prepare_end - prepare_start).count();
  bool prepare_success = ctx->prepare_acks.load() >= static_cast<int>(participants.size());
  if (auto* metrics = GetGlobalTransactionMetrics()) {
    metrics->RecordPreparePhase(ctx->txn_id, prepare_latency_us, prepare_success);
  }

  if (!prepare_success) {
    ctx->state.store(TransactionContext::State::kAborting);

    // Abort only participants that were actually prepared (not all participants)
    int prepared_count = ctx->prepare_acks.load();
    for (int i = 0; i < prepared_count && i < static_cast<int>(participants.size()); ++i) {
      auto abort_status = participants[i]->Abort(ctx->txn_id);
      if (abort_status.ok()) {
        ctx->abort_acks.fetch_add(1);
      }
    }

    stats_.aborted_transactions++;
    return Status::IOError("Prepare phase failed");
  }

  ctx->state.store(TransactionContext::State::kPrepared);
  ctx->prepare_complete_time = std::chrono::steady_clock::now();

  // Persist commit decision before broadcasting
  CommitDecision decision;
  decision.txn_id = ctx->txn_id;
  decision.commit_ts = ctx->commit_ts;
  decision.participants = GetPartitionIDs(ctx->write_set);
  Status decision_status = PersistCommitDecision(decision);
  if (!decision_status.ok()) {
    CEDAR_LOG_ERROR() << "[Optimized2PCEngine] Failed to persist commit decision for txn="
              << ctx->txn_id << ": " << decision_status.ToString() << std::endl;
    ctx->state.store(TransactionContext::State::kAborting);
    // Abort all prepared participants to release locks
    for (auto& client : participants) {
      auto abort_status = client->Abort(ctx->txn_id);
      if (!abort_status.ok()) {
        CEDAR_LOG_ERROR() << "[Optimized2PCEngine] Abort failed for txn=" << ctx->txn_id
                  << ": " << abort_status.ToString() << std::endl;
      }
    }
    return Status::IOError("Commit decision persistence failed — aborting transaction");
  }

  // Phase 2: Commit (sequential RPC)
  ctx->state.store(TransactionContext::State::kCommitting);
  auto commit_start = std::chrono::steady_clock::now();

  for (auto& client : participants) {
    // Real Commit RPC call
    auto status = client->Commit(ctx->txn_id, ctx->commit_ts);

    if (status.ok()) {
      ctx->commit_acks.fetch_add(1);
    }
  }

  auto commit_end = std::chrono::steady_clock::now();
  auto commit_latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
      commit_end - commit_start).count();
  bool commit_success = ctx->commit_acks.load() == static_cast<int>(participants.size());
  if (auto* metrics = GetGlobalTransactionMetrics()) {
    metrics->RecordCommitPhase(ctx->txn_id, commit_latency_us, commit_success);
  }

  if (commit_success) {
    ctx->state.store(TransactionContext::State::kCommitted);
    stats_.committed_transactions++;

    auto end_time = std::chrono::steady_clock::now();
    auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
        end_time - ctx->start_time).count();
    stats_.RecordLatency(latency_us);

    return Status::OK();
  } else {
    // Commit phase incomplete but decision is persisted.
    // Recovery will read the decision log and drive remaining participants.
    ctx->state.store(TransactionContext::State::kCommitting);
    if (recovery_manager_) {
      recovery_manager_->StartRecovery(ctx->txn_id);
    }
    return Status::IOError("Commit phase incomplete — recovery will complete remaining participants");
  }
}

Status Optimized2PCEngine::ExecuteParallel2PC(
    const std::shared_ptr<TransactionContext>& ctx) {

  auto participants = GetParticipants(ctx->write_set);

  // Phase 1: Prepare (parallel RPC)
  ctx->state.store(TransactionContext::State::kPreparing);
  auto prepare_start = std::chrono::steady_clock::now();

  // Phase 1: Prepare (parallel RPC via thread pool)
  // Use shared_ptr to prevent UAF if lambda outlives the function
  auto prepare_promises = std::make_shared<std::vector<std::promise<bool>>>(participants.size());
  std::vector<std::future<bool>> prepare_futures;
  prepare_futures.reserve(participants.size());
  for (auto& p : *prepare_promises) {
    prepare_futures.push_back(p.get_future());
  }
  for (size_t i = 0; i < participants.size(); ++i) {
    thread_pool_->Schedule([client = participants[i], ctx, prepare_promises, i]() {
      try {
        auto result = client->Prepare(
            ctx->txn_id,
            ctx->read_set,
            ctx->write_set,
            ctx->commit_ts,
            ctx->read_timestamp);
        if (result.ok() && result.ValueOrDie()) {
          ctx->prepare_acks.fetch_add(1);
          (*prepare_promises)[i].set_value(true);
        } else {
          ctx->prepare_nacks.fetch_add(1);
          (*prepare_promises)[i].set_value(false);
        }
      } catch (...) {
        CEDAR_LOG_ERROR() << "[2PC] Prepare RPC exception for txn_id=" << ctx->txn_id << std::endl;
        ctx->prepare_nacks.fetch_add(1);
        (*prepare_promises)[i].set_value(false);
      }
    });
  }

  // Wait for all prepare tasks to complete
  for (auto& f : prepare_futures) {
    f.wait();
  }
  bool all_prepared = WaitForPrepareQuorum(ctx, prepare_futures);

  auto prepare_end = std::chrono::steady_clock::now();
  auto prepare_latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
      prepare_end - prepare_start).count();
  if (auto* metrics = GetGlobalTransactionMetrics()) {
    metrics->RecordPreparePhase(ctx->txn_id, prepare_latency_us, all_prepared);
  }

  if (!all_prepared) {
    ctx->state.store(TransactionContext::State::kAborting);

    // Send Abort to participants that prepared successfully
    std::vector<std::promise<void>> abort_promises(participants.size());
    std::vector<std::future<void>> abort_futures;
    abort_futures.reserve(participants.size());
    for (auto& p : abort_promises) {
      abort_futures.push_back(p.get_future());
    }
    for (size_t i = 0; i < participants.size(); ++i) {
      thread_pool_->Schedule([client = participants[i], ctx, &abort_promises, i]() {
        try {
          auto abort_status = client->Abort(ctx->txn_id);
          if (abort_status.ok()) {
            ctx->abort_acks.fetch_add(1);
          }
        } catch (...) {
          CEDAR_LOG_ERROR() << "[2PC] Abort RPC exception for txn_id=" << ctx->txn_id << std::endl;
          // Abort 异常不应导致线程崩溃
        }
        abort_promises[i].set_value();
      });
    }
    for (auto& f : abort_futures) {
      f.wait();
    }

    stats_.aborted_transactions++;
    return Status::IOError("Prepare phase failed or timed out");
  }

  ctx->state.store(TransactionContext::State::kPrepared);
  ctx->prepare_complete_time = std::chrono::steady_clock::now();

  // Persist commit decision before broadcasting
  CommitDecision decision;
  decision.txn_id = ctx->txn_id;
  decision.commit_ts = ctx->commit_ts;
  decision.participants = GetPartitionIDs(ctx->write_set);
  Status decision_status = PersistCommitDecision(decision);
  if (!decision_status.ok()) {
    CEDAR_LOG_ERROR() << "[Optimized2PCEngine] Failed to persist commit decision for txn="
              << ctx->txn_id << ": " << decision_status.ToString() << std::endl;
    ctx->state.store(TransactionContext::State::kAborting);
    // Abort all prepared participants to release locks
    for (auto& client : participants) {
      auto abort_status = client->Abort(ctx->txn_id);
      if (!abort_status.ok()) {
        CEDAR_LOG_ERROR() << "[Optimized2PCEngine] Abort failed for txn=" << ctx->txn_id
                  << ": " << abort_status.ToString() << std::endl;
      }
    }
    return Status::IOError("Commit decision persistence failed — aborting transaction");
  }

  if (state_manager_) {
    state_manager_->UpdateState(ctx->txn_id, TxnState::kPrepared);
  }

  // Phase 2: Commit (parallel RPC)
  ctx->state.store(TransactionContext::State::kCommitting);
  auto commit_start = std::chrono::steady_clock::now();

  // Phase 2: Commit (parallel RPC via thread pool)
  // Use shared_ptr to prevent UAF if lambda outlives the function
  auto commit_promises = std::make_shared<std::vector<std::promise<bool>>>(participants.size());
  std::vector<std::future<bool>> commit_futures;
  commit_futures.reserve(participants.size());
  for (auto& p : *commit_promises) {
    commit_futures.push_back(p.get_future());
  }
  for (size_t i = 0; i < participants.size(); ++i) {
    thread_pool_->Schedule([client = participants[i], ctx, commit_promises, i]() {
      try {
        auto status = client->Commit(ctx->txn_id, ctx->commit_ts);
        if (status.ok()) {
          ctx->commit_acks.fetch_add(1);
          (*commit_promises)[i].set_value(true);
        } else {
          (*commit_promises)[i].set_value(false);
        }
      } catch (...) {
        CEDAR_LOG_ERROR() << "[2PC] Commit RPC exception for txn_id=" << ctx->txn_id << std::endl;
        (*commit_promises)[i].set_value(false);
      }
    });
  }

  // Wait for all commit tasks to complete
  for (auto& f : commit_futures) {
    f.wait();
  }
  bool all_committed = WaitForCommitQuorum(ctx, commit_futures);

  auto commit_end = std::chrono::steady_clock::now();
  auto commit_latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
      commit_end - commit_start).count();
  if (auto* metrics = GetGlobalTransactionMetrics()) {
    metrics->RecordCommitPhase(ctx->txn_id, commit_latency_us, all_committed);
  }

  if (all_committed) {
    // Persist state FIRST, then update in-memory state
    // This ensures crash recovery sees the correct state
    if (state_manager_) {
      state_manager_->UpdateState(ctx->txn_id, TxnState::kCommitted);
    }
    ctx->state.store(TransactionContext::State::kCommitted);
    stats_.committed_transactions++;
    stats_.total_transactions++;
    
    auto end_time = std::chrono::steady_clock::now();
    auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
        end_time - ctx->start_time).count();
    stats_.RecordLatency(latency_us);
    
    return Status::OK();
  } else {
    // Commit phase incomplete but decision is persisted.
    // Recovery will read the decision log and drive remaining participants.
    ctx->state.store(TransactionContext::State::kCommitting);
    if (state_manager_) {
      state_manager_->UpdateState(ctx->txn_id, TxnState::kCommitting);
    }
    if (recovery_manager_) {
      recovery_manager_->StartRecovery(ctx->txn_id);
    }
    return Status::IOError("Commit phase incomplete — recovery will complete remaining participants");
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
  // Set up promise/future to avoid busy-waiting for completion.
  auto done_promise = std::make_shared<std::promise<void>>();
  ctx->done_promise = done_promise;
  auto future = done_promise->get_future();

  // Add to batch buffer
  {
    std::lock_guard<std::mutex> lock(batch_mutex_);
    batch_buffer_.push_back(ctx);

    // If batch is full or timeout, process it
    if (batch_buffer_.size() >= static_cast<size_t>(atomic_batch_size_.load())) {
      batch_cv_.notify_one();
    }
  }

  // Wait for completion with timeout.
  auto timeout_ms = config_.prepare_timeout_ms + config_.commit_timeout_ms;
  if (future.wait_for(std::chrono::milliseconds(timeout_ms)) == std::future_status::timeout) {
    // Use CAS to abort only if the worker hasn't started yet. If the worker
    // already began processing, wait for it to finish to avoid reporting a
    // timeout when the transaction may have already committed.
    auto expected = TransactionContext::State::kInit;
    if (ctx->state.compare_exchange_strong(expected, TransactionContext::State::kAborted)) {
      if (state_manager_) {
        state_manager_->UpdateState(ctx->txn_id, TxnState::kAborted);
      }
      stats_.timeout_transactions++;
      return Status::IOError("Transaction timeout");
    }
    // Worker already started — wait for completion and return accurate status.
    future.wait();
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
    int max_batch = atomic_batch_size_.load() > 0 ? atomic_batch_size_.load() : 4;
    
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

        // Check if transaction was aborted by timeout before we start
        if (ctx->state.load() == TransactionContext::State::kAborted) {
          if (ctx->done_promise) {
            ctx->done_promise->set_value();
          }
          continue;
        }
        
        if (participants.empty()) {
          ctx->state.store(TransactionContext::State::kAborted);
          if (state_manager_) {
            state_manager_->UpdateState(ctx->txn_id, TxnState::kAborted);
          }
          if (ctx->done_promise) {
            ctx->done_promise->set_value();
          }
          continue;
        }
        
        // Phase 1: Prepare all in parallel
        ctx->state.store(TransactionContext::State::kPreparing);
        std::vector<std::promise<bool>> prepare_promises(participants.size());
        std::vector<std::future<bool>> prepare_results;
        prepare_results.reserve(participants.size());
        for (auto& p : prepare_promises) {
          prepare_results.push_back(p.get_future());
        }
        for (size_t i = 0; i < participants.size(); ++i) {
          thread_pool_->Schedule([client = participants[i], ctx, &prepare_promises, i]() {
            try {
              auto result = client->Prepare(ctx->txn_id, ctx->read_set, ctx->write_set, ctx->commit_ts, ctx->read_timestamp);
              if (result.ok() && result.ValueOrDie()) {
                ctx->prepare_acks.fetch_add(1);
                prepare_promises[i].set_value(true);
              } else {
                ctx->prepare_nacks.fetch_add(1);
                prepare_promises[i].set_value(false);
              }
            } catch (...) {
              CEDAR_LOG_ERROR() << "[2PC] Prepare RPC exception for txn_id=" << ctx->txn_id << std::endl;
              ctx->prepare_nacks.fetch_add(1);
              prepare_promises[i].set_value(false);
            }
          });
        }
        
        for (auto& f : prepare_results) {
          f.get();
        }
        
        bool all_prepared = (ctx->prepare_acks.load() == static_cast<int>(participants.size()));
        if (!all_prepared) {
          ctx->state.store(TransactionContext::State::kAborting);
          for (auto& client : participants) {
            try {
              auto abort_status = client->Abort(ctx->txn_id);
              if (abort_status.ok()) {
                ctx->abort_acks.fetch_add(1);
              }
            } catch (...) {
              CEDAR_LOG_ERROR() << "[2PC] Abort RPC exception for txn_id=" << ctx->txn_id << std::endl;
            }
          }
          ctx->state.store(TransactionContext::State::kAborted);
          if (state_manager_) {
            state_manager_->UpdateState(ctx->txn_id, TxnState::kAborted);
          }
          stats_.aborted_transactions++;
          if (ctx->done_promise) {
            ctx->done_promise->set_value();
          }
          continue;
        }
        
        ctx->state.store(TransactionContext::State::kPrepared);
        if (state_manager_) {
          state_manager_->UpdateState(ctx->txn_id, TxnState::kPrepared);
        }

        // Persist commit decision before broadcasting
        CommitDecision decision;
        decision.txn_id = ctx->txn_id;
        decision.commit_ts = ctx->commit_ts;
        decision.participants = GetPartitionIDs(ctx->write_set);
        Status decision_status = PersistCommitDecision(decision);
        if (!decision_status.ok()) {
          CEDAR_LOG_ERROR() << "[Optimized2PCEngine] Failed to persist commit decision for pipelined txn="
                    << ctx->txn_id << ": " << decision_status.ToString() << std::endl;
          ctx->state.store(TransactionContext::State::kAborting);
          // Abort all prepared participants to release locks
          for (auto& client : participants) {
            try {
              auto abort_status = client->Abort(ctx->txn_id);
              if (!abort_status.ok()) {
                CEDAR_LOG_ERROR() << "[Optimized2PCEngine] Abort failed for txn=" << ctx->txn_id
                          << ": " << abort_status.ToString() << std::endl;
              }
            } catch (...) {
              CEDAR_LOG_ERROR() << "[2PC] Abort RPC exception for txn_id=" << ctx->txn_id << std::endl;
            }
          }
          if (state_manager_) {
            state_manager_->UpdateState(ctx->txn_id, TxnState::kAborted);
          }
          stats_.aborted_transactions++;
          if (ctx->done_promise) {
            ctx->done_promise->set_value();
          }
          continue;  // Process next transaction in batch
        }

        // Phase 2: Commit all in parallel
        ctx->state.store(TransactionContext::State::kCommitting);
        std::vector<std::promise<bool>> commit_promises(participants.size());
        std::vector<std::future<bool>> commit_results;
        commit_results.reserve(participants.size());
        for (auto& p : commit_promises) {
          commit_results.push_back(p.get_future());
        }
        for (size_t i = 0; i < participants.size(); ++i) {
          thread_pool_->Schedule([client = participants[i], ctx, &commit_promises, i]() {
            try {
              auto status = client->Commit(ctx->txn_id, ctx->commit_ts);
              if (status.ok()) {
                ctx->commit_acks.fetch_add(1);
                commit_promises[i].set_value(true);
              } else {
                commit_promises[i].set_value(false);
              }
            } catch (...) {
              CEDAR_LOG_ERROR() << "[2PC] Commit RPC exception for txn_id=" << ctx->txn_id << std::endl;
              commit_promises[i].set_value(false);
            }
          });
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
          // Once commit phase has started, we cannot abort. Some participants
          // may have already committed. Leave state as kCommitting for recovery.
          if (state_manager_) {
            state_manager_->UpdateState(ctx->txn_id, TxnState::kCommitting);
          }
          if (recovery_manager_) {
            recovery_manager_->StartRecovery(ctx->txn_id);
          }
        }

        // Notify synchronous waiter (e.g., SubmitPipelined)
        if (ctx->done_promise) {
          ctx->done_promise->set_value();
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
      atomic_batch_size_.store(tuning_state_.current_batch_size);
    }
  }
  
  tuning_state_.last_throughput = current_throughput;
  tuning_state_.last_latency = avg_latency;
  
  // Reset throughput counter
  stats_.current_throughput.store(stats_.ops_last_second.exchange(0));
}

void Optimized2PCEngine::EnableAdaptiveTuning(bool enable) {
  std::lock_guard<std::mutex> lock(config_mutex_);
  config_.enable_adaptive_tuning = enable;
  atomic_enable_adaptive_tuning_.store(enable);
}

void Optimized2PCEngine::UpdateConfiguration(const TwoPCConfig& config) {
  std::lock_guard<std::mutex> lock(config_mutex_);
  config_ = config;
  atomic_batch_size_.store(config.batch_size);
  atomic_enable_adaptive_tuning_.store(config.enable_adaptive_tuning);
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
    const std::vector<::cedar::CedarKey>& keys) {
  std::vector<std::shared_ptr<StorageClient>> participants;
  
  {
    std::lock_guard<std::mutex> lock(clients_mutex_);
    if (clients_.empty()) {
      return participants;
    }
    
    if (keys.empty()) {
      // Fallback: return all clients for operations without explicit keys
      participants = clients_;
      return participants;
    }
    
    // Deduplicate by partition ID, not by pointer
    std::unordered_set<PartitionID> seen_partitions;
    for (const auto& key : keys) {
      PartitionID pid = key.part_id();
      if (pid == 0) {
        // Default partition: use entity_id hash
        pid = static_cast<PartitionID>(key.entity_id() % std::max<size_t>(1, clients_.size()));
      }
      if (seen_partitions.insert(pid).second) {
        size_t client_idx = pid % clients_.size();
        participants.push_back(clients_[client_idx]);
      }
    }
  }
  
  return participants;
}

std::vector<PartitionID> Optimized2PCEngine::GetPartitionIDs(
    const std::vector<::cedar::CedarKey>& keys) {
  std::vector<PartitionID> pids;
  
  {
    std::lock_guard<std::mutex> lock(clients_mutex_);
    if (clients_.empty()) {
      return pids;
    }
    
    if (keys.empty()) {
      // Fallback: return indices of all clients as partition IDs
      for (size_t i = 0; i < clients_.size(); ++i) {
        pids.push_back(static_cast<PartitionID>(i));
      }
      return pids;
    }
    
    std::unordered_set<PartitionID> selected;
    for (const auto& key : keys) {
      PartitionID pid = key.part_id();
      if (pid == 0) {
        // Default partition: use entity_id hash consistent with GetParticipants
        pid = static_cast<PartitionID>(key.entity_id() % std::max<size_t>(1, clients_.size()));
      }
      selected.insert(pid);
    }
    
    pids.assign(selected.begin(), selected.end());
    std::sort(pids.begin(), pids.end());
  }
  
  return pids;
}

void Optimized2PCEngine::AddClient(std::shared_ptr<StorageClient> client) {
  if (!client) return;
  {
    std::lock_guard<std::mutex> lock(clients_mutex_);
    // Avoid duplicates by checking server address
    for (const auto& existing : clients_) {
      if (existing && existing->IsConnected() && client->IsConnected()) {
        // Note: StorageClient doesn't expose address directly.
        // In practice, caller should ensure uniqueness.
        // For now, rely on GraphServiceRouter to avoid duplicate calls.
      }
    }
    clients_.push_back(std::move(client));
  }
  SyncRecoveryRpcClient();
}

void Optimized2PCEngine::RemoveClient(const std::string& server_address) {
  std::lock_guard<std::mutex> lock(clients_mutex_);
  clients_.erase(
      std::remove_if(clients_.begin(), clients_.end(),
                     [&server_address](const std::shared_ptr<StorageClient>& client) {
                       return client && client->GetServerAddress() == server_address;
                     }),
      clients_.end());
}

size_t Optimized2PCEngine::GetClientCount() const {
  std::lock_guard<std::mutex> lock(clients_mutex_);
  return clients_.size();
}

void Optimized2PCEngine::SyncRecoveryRpcClient() {
  if (!recovery_manager_) {
    return;
  }
  
  recovery_manager_->SetDecisionLogLoader(
      [this](dtx::TxnID txn_id,
             std::vector<dtx::PartitionID>* participants,
             ::cedar::Timestamp* commit_ts) -> Status {
        CommitDecision decision;
        Status s = LoadCommitDecision(txn_id, &decision);
        if (!s.ok()) {
          CEDAR_LOG_ERROR() << "[Optimized2PCEngine] Decision log missing or unreadable for txn=" << txn_id
                    << ": " << s.ToString() << std::endl;
          return s;
        }
        *participants = std::move(decision.participants);
        *commit_ts = decision.commit_ts;
        return Status::OK();
      });
  
  std::lock_guard<std::mutex> lock(clients_mutex_);
  if (clients_.empty()) {
    return;
  }
  
  if (!dtx_rpc_client_) {
    dtx::DTXRpcConfig rpc_config;
    rpc_config.tls_config.enabled = false;  // Disable TLS for local development
    dtx_rpc_client_ = std::make_shared<dtx::DTXRpcClient>(rpc_config);
  }
  
  for (size_t i = 0; i < clients_.size(); ++i) {
    const auto& client = clients_[i];
    if (!client) continue;
    dtx_rpc_client_->AddParticipant(
        static_cast<dtx::NodeID>(i), client->GetServerAddress());
  }
  
  recovery_manager_->SetRpcClient(dtx_rpc_client_);
  
  // Use the same hash-based mapping as GetParticipants:
  // partition -> client_index = pid % clients_.size()
  recovery_manager_->SetPartitionResolver(
      [num_clients = clients_.size()](dtx::PartitionID pid) -> dtx::NodeID {
        if (num_clients == 0) return dtx::kInvalidNodeID;
        return static_cast<dtx::NodeID>(pid % num_clients);
      });
}

bool Optimized2PCEngine::WaitForPrepareQuorum(
    const std::shared_ptr<TransactionContext>& ctx,
    std::vector<std::future<bool>>& futures) {

  int timeout_ms = config_.prepare_timeout_ms;
  auto start = std::chrono::steady_clock::now();

  int success_count = 0;
  int failure_count = 0;
  const int total = static_cast<int>(futures.size());
  // In 2PC, ALL participants must prepare successfully for atomicity.
  // Majority quorum is incorrect and leaves unprepared participants in inconsistent state.
  const int required_successes = total;

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

    if (f.get()) {
      success_count++;
      if (success_count >= required_successes) {
        return true;  // All participants prepared successfully
      }
    } else {
      failure_count++;
      if (failure_count > 0) {
        return false;  // Any failure means Prepare phase cannot succeed
      }
    }
  }

  return success_count >= required_successes;
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

// =============================================================================
// Batch Worker
// =============================================================================

void Optimized2PCEngine::BatchWorkerLoop() {
  while (!shutdown_.load()) {
    std::unique_lock<std::mutex> lock(batch_mutex_);
    batch_cv_.wait_for(lock, std::chrono::milliseconds(100),
                       [this]() { return !batch_buffer_.empty() || shutdown_.load(); });
    if (shutdown_.load()) break;
    if (batch_buffer_.empty()) continue;
    
    std::vector<std::shared_ptr<TransactionContext>> batch = std::move(batch_buffer_);
    batch_buffer_.clear();
    lock.unlock();
    
    for (auto& ctx : batch) {
      try {
        // If the client timed out and aborted before we started, skip processing.
        if (ctx->state.load() == TransactionContext::State::kAborted) {
          if (ctx->done_promise) {
            ctx->done_promise->set_value();
          }
          continue;
        }
        auto status = ExecuteParallel2PC(ctx);
        if (!status.ok()) {
          ctx->state.store(TransactionContext::State::kAborted);
        } else {
          ctx->state.store(TransactionContext::State::kCommitted);
        }
        if (ctx->done_promise) {
          ctx->done_promise->set_value();
        }
      } catch (const std::exception& e) {
        CEDAR_LOG_ERROR() << "[2PC] BatchWorkerLoop exception: " << e.what() << std::endl;
        if (ctx) {
          ctx->state.store(TransactionContext::State::kAborted);
          if (ctx->done_promise) {
            ctx->done_promise->set_value();
          }
        }
      } catch (...) {
        CEDAR_LOG_ERROR() << "[2PC] BatchWorkerLoop unknown exception" << std::endl;
        if (ctx) {
          ctx->state.store(TransactionContext::State::kAborted);
          if (ctx->done_promise) {
            ctx->done_promise->set_value();
          }
        }
      }
    }
  }
}

}  // namespace dtx
}  // namespace cedar
