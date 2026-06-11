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

#include "cedar/transaction/occ_transaction.h"


#include "cedar/storage/cedar_memtable.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/transaction/batch_api.h"

namespace cedar {

// ========== TransactionManager ==========

TransactionManager::TransactionManager() = default;

uint64_t TransactionManager::AllocateTransactionId() {
  return next_txn_id_.fetch_add(1, std::memory_order_acq_rel);
}

Timestamp TransactionManager::AllocateTimestamp() {
  // 使用分片时间戳分配器 - 消除高并发下的原子操作竞争
  // ShardedTimestampAllocator 为每个线程提供本地缓存
  return timestamp_allocator_.Allocate();
}

Timestamp TransactionManager::AllocateTimestampBatch(uint32_t count) {
  if (count == 0) count = 1;
  // 使用分片分配器的批量分配接口
  // AllocateBatch 返回批次中的第一个时间戳
  return timestamp_allocator_.AllocateBatch(count);
}

Timestamp TransactionManager::AllocateGlobalTimestamp() {
  return timestamp_allocator_.AllocateGlobal();
}

Timestamp TransactionManager::CurrentTimestamp() const {
  return timestamp_allocator_.CurrentTimestamp();
}

void TransactionManager::RegisterActiveTransaction(uint64_t txn_id, 
                                                    Timestamp start_ts) {
  std::lock_guard<std::mutex> lock(active_txns_mutex_);
  active_txns_[txn_id] = start_ts;
  stats_.txn_started.fetch_add(1, std::memory_order_relaxed);
}

void TransactionManager::UnregisterActiveTransaction(uint64_t txn_id) {
  std::lock_guard<std::mutex> lock(active_txns_mutex_);
  active_txns_.erase(txn_id);
}

std::vector<std::pair<uint64_t, Timestamp>> 
TransactionManager::GetActiveTransactions() const {
  std::lock_guard<std::mutex> lock(active_txns_mutex_);
  std::vector<std::pair<uint64_t, Timestamp>> result;
  result.reserve(active_txns_.size());
  for (const auto& [txn_id, ts] : active_txns_) {
    result.emplace_back(txn_id, ts);
  }
  return result;
}

bool TransactionManager::HasActiveTransactionBefore(Timestamp ts) const {
  std::lock_guard<std::mutex> lock(active_txns_mutex_);
  for (const auto& [txn_id, start_ts] : active_txns_) {
    if (start_ts < ts) {
      return true;
    }
  }
  return false;
}

Timestamp TransactionManager::GetMinActiveTimestamp() const {
  std::lock_guard<std::mutex> lock(active_txns_mutex_);
  if (active_txns_.empty()) {
    // 使用分片分配器的当前全局计数
    return Timestamp(timestamp_allocator_.GetGlobalNext());
  }
  
  Timestamp min_ts = Timestamp::Max();
  for (const auto& [txn_id, start_ts] : active_txns_) {
    if (start_ts < min_ts) {
      min_ts = start_ts;
    }
  }
  return min_ts;
}

// ========== OCCTransaction ==========

OCCTransaction::OCCTransaction(TransactionManager* txn_manager,
                                VSLMemTable* memtable,
                                LsmEngine* lsm_engine,
                                WalWriter* wal_writer,
                                const TransactionOptions& options)
    : txn_manager_(txn_manager),
      memtable_(memtable),
      lsm_engine_(lsm_engine),
      wal_writer_(wal_writer),
      options_(options) {}

OCCTransaction::~OCCTransaction() {
  // 如果事务还在活跃状态，自动回滚
  if (state_.load() == TransactionState::kActive) {
    Abort();
  }
}

Status OCCTransaction::Begin() {
  if (state_.load() != TransactionState::kActive) {
    return Status::InvalidArgument("OCCTransaction", 
        "Transaction already started or finished");
  }
  
  // 分配事务 ID 和读取时间戳
  txn_id_ = txn_manager_->AllocateTransactionId();
  // P0-6 FIX: Use the current global timestamp for the read snapshot so that
  // the snapshot includes all commits that completed before this txn began.
  // The sharded allocator's per-thread caches do NOT provide cross-thread
  // monotonicity, which breaks visibility under concurrent commits.
  read_timestamp_ = txn_manager_->CurrentTimestamp();
  
  // 注册到活跃事务表
  txn_manager_->RegisterActiveTransaction(txn_id_, read_timestamp_);
  
  return Status::OK();
}

Status OCCTransaction::Commit() {
  // 状态检查
  if (state_.load() != TransactionState::kActive) {
    return Status::InvalidArgument("OCCTransaction",
        "Transaction not in active state");
  }

  state_.store(TransactionState::kValidating);

  // 空事务直接成功
  if (write_set_.empty() && read_set_.empty()) {
    state_.store(TransactionState::kCommitted);
    txn_manager_->UnregisterActiveTransaction(txn_id_);
    txn_manager_->mutable_stats().txn_committed.fetch_add(1, std::memory_order_relaxed);
    return Status::OK();
  }

  // 只读事务也直接成功
  if (write_set_.empty()) {
    state_.store(TransactionState::kCommitted);
    txn_manager_->UnregisterActiveTransaction(txn_id_);
    txn_manager_->mutable_stats().txn_committed.fetch_add(1, std::memory_order_relaxed);
    return Status::OK();
  }

  // ===================================================================
  // P0-6 FIX: Global commit serialization
  // Acquire the engine-wide mutex BEFORE allocating the commit timestamp
  // and Validate() and hold it until MemTable writes are complete. This
  // ensures monotonic commit timestamps, correct MVCC visibility, and
  // that no other transaction can interleave its WAL/MemTable writes.
  // ===================================================================
  std::lock_guard<std::mutex> commit_lock(lsm_engine_->global_commit_mutex_);

  // P0-6 FIX: Allocate the commit timestamp from the global counter while
  // holding global_commit_mutex_. This guarantees commit_timestamp_ is
  // strictly greater than any read_timestamp_ that was assigned before this
  // commit started, which is required for correct MVCC visibility and
  // conflict detection across threads.
  commit_timestamp_ = txn_manager_->AllocateGlobalTimestamp();

  // 修复: 更新写集中的 txn_version 为 commit_timestamp_
  for (auto& entry : write_set_) {
    entry.txn_version = commit_timestamp_;
  }

  // 修复: 重建 WAL batch，使用正确的 commit_timestamp_
  if (wal_writer_) {
    wal_batch_.Clear();
    for (const auto& entry : write_set_) {
      wal_batch_.Put(entry.key, entry.descriptor, entry.txn_version);
    }
  }

  // 验证阶段
  Status validation_status = Validate();
  if (!validation_status.ok()) {
    state_.store(TransactionState::kAborted);
    txn_manager_->UnregisterActiveTransaction(txn_id_);
    txn_manager_->mutable_stats().txn_aborted.fetch_add(1, std::memory_order_relaxed);
    txn_manager_->mutable_stats().validation_failures.fetch_add(1, std::memory_order_relaxed);
    return validation_status;
  }

  state_.store(TransactionState::kCommitting);

  // 写入 WAL (必须先于 MemTable，保证崩溃恢复时 WAL 有记录)
  if (wal_writer_) {
    Status wal_status = WriteToWAL();
    if (!wal_status.ok()) {
      state_.store(TransactionState::kAborted);
      txn_manager_->UnregisterActiveTransaction(txn_id_);
      txn_manager_->mutable_stats().txn_aborted.fetch_add(1, std::memory_order_relaxed);
      return wal_status;
    }
  }

  // 写入 MemTable
  Status write_status = WriteToMemTable();
  if (!write_status.ok()) {
    // ===================================================================
    // P0-6 PARTIAL-WRITE SAFETY: If MemTable write fails after WAL succeeded,
    // we MUST leave a deterministic state. Since we hold global_commit_mutex_,
    // no other txn can commit concurrently. We abort; the WAL entry will be
    // replayed on recovery (idempotent because MVCC version is fixed).
    // ===================================================================
    state_.store(TransactionState::kAborted);
    txn_manager_->UnregisterActiveTransaction(txn_id_);
    txn_manager_->mutable_stats().txn_aborted.fetch_add(1, std::memory_order_relaxed);
    return write_status;
  }

  // 标记提交完成
  state_.store(TransactionState::kCommitted);
  txn_manager_->UnregisterActiveTransaction(txn_id_);
  txn_manager_->mutable_stats().txn_committed.fetch_add(1, std::memory_order_relaxed);

  return Status::OK();
}

Status OCCTransaction::Abort() {
  TransactionState current = state_.load();
  if (current == TransactionState::kCommitted) {
    return Status::InvalidArgument("OCCTransaction", 
        "Cannot abort committed transaction");
  }
  
  if (current == TransactionState::kAborted) {
    return Status::OK();  // 已经是中止状态
  }
  
  state_.store(TransactionState::kAborted);
  txn_manager_->UnregisterActiveTransaction(txn_id_);
  txn_manager_->mutable_stats().txn_aborted.fetch_add(1, std::memory_order_relaxed);
  
  // 写入 WAL 中止记录 (用于恢复)
  if (wal_writer_) {
    wal_writer_->WriteAbort(txn_id_, commit_timestamp_);
  }
  
  Cleanup();
  return Status::OK();
}

Status OCCTransaction::Get(uint64_t entity_id,
                            EntityType entity_type,
                            uint16_t column_id,
                            Descriptor* descriptor,
                            Timestamp* version_ts) {
  if (!descriptor || !version_ts) {
    return Status::InvalidArgument("OCCTransaction", "null pointer");
  }
  
  if (state_.load() != TransactionState::kActive) {
    return Status::InvalidArgument("OCCTransaction", "transaction not active");
  }
  
  std::lock_guard<std::mutex> lock(mutex_);
  
  // 1. 检查写集 (读己之写)
  // 使用带分隔符的复合键，避免字符串拼接碰撞（如 1+23+4 = 12+3+4）
  auto MakeWriteSetKey = [](uint64_t eid, EntityType etype, uint16_t cid) -> std::string {
    return std::to_string(eid) + ":" + std::to_string(static_cast<int>(etype)) + ":" + std::to_string(cid);
  };
  
  std::string target_key = MakeWriteSetKey(entity_id, entity_type, column_id);
  if (write_set_keys_.find(target_key) != write_set_keys_.end()) {
    for (const auto& write_entry : write_set_) {
      if (write_entry.entity_id == entity_id &&
          write_entry.entity_type == entity_type &&
          write_entry.column_id == column_id) {
        *descriptor = write_entry.descriptor;
        *version_ts = read_timestamp_;
        return Status::OK();
      }
    }
  }
  
  // 2. 从 MemTable 读取
  auto chain_opt = memtable_->GetVersionChain(entity_id, entity_type, column_id);
  
  // 3. 根据隔离级别和事务版本号选择版本
  // 快照隔离: 读取 read_timestamp_ (事务版本号) 之前的最新版本
  // 修复: 使用 txn_version 而非业务时间戳进行 MVCC 版本比较
  for (const auto& entry : chain_opt) {
    if (entry.txn_version <= read_timestamp_) {
      *descriptor = entry.descriptor;
      *version_ts = entry.timestamp;  // 返回业务时间戳给用户
      
      // 记录到读集（业务时间戳 + 事务版本号）
      read_set_.push_back({entity_id, entity_type, column_id, 
                           entry.timestamp, entry.txn_version});
      
      return Status::OK();
    }
  }
  
  // 4. MemTable 中没有找到，查询 LsmEngine (SST 文件)
  // SST 文件中的数据已经过 compaction，视为已提交数据。
  // Zone-5 已添加：SST 中存储了 txn_version，支持 MVCC 快照隔离。
  if (lsm_engine_) {
    auto all_versions = lsm_engine_->GetAll(entity_id, entity_type, column_id);
    for (const auto& entry : all_versions) {
      // 使用 txn_version 进行 MVCC 版本比较
      if (entry.txn_version <= read_timestamp_) {
        *descriptor = entry.descriptor;
        *version_ts = entry.timestamp;
        
        read_set_.push_back({entity_id, entity_type, column_id, 
                             entry.timestamp, entry.txn_version});
        
        return Status::OK();
      }
    }
  }
  
  return Status::NotFound("OCCTransaction", "no valid version found");
}

Status OCCTransaction::GetAllColumns(uint64_t entity_id,
                                      EntityType entity_type,
                                      std::vector<std::pair<uint16_t, Descriptor>>* columns) {
  if (!columns) {
    return Status::InvalidArgument("OCCTransaction", "null pointer");
  }
  
  if (state_.load() != TransactionState::kActive) {
    return Status::InvalidArgument("OCCTransaction", "transaction not active");
  }
  
  std::lock_guard<std::mutex> lock(mutex_);
  columns->clear();
  
  // 获取所有列 ID
  // GetColumnIds not implemented in VSLMemTable
  std::vector<uint16_t> column_ids;
  
  // 读取每一列
  for (uint16_t col_id : column_ids) {
    // 1. 检查写集
    bool found_in_write_set = false;
    for (const auto& write_entry : write_set_) {
      if (write_entry.entity_id == entity_id &&
          write_entry.entity_type == entity_type &&
          write_entry.column_id == col_id) {
        columns->emplace_back(col_id, write_entry.descriptor);
        found_in_write_set = true;
        break;
      }
    }
    
    if (found_in_write_set) {
      continue;
    }
    
    // 2. 从 MemTable 读取
    auto chain_opt = memtable_->GetVersionChain(entity_id, entity_type, col_id);
    if (!chain_opt.empty()) {
      for (const auto& entry : chain_opt) {
        // 修复: 使用 txn_version 而非业务时间戳进行 MVCC 版本比较
        if (entry.txn_version <= read_timestamp_) {
          columns->emplace_back(col_id, entry.descriptor);
          
          // 记录到读集（业务时间戳 + 事务版本号）
          read_set_.push_back({entity_id, entity_type, col_id, 
                               entry.timestamp, entry.txn_version});
          break;
        }
      }
    } else if (lsm_engine_) {
      // 3. MemTable 中没有找到，查询 LsmEngine (SST 文件)
      auto desc_opt = lsm_engine_->GetAtTime(entity_id, entity_type, col_id, Timestamp::Max());
      if (desc_opt.has_value()) {
        columns->emplace_back(col_id, desc_opt.value());
        read_set_.push_back({entity_id, entity_type, col_id, 
                             Timestamp::Max(), Timestamp::Max()});
      }
    }
  }
  
  return Status::OK();
}

Status OCCTransaction::Put(uint64_t entity_id,
                            EntityType entity_type,
                            uint16_t column_id,
                            const Descriptor& descriptor,
                            Timestamp user_timestamp,
                            uint64_t target_id) {
  if (state_.load() != TransactionState::kActive) {
    return Status::InvalidArgument("OCCTransaction", "transaction not active");
  }
  
  std::lock_guard<std::mutex> lock(mutex_);
  
  // 修复 1: 使用系统时间作为默认业务时间戳
  // 如果用户提供了业务时间戳，使用它；否则使用当前系统时间
  // 绝对不使用 read_timestamp_ 或 commit_timestamp_（事务版本号）作为 Key 的时间戳
  Timestamp ts = user_timestamp.value() > 0 ? user_timestamp : Timestamp::Now();
  
  // 事务版本号在 Commit() 验证通过后分配 commit_timestamp_
  // 这里使用哨兵值 Timestamp(0)，在 Commit() 时会被覆盖
  Timestamp txn_version = Timestamp(0);
  
  CedarKey temp_key = MakeKey(entity_id, entity_type, column_id, ts, target_id);
  write_set_.push_back({entity_id, entity_type, column_id, descriptor, temp_key, ts, txn_version, target_id});
  
  // Track composite key for fast read-your-writes lookup
  auto MakeWriteSetKey = [](uint64_t eid, EntityType etype, uint16_t cid) -> std::string {
    return std::to_string(eid) + ":" + std::to_string(static_cast<int>(etype)) + ":" + std::to_string(cid);
  };
  write_set_keys_.insert(MakeWriteSetKey(entity_id, entity_type, column_id));
  
  return Status::OK();
}

Status OCCTransaction::Delete(uint64_t entity_id,
                               EntityType entity_type,
                               uint16_t column_id) {
  // 删除就是写入 Tombstone
  return Put(entity_id, entity_type, column_id, 
             Descriptor::Tombstone(column_id));
}

Status OCCTransaction::PutBatch(const std::vector<WriteSetEntry>& entries) {
  if (state_.load() != TransactionState::kActive) {
    return Status::InvalidArgument("OCCTransaction", "transaction not active");
  }
  
  std::lock_guard<std::mutex> lock(mutex_);
  
  for (const auto& entry : entries) {
    // 检查是否已在写集中
    bool updated = false;
    for (auto& existing : write_set_) {
      if (existing.entity_id == entry.entity_id &&
          existing.entity_type == entry.entity_type &&
          existing.column_id == entry.column_id) {
        existing.descriptor = entry.descriptor;
        existing.key = entry.key;
        updated = true;
        break;
      }
    }
    
    if (!updated) {
      write_set_.push_back(entry);
    }
    
    // Track composite key for fast read-your-writes lookup
    auto MakeWriteSetKey = [](uint64_t eid, EntityType etype, uint16_t cid) -> std::string {
      return std::to_string(eid) + ":" + std::to_string(static_cast<int>(etype)) + ":" + std::to_string(cid);
    };
    write_set_keys_.insert(MakeWriteSetKey(entry.entity_id, entry.entity_type, entry.column_id));
    
    // 添加到 WAL 批次
    // 修复: 传递 txn_version 给 WAL
    if (wal_writer_) {
      wal_batch_.Put(entry.key, entry.descriptor, entry.txn_version);
    }
  }
  
  return Status::OK();
}

Status OCCTransaction::GetVersionHistory(uint64_t entity_id,
                                          EntityType entity_type,
                                          uint16_t column_id,
                                          std::vector<std::pair<Timestamp, Descriptor>>* history) {
  if (!history) {
    return Status::InvalidArgument("OCCTransaction", "null pointer");
  }
  
  std::lock_guard<std::mutex> lock(mutex_);
  history->clear();
  
  // 从 MemTable 读取完整历史
  auto chain_opt = memtable_->GetVersionChain(entity_id, entity_type, column_id);
  
  for (const auto& entry : chain_opt) {
    // 只包含在读取时间戳之前的版本
    // 修复: 使用 txn_version 而非业务时间戳进行 MVCC 版本比较
    if (entry.txn_version <= read_timestamp_) {
      history->emplace_back(entry.timestamp, entry.descriptor);
    }
  }
  
  // 如果 MemTable 中没有历史，查询 SST 文件
  // Zone-5 已添加：SST 中存储了 txn_version，支持 MVCC 快照隔离。
  if (history->empty() && lsm_engine_) {
    auto all_versions = lsm_engine_->GetAll(entity_id, entity_type, column_id);
    for (const auto& entry : all_versions) {
      if (entry.txn_version <= read_timestamp_) {
        history->emplace_back(entry.timestamp, entry.descriptor);
      }
    }
  }
  
  return Status::OK();
}

Status OCCTransaction::Validate() {
  conflicts_.clear();
  
  // 获取当前活跃事务
  std::vector<std::pair<uint64_t, Timestamp>> active_txns = 
      txn_manager_->GetActiveTransactions();
  
  // 对于快照隔离，我们需要验证:
  // 1. 读集中的记录在读取后没有被其他已提交事务修改
  // 2. 写集中没有与其他并发事务产生冲突
  
  // 验证读集 (支持并行验证)
  if (options_.parallel_validation && read_set_.size() > 100) {
    ValidateReadSetParallel();
  } else {
    // 串行验证
    for (const auto& read_entry : read_set_) {
      if (!ValidateReadEntry(read_entry)) {
        ConflictInfo conflict;
        conflict.type = ConflictType::kReadWrite;
        conflict.entity_id = read_entry.entity_id;
        conflict.entity_type = read_entry.entity_type;
        conflict.column_id = read_entry.column_id;
        conflict.conflicting_txn_id = 0;
        conflicts_.push_back(conflict);
      }
    }
  }
  
  // 验证写集 (写写冲突检测) - 通常较小，使用串行验证
  for (const auto& write_entry : write_set_) {
    // 检查是否有其他事务在当前事务开始后修改了相同键
    auto chain_opt = memtable_->GetVersionChain(
        write_entry.entity_id, 
        write_entry.entity_type, 
        write_entry.column_id);
    
    if (!chain_opt.empty()) {
      // 获取最新版本
      const auto& latest = chain_opt.front();

      // ===================================================================
      // P0-6 FIX: Under serialized commits (global_commit_mutex_), timestamp
      // values from the sharded allocator are NOT monotonically ordered across
      // OS threads. Comparing latest.txn_version > read_timestamp_ is therefore
      // unreliable for cross-thread conflict detection. Instead, compare the
      // latest committed version with the version this transaction actually
      // read (if any). A blind write (no read entry) conflicts with any
      // existing committed version.
      // ===================================================================
      Timestamp read_txn_version = Timestamp::Min();
      bool has_read_entry = false;
      for (const auto& read_entry : read_set_) {
        if (read_entry.entity_id == write_entry.entity_id &&
            read_entry.entity_type == write_entry.entity_type &&
            read_entry.column_id == write_entry.column_id) {
          read_txn_version = read_entry.read_txn_version;
          has_read_entry = true;
          break;
        }
      }

      bool ww_conflict = false;
      if (!has_read_entry) {
        // Blind write: any existing committed version means another txn
        // committed this key while we held no read lock.
        ww_conflict = true;
      } else if (read_txn_version == Timestamp::Max()) {
        // We read this key from immutable SST. Any version now in the
        // memtable was committed after our read.
        ww_conflict = true;
      } else if (latest.txn_version != read_txn_version) {
        // The latest version no longer matches what we read.
        ww_conflict = true;
      }

      if (ww_conflict) {
        ConflictInfo conflict;
        conflict.type = ConflictType::kWriteWrite;
        conflict.entity_id = write_entry.entity_id;
        conflict.entity_type = write_entry.entity_type;
        conflict.column_id = write_entry.column_id;
        conflict.conflicting_txn_id = 0;
        conflicts_.push_back(conflict);
        continue;
      }
      
      // 2. 未提交事务（活跃事务）也修改了相同键
      for (const auto& [active_txn_id, active_ts] : active_txns) {
        if (active_txn_id == txn_id_) continue;  // 跳过自己
        for (const auto& version : chain_opt) {
          if (version.txn_version == active_ts) {
            ConflictInfo conflict;
            conflict.type = ConflictType::kWriteWrite;
            conflict.entity_id = write_entry.entity_id;
            conflict.entity_type = write_entry.entity_type;
            conflict.column_id = write_entry.column_id;
            conflict.conflicting_txn_id = active_txn_id;
            conflicts_.push_back(conflict);
            break;
          }
        }
      }
    }
  }
  
  if (!conflicts_.empty()) {
    txn_manager_->mutable_stats().conflicts_detected.fetch_add(
        conflicts_.size(), std::memory_order_relaxed);
    return Status::Conflict("OCCTransaction", "validation failed");
  }
  
  return Status::OK();
}

void OCCTransaction::ValidateReadSetParallel() {
  const size_t num_threads = options_.validation_threads;
  const size_t chunk_size = (read_set_.size() + num_threads - 1) / num_threads;
  
  std::vector<std::thread> threads;
  std::vector<std::vector<ConflictInfo>> thread_conflicts(num_threads);
  
  for (size_t t = 0; t < num_threads; t++) {
    threads.emplace_back([this, t, chunk_size, &thread_conflicts]() {
      size_t start = t * chunk_size;
      size_t end = std::min(start + chunk_size, read_set_.size());
      
      for (size_t i = start; i < end; i++) {
        if (!ValidateReadEntry(read_set_[i])) {
          ConflictInfo conflict;
          conflict.type = ConflictType::kReadWrite;
          conflict.entity_id = read_set_[i].entity_id;
          conflict.entity_type = read_set_[i].entity_type;
          conflict.column_id = read_set_[i].column_id;
          conflict.conflicting_txn_id = 0;
          thread_conflicts[t].push_back(conflict);
        }
      }
    });
  }
  
  // 等待所有线程完成
  for (auto& t : threads) {
    t.join();
  }
  
  // 合并冲突结果
  for (const auto& tc : thread_conflicts) {
    conflicts_.insert(conflicts_.end(), tc.begin(), tc.end());
  }
}

bool OCCTransaction::ValidateReadEntry(const ReadSetEntry& entry) {
  // 如果读取的是 SST 文件中的数据（标记为 Max()），不需要验证
  // 因为 SST 数据是不可变的，不会发生冲突
  if (entry.read_txn_version == Timestamp::Max()) {
    return true;
  }
  
  // 获取当前最新版本
  auto chain_opt = memtable_->GetVersionChain(
      entry.entity_id, entry.entity_type, entry.column_id);
  
  if (chain_opt.empty()) {
    // 记录被删除，但如果读时存在则冲突
    // 但如果读取的是 SST 数据，MemTable 为空是正常的
    // 需要检查 SST 中是否仍有该数据
    if (lsm_engine_) {
      auto desc_opt = lsm_engine_->GetAtTime(
          entry.entity_id, entry.entity_type, entry.column_id, Timestamp::Max());
      // 如果 SST 中仍有数据，则验证通过（SST 数据不可变）
      return desc_opt.has_value();
    }
    return false;
  }
  
  // 检查最新版本是否仍是读取时的版本
  const auto& latest = chain_opt.front();
  
  // ✅ 修复: 使用 entry.read_txn_version（读取时的事务版本号）进行比较
  // 如果最新版本的事务版本号 > 读取时记录的事务版本号，说明被修改过
  if (latest.txn_version > entry.read_txn_version) {
    return false;
  }
  
  return true;
}

Status OCCTransaction::WriteToMemTable() {
  if (!memtable_) {
    return Status::InvalidArgument("OCCTransaction", "memtable is null");
  }

  size_t written = 0;
  for (const auto& entry : write_set_) {
    // 修复 2: 直接使用写入集中存储的业务时间戳
    // user_timestamp 在 Put() 阶段已经确定是业务时间戳（系统时间或用户指定）
    // 不再回退到 commit_timestamp_（事务版本号）
    Timestamp ts = entry.user_timestamp;

    // 构建键 - 使用业务时间戳和 target_id
    CedarKey key = MakeKey(entry.entity_id, entry.entity_type,
                          entry.column_id, ts, entry.target_id);

    // 写入 MemTable - 同时传递事务版本号用于 MVCC
    // 注意：这里需要支持传递 txn_version 给 MemTable
    Status s = memtable_->Put(key, entry.descriptor, entry.txn_version);
    if (!s.ok()) {
      // ===================================================================
      // P0-6 PARTIAL-FAILURE ROLLBACK: If a middle entry fails to insert,
      // tombstone the entries we already wrote so the transaction remains
      // atomic. We hold global_commit_mutex_, so no concurrent commit can
      // observe the partial state between the failed Put and these rollback
      // deletes.
      // ===================================================================
      RollbackMemTableWrites(written);
      return s;
    }
    ++written;
  }
  return Status::OK();
}

void OCCTransaction::RollbackMemTableWrites(size_t written_count) {
  if (!memtable_ || written_count == 0) {
    return;
  }

  // Iterate backwards over the entries we successfully wrote and tombstone
  // each one.  Backwards order is not required for correctness, but it is
  // the natural undo of the forward loop above.
  for (size_t i = written_count; i-- > 0;) {
    const auto& entry = write_set_[i];
    Timestamp ts = entry.user_timestamp;
    CedarKey key = MakeKey(entry.entity_id, entry.entity_type,
                          entry.column_id, ts, entry.target_id);
    // Best-effort tombstone; ignore failure because we are already on the
    // error path and the WAL will be replayed idempotently on recovery.
    memtable_->Put(key, Descriptor::Tombstone(entry.column_id),
                   entry.txn_version);
  }
}

Status OCCTransaction::WriteToWAL() {
  if (!wal_writer_ || wal_batch_.empty()) {
    return Status::OK();
  }
  
  return wal_writer_->WriteBatch(wal_batch_);
}

void OCCTransaction::Cleanup() {
  read_set_.clear();
  write_set_.clear();
  write_set_keys_.clear();
  conflicts_.clear();
  wal_batch_.Clear();
  state_.store(TransactionState::kActive);
}

CedarKey OCCTransaction::MakeKey(uint64_t entity_id, EntityType type, 
                                 uint16_t column_id, Timestamp ts,
                                 uint64_t target_id) {
  // Use factory methods to ensure proper byte order encoding
  // Note: sequence=0 for query compatibility
  uint16_t seq = 0;
  if (type == EntityType::EdgeOut) {
    // EdgeOut: src=entity_id, dst=target_id, edge_type=column_id
    return CedarKey::EdgeOut(entity_id, target_id, column_id, ts, seq);
  } else if (type == EntityType::EdgeIn) {
    // EdgeIn: dst=entity_id, src=target_id, edge_type=column_id
    return CedarKey::EdgeIn(entity_id, target_id, column_id, ts, seq);
  } else {
    // Vertex: extension data stored in target_id
    return CedarKey::Vertex(entity_id, VertexColumnId(column_id), ts, seq, target_id);
  }
}

// ========== TransactionRetryWrapper ==========

TransactionRetryWrapper::TransactionRetryWrapper(LsmEngine* lsm_engine,
                                                  const TransactionOptions& options)
    : lsm_engine_(lsm_engine),
      options_(options),
      txn_manager_(std::make_unique<TransactionManager>()),
      memtable_(std::make_unique<VSLMemTable>()) {}

}  // namespace cedar
