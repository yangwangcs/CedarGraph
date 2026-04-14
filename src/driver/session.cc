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

#include "cedar/driver/session.h"

#include <iostream>

#include "cedar/transaction/occ_transaction.h"
#include "cedar/storage/vsl_memtable.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/transaction/wal.h"

namespace cedar {
namespace driver {

// ==================== ManagedTxn ====================

ManagedTxn::ManagedTxn(OCCTransaction* txn, Session* session)
    : txn_(txn),
      session_(session),
      committed_(false),
      aborted_(false) {}

ManagedTxn::ManagedTxn(ManagedTxn&& other) noexcept
    : txn_(std::move(other.txn_)),
      session_(other.session_),
      committed_(other.committed_),
      aborted_(other.aborted_) {
  other.session_ = nullptr;
  other.committed_ = false;
  other.aborted_ = true;  // 标记为已处理
}

ManagedTxn& ManagedTxn::operator=(ManagedTxn&& other) noexcept {
  if (this != &other) {
    // 清理当前状态
    if (txn_ && !committed_ && !aborted_) {
      txn_->Abort();
    }
    
    txn_ = std::move(other.txn_);
    session_ = other.session_;
    committed_ = other.committed_;
    aborted_ = other.aborted_;
    
    other.session_ = nullptr;
    other.committed_ = false;
    other.aborted_ = true;
  }
  return *this;
}

ManagedTxn::~ManagedTxn() {
  if (txn_ && !committed_ && !aborted_) {
    // 自动回滚未提交的事务
    txn_->Abort();
  }
}

BookmarkResult ManagedTxn::Commit() {
  if (committed_) {
    return BookmarkResult::Err(
        ConflictInfo(ConflictType::kNone, "Transaction already committed"));
  }
  if (aborted_) {
    return BookmarkResult::Err(
        ConflictInfo(ConflictType::kNone, "Transaction already aborted"));
  }
  
  auto status = txn_->Commit();
  
  if (!status.ok()) {
    // 解析错误类型
    ConflictType error_type = ConflictType::kNone;
    std::string msg = status.ToString();
    
    if (msg.find("Conflict") != std::string::npos) {
      error_type = ConflictType::kWriteWrite;
    } else if (msg.find("Busy") != std::string::npos) {
      error_type = ConflictType::kTimeout;
    }
    
    return BookmarkResult::Err(ConflictInfo(error_type, msg));
  }
  
  // 提交成功，提取书签
  committed_ = true;
  Bookmark bookmark(txn_->GetCommitTimestamp().value(), txn_->GetTransactionId());
  
  // 更新会话书签
  if (session_) {
    session_->UpdateBookmark(bookmark);
  }
  
  return BookmarkResult::Ok(bookmark);
}

void ManagedTxn::Rollback() {
  if (committed_) {
    std::cerr << "Warning: Cannot rollback already committed transaction\n";
    return;
  }
  if (aborted_) {
    return;  // 已经回滚
  }
  
  txn_->Abort();
  aborted_ = true;
}

// ==================== Session ====================

Session::Session(TransactionManager* txn_manager,
                 VSLMemTable* memtable,
                 LsmEngine* lsm_engine,
                 WalWriter* wal_writer,
                 const SessionConfig& config)
    : txn_manager_(txn_manager),
      memtable_(memtable),
      lsm_engine_(lsm_engine),
      wal_writer_(wal_writer),
      config_(config),
      is_open_(true),
      last_bookmark_(config.initial_bookmark.value_or(Bookmark())) {}

Session::~Session() = default;

Session::Session(Session&& other) noexcept
    : txn_manager_(other.txn_manager_),
      memtable_(other.memtable_),
      lsm_engine_(other.lsm_engine_),
      wal_writer_(other.wal_writer_),
      config_(std::move(other.config_)),
      is_open_(other.is_open_),
      last_bookmark_(std::move(other.last_bookmark_)) {
  other.txn_manager_ = nullptr;
  other.is_open_ = false;
}

Session& Session::operator=(Session&& other) noexcept {
  if (this != &other) {
    txn_manager_ = other.txn_manager_;
    memtable_ = other.memtable_;
    lsm_engine_ = other.lsm_engine_;
    wal_writer_ = other.wal_writer_;
    config_ = std::move(other.config_);
    is_open_ = other.is_open_;
    last_bookmark_ = std::move(other.last_bookmark_);
    
    other.txn_manager_ = nullptr;
    other.is_open_ = false;
  }
  return *this;
}

ManagedTxn Session::BeginTransaction(const TransactionConfig& config) {
  if (!is_open_) {
    throw std::runtime_error("Cannot begin transaction on closed session");
  }
  
  auto txn_options = config.options;
  if (txn_options.timeout_ms == 0) {
    txn_options.timeout_ms = config_.default_timeout.count();
  }
  
  auto txn = new OCCTransaction(txn_manager_, memtable_, lsm_engine_, 
                                 wal_writer_, txn_options);
  
  auto status = txn->Begin();
  if (!status.ok()) {
    delete txn;
    throw std::runtime_error("Failed to begin transaction: " + status.ToString());
  }
  
  return ManagedTxn(txn, this);
}

ManagedTxn Session::BeginTransaction(const Bookmark& bookmark,
                                      const TransactionConfig& config) {
  // 书签版本检查
  if (bookmark > last_bookmark_) {
    // 实际实现中可能需要等待
  }
  
  last_bookmark_ = bookmark;
  return BeginTransaction(config);
}

void Session::UpdateBookmark(const Bookmark& bookmark) {
  if (bookmark > last_bookmark_) {
    last_bookmark_ = bookmark;
  }
}

}  // namespace driver
}  // namespace cedar
