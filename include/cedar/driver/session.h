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

// =============================================================================
// Session - Neo4j 风格事务 API
// =============================================================================
// 直接管理 OCCTransaction，提供简洁的 RAII 接口
// =============================================================================

#ifndef CEDAR_DRIVER_SESSION_H_
#define CEDAR_DRIVER_SESSION_H_

#include <memory>
#include <functional>

#include "cedar/driver/bookmark.h"
#include "cedar/driver/retry_policy.h"
#include "cedar/driver/transaction_types.h"

namespace cedar {

class TransactionManager;
class VSLMemTable;
class LsmEngine;
class WalWriter;

namespace driver {

// 会话配置
struct SessionConfig {
  // 默认超时
  std::chrono::milliseconds default_timeout{30000};
  
  // 默认元数据
  std::unordered_map<std::string, std::string> default_metadata;
  
  // 是否自动管理书签
  bool auto_bookmark_management = true;
  
  // 默认重试策略
  RetryConfig default_retry_config = RetryPolicies::Default();
  
  // 初始书签（用于因果一致性）
  std::optional<Bookmark> initial_bookmark;
};

// 前向声明
class Session;

// ManagedTxn - OCCTransaction 的轻量级 RAII 包装
// 只负责生命周期管理和书签提取，不隐藏 OCCTransaction
class ManagedTxn {
 public:
  // 禁止拷贝
  ManagedTxn(const ManagedTxn&) = delete;
  ManagedTxn& operator=(const ManagedTxn&) = delete;
  
  // 允许移动
  ManagedTxn(ManagedTxn&&) noexcept;
  ManagedTxn& operator=(ManagedTxn&&) noexcept;
  
  ~ManagedTxn();
  
  // 访问底层 OCCTransaction（直接暴露所有功能）
  OCCTransaction* operator->() { return txn_.get(); }
  OCCTransaction& operator*() { return *txn_; }
  OCCTransaction* get() { return txn_.get(); }
  
  // 提交并返回书签
  BookmarkResult Commit();
  
  // 回滚
  void Rollback();
  
  // 获取事务状态
  bool IsCommitted() const { return committed_; }
  bool IsAborted() const { return aborted_; }
  
 private:
  friend class Session;
  
  ManagedTxn(OCCTransaction* txn, Session* session);
  
  std::unique_ptr<OCCTransaction> txn_;
  Session* session_;
  bool committed_;
  bool aborted_;
};

// Session - 事务容器
class Session {
 public:
  // 构造函数
  Session(TransactionManager* txn_manager,
          VSLMemTable* memtable,
          LsmEngine* lsm_engine,
          WalWriter* wal_writer,
          const SessionConfig& config = {});
  
  ~Session();
  
  // 禁止拷贝，允许移动
  Session(const Session&) = delete;
  Session& operator=(const Session&) = delete;
  Session(Session&&) noexcept;
  Session& operator=(Session&&) noexcept;
  
  // ========== L3 API: 显式事务 ==========
  
  // 开始事务（返回 ManagedTxn，RAII 管理）
  ManagedTxn BeginTransaction(const TransactionConfig& config = {});
  
  // 开始事务（带书签的因果一致性）
  ManagedTxn BeginTransaction(const Bookmark& bookmark,
                               const TransactionConfig& config = {});
  
  // ========== L2 API: 托管事务（自动重试）==========
  
  // 执行写事务（自动处理 OCC Conflict 重试）
  // 回调函数签名: bool(OCCTransaction&) 返回 true 表示成功，false 触发重试
  template<typename Func>
  BookmarkResult ExecuteWrite(Func&& func, 
                               const TransactionConfig& config = {},
                               const RetryConfig& retry_config = {});
  
  // 执行读事务
  template<typename Func>
  auto ExecuteRead(Func&& func, const TransactionConfig& config = {})
      -> decltype(func(std::declval<OCCTransaction&>()));
  
  // ========== 书签管理 ==========
  
  Bookmark GetLastBookmark() const { return last_bookmark_; }
  void SetBookmark(const Bookmark& bookmark) { last_bookmark_ = bookmark; }
  void UpdateBookmark(const Bookmark& bookmark);
  
  // ========== 状态 ==========
  
  bool IsOpen() const { return is_open_; }
  void Close() { is_open_ = false; }
  
 private:
  TransactionManager* txn_manager_;
  VSLMemTable* memtable_;
  LsmEngine* lsm_engine_;
  WalWriter* wal_writer_;
  SessionConfig config_;
  bool is_open_;
  Bookmark last_bookmark_;
};

// 模板实现
template<typename Func>
BookmarkResult Session::ExecuteWrite(Func&& func, 
                                      const TransactionConfig& config,
                                      const RetryConfig& retry_config) {
  RetryConfig retry_cfg = retry_config;
  if (retry_cfg.max_attempts == 0) {
    retry_cfg = config_.default_retry_config;
  }
  
  RetryPolicy retry_policy(retry_cfg);
  BookmarkResult result = BookmarkResult::Err(
      ConflictInfo(ConflictType::kNone, "No attempts made"));
  
  retry_policy.Execute([&]() -> Status {
    auto txn = BeginTransaction(config);
    
    try {
      // 执行用户逻辑
      bool success = func(*txn);
      
      if (!success) {
        txn.Rollback();
        return Status::IOError("ExecuteWrite", "Logic returned false");
      }
      
      // 提交
      result = txn.Commit();
      
      if (result.ok()) {
        UpdateBookmark(result.value());
        return Status::OK();
      } else {
        // 检查是否可重试
        const auto& error = result.error();
        if (error.type == ConflictType::kReadWrite ||
            error.type == ConflictType::kWriteWrite) {
          return Status::IOError("OCC", "Conflict detected");
        }
        return Status::InvalidArgument("ExecuteWrite", error.message);
      }
      
    } catch (const std::exception& e) {
      txn.Rollback();
      return Status::IOError("ExecuteWrite", e.what());
    }
  });
  
  return result;
}

template<typename Func>
auto Session::ExecuteRead(Func&& func, const TransactionConfig& config)
    -> decltype(func(std::declval<OCCTransaction&>())) {
  auto txn = BeginTransaction(config);
  
  try {
    auto result = func(*txn);
    // 读事务不需要提交，但调用 Commit 获取书签
    txn.Commit();
    return result;
  } catch (...) {
    txn.Rollback();
    throw;
  }
}

}  // namespace driver
}  // namespace cedar

#endif  // CEDAR_DRIVER_SESSION_H_
