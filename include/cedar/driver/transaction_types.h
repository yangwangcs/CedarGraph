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
// Transaction Types - 事务相关类型定义
// =============================================================================

#ifndef CEDAR_DRIVER_TRANSACTION_TYPES_H_
#define CEDAR_DRIVER_TRANSACTION_TYPES_H_

#include <chrono>
#include <optional>
#include <string>
#include <unordered_map>

#include "cedar/driver/bookmark.h"
#include "cedar/core/status.h"
#include "cedar/transaction/occ_transaction.h"  // for TransactionOptions, StrictLevel

namespace cedar {
namespace driver {

// 事务状态
enum class TransactionState {
  kPending,      // 初始状态
  kActive,       // 活跃（已开始）
  kCommitting,   // 提交中
  kCommitted,    // 已提交
  kAborted,      // 已回滚
  kClosed,       // 已关闭
};

// 冲突类型
enum class ConflictType {
  kNone,               // 无冲突
  kReadWrite,          // 读集被修改（OCC冲突）
  kWriteWrite,         // 写冲突
  kConstraintViolation, // 约束违反
  kTimeout,            // 超时
};

// 冲突信息
struct ConflictInfo {
  ConflictType type = ConflictType::kNone;
  uint64_t entity_id = 0;  // 冲突实体ID
  uint16_t column_id = 0;  // 冲突列ID
  std::string message;     // 详细描述
  
  ConflictInfo() = default;
  ConflictInfo(ConflictType t, const std::string& msg) 
      : type(t), message(msg) {}
  
  std::string ToString() const;
};

// 事务配置（基于 OCCTransaction）
struct TransactionConfig {
  // 底层 OCC 事务选项（传给 OCCTransaction）
  TransactionOptions options;
  
  // 超时配置
  std::chrono::milliseconds timeout{30000};
  
  // 元数据（用于监控/日志）
  std::unordered_map<std::string, std::string> metadata;
  
  // 书签（因果一致性）
  std::optional<Bookmark> bookmark;
};

// 结果模板（成功时返回值，失败时返回错误）
template<typename T, typename E = ConflictInfo>
class Result {
 public:
  // 成功构造
  static Result Ok(T value) {
    return Result(std::move(value), std::nullopt, std::nullopt);
  }
  
  // 失败构造
  static Result Err(E error) {
    return Result(std::nullopt, std::move(error), std::nullopt);
  }
  
  // 状态码失败
  static Result Err(Status status) {
    return Result(std::nullopt, std::nullopt, std::move(status));
  }
  
  // 判断是否成功
  bool ok() const {
    return value_.has_value();
  }
  
  // 获取值（成功时）
  T& value() & { return value_.value(); }
  const T& value() const & { return value_.value(); }
  T&& value() && { return std::move(value_.value()); }
  
  // 获取错误（失败时）
  E& error() & { return error_.value(); }
  const E& error() const & { return error_.value(); }
  
  // 获取状态
  Status status() const {
    if (ok()) return Status::OK();
    if (status_.has_value()) return status_.value();
    return Status::IOError("Transaction", error_->message);
  }
  
 private:
  Result(std::optional<T> value, std::optional<E> error, std::optional<Status> status)
      : value_(std::move(value)), error_(std::move(error)), status_(std::move(status)) {}
  
  std::optional<T> value_;
  std::optional<E> error_;
  std::optional<Status> status_;
};

// 特化：Bookmark 结果
typedef Result<Bookmark, ConflictInfo> BookmarkResult;

// 访问模式
enum class AccessMode {
  kRead,   // 只读
  kWrite,  // 读写
};

}  // namespace driver
}  // namespace cedar

#endif  // CEDAR_DRIVER_TRANSACTION_TYPES_H_
