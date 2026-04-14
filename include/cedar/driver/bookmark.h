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
// Bookmark - 因果一致性令牌
// =============================================================================
// 基于 OCCTransaction 的 commit timestamp 和 txn_id
// =============================================================================

#ifndef CEDAR_DRIVER_BOOKMARK_H_
#define CEDAR_DRIVER_BOOKMARK_H_

#include <cstdint>
#include <optional>
#include <string>
#include <vector>
#include <functional>

namespace cedar {
namespace driver {

// 书签 - 代表数据库的某个逻辑状态点
// 基于 OCCTransaction 的 timestamp 和 txn_id
class Bookmark {
 public:
  // 默认构造（空书签）
  Bookmark() = default;
  
  // 从 timestamp 和 txn_id 构造（来自 OCCTransaction）
  Bookmark(uint64_t timestamp, uint64_t txn_id) 
      : timestamp_(timestamp), txn_id_(txn_id) {}
  
  // 从字符串反序列化
  static std::optional<Bookmark> FromString(const std::string& str);
  
  // 序列化为字符串
  std::string ToString() const;
  
  // 获取 timestamp（对应 OCCTransaction::GetCommitTimestamp()）
  uint64_t GetTimestamp() const { return timestamp_; }
  
  // 获取 sequence number（兼容旧接口，实际返回 timestamp）
  uint64_t GetSequenceNumber() const { return timestamp_; }
  
  // 获取事务 ID（对应 OCCTransaction::GetTransactionId()）
  uint64_t GetTransactionId() const { return txn_id_; }
  
  // 是否为空书签
  bool IsEmpty() const { return timestamp_ == 0 && txn_id_ == 0; }
  
  // 比较操作（基于 timestamp）
  bool operator==(const Bookmark& other) const {
    return timestamp_ == other.timestamp_ && txn_id_ == other.txn_id_;
  }
  bool operator!=(const Bookmark& other) const {
    return !(*this == other);
  }
  bool operator<(const Bookmark& other) const {
    return timestamp_ < other.timestamp_;
  }
  bool operator<=(const Bookmark& other) const {
    return timestamp_ <= other.timestamp_;
  }
  bool operator>(const Bookmark& other) const {
    return timestamp_ > other.timestamp_;
  }
  bool operator>=(const Bookmark& other) const {
    return timestamp_ >= other.timestamp_;
  }
  
  // 合并多个书签（取最大 timestamp）
  static Bookmark Combine(const std::vector<Bookmark>& bookmarks);
  
  // 获取空书签
  static Bookmark Empty() { return Bookmark(); }
  
 private:
  uint64_t timestamp_ = 0;  // OCCTransaction::GetCommitTimestamp()
  uint64_t txn_id_ = 0;     // OCCTransaction::GetTransactionId()
};

// 哈希函数支持
struct BookmarkHash {
  std::size_t operator()(const Bookmark& bookmark) const {
    return std::hash<uint64_t>{}(bookmark.GetTimestamp()) ^
           (std::hash<uint64_t>{}(bookmark.GetTransactionId()) << 1);
  }
};

}  // namespace driver
}  // namespace cedar

#endif  // CEDAR_DRIVER_BOOKMARK_H_
