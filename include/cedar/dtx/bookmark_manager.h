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
// BBCC: Bookmark-Based Causal Consistency
// =============================================================================
// 核心思想：利用 Bookmark 实现轻量级因果一致性
// =============================================================================

#ifndef CEDAR_DTX_BOOKMARK_MANAGER_H_
#define CEDAR_DTX_BOOKMARK_MANAGER_H_

#include <cstdint>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <atomic>
#include <chrono>
#include <future>

#include "cedar/core/status.h"
#include "cedar/driver/bookmark.h"
#include "cedar/dtx/types.h"
#include "cedar/dtx/txn_context.h"

namespace cedar {
namespace dtx {

/**
 * @brief 混合逻辑时钟 (HLC)
 * 
 * 结合物理时间和逻辑时间，用于分布式因果排序
 */
struct HybridLogicalClock {
  uint64_t wall_time{0};  // 物理时间（微秒）
  uint64_t logical{0};    // 逻辑计数
  
  // 默认构造函数
  HybridLogicalClock() = default;
  HybridLogicalClock(uint64_t wt, uint64_t l = 0) : wall_time(wt), logical(l) {}
  
  // 比较操作
  bool operator<(const HybridLogicalClock& other) const {
    if (wall_time != other.wall_time) return wall_time < other.wall_time;
    return logical < other.logical;
  }
  
  bool operator>(const HybridLogicalClock& other) const {
    return other < *this;
  }
  
  bool operator==(const HybridLogicalClock& other) const {
    return wall_time == other.wall_time && logical == other.logical;
  }
  
  bool operator!=(const HybridLogicalClock& other) const {
    return !(*this == other);
  }
  
  // 检查是否为因果前序
  bool HappensBefore(const HybridLogicalClock& other) const {
    return *this < other;
  }
  
  // 检查是否并发（无因果关系）
  bool IsConcurrentWith(const HybridLogicalClock& other) const {
    // 相等的不是并发的（它们是同一个）
    if (*this == other) return false;
    // 如果既不是 HappensBefore 也不是 HappensAfter，则是并发的
    return !(*this < other) && !(other < *this);
  }
  
  // 序列化
  std::string ToString() const {
    return std::to_string(wall_time) + "." + std::to_string(logical);
  }
  
  // 反序列化
  static HybridLogicalClock FromString(const std::string& str);
  
  // 获取当前HLC（静态方法）
  static HybridLogicalClock Now();
};

/**
 * @brief 分布式 Bookmark
 * 
 * 扩展单机 Bookmark，支持分布式因果一致性
 */
struct DistributedBookmark {
  // 基础信息（继承单机Bookmark）
  uint64_t timestamp{0};  // 提交时间戳
  uint64_t txn_id{0};     // 事务ID
  
  // 分布式扩展
  std::vector<std::pair<PartitionID, uint64_t>> shard_watermarks;  // 各分片水位
  HybridLogicalClock hlc;  // HLC时间戳
  
  // 构造函数
  DistributedBookmark() = default;
  DistributedBookmark(uint64_t ts, uint64_t tid) 
      : timestamp(ts), txn_id(tid), hlc(HybridLogicalClock::Now()) {}
  
  // 从单机 Bookmark 构造
  explicit DistributedBookmark(const driver::Bookmark& bm);
  
  // 转换为单机 Bookmark
  driver::Bookmark ToSimpleBookmark() const {
    return driver::Bookmark(timestamp, txn_id);
  }
  
  // 设置分片水位
  void SetShardWatermark(PartitionID pid, uint64_t watermark);
  uint64_t GetShardWatermark(PartitionID pid) const;
  
  // 因果序判断
  bool HappensBefore(const DistributedBookmark& other) const;
  bool IsConcurrentWith(const DistributedBookmark& other) const;
  
  // 序列化格式: v3:{timestamp}:{txn_id}:{hlc}:{shard_watermarks}
  std::string Serialize() const;
  static std::optional<DistributedBookmark> Deserialize(const std::string& str);
  
  // 合并多个Bookmark（取最大值）
  static DistributedBookmark Merge(
      const std::vector<DistributedBookmark>& bookmarks);
  
  // 空Bookmark检查
  bool IsEmpty() const { return timestamp == 0 && txn_id == 0; }
  
  // 相等比较
  bool operator==(const DistributedBookmark& other) const {
    return timestamp == other.timestamp && txn_id == other.txn_id && hlc == other.hlc;
  }
  
  bool operator!=(const DistributedBookmark& other) const {
    return !(*this == other);
  }
};

/**
 * @brief 因果一致性级别
 */
enum class CausalConsistencyLevel : uint8_t {
  kNone = 0,        // 无因果一致性（最高性能）
  kSession = 1,     // 会话级（默认）
  kCrossSession = 2,// 跨会话
  kGlobal = 3,      // 全局因果（最严格）
};

/**
 * @brief 因果一致性检查器
 */
class CausalConsistencyChecker {
 public:
  // 检查是否满足因果一致性
  static bool CheckReadYourWrites(
      const DistributedBookmark& write_bm,
      const DistributedBookmark& read_bm);
  
  static bool CheckMonotonicReads(
      const DistributedBookmark& prev_read_bm,
      const DistributedBookmark& curr_read_bm);
  
  static bool CheckWritesFollowReads(
      const DistributedBookmark& read_bm,
      const DistributedBookmark& write_bm);
  
  static bool CheckMonotonicWrites(
      const DistributedBookmark& prev_write_bm,
      const DistributedBookmark& curr_write_bm);
};

/**
 * @brief Bookmark 管理器
 * 
 * 管理集群的 Bookmark，实现因果一致性
 */
class BookmarkManager {
 public:
  BookmarkManager();
  ~BookmarkManager();
  
  // ==================== HLC 管理 ====================
  
  // 获取当前 HLC
  HybridLogicalClock GetCurrentHLC();
  
  // 更新本地 HLC（收到远程事件时调用）
  void UpdateHLC(const HybridLogicalClock& remote);
  
  // ==================== Bookmark 操作 ====================
  
  // 从事务上下文生成 Bookmark
  DistributedBookmark CreateBookmark(const DistributedTxnContext& ctx);
  
  // 等待 Bookmark 满足（因果一致性读）
  // 阻塞直到本地状态至少达到 Bookmark 指定的时间点
  Status WaitForBookmark(const DistributedBookmark& bookmark,
                          std::chrono::milliseconds timeout);
  
  // 检查 Bookmark 是否已满足
  bool IsBookmarkSatisfied(const DistributedBookmark& bookmark);
  
  // ==================== 分片水位管理 ====================
  
  // 更新本地分片水位
  void UpdateLocalWatermark(PartitionID pid, uint64_t watermark);
  
  // 获取分片水位
  uint64_t GetWatermark(PartitionID pid) const;
  
  // 获取全局最小水位（用于GC）
  uint64_t GetGlobalMinWatermark() const;
  
  // ==================== 会话管理 ====================
  
  // 设置会话的 Bookmark
  void SetSessionBookmark(uint64_t session_id, const DistributedBookmark& bm);
  
  // 获取会话的 Bookmark
  DistributedBookmark GetSessionBookmark(uint64_t session_id) const;
  
  // 更新会话 Bookmark
  void UpdateSessionBookmark(uint64_t session_id, const DistributedBookmark& bm);
  
  // ==================== 统计 ====================
  
  struct Stats {
    uint64_t bookmarks_created{0};
    uint64_t bookmarks_merged{0};
    uint64_t causal_waits{0};      // 因果等待次数
    uint64_t causal_wait_time_us{0};  // 总等待时间
  };
  
  Stats GetStats() const;
  
 private:
  // HLC
  std::mutex hlc_mutex_;
  HybridLogicalClock current_hlc_;
  
  // 分片水位
  mutable std::mutex watermarks_mutex_;
  std::unordered_map<PartitionID, std::atomic<uint64_t>> watermarks_;
  
  // 会话 Bookmark
  mutable std::mutex sessions_mutex_;
  std::unordered_map<uint64_t, DistributedBookmark> session_bookmarks_;
  
  // 统计
  std::atomic<uint64_t> bookmarks_created_{0};
  std::atomic<uint64_t> bookmarks_merged_{0};
  std::atomic<uint64_t> causal_waits_{0};
  std::atomic<uint64_t> causal_wait_time_us_{0};
};

/**
 * @brief 轻量级因果一致性等待器
 * 
 * 非阻塞方式实现因果一致性
 */
class LightweightCausalWaiter {
 public:
  explicit LightweightCausalWaiter(BookmarkManager* manager);
  
  // 异步等待 Bookmark
  // 返回 Future，可以轮询检查是否完成
  std::future<Status> WaitAsync(const DistributedBookmark& bookmark);
  
  // 检查是否已满足（非阻塞）
  bool CheckSatisfied(const DistributedBookmark& bookmark);
  
 private:
  BookmarkManager* manager_{nullptr};
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_BOOKMARK_MANAGER_H_
