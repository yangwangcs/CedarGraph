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
// TemporalWindow - TW-CD核心数据结构
// =============================================================================

#ifndef CEDAR_DTX_TEMPORAL_WINDOW_H_
#define CEDAR_DTX_TEMPORAL_WINDOW_H_

#include <cstdint>
#include <string>
#include <algorithm>

#include "cedar/types/cedar_key.h"
#include "cedar/dtx/types.h"

namespace cedar {
namespace dtx {

/**
 * @brief 时序窗口 - TW-CD的核心数据结构
 * 
 * 事务声明其感兴趣的时间范围，冲突检测只检查窗口重叠的写入
 */
struct TemporalWindow {
  Timestamp start{0};  // 窗口起点（包含）
  Timestamp end{0};    // 窗口终点（包含），0表示无上限
  
  // 默认构造函数（全时间范围）
  TemporalWindow() = default;
  
  // 指定范围
  TemporalWindow(Timestamp s, Timestamp e) : start(s), end(e) {}
  
  // 从单个时间点构造（精确查询）
  explicit TemporalWindow(Timestamp point) : start(point), end(point) {}
  
  // 从uint64_t构造（便捷接口）
  TemporalWindow(uint64_t start_us, uint64_t end_us) 
      : start(Timestamp(start_us)), end(Timestamp(end_us)) {}
  
  // 检查两个窗口是否重叠
  bool Overlaps(const TemporalWindow& other) const {
    // 如果任一方是无上限的（end=0），只检查start
    if (end.value() == 0 || other.end.value() == 0) {
      return other.start.value() <= (end.value() == 0 ? 
          std::numeric_limits<uint64_t>::max() : end.value()) &&
          start.value() <= (other.end.value() == 0 ? 
          std::numeric_limits<uint64_t>::max() : other.end.value());
    }
    // 标准重叠检查: [a1, a2] 与 [b1, b2] 重叠当且仅当 a1 <= b2 && b1 <= a2
    return start <= other.end && other.start <= end;
  }
  
  // 检查时间点是否在窗口内
  bool Contains(Timestamp ts) const {
    return ts >= start && (end.value() == 0 || ts <= end);
  }
  
  // 检查是否包含指定时间戳
  bool Contains(uint64_t ts_us) const {
    return Contains(Timestamp(ts_us));
  }
  
  // 合并两个窗口（取并集）
  void Merge(const TemporalWindow& other) {
    start = Timestamp(std::min(start.value(), other.start.value()));
    if (end.value() == 0 || other.end.value() == 0) {
      end = Timestamp(0);  // 无上限
    } else {
      end = Timestamp(std::max(end.value(), other.end.value()));
    }
  }
  
  // 计算窗口交集
  TemporalWindow Intersect(const TemporalWindow& other) const {
    Timestamp new_start = Timestamp(std::max(start.value(), other.start.value()));
    Timestamp new_end;
    
    if (end.value() == 0) {
      new_end = other.end;
    } else if (other.end.value() == 0) {
      new_end = end;
    } else {
      new_end = Timestamp(std::min(end.value(), other.end.value()));
    }
    
    // 如果没有重叠，返回空窗口
    if (new_start > new_end && new_end.value() != 0) {
      return TemporalWindow(Timestamp(0), Timestamp(0));
    }
    
    return TemporalWindow(new_start, new_end);
  }
  
  // 检查是否为空窗口
  bool IsEmpty() const {
    return start.value() == 0 && end.value() == 0;
  }
  
  // 检查是否为单点窗口
  bool IsPoint() const {
    return start == end && start.value() != 0;
  }
  
  // 获取窗口跨度（微秒），0表示无限
  uint64_t Span() const {
    if (end.value() == 0) return 0;  // 无限
    if (end < start) return 0;
    return end.value() - start.value();
  }
  
  // 序列化为字符串: "start:end" 或 "start:inf"
  std::string ToString() const {
    std::string result = std::to_string(start.value()) + ":";
    if (end.value() == 0) {
      result += "inf";
    } else {
      result += std::to_string(end.value());
    }
    return result;
  }
  
  // 从字符串解析: "100:200" 或 "100:inf"
  static TemporalWindow FromString(const std::string& str) {
    size_t pos = str.find(':');
    if (pos == std::string::npos) {
      return TemporalWindow();  // 无效格式，返回空窗口
    }
    
    uint64_t start = std::stoull(str.substr(0, pos));
    std::string end_str = str.substr(pos + 1);
    
    if (end_str == "inf" || end_str == "0") {
      return TemporalWindow(Timestamp(start), Timestamp(0));
    } else {
      uint64_t end = std::stoull(end_str);
      return TemporalWindow(Timestamp(start), Timestamp(end));
    }
  }
  
  // 比较操作符
  bool operator==(const TemporalWindow& other) const {
    return start == other.start && end == other.end;
  }
  
  bool operator!=(const TemporalWindow& other) const {
    return !(*this == other);
  }
  
  // 序列化大小（用于网络传输）
  static constexpr size_t kSerializedSize = sizeof(uint64_t) * 2;
  
  // 序列化到缓冲区
  void SerializeTo(void* buffer) const {
    uint8_t* p = static_cast<uint8_t*>(buffer);
    *reinterpret_cast<uint64_t*>(p) = start.value();
    *reinterpret_cast<uint64_t*>(p + sizeof(uint64_t)) = end.value();
  }
  
  // 从缓冲区反序列化
  static TemporalWindow DeserializeFrom(const void* buffer) {
    const uint8_t* p = static_cast<const uint8_t*>(buffer);
    uint64_t s = *reinterpret_cast<const uint64_t*>(p);
    uint64_t e = *reinterpret_cast<const uint64_t*>(p + sizeof(uint64_t));
    return TemporalWindow(Timestamp(s), Timestamp(e));
  }
};

/**
 * @brief 带时序窗口的写集项
 */
struct TemporalWriteSetItem {
  CedarKey key;
  TemporalWindow window;
  uint64_t version{0};  // 写入版本
  
  // 构造函数
  TemporalWriteSetItem() = default;
  TemporalWriteSetItem(const CedarKey& k, const TemporalWindow& w, uint64_t v = 0)
      : key(k), window(w), version(v) {}
  
  // 获取分区ID（从Key中提取）
  PartitionID GetPartitionID() const {
    return key.part_id();
  }
};

/**
 * @brief 带时序窗口的读集项
 */
struct TemporalReadSetItem {
  CedarKey key;
  TemporalWindow window;
  uint64_t read_version{0};     // 读取时的版本
  Timestamp read_timestamp{0};  // 读取时间戳
  
  // 构造函数
  TemporalReadSetItem() = default;
  TemporalReadSetItem(const CedarKey& k, const TemporalWindow& w, 
                      uint64_t v = 0, Timestamp ts = Timestamp(0))
      : key(k), window(w), read_version(v), read_timestamp(ts) {}
  
  // 获取分区ID
  PartitionID GetPartitionID() const {
    return key.part_id();
  }
};

/**
 * @brief 时序锁记录
 * 
 * 存储在Lock Manager中，支持时序粒度加锁
 */
struct TemporalLock {
  TxnID txn_id{0};
  TemporalWindow window;
  LockType type{LockType::kNone};
  Timestamp acquire_time{0};  // 锁获取时间（用于死锁检测）
  
  // 检查是否与另一个锁冲突
  bool ConflictsWith(const TemporalLock& other) const {
    // 首先检查时序窗口重叠
    if (!window.Overlaps(other.window)) {
      return false;  // 时间不重叠，无冲突
    }
    
    // 标准锁冲突矩阵
    if (type == LockType::kWrite || other.type == LockType::kWrite) {
      return true;  // 写锁与任何锁冲突
    }
    // 读-读不冲突
    return false;
  }
  
  // 检查是否超时
  bool IsTimeout(Timestamp now, std::chrono::milliseconds timeout) const {
    return (now.value() - acquire_time.value()) > 
           static_cast<uint64_t>(timeout.count());
  }
};

/**
 * @brief 窗口合并优化器
 * 
 * 用于合并相邻小窗口以减少检测开销
 */
class WindowMergeOptimizer {
 public:
  // 合并相邻小窗口
  static TemporalWindow MergeAdjacentWindows(
      const std::vector<TemporalWindow>& windows,
      uint64_t gap_threshold_us);  // 小于此阈值视为相邻
  
  // 分裂大窗口（对于热点Key）
  static std::vector<TemporalWindow> SplitLargeWindow(
      const TemporalWindow& window,
      uint64_t max_span_us);  // 最大时间跨度
  
  // 优化一组窗口（合并+分裂）
  static std::vector<TemporalWindow> OptimizeWindows(
      const std::vector<TemporalWindow>& windows,
      uint64_t merge_threshold_us,
      uint64_t split_threshold_us);
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_TEMPORAL_WINDOW_H_
