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

#include "cedar/dtx/bookmark_manager.h"

#include <iostream>
#include <sstream>
#include <thread>
#include <chrono>

namespace cedar {
namespace dtx {

// =============================================================================
// BookmarkHlc 实现
// =============================================================================

BookmarkHlc BookmarkHlc::FromString(const std::string& str) {
  try {
    size_t pos = str.find('.');
    if (pos == std::string::npos) {
      return BookmarkHlc(std::stoull(str), 0);
    }
    
    uint64_t wt = std::stoull(str.substr(0, pos));
    uint64_t l = std::stoull(str.substr(pos + 1));
    return BookmarkHlc(wt, l);
  } catch (...) {
    std::cerr << "[BookmarkManager] Failed to parse bookmark string: " << str << std::endl;
    return BookmarkHlc(0, 0);
  }
}

BookmarkHlc BookmarkHlc::Now() {
  auto now = std::chrono::system_clock::now();
  auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
      now.time_since_epoch()).count();
  return BookmarkHlc(static_cast<uint64_t>(micros), 0);
}

// =============================================================================
// DistributedBookmark 实现
// =============================================================================

DistributedBookmark::DistributedBookmark(const driver::Bookmark& bm)
    : timestamp(bm.GetTimestamp()),
      txn_id(bm.GetTransactionId()),
      hlc(BookmarkHlc::Now()) {}

void DistributedBookmark::SetShardWatermark(PartitionID pid, uint64_t watermark) {
  // 查找是否已存在
  for (auto& [p, w] : shard_watermarks) {
    if (p == pid) {
      w = watermark;
      return;
    }
  }
  // 不存在，添加新条目
  shard_watermarks.emplace_back(pid, watermark);
}

uint64_t DistributedBookmark::GetShardWatermark(PartitionID pid) const {
  for (const auto& [p, w] : shard_watermarks) {
    if (p == pid) {
      return w;
    }
  }
  return 0;
}

bool DistributedBookmark::HappensBefore(const DistributedBookmark& other) const {
  // 优先使用 HLC 比较
  if (hlc != other.hlc) {
    return hlc < other.hlc;
  }
  
  // 回退到时间戳比较
  if (timestamp != other.timestamp) {
    return timestamp < other.timestamp;
  }
  
  return txn_id < other.txn_id;
}

bool DistributedBookmark::IsConcurrentWith(const DistributedBookmark& other) const {
  return !HappensBefore(other) && !other.HappensBefore(*this);
}

std::string DistributedBookmark::Serialize() const {
  std::ostringstream oss;
  oss << "v3:" << timestamp << ":" << txn_id 
      << ":" << hlc.ToString();
  
  // 序列化分片水位
  oss << ":" << shard_watermarks.size();
  for (const auto& [pid, wm] : shard_watermarks) {
    oss << ":" << pid << "," << wm;
  }
  
  return oss.str();
}

std::optional<DistributedBookmark> DistributedBookmark::Deserialize(const std::string& str) {
  try {
    if (str.size() < 3 || str.substr(0, 3) != "v3:") {
      // 尝试解析旧版本格式
      if (str.size() >= 3 && str.substr(0, 3) == "v2:") {
        auto bm = driver::Bookmark::FromString(str);
        if (bm) {
          return DistributedBookmark(*bm);
        }
      }
      return std::nullopt;
    }
    
    DistributedBookmark result;
    std::istringstream iss(str.substr(3));  // 跳过 "v3:"
    std::string token;
    
    // 解析 timestamp
    std::getline(iss, token, ':');
    result.timestamp = std::stoull(token);
    
    // 解析 txn_id
    std::getline(iss, token, ':');
    result.txn_id = std::stoull(token);
    
    // 解析 HLC
    std::getline(iss, token, ':');
    result.hlc = BookmarkHlc::FromString(token);
    
    // 解析分片水位数量
    std::getline(iss, token, ':');
    size_t watermark_count = std::stoul(token);
  
    for (size_t i = 0; i < watermark_count; ++i) {
      std::getline(iss, token, ':');
      size_t pos = token.find(',');
      if (pos != std::string::npos) {
        PartitionID pid = static_cast<PartitionID>(std::stoul(token.substr(0, pos)));
        uint64_t wm = std::stoull(token.substr(pos + 1));
        result.shard_watermarks.emplace_back(pid, wm);
      }
    }
    
    return result;
  } catch (...) {
    std::cerr << "[BookmarkManager] Failed to deserialize bookmark" << std::endl;
    return std::nullopt;
  }
}

DistributedBookmark DistributedBookmark::Merge(
    const std::vector<DistributedBookmark>& bookmarks) {
  
  if (bookmarks.empty()) {
    return DistributedBookmark();
  }
  
  DistributedBookmark result = bookmarks[0];
  
  for (size_t i = 1; i < bookmarks.size(); ++i) {
    const auto& bm = bookmarks[i];
    
    // 优先使用 HLC 比较，取最大值
    if (bm.hlc > result.hlc) {
      result.timestamp = bm.timestamp;
      result.txn_id = bm.txn_id;
      result.hlc = bm.hlc;
    } else if (bm.hlc == result.hlc && bm.timestamp > result.timestamp) {
      result.timestamp = bm.timestamp;
      result.txn_id = bm.txn_id;
    }
    
    // 合并分片水位（取最大值）
    for (const auto& [pid, wm] : bm.shard_watermarks) {
      uint64_t current_wm = result.GetShardWatermark(pid);
      if (wm > current_wm) {
        result.SetShardWatermark(pid, wm);
      }
    }
  }
  
  return result;
}

// =============================================================================
// CausalConsistencyChecker 实现
// =============================================================================

bool CausalConsistencyChecker::CheckReadYourWrites(
    const DistributedBookmark& write_bm,
    const DistributedBookmark& read_bm) {
  // 读取的 Bookmark 应该 HappensAfter 写入的 Bookmark
  return write_bm.HappensBefore(read_bm) || write_bm == read_bm;
}

bool CausalConsistencyChecker::CheckMonotonicReads(
    const DistributedBookmark& prev_read_bm,
    const DistributedBookmark& curr_read_bm) {
  // 当前读取应该 HappensAfter 之前的读取
  return prev_read_bm.HappensBefore(curr_read_bm) || prev_read_bm == curr_read_bm;
}

bool CausalConsistencyChecker::CheckWritesFollowReads(
    const DistributedBookmark& read_bm,
    const DistributedBookmark& write_bm) {
  // 写入应该 HappensAfter 读取
  return read_bm.HappensBefore(write_bm) || read_bm == write_bm;
}

bool CausalConsistencyChecker::CheckMonotonicWrites(
    const DistributedBookmark& prev_write_bm,
    const DistributedBookmark& curr_write_bm) {
  // 当前写入应该 HappensAfter 之前的写入
  return prev_write_bm.HappensBefore(curr_write_bm) || prev_write_bm == curr_write_bm;
}

// =============================================================================
// BookmarkManager 实现
// =============================================================================

BookmarkManager::BookmarkManager() {
  current_hlc_ = BookmarkHlc::Now();
}

BookmarkManager::~BookmarkManager() = default;

BookmarkHlc BookmarkManager::GetCurrentHLC() {
  std::lock_guard<std::mutex> lock(hlc_mutex_);
  
  // 获取当前物理时间
  auto now = BookmarkHlc::Now();
  
  // 如果物理时间前进，重置逻辑计数
  if (now.wall_time > current_hlc_.wall_time) {
    current_hlc_.wall_time = now.wall_time;
    current_hlc_.logical = 0;
  } else {
    // 物理时间相同，增加逻辑计数
    ++current_hlc_.logical;
  }
  
  return current_hlc_;
}

void BookmarkManager::UpdateHLC(const BookmarkHlc& remote) {
  std::lock_guard<std::mutex> lock(hlc_mutex_);
  
  // HLC 更新规则（标准 HLC 算法）
  if (remote.wall_time > current_hlc_.wall_time) {
    // 远程物理时间更新
    current_hlc_.wall_time = remote.wall_time;
    current_hlc_.logical = remote.logical + 1;
  } else if (remote.wall_time == current_hlc_.wall_time) {
    // 物理时间相同，取最大逻辑计数后 +1
    current_hlc_.logical = std::max(current_hlc_.logical, remote.logical) + 1;
  } else {
    // 远程时间更旧，只增加本地逻辑计数
    ++current_hlc_.logical;
  }
}

DistributedBookmark BookmarkManager::CreateBookmark(const DistributedTxnContext& ctx) {
  DistributedBookmark bm;
  bm.timestamp = ctx.GetCommitTimestamp();
  bm.txn_id = ctx.GetTxnID();
  bm.hlc = GetCurrentHLC();
  
  // 添加各分片水位
  std::lock_guard<std::mutex> lock(watermarks_mutex_);
  for (const auto& [pid, wm] : watermarks_) {
    bm.SetShardWatermark(pid, wm);
  }
  
  ++bookmarks_created_;
  return bm;
}

Status BookmarkManager::WaitForBookmark(const DistributedBookmark& bookmark,
                                         std::chrono::milliseconds timeout) {
  ++causal_waits_;
  auto start = std::chrono::steady_clock::now();
  
  auto deadline = start + timeout;
  
  while (std::chrono::steady_clock::now() < deadline) {
    if (IsBookmarkSatisfied(bookmark)) {
      auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - start);
      causal_wait_time_us_ += elapsed.count();
      return Status::OK();
    }
    
    // 短暂等待后重试
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  
  auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now() - start);
  causal_wait_time_us_ += elapsed.count();
  
  return Status::IOError("BookmarkManager", "WaitForBookmark timeout");
}

bool BookmarkManager::IsBookmarkSatisfied(const DistributedBookmark& bookmark) {
  // 检查本地 HLC 是否已经超过 Bookmark 的 HLC
  auto current = GetCurrentHLC();
  if (bookmark.hlc > current) {
    return false;  // HLC 未满足
  }
  
  // 检查分片水位（即使 HLC 满足，也必须验证各分片水位）
  std::lock_guard<std::mutex> lock(watermarks_mutex_);
  for (const auto& [pid, wm] : bookmark.shard_watermarks) {
    auto it = watermarks_.find(pid);
    if (it == watermarks_.end()) {
      return false;  // 未知分片
    }
    if (it->second < wm) {
      return false;  // 水位未满足
    }
  }
  
  return true;
}

void BookmarkManager::UpdateLocalWatermark(PartitionID pid, uint64_t watermark) {
  std::lock_guard<std::mutex> lock(watermarks_mutex_);
  
  auto& current = watermarks_[pid];
  
  // 只更新更大的水位
  if (watermark > current) {
    current = watermark;
  }
}

uint64_t BookmarkManager::GetWatermark(PartitionID pid) const {
  std::lock_guard<std::mutex> lock(watermarks_mutex_);
  auto it = watermarks_.find(pid);
  return (it != watermarks_.end()) ? it->second : 0;
}

uint64_t BookmarkManager::GetGlobalMinWatermark() const {
  std::lock_guard<std::mutex> lock(watermarks_mutex_);
  
  if (watermarks_.empty()) {
    return 0;
  }
  
  uint64_t min_wm = std::numeric_limits<uint64_t>::max();
  for (const auto& [_, wm] : watermarks_) {
    if (wm < min_wm) {
      min_wm = wm;
    }
  }
  
  return min_wm;
}

void BookmarkManager::SetSessionBookmark(uint64_t session_id, 
                                          const DistributedBookmark& bm) {
  std::lock_guard<std::mutex> lock(sessions_mutex_);
  session_bookmarks_[session_id] = bm;
}

DistributedBookmark BookmarkManager::GetSessionBookmark(uint64_t session_id) const {
  std::lock_guard<std::mutex> lock(sessions_mutex_);
  auto it = session_bookmarks_.find(session_id);
  return (it != session_bookmarks_.end()) ? it->second : DistributedBookmark();
}

void BookmarkManager::UpdateSessionBookmark(uint64_t session_id, 
                                             const DistributedBookmark& bm) {
  std::lock_guard<std::mutex> lock(sessions_mutex_);
  
  auto it = session_bookmarks_.find(session_id);
  if (it == session_bookmarks_.end()) {
    session_bookmarks_[session_id] = bm;
  } else {
    // 合并 Bookmark
    std::vector<DistributedBookmark> bookmarks = {it->second, bm};
    it->second = DistributedBookmark::Merge(bookmarks);
    ++bookmarks_merged_;
  }
}

BookmarkManager::Stats BookmarkManager::GetStats() const {
  Stats stats;
  stats.bookmarks_created = bookmarks_created_.load();
  stats.bookmarks_merged = bookmarks_merged_.load();
  stats.causal_waits = causal_waits_.load();
  stats.causal_wait_time_us = causal_wait_time_us_.load();
  return stats;
}

}  // namespace dtx
}  // namespace cedar
