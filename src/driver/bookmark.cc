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

#include "cedar/driver/bookmark.h"

#include <algorithm>
#include <sstream>
#include <iomanip>

namespace cedar {
namespace driver {

// 序列化格式: "v2:<timestamp>:<txn_id>"
// v2 表示版本号，包含 timestamp 和 txn_id
std::string Bookmark::ToString() const {
  std::ostringstream oss;
  oss << "v2:" << timestamp_ << ":" << txn_id_;
  return oss.str();
}

std::optional<Bookmark> Bookmark::FromString(const std::string& str) {
  if (str.empty()) {
    return std::nullopt;
  }
  
  // 检查版本前缀
  if (str.substr(0, 3) == "v1:") {
    // v1 格式：只有 sequence number（实际是 timestamp）
    try {
      uint64_t ts = std::stoull(str.substr(3));
      return Bookmark(ts, 0);
    } catch (...) {
      return std::nullopt;
    }
  }
  
  if (str.substr(0, 3) == "v2:") {
    // v2 格式：v2:timestamp:txn_id
    size_t first_colon = str.find(':', 3);
    if (first_colon == std::string::npos) {
      return std::nullopt;
    }
    
    // first_colon 指向 timestamp 后的冒号
    // txn_id 从这个冒号后开始到结束
    try {
      uint64_t ts = std::stoull(str.substr(3, first_colon - 3));
      uint64_t txn_id = std::stoull(str.substr(first_colon + 1));
      return Bookmark(ts, txn_id);
    } catch (...) {
      return std::nullopt;
    }
  }
  
  return std::nullopt;
}

Bookmark Bookmark::Combine(const std::vector<Bookmark>& bookmarks) {
  if (bookmarks.empty()) {
    return Bookmark();
  }
  
  // 取最大 timestamp
  uint64_t max_ts = 0;
  uint64_t max_txn_id = 0;
  
  for (const auto& bm : bookmarks) {
    if (bm.timestamp_ > max_ts) {
      max_ts = bm.timestamp_;
      max_txn_id = bm.txn_id_;
    }
  }
  
  return Bookmark(max_ts, max_txn_id);
}

}  // namespace driver
}  // namespace cedar
