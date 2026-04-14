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

#include "cedar/types/cedar_key.h"

#include <sstream>
#include <iomanip>
#include <ctime>

namespace cedar {

// DebugString: 简洁的调试格式
std::string CedarKey::DebugString() const {
  std::ostringstream oss;
  oss << "CK{"
      << "eid=" << entity_id()
      << ",ts=" << timestamp().value()
      << ",type=" << static_cast<int>(entity_type_)
      << ",col=" << column_id()
      << ",tgt=" << target_id()
      << ",seq=" << sequence()
      << ",flags=" << static_cast<int>(flags())
      << ",part=" << part_id()
      << "}";
  return oss.str();
}

// ToString: 人类可读的详细格式
std::string CedarKey::ToString() const {
  std::ostringstream oss;
  
  // 格式化时间戳为可读格式
  auto ts = timestamp().value();
  time_t seconds = ts / 1000000;
  uint64_t micros = ts % 1000000;
  
  char time_buf[32];
  struct tm* tm_info = localtime(&seconds);
  strftime(time_buf, sizeof(time_buf), "%Y-%m-%dT%H:%M:%S", tm_info);
  
  oss << "{";
  
  // Entity ID
  oss << "ID:" << entity_id();
  
  // Entity Type
  oss << ", Type:";
  if (IsVertex()) {
    oss << "Vertex";
  } else if (IsEdgeOut()) {
    oss << "EdgeOut(" << entity_id() << "->" << target_id() << ")";
  } else if (IsEdgeIn()) {
    oss << "EdgeIn(" << target_id() << "->" << entity_id() << ")";
  } else {
    oss << "Unknown(" << static_cast<int>(entity_type_) << ")";
  }
  
  // Time
  oss << ", Time:" << time_buf << "." << std::setfill('0') << std::setw(6) << micros;
  
  // Column/Edge Type
  if (IsVertex()) {
    oss << ", Col:" << column_id();
  } else {
    oss << ", EdgeType:" << column_id();
  }
  
  // Sequence
  if (sequence() != 0) {
    oss << ", Seq:" << sequence();
  }
  
  // Flags
  if (flags() != 0) {
    oss << ", Flags:[";
    bool first = true;
    if (IsCreate()) { oss << (first ? "" : ",") << "Create"; first = false; }
    if (IsUpdate()) { oss << (first ? "" : ",") << "Update"; first = false; }
    if (IsDelete()) { oss << (first ? "" : ",") << "Delete"; first = false; }
    if (IsTombstone()) { oss << (first ? "" : ",") << "Tombstone"; first = false; }
    if (HasVInline()) { oss << (first ? "" : ",") << "VInline"; first = false; }
    if (IsDistributed()) { oss << (first ? "" : ",") << "Distributed"; first = false; }
    oss << "]";
  }
  
  // Part ID
  if (part_id() != 0) {
    oss << ", Part:" << part_id();
  }
  
  // Extension/Target value for vertices
  if (IsVertex() && target_id() != 0) {
    oss << ", Ext:" << target_id();
  }
  
  oss << "}";
  return oss.str();
}

// ToHexString: 十六进制表示（用于调试二进制格式）
std::string CedarKey::ToHexString() const {
  std::ostringstream oss;
  oss << "0x";
  const uint8_t* data = reinterpret_cast<const uint8_t*>(this);
  for (size_t i = 0; i < kKeySize; ++i) {
    oss << std::hex << std::setfill('0') << std::setw(2) << static_cast<int>(data[i]);
  }
  return oss.str();
}

}  // namespace cedar
