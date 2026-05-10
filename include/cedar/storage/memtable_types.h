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

#ifndef CEDAR_MEMTABLE_TYPES_H_
#define CEDAR_MEMTABLE_TYPES_H_

#include <cstdint>
#include <optional>

#include "cedar/types/descriptor.h"
#include "cedar/types/cedar_key.h"

namespace cedar {

// MemTable 中的条目 (保留所有历史版本)
struct MemTableEntry {
  Timestamp timestamp;      // 业务时间戳
  Timestamp txn_version;    // 事务版本号（用于 MVCC）
  Descriptor descriptor;
  std::optional<uint64_t> dst_id;  // 目标节点ID (用于边)
  uint16_t sequence = 0;    // 微秒内序列号
  uint8_t flags = 0;        // 标志位
  uint16_t part_id = 0;     // 分区ID

  MemTableEntry() = default;
  MemTableEntry(Timestamp ts, const Descriptor& desc, Timestamp txn_ver)
      : timestamp(ts), txn_version(txn_ver), descriptor(desc) {}
  MemTableEntry(Timestamp ts, const Descriptor& desc, std::optional<uint64_t> dst, Timestamp txn_ver)
      : timestamp(ts), txn_version(txn_ver), descriptor(desc), dst_id(dst) {}
  MemTableEntry(Timestamp ts, const Descriptor& desc, std::optional<uint64_t> dst, 
                Timestamp txn_ver, uint16_t seq, uint8_t f, uint16_t part)
      : timestamp(ts), txn_version(txn_ver), descriptor(desc), dst_id(dst),
        sequence(seq), flags(f), part_id(part) {}
  
  // 显式构造函数，避免 emplace_back 歧义
  static MemTableEntry Make(Timestamp ts, const Descriptor& desc, 
                            std::optional<uint64_t> dst, Timestamp txn_ver) {
    return MemTableEntry(ts, desc, dst, txn_ver);
  }
  
  static MemTableEntry MakeWithMetadata(Timestamp ts, const Descriptor& desc, 
                                        std::optional<uint64_t> dst, Timestamp txn_ver,
                                        uint16_t seq, uint8_t f, uint16_t part) {
    return MemTableEntry(ts, desc, dst, txn_ver, seq, f, part);
  }
};

// MVCC 版本链节点 - 用于高效遍历实体的所有时间版本
struct TemporalVersionNode {
  Timestamp timestamp;      // 业务时间戳（用于时序查询和 Key 排序）
  Timestamp txn_version;    // 事务版本号（用于 MVCC 隔离）
  Descriptor descriptor;
  
  // 版本链指针 (按时间降序: newer -> older)
  TemporalVersionNode* newer;  // 更新的版本 (时间戳更大)
  TemporalVersionNode* older;  // 更旧的版本 (时间戳更小)
  
  TemporalVersionNode(Timestamp ts, const Descriptor& desc, Timestamp txn_ver)
      : timestamp(ts), txn_version(txn_ver), descriptor(desc), newer(nullptr), older(nullptr) {}
};

}  // namespace cedar

#endif  // CEDAR_MEMTABLE_TYPES_H_
