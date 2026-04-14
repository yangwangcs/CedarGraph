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
// CedarGraph-DTx 基础类型定义
// =============================================================================

#ifndef CEDAR_DTX_TYPES_H_
#define CEDAR_DTX_TYPES_H_

#include <cstdint>
#include <string>
#include <chrono>
#include <atomic>
#include <unordered_set>

#include "cedar/types/cedar_key.h"
#include "cedar/driver/bookmark.h"

namespace cedar {
namespace dtx {

// =============================================================================
// 基础ID类型
// =============================================================================

// 分区ID (16bit，与CedarKey::part_id_匹配，支持65536个分区)
using PartitionID = uint16_t;
constexpr PartitionID kInvalidPartitionID = 0xFFFF;

// 节点ID
using NodeID = uint32_t;
constexpr NodeID kInvalidNodeID = 0xFFFFFFFF;

// 子图ID（用于GLTR）
using SubgraphID = uint32_t;
constexpr SubgraphID kInvalidSubgraphID = 0xFFFFFFFF;

// 事务ID
using TxnID = uint64_t;
constexpr TxnID kInvalidTxnID = 0;

// =============================================================================
// 事务类型（分层模型）
// =============================================================================

enum class TxnType : uint8_t {
  kSinglePartition = 1,     // 单分区事务（Layer 1）- 无需协调
  kSameTemporalRange = 2,   // 同时序范围跨分区（Layer 2）- 轻量协调
  kCrossTemporalRange = 3,  // 跨时序范围（Layer 3）- 完整2PC
  kDeterministic = 4,       // 确定性批量事务（Layer 4）- Calvin风格
};

inline std::string TxnTypeToString(TxnType type) {
  switch (type) {
    case TxnType::kSinglePartition: return "SinglePartition";
    case TxnType::kSameTemporalRange: return "SameTemporalRange";
    case TxnType::kCrossTemporalRange: return "CrossTemporalRange";
    case TxnType::kDeterministic: return "Deterministic";
    default: return "Unknown";
  }
}

// =============================================================================
// 分布式事务状态
// =============================================================================

enum class DistributedTxnState : uint8_t {
  kStarted = 0,
  kPreparing = 1,      // 准备阶段（2PC）
  kPrepared = 2,       // 已准备好
  kCommitting = 3,     // 提交中
  kCommitted = 4,      // 已提交
  kAborting = 5,       // 回滚中
  kAborted = 6,        // 已回滚
};

inline std::string TxnStateToString(DistributedTxnState state) {
  switch (state) {
    case DistributedTxnState::kStarted: return "Started";
    case DistributedTxnState::kPreparing: return "Preparing";
    case DistributedTxnState::kPrepared: return "Prepared";
    case DistributedTxnState::kCommitting: return "Committing";
    case DistributedTxnState::kCommitted: return "Committed";
    case DistributedTxnState::kAborting: return "Aborting";
    case DistributedTxnState::kAborted: return "Aborted";
    default: return "Unknown";
  }
}

// =============================================================================
// 锁类型
// =============================================================================

enum class LockType : uint8_t {
  kNone = 0,
  kRead = 1,     // 读锁
  kWrite = 2,    // 写锁
  kIntent = 3,   // 意向锁（用于层次锁）
};

// =============================================================================
// 验证结果
// =============================================================================

enum class ValidationResult : uint8_t {
  kValid = 0,          // 验证通过
  kInvalid = 1,        // 验证失败（冲突）
  kNeedFullCheck = 2,  // 需要完整检查（O(1)无法确定）
  kTimeout = 3,        // 验证超时
};

// =============================================================================
// 一致性级别
// =============================================================================

enum class TemporalConsistencyLevel : uint8_t {
  kStrict = 0,     // 严格一致性：读取必须看到所有已提交写入
  kBounded = 1,    // 边界一致性：允许读取到N秒前的状态
  kSession = 2,    // 会话一致性：保证会话内因果一致性
  kEventual = 3,   // 最终一致性：无保证，最高性能
};

// =============================================================================
// 配置常量
// =============================================================================

struct DTxConfig {
  // 超时配置
  std::chrono::milliseconds prepare_timeout{100};      // 准备阶段超时
  std::chrono::milliseconds commit_timeout{1000};      // 提交阶段超时
  std::chrono::milliseconds validation_timeout{50};    // 验证超时
  
  // 重试配置
  uint32_t max_retry_count{3};
  std::chrono::milliseconds retry_base_delay{10};
  
  // 分区配置
  PartitionID default_partition_count{256};
  
  // 版本链配置
  uint32_t max_version_chain_length{100};  // 最大版本链长度
  uint32_t gc_interval_ms{1000};           // GC间隔
  
  // TW-CD配置
  bool enable_twcd{true};                  // 启用时序窗口冲突检测
  uint64_t default_temporal_window_us{0};  // 默认时序窗口大小（0=无限）
  
  // RPC配置
  uint32_t rpc_timeout_ms{5000};           // RPC 调用超时（毫秒）
};

// =============================================================================
// 工具函数
// =============================================================================

// 生成分区哈希（用于默认分区策略）
inline PartitionID HashToPartition(const CedarKey& key, PartitionID num_partitions) {
  // 使用entity_id和column_id的组合哈希
  uint64_t hash = std::hash<uint64_t>{}(key.entity_id());
  hash ^= std::hash<uint16_t>{}(key.column_id()) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
  return static_cast<PartitionID>(hash % num_partitions);
}

// 检查Key是否属于指定分区
inline bool KeyBelongsToPartition(const CedarKey& key, PartitionID partition_id) {
  return key.part_id() == partition_id;
}

// CedarKey 的哈希函数（用于 unordered_map/unordered_set）
struct CedarKeyHash {
  std::size_t operator()(const CedarKey& key) const noexcept {
    // 使用 entity_id 和 column_id 的组合哈希
    std::size_t hash = std::hash<uint64_t>{}(key.entity_id());
    hash ^= std::hash<uint16_t>{}(key.column_id()) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
    hash ^= std::hash<uint64_t>{}(key.target_id()) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
    return hash;
  }
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_TYPES_H_
