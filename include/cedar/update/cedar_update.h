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
// CedarUpdate - 时态图事件到 CedarKey 的转换引擎（集中式严格模式版）
// =============================================================================
// 
// CedarUpdate 是图事件 ε 到 32B CedarKey 的转换引擎。
// 支持集中式严格模式：三重门校验（拓扑门、时态门、写入门）
//
// 使用示例：
//   auto update = CedarUpdate::Create(StrictLevel::CHECK_EXISTS);
//   update.At(Timestamp::Now())
//         .CreateEdge(1001, 1002, 2, descriptor);
//   
//   auto status = update.Apply(storage);
//   if (!status.ok()) {
//       LOG(ERROR) << status.ToString();  // [kSrcNodeNotFound] ...
//   }
// =============================================================================

#ifndef CEDAR_UPDATE_H_
#define CEDAR_UPDATE_H_

#include <cstdint>
#include <vector>
#include <memory>
#include <optional>
#include <unordered_map>
#include <unordered_set>

#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include "cedar/core/cedar_status.h"
#include "cedar/core/status.h"

namespace cedar {

// 前向声明
class CedarGraphStorage;
class LsmEngine;

// =============================================================================
// 严格模式级别
// =============================================================================
enum class StrictLevel : uint8_t {
  PERMISSIVE = 0,      // 最终一致性，无检查
  CHECK_EXISTS = 1,    // 拓扑门：检查存在性
  STRICT_TEMPORAL = 2, // +时态门：检查时态约束
  FULL_ISOLATION = 3,  // 完整 ACID（含写入门锁定）
};

// =============================================================================
// 操作类型
// =============================================================================
enum class UpdateOpType : uint8_t {
  CREATE_VERTEX = 0,
  UPDATE_VERTEX = 1,
  DELETE_VERTEX = 2,
  CREATE_EDGE = 3,
  UPDATE_EDGE = 4,
  DELETE_EDGE = 5,
};

// =============================================================================
// 实体快照（用于校验）
// =============================================================================
struct EntitySnapshot {
  bool exists = false;
  bool is_deleted = false;
  Timestamp create_time;
  Timestamp latest_version_time;
  uint8_t latest_op_type = 0;
};

// =============================================================================
// 内部操作记录
// =============================================================================
struct UpdateRecord {
  UpdateOpType op;
  CedarKey key;
  Descriptor value;
  
  // 边的额外信息（用于约束检查）
  struct EdgeInfo {
    uint64_t src_id;
    uint64_t dst_id;
    bool check_src_exists;
    bool check_dst_exists;
  };
  std::optional<EdgeInfo> edge_info;
};

// =============================================================================
// CedarUpdate - 主类
// =============================================================================
class CedarUpdate {
 public:
  // ===========================================================================
  // 工厂方法
  // ===========================================================================
  static CedarUpdate Create(StrictLevel level = StrictLevel::PERMISSIVE);
  
  // ===========================================================================
  // 配置
  // ===========================================================================
  // 设置全局时间戳（该 Update 内所有操作共享）
  CedarUpdate& At(Timestamp ts);
  
  // 设置序列号起始值（自动递增）
  CedarUpdate& WithSequence(uint16_t start_seq);
  
  // 获取严格级别
  StrictLevel GetStrictLevel() const { return strict_level_; }
  
  // ===========================================================================
  // 节点操作
  // ===========================================================================
  // 创建/更新节点（自动检测是 CREATE 还是 UPDATE）
  CedarUpdate& PutVertex(uint64_t vertex_id, 
                         uint16_t label_id,
                         const Descriptor& descriptor);
  
  // 显式创建节点（失败如果已存在）
  CedarUpdate& CreateVertex(uint64_t vertex_id, 
                            uint16_t label_id,
                            const Descriptor& descriptor);
  
  // 更新节点属性
  CedarUpdate& UpdateVertex(uint64_t vertex_id,
                            uint16_t property_id,
                            const Descriptor& descriptor);
  
  // 删除节点（时态墓碑）
  CedarUpdate& DeleteVertex(uint64_t vertex_id);
  
  // ===========================================================================
  // 边操作（自动生成 EdgeOut + EdgeIn）
  // ===========================================================================
  // 创建边（严格模式下自动检查端点存在性）
  CedarUpdate& PutEdge(uint64_t src_id,
                       uint64_t dst_id,
                       uint16_t edge_type,
                       const Descriptor& descriptor);
  
  // 带显式约束的边操作
  CedarUpdate& CreateEdge(uint64_t src_id,
                          uint64_t dst_id,
                          uint16_t edge_type,
                          const Descriptor& descriptor,
                          bool ensure_src_exists = true,
                          bool ensure_dst_exists = true);
  
  // 删除边
  CedarUpdate& DeleteEdge(uint64_t src_id,
                          uint64_t dst_id,
                          uint16_t edge_type);
  
  // ===========================================================================
  // 属性操作（支持自动内联）
  // ===========================================================================
  // 设置属性
  CedarUpdate& Property(uint64_t entity_id,
                        EntityType type,
                        uint16_t prop_id,
                        const Descriptor& value);
  
  // ===========================================================================
  // 执行（集中式严格模式）
  // ===========================================================================
  // 提交到存储引擎（执行三重门校验）
  CedarStatus Apply(CedarGraphStorage* storage);
  CedarStatus Apply(LsmEngine* engine);
  
  // ===========================================================================
  // 内部访问（测试用）
  // ===========================================================================
  const std::vector<UpdateRecord>& GetRecords() const { return records_; }
  size_t Count() const { return records_.size(); }
  void Clear() { records_.clear(); cache_.clear(); }

 private:
  StrictLevel strict_level_;
  Timestamp timestamp_;
  uint16_t sequence_ = 0;
  std::vector<UpdateRecord> records_;
  
  // 存在性检查缓存（避免重复 Seek）
  std::unordered_map<uint64_t, EntitySnapshot> cache_;
  
  // 内部构造方法
  CedarUpdate() : strict_level_(StrictLevel::PERMISSIVE), 
                  timestamp_(Timestamp::Now()) {}
  
  // 添加记录
  void AddRecord(UpdateRecord record);
  
  // 内部辅助方法
  uint16_t DoComputePartition(uint64_t entity_id);
  uint8_t DoPackFlags(uint8_t op_type, bool distributed = true);
  
  // ===========================================================================
  // 集中式严格模式：三重门校验
  // ===========================================================================
  
  // 第一重 + 第二重：拓扑门 + 时态门
  CedarStatus ValidateCentralized(LsmEngine* engine);
  
  // 获取实体快照（利用 CedarKey 结构高效 Seek）
  EntitySnapshot GetEntitySnapshot(LsmEngine* engine,
                                   uint64_t entity_id,
                                   EntityType type,
                                   Timestamp query_time);
  
  // 检查边的时间约束（时态门）
  CedarStatus CheckTemporalConstraint(uint64_t entity_id,
                                      Timestamp edge_time,
                                      const EntitySnapshot& snapshot);
  
  // 第三重：写入门（原子提交）
  CedarStatus DoApply(LsmEngine* engine);
 
 // 锚点机制：写入状态锚点（State Anchor）
 Status WriteStateAnchor(LsmEngine* engine, const UpdateRecord& record);
};

// =============================================================================
// 便捷宏
// =============================================================================
#define CEDAR_UPDATE(name, level) \
  auto name = cedar::CedarUpdate::Create(level)

}  // namespace cedar

#endif  // CEDAR_UPDATE_H_
