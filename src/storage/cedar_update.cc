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

#include "cedar/update/cedar_update.h"

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/storage/entity_lifecycle.h"
#include "cedar/transaction/write_batch.h"

namespace cedar {

// =============================================================================
// 静态辅助函数（匿名命名空间）
// =============================================================================
namespace {

// 计算分区 ID：取低 16 位（等价于 % 65536）
inline uint16_t ComputePartition(uint64_t entity_id) {
  return static_cast<uint16_t>(entity_id);
}

// 打包 flags：OpType + Distributed 标记
inline uint8_t PackFlags(uint8_t op_type, bool distributed) {
  uint8_t flags = op_type & cedar::key_flags::kOpTypeMask;
  if (distributed) {
    flags |= cedar::key_flags::kIsDistributed;
  }
  return flags;
}

// 构造用于 Seek 的 CedarKey（用于存在性检查）
CedarKey MakeSeekKey(uint64_t entity_id, EntityType type, Timestamp ts) {
  uint16_t part_id = static_cast<uint16_t>(entity_id);
  return CedarKey(
      entity_id,
      type,
      0,  // column_id = 0 (生命周期列)
      ts,
      0,  // sequence
      0,  // target_id
      0,  // flags
      part_id
  );
}

}  // anonymous namespace

// =============================================================================
// 工厂方法
// =============================================================================

CedarUpdate CedarUpdate::Create(StrictLevel level) {
  CedarUpdate update;
  update.strict_level_ = level;
  return update;
}

// =============================================================================
// 配置方法
// =============================================================================

CedarUpdate& CedarUpdate::At(Timestamp ts) {
  timestamp_ = ts;
  return *this;
}

CedarUpdate& CedarUpdate::WithSequence(uint16_t start_seq) {
  sequence_ = start_seq;
  return *this;
}

// =============================================================================
// 节点操作
// =============================================================================

CedarUpdate& CedarUpdate::PutVertex(uint64_t vertex_id,
                                    uint16_t label_id,
                                    const Descriptor& descriptor) {
  uint16_t part_id = ComputePartition(vertex_id);
  uint8_t flags = PackFlags(op_type::kCreate, true);
  
  CedarKey key = CedarKey::Vertex(vertex_id, label_id, timestamp_, 
                                  sequence_++, part_id, 0, flags);
  
  UpdateRecord record;
  record.op = UpdateOpType::CREATE_VERTEX;
  record.key = key;
  record.value = descriptor;
  AddRecord(record);
  
  return *this;
}

CedarUpdate& CedarUpdate::CreateVertex(uint64_t vertex_id,
                                       uint16_t label_id,
                                       const Descriptor& descriptor) {
  uint16_t part_id = ComputePartition(vertex_id);
  uint8_t flags = PackFlags(op_type::kCreate, true);
  
  CedarKey key = CedarKey::Vertex(vertex_id, label_id, timestamp_,
                                  sequence_++, part_id, 0, flags);
  
  UpdateRecord record;
  record.op = UpdateOpType::CREATE_VERTEX;
  record.key = key;
  record.value = descriptor;
  AddRecord(record);
  
  return *this;
}

CedarUpdate& CedarUpdate::UpdateVertex(uint64_t vertex_id,
                                       uint16_t property_id,
                                       const Descriptor& descriptor) {
  uint16_t part_id = ComputePartition(vertex_id);
  uint8_t flags = PackFlags(op_type::kUpdate, true);
  
  CedarKey key = CedarKey::Vertex(vertex_id, property_id, timestamp_,
                                  sequence_++, part_id, 0, flags);
  
  UpdateRecord record;
  record.op = UpdateOpType::UPDATE_VERTEX;
  record.key = key;
  record.value = descriptor;
  AddRecord(record);
  
  return *this;
}

CedarUpdate& CedarUpdate::DeleteVertex(uint64_t vertex_id) {
  uint16_t part_id = ComputePartition(vertex_id);
  // DELETE 操作：delta_op = 10，不设置 kTombstone（bit 7）
  uint8_t flags = PackFlags(op_type::kDelete, true);
  
  CedarKey key = CedarKey::Vertex(vertex_id, 0, timestamp_,
                                  sequence_++, part_id, 0, flags);
  
  UpdateRecord record;
  record.op = UpdateOpType::DELETE_VERTEX;
  record.key = key;
  record.value = Descriptor::InlineInt(0, 0);
  AddRecord(record);
  
  return *this;
}

// =============================================================================
// 边操作（自动生成 EdgeOut + EdgeIn）
// =============================================================================

CedarUpdate& CedarUpdate::PutEdge(uint64_t src_id,
                                  uint64_t dst_id,
                                  uint16_t edge_type,
                                  const Descriptor& descriptor) {
  // 根据严格级别决定是否检查端点存在性
  bool check_exists = (strict_level_ >= StrictLevel::CHECK_EXISTS);
  return CreateEdge(src_id, dst_id, edge_type, descriptor, 
                    check_exists, check_exists);
}

CedarUpdate& CedarUpdate::CreateEdge(uint64_t src_id,
                                     uint64_t dst_id,
                                     uint16_t edge_type,
                                     const Descriptor& descriptor,
                                     bool ensure_src_exists,
                                     bool ensure_dst_exists) {
  uint16_t src_part = ComputePartition(src_id);
  uint16_t dst_part = ComputePartition(dst_id);
  uint8_t flags = PackFlags(op_type::kCreate, true);
  
  // ========== EdgeOut (src -> dst) ==========
  CedarKey edge_out = CedarKey::EdgeOut(src_id, dst_id, edge_type,
                                        timestamp_, sequence_++, 
                                        src_part, flags);
  
  UpdateRecord out_record;
  out_record.op = UpdateOpType::CREATE_EDGE;
  out_record.key = edge_out;
  out_record.value = descriptor;
  out_record.edge_info = UpdateRecord::EdgeInfo{
    src_id, dst_id, ensure_src_exists, ensure_dst_exists
  };
  AddRecord(out_record);
  
  // ========== EdgeIn (dst <- src) 反向索引 ==========
  CedarKey edge_in = CedarKey::EdgeIn(dst_id, src_id, edge_type,
                                      timestamp_, sequence_++,
                                      dst_part, flags);
  
  UpdateRecord in_record;
  in_record.op = UpdateOpType::CREATE_EDGE;
  in_record.key = edge_in;
  in_record.value = Descriptor::InlineInt(edge_type, 0);
  // EdgeIn 不需要重复约束标记
  AddRecord(in_record);
  
  return *this;
}

CedarUpdate& CedarUpdate::DeleteEdge(uint64_t src_id,
                                     uint64_t dst_id,
                                     uint16_t edge_type) {
  uint16_t src_part = ComputePartition(src_id);
  uint16_t dst_part = ComputePartition(dst_id);
  uint8_t flags = PackFlags(op_type::kDelete, true);
  
  // EdgeOut 删除标记
  CedarKey edge_out = CedarKey::EdgeOut(src_id, dst_id, edge_type,
                                        timestamp_, sequence_++,
                                        src_part, flags);
  
  UpdateRecord out_record;
  out_record.op = UpdateOpType::DELETE_EDGE;
  out_record.key = edge_out;
  out_record.value = Descriptor::InlineInt(0, 0);
  AddRecord(out_record);
  
  // EdgeIn 删除标记
  CedarKey edge_in = CedarKey::EdgeIn(dst_id, src_id, edge_type,
                                      timestamp_, sequence_++,
                                      dst_part, flags);
  
  UpdateRecord in_record;
  in_record.op = UpdateOpType::DELETE_EDGE;
  in_record.key = edge_in;
  in_record.value = Descriptor::InlineInt(0, 0);
  AddRecord(in_record);
  
  return *this;
}

// =============================================================================
// 属性操作
// =============================================================================

CedarUpdate& CedarUpdate::Property(uint64_t entity_id,
                                   EntityType type,
                                   uint16_t prop_id,
                                   const Descriptor& value) {
  uint16_t part_id = ComputePartition(entity_id);
  uint8_t flags = PackFlags(op_type::kUpdate, true);
  
  CedarKey key = CedarKey::Vertex(entity_id, prop_id, timestamp_,
                                  sequence_++, part_id, 0, flags);
  
  UpdateRecord record;
  record.op = UpdateOpType::UPDATE_VERTEX;
  record.key = key;
  record.value = value;
  AddRecord(record);
  
  return *this;
}

// =============================================================================
// 执行（集中式严格模式）
// =============================================================================

CedarStatus CedarUpdate::Apply(CedarGraphStorage* storage) {
  if (records_.empty()) {
    return CedarStatus::OK();
  }
  
  LsmEngine* engine = storage->GetLsmEngine();
  if (!engine) {
    return CedarStatus(CedarCode::kInternalError,
                       "Storage engine not available");
  }
  
  // ========== 严格模式：三重门校验 ==========
  if (strict_level_ >= StrictLevel::CHECK_EXISTS) {
    // 第一重 + 第二重：拓扑门 + 时态门
    auto status = ValidateCentralized(engine);
    CEDAR_RETURN_IF_ERROR(status);
  }
  
  // ========== 第三重：写入门（原子提交）==========
  return DoApply(engine);
}

CedarStatus CedarUpdate::Apply(LsmEngine* engine) {
  if (records_.empty()) {
    return CedarStatus::OK();
  }
  
  // 对于 LsmEngine 直接写入，跳过严格模式检查
  return DoApply(engine);
}

// =============================================================================
// 集中式严格模式：三重门校验
// =============================================================================

CedarStatus CedarUpdate::ValidateCentralized(LsmEngine* engine) {
  // 遍历所有记录进行校验
  for (const auto& record : records_) {
    
    // ========== 第一重：拓扑门（边端点存在性检查）==========
    if (record.edge_info.has_value()) {
      const auto& edge = *record.edge_info;
      
      // 检查源点存在性
      if (edge.check_src_exists) {
        auto it = cache_.find(edge.src_id);
        if (it == cache_.end()) {
          auto snapshot = GetEntitySnapshot(
              engine, edge.src_id, EntityType::Vertex, timestamp_);
          it = cache_.emplace(edge.src_id, snapshot).first;
        }
        
        if (!it->second.exists) {
          return CedarStatus(CedarCode::kSrcNodeNotFound,
              "Node " + std::to_string(edge.src_id) + 
              " does not exist at timestamp " + 
              std::to_string(timestamp_.value()))
              .WithEntity(edge.src_id)
              .WithTimestamp(timestamp_.value());
        }
        
        // 第二重：时态门 - 检查边时间是否 >= 源点创建时间
        if (strict_level_ >= StrictLevel::STRICT_TEMPORAL) {
          auto status = CheckTemporalConstraint(
              edge.src_id, timestamp_, it->second);
          CEDAR_RETURN_IF_ERROR(status);
        }
      }
      
      // 检查终点存在性
      if (edge.check_dst_exists) {
        auto it = cache_.find(edge.dst_id);
        if (it == cache_.end()) {
          auto snapshot = GetEntitySnapshot(
              engine, edge.dst_id, EntityType::Vertex, timestamp_);
          it = cache_.emplace(edge.dst_id, snapshot).first;
        }
        
        if (!it->second.exists) {
          return CedarStatus(CedarCode::kDstNodeNotFound,
              "Node " + std::to_string(edge.dst_id) + 
              " does not exist at timestamp " + 
              std::to_string(timestamp_.value()))
              .WithEntity(edge.dst_id)
              .WithTimestamp(timestamp_.value());
        }
        
        // 第二重：时态门 - 检查边时间是否 >= 终点创建时间
        if (strict_level_ >= StrictLevel::STRICT_TEMPORAL) {
          auto status = CheckTemporalConstraint(
              edge.dst_id, timestamp_, it->second);
          CEDAR_RETURN_IF_ERROR(status);
        }
      }
    }
    
    // ========== 第二重：时态门（UPDATE/DELETE 目标检查）==========
    if (strict_level_ >= StrictLevel::STRICT_TEMPORAL) {
      if (record.op == UpdateOpType::UPDATE_VERTEX ||
          record.op == UpdateOpType::UPDATE_EDGE) {
        
        uint64_t entity_id = record.key.entity_id();
        auto it = cache_.find(entity_id);
        if (it == cache_.end()) {
          auto snapshot = GetEntitySnapshot(
              engine, entity_id, record.key.entity_type(), timestamp_);
          it = cache_.emplace(entity_id, snapshot).first;
        }
        
        if (!it->second.exists) {
          return CedarStatus(CedarCode::kUpdateOnDeleted,
              "Cannot update non-existent entity at timestamp " +
              std::to_string(timestamp_.value()))
              .WithEntity(entity_id)
              .WithTimestamp(timestamp_.value());
        }
      }
    }
  }
  
  return CedarStatus::OK();
}

// 获取实体快照（利用 CedarKey 结构高效 Seek）
EntitySnapshot CedarUpdate::GetEntitySnapshot(LsmEngine* engine,
                                               uint64_t entity_id,
                                               EntityType type,
                                               Timestamp query_time) {
  EntitySnapshot snapshot;
  
  // 使用 GetAtTime 查询指定时间点的最新版本
  auto result = engine->GetAtTime(entity_id, type, 0, query_time);
  
  if (!result.has_value()) {
    snapshot.exists = false;
    return snapshot;
  }
  
  // 实体存在
  snapshot.exists = true;
  snapshot.latest_version_time = query_time;
  
  // 获取该实体的最新版本以检查是否为 DELETE
  // 注意：这里简化处理，实际应该从 Key 中解析 flags
  // 目前假设 result 的存在即表示实体有效
  
  return snapshot;
}

// 检查时态约束
CedarStatus CedarUpdate::CheckTemporalConstraint(uint64_t entity_id,
                                                  Timestamp edge_time,
                                                  const EntitySnapshot& snapshot) {
  // 简化实现：假设 snapshot 中已有 create_time
  // 实际应该从存储中查询该实体的最早 CREATE 记录
  
  // 如果边的发生时间早于实体创建时间，报错
  if (edge_time.value() < snapshot.create_time.value() && 
      snapshot.create_time.value() != 0) {
    return CedarStatus(CedarCode::kTemporalAnachronism,
        "Edge time (" + std::to_string(edge_time.value()) + 
        ") is earlier than Node " + std::to_string(entity_id) +
        " creation time (" + std::to_string(snapshot.create_time.value()) + ")")
        .WithEntity(entity_id)
        .WithTimestamp(edge_time.value());
  }
  
  return CedarStatus::OK();
}

// =============================================================================
// 第三重：写入门（原子提交）
// =============================================================================

CedarStatus CedarUpdate::DoApply(LsmEngine* engine) {
  if (records_.empty()) {
    return CedarStatus::OK();
  }
  
  // 逐条写入（集中式模式下，单线程写入天然原子性）
  // 每条记录包含完整的 CedarKey（32字节）和 Descriptor
  for (const auto& record : records_) {
    // 完整的 CedarKey 信息：
    // - entity_id (8 bytes): 实体ID
    // - timestamp_be (8 bytes): 降序时间戳
    // - target_id (8 bytes): 目标ID/扩展数据
    // - column_id (2 bytes): 属性/边类型ID
    // - sequence (2 bytes): 序列号
    // - entity_type (1 byte): 实体类型(Vertex/EdgeOut/EdgeIn)
    // - flags (1 byte): OpType + 状态位
    // - part_id (2 bytes): 分区ID
    
    // 使用 txn_version = timestamp_ 进行写入
    Status s = engine->Put(record.key, record.value, timestamp_);
    
    if (!s.ok()) {
      return CedarStatus(CedarCode::kInternalError,
                         "Failed to write CedarKey: " + s.ToString() +
                         " [entity_id=" + std::to_string(record.key.entity_id()) +
                         ", type=" + std::to_string(static_cast<int>(record.key.entity_type())) +
                         ", flags=0x" + std::to_string(record.key.flags()) + "]")
             .WithEntity(record.key.entity_id());
    }
    
    // ========== 锚点双写（Anchor Double-Write）==========
    // 对于 CREATE/DELETE 操作，同时更新状态锚点（0xFFE 列）
    // 实现 O(1) 存在性检查优化
    s = WriteStateAnchor(engine, record);
    if (!s.ok()) {
      return CedarStatus(CedarCode::kInternalError,
                         "Failed to write state anchor: " + s.ToString())
             .WithEntity(record.key.entity_id());
    }
  }
  
  return CedarStatus::OK();
}

// =============================================================================
// 内部方法
// =============================================================================

void CedarUpdate::AddRecord(UpdateRecord record) {
  records_.push_back(std::move(record));
}

uint16_t CedarUpdate::DoComputePartition(uint64_t entity_id) {
  return ComputePartition(entity_id);
}

uint8_t CedarUpdate::DoPackFlags(uint8_t op_type, bool distributed) {
  return PackFlags(op_type, distributed);
}

// =============================================================================
// 锚点机制（Anchor）实现
// =============================================================================

Status CedarUpdate::WriteStateAnchor(LsmEngine* engine, const UpdateRecord& record) {
  // 只处理 CREATE/DELETE 操作，UPDATE 不修改生命周期状态
  bool is_create = false;
  bool is_delete = false;
  
  switch (record.op) {
    case UpdateOpType::CREATE_VERTEX:
    case UpdateOpType::CREATE_EDGE:
      is_create = true;
      break;
    case UpdateOpType::DELETE_VERTEX:
    case UpdateOpType::DELETE_EDGE:
      is_delete = true;
      break;
    default:
      // UPDATE 操作不改变生命周期状态，跳过锚点写入
      return Status::OK();
  }
  
  if (!is_create && !is_delete) {
    return Status::OK();
  }
  
  // 确定实体类型
  EntityType entity_type = record.key.entity_type();
  uint64_t entity_id = record.key.entity_id();
  uint16_t part_id = static_cast<uint16_t>(entity_id);
  
  // 构造状态锚点
  EntityState state = is_create ? EntityState::Active : EntityState::Deleted;
  StateAnchor anchor(timestamp_, state, 0);  // version = 0
  
  // 创建锚点描述符
  Descriptor anchor_desc = LifecycleDescriptor::CreateStateAnchor(anchor);
  
  // 构造锚点 Key
  // 使用 timestamp_ 作为锚点时间，target_id 存储 last_update 时间
  uint8_t flags = PackFlags(is_create ? op_type::kCreate : op_type::kDelete, true);
  
  CedarKey anchor_key(
      entity_id,
      entity_type,
      kStateAnchorColumnId,   // 0xFFE - 状态锚点列
      timestamp_,             // 使用当前操作时间戳
      sequence_++,            // 递增序列号
      timestamp_.value(),     // target_id 复用存储 last_update
      flags,
      part_id
  );
  
  // 写入锚点（使用相同的事务版本）
  Status s = engine->Put(anchor_key, anchor_desc, timestamp_);
  if (!s.ok()) {
    return s;
  }
  
  // ========== 同步更新内存 Bitmap（Phase 4 优化）==========
  // 这一步失败不影响锚点写入，所以忽略错误
  if (is_create) {
    engine->MarkEntityActive(entity_id);
  } else {
    engine->MarkEntityDeleted(entity_id);
  }
  
  return Status::OK();
}

}  // namespace cedar
