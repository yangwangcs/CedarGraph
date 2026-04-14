// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Compaction Merger implementation

#include "cedar/storage/compaction_merger.h"

#include <algorithm>
#include <numeric>

#include "cedar/types/cedar_key.h"

#include "cedar/sst/zone_columnar_format.h"
#include "cedar/sst/zone_columnar_builder.h"
#include "cedar/core/env.h"
#include "cedar/storage/entity_lifecycle.h"

namespace cedar {

CompactionMerger::CompactionMerger(const std::vector<SstReader*>& readers,
                                   uint8_t entity_type,
                                   uint16_t column_id)
    : entity_type_(entity_type), column_id_(column_id) {
  
  // 为每个 reader 创建迭代器
  for (auto* reader : readers) {
    auto iter = std::unique_ptr<SstReader::Iterator>(reader->NewIterator());
    iter->SeekToFirst();
    iterators_.push_back(std::move(iter));
  }
  
  InitHeap();
}

CompactionMerger::~CompactionMerger() = default;

void CompactionMerger::InitHeap() {
  for (size_t i = 0; i < iterators_.size(); ++i) {
    if (iterators_[i]->Valid()) {
      MergeHeapItem item;
      item.key = iterators_[i]->Key();
      item.value = iterators_[i]->Value();
      item.source_idx = i;
      heap_.push(item);
      stats_.input_entries++;
    }
  }
}

bool CompactionMerger::IsDuplicate(const CedarKey& a, const CedarKey& b) const {
  // 判断是否是同一实体的同一版本
  // 考虑 entity_id, timestamp, target_id
  if (a.entity_id() != b.entity_id()) return false;
  if (a.timestamp().value() != b.timestamp().value()) return false;
  if (a.target_id() != b.target_id()) return false;
  return true;
}

bool CompactionMerger::CanDropTombstone(const CedarKey& key) const {
  // L0-L2: 保留 tombstone（可能有旧快照读）
  if (output_level_ < 3) {
    return false;
  }
  
  // L3+: 可以安全删除 tombstone（假设没有读快照依赖）
  // TODO: 检查是否有读快照依赖该版本
  // 注意：is_tombstone (bit 7) 是物理墓碑标记，仅由 Compaction 设置
  // 业务 DELETE (delta_op=10, bit 0-1) 不应触发物理删除
  return key.IsTombstone();  // 使用 kTombstone (bit 7 = 0x80)
}

std::unique_ptr<ZoneSstMeta> CompactionMerger::Run(const std::string& output_path,
                                                   const std::string& db_path) {
  if (heap_.empty()) {
    return nullptr;
  }
  
  // 创建输出文件
  Env* env = Env::Default();
  WritableFile* file = nullptr;
  Status s = env->NewWritableFile(output_path, &file);
  if (!s.ok()) {
    return nullptr;
  }
  
  // 创建 builder
  SstBuilder::Options options;
  options.db_path = db_path;
  SstBuilder builder(options, file);
  
  // 归并循环
  CedarKey last_key;
  bool has_last_key = false;
  
  while (!heap_.empty()) {
    auto item = heap_.top();
    heap_.pop();
    
    // 去重：如果和上一个 key 相同，跳过（保留最新的）
    if (has_last_key && IsDuplicate(item.key, last_key)) {
      stats_.dropped_duplicates++;
      
      // 推进该迭代器
      size_t src_idx = item.source_idx;
      iterators_[src_idx]->Next();
      if (iterators_[src_idx]->Valid()) {
        MergeHeapItem new_item;
        new_item.key = iterators_[src_idx]->Key();
        new_item.value = iterators_[src_idx]->Value();
        new_item.source_idx = src_idx;
        heap_.push(new_item);
      }
      continue;
    }
    
    // 检查 tombstone
    if (CanDropTombstone(item.key)) {
      stats_.dropped_tombstones++;
      
      // 推进该迭代器
      size_t src_idx = item.source_idx;
      iterators_[src_idx]->Next();
      if (iterators_[src_idx]->Valid()) {
        MergeHeapItem new_item;
        new_item.key = iterators_[src_idx]->Key();
        new_item.value = iterators_[src_idx]->Value();
        new_item.source_idx = src_idx;
        heap_.push(new_item);
      }
      continue;
    }
    
    // 写入 builder
    builder.Add(item.key, item.value);
    stats_.output_entries++;
    
    // 收集生命周期事件（用于生成区间锚点）
    if (column_id_ == kLifecycleColumnId) {
      CollectLifecycleEvent(item.key.entity_id(), item.key);
    }
    
    last_key = item.key;
    has_last_key = true;
    
    // 推进该迭代器
    size_t src_idx = item.source_idx;
    iterators_[src_idx]->Next();
    if (iterators_[src_idx]->Valid()) {
      MergeHeapItem new_item;
      new_item.key = iterators_[src_idx]->Key();
      new_item.value = iterators_[src_idx]->Value();
      new_item.source_idx = src_idx;
      heap_.push(new_item);
    }
  }
  
  // 生成区间锚点（0xFFD 列）
  if (!lifecycle_events_.empty()) {
    Status anchor_status = GenerateIntervalAnchors(builder);
    if (!anchor_status.ok()) {
      // 区间锚点生成失败不影响主流程，仅记录
      // TODO: 添加日志
    }
  }
  
  // 完成 builder
  s = builder.Finish();
  delete file;
  
  if (!s.ok()) {
    env->RemoveFile(output_path);
    return nullptr;
  }
  
  // 获取文件大小
  uint64_t file_size = 0;
  s = env->GetFileSize(output_path, &file_size);
  if (!s.ok()) {
    return nullptr;
  }
  
  // 创建元数据
  auto meta = std::make_unique<ZoneSstMeta>();
  meta->file_number = 0;  // 由调用者设置
  meta->file_size = file_size;
  meta->num_entries = stats_.output_entries;
  meta->level = output_level_;
  meta->column_id = column_id_;
  meta->entity_type = entity_type_;
  meta->path = output_path;
  
  // 计算压缩率
  if (stats_.input_entries > 0) {
    // 简化计算，实际应该比较字节数
    stats_.compression_ratio = static_cast<double>(stats_.output_entries) / 
                               static_cast<double>(stats_.input_entries);
  }
  
  return meta;
}

void CompactionMerger::CollectLifecycleEvent(uint64_t entity_id, const CedarKey& key) {
  // 解析操作类型
  bool is_create = key.IsCreate();
  bool is_delete = key.IsDelete();
  
  if (!is_create && !is_delete) {
    return;  // 只关心 Create/Delete 事件
  }
  
  lifecycle_events_[entity_id].push_back({
    key.timestamp(),
    is_create
  });
}

Status CompactionMerger::GenerateIntervalAnchors(SstBuilder& builder) {
  // 为每个实体生成区间锚点
  for (const auto& [entity_id, events] : lifecycle_events_) {
    if (events.empty()) continue;
    
    // 按时间排序
    std::vector<LifecycleEvent> sorted_events = events;
    std::sort(sorted_events.begin(), sorted_events.end(),
              [](const auto& a, const auto& b) { return a.time < b.time; });
    
    // 计算存活区间
    // 简化实现：取最早的 create 和最晚的 delete
    // 实际应该处理多次 create/delete 的情况
    Timestamp first_create = Timestamp::Max();
    Timestamp last_delete = Timestamp(0);
    bool has_create = false;
    bool has_delete = false;
    
    for (const auto& event : sorted_events) {
      if (event.is_create) {
        if (event.time < first_create) {
          first_create = event.time;
        }
        has_create = true;
      } else {
        if (event.time > last_delete) {
          last_delete = event.time;
        }
        has_delete = true;
      }
    }
    
    if (!has_create) continue;  // 没有创建事件，无法确定区间
    
    // 构造区间锚点
    // 使用 target_id 存储区间信息：高32位=start，低32位=end（简化）
    // 或者使用 Descriptor 序列化区间列表
    
    // 构造区间锚点 Key（0xFFD 列）
    uint16_t part_id = static_cast<uint16_t>(entity_id);
    uint8_t flags = 0;
    
    // 使用 target_id 编码区间：高32位=start，低32位=end
    uint64_t interval_data = (first_create.value() << 32) | 
                             (has_delete ? last_delete.value() : 0xFFFFFFFF);
    
    CedarKey interval_key(
        entity_id,
        static_cast<EntityType>(entity_type_),
        kIntervalAnchorColumnId,  // 0xFFD
        Timestamp::Max(),         // 使用 Max 时间戳便于 Seek
        0,                        // sequence
        interval_data,            // target_id 存储区间
        flags,
        part_id
    );
    
    // 创建区间锚点描述符（可以存储更详细的区间列表）
    // 简化实现：使用 InlineInt 存储区间数量
    Descriptor interval_desc = Descriptor::InlineInt(
        kIntervalAnchorColumnId, 
        static_cast<int32_t>(sorted_events.size())
    );
    
    builder.Add(interval_key, interval_desc);
    // builder.Add 返回 void，无法检查错误
  }
  
  return Status::OK();
}

}  // namespace cedar
