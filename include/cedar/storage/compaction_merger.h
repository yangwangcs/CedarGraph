// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Compaction Merger - K-Way Merge for Zone-Columnar SST files

#ifndef CEDAR_STORAGE_COMPACTION_MERGER_H_
#define CEDAR_STORAGE_COMPACTION_MERGER_H_

#include <memory>
#include <queue>
#include <vector>
#include <string>

#include "cedar/sst/zone_columnar_format.h"
#include "cedar/sst/zone_columnar_reader.h"
#include "cedar/sst/sst_builder_factory.h"
#include "cedar/storage/size_tiered_compaction.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

namespace cedar {

// 归并堆项
struct MergeHeapItem {
  CedarKey key;
  Descriptor value;
  Timestamp txn_version;
  size_t source_idx;  // 来自哪个输入迭代器
  
  bool operator>(const MergeHeapItem& other) const {
    return key > other.key;  // 最小堆
  }
};

// Zone-Columnar Compaction 归并器
class CompactionMerger {
 public:
  // 输入：多个 SST 文件的 reader
  CompactionMerger(const std::vector<SstReader*>& readers,
                   uint8_t entity_type,
                   uint16_t column_id);
  
  ~CompactionMerger();
  
  // 执行归并，输出到新的 SST 文件
  // output_path: 输出文件路径
  // db_path: 用于 Blob 文件定位
  // 返回：输出文件的元数据，失败返回 nullptr
  std::unique_ptr<ZoneSstMeta> Run(const std::string& output_path,
                                   const std::string& db_path);
  
  // 获取统计信息
  struct Stats {
    size_t input_entries = 0;
    size_t output_entries = 0;
    size_t dropped_duplicates = 0;
    size_t dropped_tombstones = 0;
    double compression_ratio = 0.0;
  };
  
  Stats GetStats() const { return stats_; }
  
  // 设置输出层级（用于 tombstone 清理决策）
  void SetOutputLevel(int level) { output_level_ = level; }

 private:
  void InitHeap();
  bool IsDuplicate(const CedarKey& a, const CedarKey& b) const;
  bool CanDropTombstone(const CedarKey& key) const;
  
  // ========== 区间锚点生成 ==========
  // 收集实体生命周期事件
  void CollectLifecycleEvent(uint64_t entity_id, const CedarKey& key);
  // 生成区间锚点并写入 builder
  Status GenerateIntervalAnchors(SstBuilderInterface& builder);
  
  std::vector<std::unique_ptr<SstReader::Iterator>> iterators_;
  std::priority_queue<MergeHeapItem, std::vector<MergeHeapItem>, std::greater<>> heap_;
  
  uint8_t entity_type_;
  uint16_t column_id_;
  int output_level_ = 0;  // 输出层级（用于决定是否清理 tombstone）
  
  Stats stats_;
  
  // 实体生命周期跟踪（用于生成区间锚点）
  struct LifecycleEvent {
    Timestamp time;
    bool is_create;  // true=create, false=delete
  };
  std::unordered_map<uint64_t, std::vector<LifecycleEvent>> lifecycle_events_;
};

// =============================================================================
// Compaction Merger V2 - Zone-Synchronized K-Way Merge for SST V2
// =============================================================================
struct CompactionOptions {
  bool remove_tombstones = false;   // 是否移除墓碑
  size_t target_block_size = 64 * 1024;  // 目标块大小
  size_t max_output_file_size = 64 * 1024 * 1024;  // 最大输出文件大小 (64MB)
};

// Zone-Columnar SST V2 Compaction Merger
class CompactionMergerV2 {
 public:
  CompactionMergerV2(const CompactionOptions& options,
                     const std::vector<std::string>& input_files,
                     const std::string& output_path,
                     Env* fs);
  ~CompactionMergerV2();
  
  // 禁止拷贝
  CompactionMergerV2(const CompactionMergerV2&) = delete;
  CompactionMergerV2& operator=(const CompactionMergerV2&) = delete;
  
  // 执行归并
  std::unique_ptr<ZoneSstMeta> Run();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace cedar

#endif  // FERN_STORAGE_COMPACTION_MERGER_H_
