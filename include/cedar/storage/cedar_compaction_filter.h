//===----------------------------------------------------------------------===//
// CedarGraph Compaction Filter
// 
// 自定义 Compaction 过滤器，实现：
// 1. 只有明确标记 is_tombstone 且超出全局最小保留时间的记录才能删除
// 2. 支持冷热分离：旧数据推送到冷存储
// 3. 保留所有历史版本（全历史保存策略）
//===----------------------------------------------------------------------===//

#ifndef CEDAR_COMPACTION_FILTER_H
#define CEDAR_COMPACTION_FILTER_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

namespace cedar {

// Compaction 决策结果
enum class CompactionDecision {
  kKeep,           // 保留在热存储
  kRemove,         // 永久删除
  kMoveToCold,     // 移动到冷存储
  kKeepAsTombstone // 保留墓碑（用于一致性）
};

// 冷存储接口
class ColdStorage {
 public:
  virtual ~ColdStorage() = default;
  
  // 将数据写入冷存储
  virtual bool Write(const CedarKey& key, const Descriptor& descriptor) = 0;
  
  // 从冷存储读取
  virtual std::optional<Descriptor> Read(const CedarKey& key) = 0;
  
  // 检查数据是否在冷存储中
  virtual bool Exists(const CedarKey& key) const = 0;
};

// CedarGraph Compaction Filter
// 
// 策略说明：
// 1. 全历史保存：普通版本永不删除，除非被显式删除
// 2. 墓碑清理：只有 is_tombstone 标记的记录在超出保留期后才可删除
// 3. 冷热分离：超过 retention_period 的旧数据移至冷存储
class CedarCompactionFilter {
 public:
  struct Options {
    // 最小保留时间（微秒），默认：永不删除（max）
    // 用于墓碑清理：只有超过此时间的墓碑才能被删除
    uint64_t min_retention_us = std::numeric_limits<uint64_t>::max();
    
    // 冷热分离阈值（微秒），默认：不进行冷热分离（max）
    // 超过此时间的数据会被移动到冷存储
    uint64_t cold_storage_threshold_us = std::numeric_limits<uint64_t>::max();
    
    // 当前时间（用于判断保留期）
    uint64_t current_time_us = 0;
    
    // 是否启用冷存储
    bool enable_cold_storage = false;
    
    // GC safe point: Raft logical timestamp.
    // When set (>0), tombstones with timestamps >= gc_safe_point are NEVER removed.
    // This replaces wall-clock-based retention in multi-replica setups.
    uint64_t gc_safe_point = 0;
  };
  
  explicit CedarCompactionFilter(const Options& options);
  
  // 主过滤接口
  // 
  // @param key CedarKey（32字节）
  // @param descriptor 值描述符
  // @return CompactionDecision 处理决策
  CompactionDecision Filter(const CedarKey& key, const Descriptor& descriptor);
  
  // 批量过滤接口（优化性能）
  void FilterBatch(const std::vector<std::pair<CedarKey, Descriptor>>& entries,
                   std::vector<CompactionDecision>* decisions);
  
  // 设置冷存储后端
  void SetColdStorage(std::shared_ptr<ColdStorage> cold_storage);
  
  // 获取统计信息
  struct Stats {
    uint64_t total_filtered = 0;
    uint64_t kept_in_hot = 0;
    uint64_t moved_to_cold = 0;
    uint64_t removed = 0;
    uint64_t tombstones_retained = 0;
  };
  Stats GetStats() const { return stats_; }
  void ResetStats() { stats_ = Stats(); }

 private:
  Options options_;
  std::shared_ptr<ColdStorage> cold_storage_;
  Stats stats_;
  
  // 判断是否应该移动到冷存储
  bool ShouldMoveToCold(const CedarKey& key) const;
  
  // 判断是否应该删除（墓碑清理）
  bool ShouldRemoveTombstone(const CedarKey& key) const;
};

// 全局最小保留时间管理器
// 
// 用于管理集群级别的最小保留时间，确保跨节点一致性
class GlobalRetentionManager {
 public:
  static GlobalRetentionManager& Instance();
  
  // 获取全局最小保留时间（微秒）
  uint64_t GetMinRetentionUs() const;
  
  // 设置全局最小保留时间
  void SetMinRetentionUs(uint64_t retention_us);
  
  // 获取全局冷热分离阈值
  uint64_t GetColdStorageThresholdUs() const;
  
  // 设置全局冷热分离阈值
  void SetColdStorageThresholdUs(uint64_t threshold_us);

 private:
  GlobalRetentionManager() = default;
  
  std::atomic<uint64_t> min_retention_us_{std::numeric_limits<uint64_t>::max()};
  std::atomic<uint64_t> cold_storage_threshold_us_{std::numeric_limits<uint64_t>::max()};
};

}  // namespace cedar

#endif  // CEDAR_COMPACTION_FILTER_H
