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
// DVC-Val: Distributed Version-Chain Validation
// =============================================================================
// 核心思想：利用版本链实现 O(1) 冲突检测
// =============================================================================

#ifndef CEDAR_DTX_VERSION_CHAIN_H_
#define CEDAR_DTX_VERSION_CHAIN_H_

#include <cstdint>
#include <vector>
#include <memory>
#include <atomic>
#include <unordered_map>
#include <shared_mutex>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"
#include "cedar/types/cedar_key.h"

namespace cedar {
namespace dtx {

// 前向声明
class DTxRpcClient;
struct DistributedTxnContext;

/**
 * @brief 版本信息（用于跨分片传输）
 */
struct VersionInfo {
  CedarKey key;
  uint64_t version{0};
  Timestamp commit_ts{0};
  bool visible{false};
};

/**
 * @brief 版本链节点
 * 
 * 存储在内存中，形成单向链表（从新到旧）
 */
struct VersionChainNode {
  uint64_t txn_id{0};           // 创建此版本的事务ID
  Timestamp commit_ts{0};       // 提交时间戳
  uint64_t version{0};          // 版本号（单调递增）
  
  // 序列化数据位置（用于延迟加载）
  uint64_t wal_offset{0};       // WAL中的位置
  uint32_t data_size{0};        // 数据大小
  
  // 链表指针
  std::atomic<VersionChainNode*> next{nullptr};  // 下一个版本（更旧）
  
  // 可见性（提交后才可见）
  std::atomic<bool> visible{false};
  
  // 引用计数（用于GC）
  std::atomic<uint32_t> ref_count{0};
  
  // 构造函数
  VersionChainNode(uint64_t tid, Timestamp ts, uint64_t ver)
      : txn_id(tid), commit_ts(ts), version(ver) {}
  
  // 增加引用
  void AddRef() { ref_count.fetch_add(1, std::memory_order_relaxed); }
  
  // 释放引用
  void Release() {
    if (ref_count.fetch_sub(1, std::memory_order_release) == 1) {
      delete this;
    }
  }
};

/**
 * @brief 版本链头
 * 
 * 每个Key一个，包含最新版本指针
 */
struct VersionChainHead {
  // 最新版本（原子操作保证线程安全）
  std::atomic<VersionChainNode*> latest{nullptr};
  
  // 当前读取者数量（用于GC决策）
  std::atomic<uint64_t> reader_count{0};
  
  // 版本链长度（用于GC决策）
  std::atomic<uint32_t> chain_length{0};
  
  // 保护链表结构修改的互斥锁（读者用 shared_lock，GC 用 unique_lock）
  mutable std::shared_mutex chain_mutex_;

  ~VersionChainHead();
  
  // 获取最新可见版本
  VersionChainNode* GetLatestVisible() const;
  
  // 获取指定版本
  VersionChainNode* GetVersion(uint64_t version) const;
  
  // 插入新版本（CAS操作）
  bool InsertVersion(VersionChainNode* new_node);
  
  // 增加读取者计数
  void EnterRead() { reader_count.fetch_add(1, std::memory_order_relaxed); }
  void ExitRead() { reader_count.fetch_sub(1, std::memory_order_relaxed); }
};

/**
 * @brief 版本链索引
 * 
 * 每个存储分片维护的本地索引
 */
class VersionChainIndex {
 public:
  VersionChainIndex();
  ~VersionChainIndex();
  
  // 禁止拷贝
  VersionChainIndex(const VersionChainIndex&) = delete;
  VersionChainIndex& operator=(const VersionChainIndex&) = delete;
  
  // ==================== 版本操作 ====================
  
  // 获取或创建版本链头
  VersionChainHead* GetOrCreateHead(const CedarKey& key);
  
  // 获取版本链头（如果不存在返回nullptr）
  VersionChainHead* GetHead(const CedarKey& key) const;
  
  // 读取特定版本
  Status ReadVersion(const CedarKey& key, 
                      uint64_t version,
                      VersionChainNode** node);
  
  // 读取最新可见版本
  Status ReadLatestVisible(const CedarKey& key,
                            VersionChainNode** node);
  
  // 提交新版本
  Status CommitVersion(const CedarKey& key,
                        uint64_t txn_id,
                        Timestamp commit_ts,
                        uint64_t version);
  
  // ==================== 验证接口 (DVC-Val核心) ====================
  
  /**
   * @brief O(1)快速验证（核心优化）
   * 
   * 只检查版本链头部，避免遍历
   * 
   * @param key 要验证的Key
   * @param read_version 事务读取时的版本
   * @param commit_ts 事务计划提交时间戳
   * @return ValidationResult 验证结果
   */
  ValidationResult FastValidate(const CedarKey& key,
                                 uint64_t read_version,
                                 Timestamp commit_ts);
  
  /**
   * @brief 完整验证（必要时回退）
   * 
   * 当FastValidate无法确定时，遍历版本链
   */
  ValidationResult FullValidate(const CedarKey& key,
                                 uint64_t read_version,
                                 Timestamp commit_ts);
  
  /**
   * @brief 批量验证
   * 
   * 并行验证多个Key
   */
  std::vector<std::pair<CedarKey, ValidationResult>> BatchValidate(
      const std::vector<std::pair<CedarKey, uint64_t>>& read_set,
      Timestamp commit_ts);
  
  // ==================== 统计 ====================
  
  size_t GetKeyCount() const;
  size_t GetTotalVersionCount() const;
  
  // ==================== GC ====================
  
  // 运行垃圾回收（删除旧版本）
  void RunGC(Timestamp global_safe_ts);
  
  // 获取GC统计
  struct GCStats {
    uint64_t versions_removed{0};
    uint64_t bytes_freed{0};
    Timestamp safe_timestamp{0};
  };
  GCStats GetGCStats() const;
  
 private:
  // Key -> 版本链头映射
  mutable std::shared_mutex index_mutex_;
  std::unordered_map<CedarKey, std::unique_ptr<VersionChainHead>, CedarKeyHash> index_;
  
  // GC统计
  std::atomic<uint64_t> gc_versions_removed_{0};
  std::atomic<uint64_t> gc_bytes_freed_{0};
  
  // O(1)验证逻辑
  ValidationResult ValidateWithHead(VersionChainHead* head,
                                     uint64_t read_version,
                                     Timestamp commit_ts);
};

/**
 * @brief 跨分片版本链视图
 * 
 * 协调器使用，聚合多个分片的版本信息
 */
class CrossShardVersionView {
 public:
  // 添加分片的版本信息
  void AddShardView(PartitionID pid, 
                     const std::vector<VersionInfo>& versions);
  
  // 全局验证：检查所有读集是否仍然有效
  bool GlobalValidate(const std::vector<std::pair<CedarKey, uint64_t>>& read_set,
                       Timestamp commit_ts);
  
  // 计算全局安全时间戳（用于GC）
  Timestamp ComputeGlobalSafeTimestamp() const;
  
  // 清除视图
  void Clear();
  
 private:
  mutable std::mutex mutex_;
  std::unordered_map<PartitionID, std::vector<VersionInfo>> shard_views_;
};

/**
 * @brief 分布式验证协调器
 * 
 * 协调者角色：收集各分片验证结果，做出全局决策
 */
class DistributedValidationCoordinator {
 public:
  explicit DistributedValidationCoordinator(DTxRpcClient* rpc_client);
  
  /**
   * @brief 执行跨分片验证
   * 
   * 流程：
   * 1. 向各参与分片发送验证请求
   * 2. 收集验证结果
   * 3. 汇总决策
   */
  ValidationResult CoordinateValidation(
      const DistributedTxnContext& ctx);
  
  /**
   * @brief 处理分片验证请求（分片端）
   */
  ValidationResult HandleValidationRequest(
      VersionChainIndex* index,
      const std::vector<std::pair<CedarKey, uint64_t>>& read_set,
      Timestamp commit_ts);
  
  /**
   * @brief 注册本地分片的版本链索引
   */
  void RegisterPartitionIndex(PartitionID pid, VersionChainIndex* index);
  
 private:
  /**
   * @brief 验证单个分片
   */
  ValidationResult ValidatePartition(const DistributedTxnContext& ctx,
                                      PartitionID partition_id);
  
  DTxRpcClient* rpc_client_{nullptr};
  
  // 本地分片索引映射
  std::unordered_map<PartitionID, VersionChainIndex*> partition_indices_;
  
  // 超时配置
  std::chrono::milliseconds validation_timeout_{100};
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_VERSION_CHAIN_H_
