// Copyright (c) 2025 The Cedar Authors. All rights reserved.

#include "cedar/storage/skeleton_cache.h"
#include "cedar/storage/lsm_engine.h"

#include <algorithm>
#include <cstring>

namespace cedar {

// =============================================================================
// EdgeEntry12B Implementation
// =============================================================================

EdgeEntry12B::EdgeEntry12B(const CedarKey& key, uint32_t base_ts) : dst_id(0), packed_data(0) {
    dst_id = key.target_id();
    
    // 计算时间戳偏移
    uint64_t ts = key.timestamp().value();
    if (ts >= base_ts) {
        bits.timestamp_offset = static_cast<uint32_t>(ts - base_ts);
    } else {
        // 不应该发生：边时间早于基础时间
        bits.timestamp_offset = 0;
    }
    
    // Label ID：从 column_id 提取低 7 位
    bits.label_id = key.column_id() & 0x7F;
    
    // Flags
    bits.flags = 0;
    if (key.IsDelete()) {
        bits.flags |= kFlagDeleted;
    }
    if (key.entity_type() == EntityType::EdgeIn) {
        bits.flags |= kFlagDirection;
    }
    // has_more_props 需要外部判断（是否有属性列）
}

CedarKey EdgeEntry12B::ToCedarKey(uint64_t src_id, uint32_t base_ts) const {
    uint64_t ts = base_ts + bits.timestamp_offset;
    uint16_t col_id = static_cast<uint16_t>(bits.label_id);
    uint8_t flags = 0;
    
    if (bits.flags & kFlagDeleted) {
        flags |= 0x02;  // op_type::kDelete
    } else {
        flags |= 0x00;  // op_type::kCreate
    }
    
    if (bits.flags & kFlagDirection) {
        // EdgeIn
        return CedarKey::EdgeIn(src_id, dst_id, EdgeTypeId(col_id), 
                                Timestamp(ts), 0, 0, flags);
    } else {
        // EdgeOut
        return CedarKey::EdgeOut(src_id, dst_id, EdgeTypeId(col_id),
                                 Timestamp(ts), 0, 0, flags);
    }
}

// =============================================================================
// VertexSkeleton Implementation
// =============================================================================

VertexSkeleton::VertexSkeleton(VertexSkeleton&& other) noexcept
    : version(other.version.load()),
      base_timestamp(other.base_timestamp),
      reserved(other.reserved),
      status_hint(other.status_hint.load()),
      out_count(other.out_count),
      in_count(other.in_count),
      capacity(other.capacity),
      access_count(other.access_count),
      out_edges(other.out_edges),
      in_edges(other.in_edges) {
    // 转移指针所有权
    other.out_edges = nullptr;
    other.in_edges = nullptr;
    other.out_count = 0;
    other.in_count = 0;
    other.capacity = 0;
}

VertexSkeleton& VertexSkeleton::operator=(VertexSkeleton&& other) noexcept {
    if (this != &other) {
        // 释放现有资源
        if (out_edges) {
            delete[] out_edges;
        }
        if (in_edges) {
            delete[] in_edges;
        }
        
        // 转移数据
        version.store(other.version.load());
        base_timestamp = other.base_timestamp;
        reserved = other.reserved;
        status_hint.store(other.status_hint.load());
        out_count = other.out_count;
        in_count = other.in_count;
        capacity = other.capacity;
        access_count = other.access_count;
        out_edges = other.out_edges;
        in_edges = other.in_edges;
        
        // 清空源
        other.out_edges = nullptr;
        other.in_edges = nullptr;
        other.out_count = 0;
        other.in_count = 0;
        other.capacity = 0;
    }
    return *this;
}

VertexSkeleton::~VertexSkeleton() {
    // 释放边数据内存
    if (out_edges) {
        delete[] out_edges;
        out_edges = nullptr;
    }
    if (in_edges) {
        delete[] in_edges;
        in_edges = nullptr;
    }
}

// =============================================================================
// ShardedSkeletonCache Implementation
// =============================================================================

ShardedSkeletonCache::ShardedSkeletonCache(
    size_t num_shards, size_t max_entries_per_shard)
    : num_shards_(num_shards), max_entries_per_shard_(max_entries_per_shard) {
    
    shards_.reserve(num_shards);
    for (size_t i = 0; i < num_shards; ++i) {
        shards_.emplace_back(std::make_unique<Shard>());
    }
}

ShardedSkeletonCache::~ShardedSkeletonCache() {
    // Shard 的析构会自动清理资源
}

void ShardedSkeletonCache::Shard::EvictOldest(size_t target_size) {
    while (entries.size() > target_size && !lru_list.empty()) {
        // 淘汰最旧的（链表尾部）
        uint64_t oldest_id = lru_list.back();
        lru_list.pop_back();
        entries.erase(oldest_id);
        evictions++;
    }
}

std::pair<VertexSkeleton*, bool> ShardedSkeletonCache::Get(uint64_t vertex_id) {
    size_t shard_idx = ShardIndex(vertex_id);
    Shard& shard = *shards_[shard_idx];
    
    // 使用 shared_lock 允许多线程并发读
    std::shared_lock<std::shared_mutex> lock(shard.mutex);
    
    auto it = shard.entries.find(vertex_id);
    if (it != shard.entries.end()) {
        // 命中：只读访问，不修改 LRU 顺序（避免独占锁）
        auto& entry = it->second;
        entry.skeleton.access_count++;
        shard.hits++;
        return {&entry.skeleton, true};
    }
    
    shard.misses++;
    return {nullptr, false};
}

std::pair<VertexSkeleton*, bool> ShardedSkeletonCache::GetWithPromotion(uint64_t vertex_id) {
    size_t shard_idx = ShardIndex(vertex_id);
    Shard& shard = *shards_[shard_idx];
    
    // 需要修改 LRU 顺序，使用独占锁
    std::unique_lock<std::shared_mutex> lock(shard.mutex);
    
    auto it = shard.entries.find(vertex_id);
    if (it != shard.entries.end()) {
        // 命中：移动到 LRU 头部（最新访问）
        auto& entry = it->second;
        shard.lru_list.splice(shard.lru_list.begin(), shard.lru_list, entry.lru_iter);
        entry.skeleton.access_count++;
        shard.hits++;
        return {&entry.skeleton, true};
    }
    
    shard.misses++;
    return {nullptr, false};
}

// 线程安全的出边扫描（拷贝数据，无需保持锁）
std::pair<std::optional<std::vector<EdgeEntry12B>>, uint32_t> 
ShardedSkeletonCache::ScanOutEdgesSafe(uint64_t vertex_id) {
    
    size_t shard_idx = ShardIndex(vertex_id);
    Shard& shard = *shards_[shard_idx];
    
    // 使用 shared_lock 并发读
    std::shared_lock<std::shared_mutex> lock(shard.mutex);
    
    auto it = shard.entries.find(vertex_id);
    if (it == shard.entries.end() || it->second.skeleton.IsDeleted()) {
        shard.misses++;  // 更新统计
        return {std::nullopt, 0};  // 未命中或已删除
    }
    
    const VertexSkeleton& skeleton = it->second.skeleton;
    
    // 拷贝出边数据
    std::vector<EdgeEntry12B> results;
    if (skeleton.out_edges && skeleton.out_count > 0) {
        results.reserve(skeleton.out_count);
        for (uint32_t i = 0; i < skeleton.out_count; ++i) {
            results.push_back(skeleton.out_edges[i]);
        }
    }
    
    shard.hits++;  // 更新统计
    return {std::move(results), skeleton.base_timestamp};
}

void ShardedSkeletonCache::Put(uint64_t vertex_id, VertexSkeleton&& skeleton) {
    size_t shard_idx = ShardIndex(vertex_id);
    Shard& shard = *shards_[shard_idx];
    
    std::unique_lock<std::shared_mutex> lock(shard.mutex);
    
    auto it = shard.entries.find(vertex_id);
    if (it != shard.entries.end()) {
        // 已存在：更新并移动到头部
        auto& entry = it->second;
        entry.skeleton = std::move(skeleton);
        shard.lru_list.splice(shard.lru_list.begin(), shard.lru_list, entry.lru_iter);
    } else {
        // 新插入：检查容量，可能需要淘汰
        if (shard.entries.size() >= max_entries_per_shard_) {
            shard.EvictOldest(max_entries_per_shard_ - 1);
        }
        
        // 插入到 LRU 头部
        shard.lru_list.push_front(vertex_id);
        Shard::Entry entry;
        entry.lru_iter = shard.lru_list.begin();
        entry.skeleton = std::move(skeleton);
        shard.entries.emplace(vertex_id, std::move(entry));
        shard.insertions++;
    }
}

void ShardedSkeletonCache::MarkDeleted(uint64_t vertex_id) {
    size_t shard_idx = ShardIndex(vertex_id);
    Shard& shard = *shards_[shard_idx];
    
    std::unique_lock<std::shared_mutex> lock(shard.mutex);
    
    auto it = shard.entries.find(vertex_id);
    if (it != shard.entries.end()) {
        it->second.skeleton.SetStatus(2);  // Deleted
    }
}

void ShardedSkeletonCache::Invalidate(uint64_t vertex_id) {
    size_t shard_idx = ShardIndex(vertex_id);
    Shard& shard = *shards_[shard_idx];
    
    std::unique_lock<std::shared_mutex> lock(shard.mutex);
    
    auto it = shard.entries.find(vertex_id);
    if (it != shard.entries.end()) {
        shard.lru_list.erase(it->second.lru_iter);
        shard.entries.erase(it);
        total_invalidations_++;
    }
}

std::vector<std::pair<uint64_t, VertexSkeleton*>> ShardedSkeletonCache::BatchGet(
    const std::vector<uint64_t>& vertex_ids) {
    
    std::vector<std::pair<uint64_t, VertexSkeleton*>> results;
    results.reserve(vertex_ids.size());
    
    for (uint64_t id : vertex_ids) {
        auto [skeleton, hit] = Get(id);
        if (hit) {
            results.emplace_back(id, skeleton);
        }
    }
    
    return results;
}

ShardedSkeletonCache::Stats ShardedSkeletonCache::GetStats() const {
    Stats stats;
    for (const auto& shard_ptr : shards_) {
        const Shard& shard = *shard_ptr;
        std::shared_lock<std::shared_mutex> lock(shard.mutex);
        stats.hits += shard.hits;
        stats.misses += shard.misses;
        stats.evictions += shard.evictions;
        stats.insertions += shard.insertions;
    }
    stats.invalidations = total_invalidations_.load();
    return stats;
}

void ShardedSkeletonCache::ResetStats() {
    for (auto& shard_ptr : shards_) {
        Shard& shard = *shard_ptr;
        std::unique_lock<std::shared_mutex> lock(shard.mutex);
        shard.hits = 0;
        shard.misses = 0;
        shard.evictions = 0;
        shard.insertions = 0;
    }
    total_invalidations_ = 0;
}

size_t ShardedSkeletonCache::MemoryUsage() const {
    size_t total = 0;
    for (const auto& shard_ptr : shards_) {
        const Shard& shard = *shard_ptr;
        std::shared_lock<std::shared_mutex> lock(shard.mutex);
        
        // 计算骨架内存
        for (const auto& [id, entry] : shard.entries) {
            total += entry.skeleton.MemoryUsage();
        }
        
        // Hash 表开销（粗略估计）
        total += shard.entries.size() * 32;  // 每个 entry 约 32B 开销
    }
    
    // 分片数组本身
    total += shards_.size() * sizeof(Shard);
    
    return total;
}

void ShardedSkeletonCache::Clear() {
    for (auto& shard_ptr : shards_) {
        Shard& shard = *shard_ptr;
        std::unique_lock<std::shared_mutex> lock(shard.mutex);
        shard.entries.clear();
        shard.lru_list.clear();
    }
    total_invalidations_ = 0;
}

// =============================================================================
// Phase 5: 内存限制和预热实现
// =============================================================================

size_t ShardedSkeletonCache::EnforceMemoryLimit(size_t max_memory_bytes) {
    size_t current = CalculateMemoryUsage();
    if (current <= max_memory_bytes) {
        return 0;  // 不需要淘汰
    }
    
    size_t bytes_to_free = current - max_memory_bytes;
    size_t evicted_count = 0;
    
    // 计算需要淘汰的条目数（粗略估计）
    // 假设每个条目平均占用 100B
    size_t entries_to_evict = bytes_to_free / 100 + 1;
    
    // 从每个分片均匀淘汰
    size_t evict_per_shard = entries_to_evict / num_shards_ + 1;
    
    for (auto& shard_ptr : shards_) {
        Shard& shard = *shard_ptr;
        std::unique_lock<std::shared_mutex> lock(shard.mutex);
        
        size_t target = shard.entries.size() > evict_per_shard 
                        ? shard.entries.size() - evict_per_shard 
                        : 0;
        if (target < shard.entries.size()) {
            shard.EvictOldest(target);
            evicted_count += (shard.entries.size() - target);
        }
    }
    
    return evicted_count;
}

size_t ShardedSkeletonCache::CalculateMemoryUsage() const {
    size_t total = 0;
    
    // 分片数组本身
    total += shards_.size() * sizeof(Shard);
    
    for (const auto& shard_ptr : shards_) {
        const Shard& shard = *shard_ptr;
        std::shared_lock<std::shared_mutex> lock(shard.mutex);
        
        // LRU 链表开销
        total += shard.lru_list.size() * (sizeof(uint64_t) + sizeof(void*) * 2);
        
        // Hash 表开销
        total += shard.entries.bucket_count() * sizeof(void*);  // 桶数组
        
        // 条目开销
        for (const auto& [id, entry] : shard.entries) {
            // Entry 本身开销
            total += sizeof(id) + sizeof(entry);
            // VertexSkeleton 开销
            total += entry.skeleton.MemoryUsage();
        }
    }
    
    return total;
}

// =============================================================================
// SkeletonHydrator Implementation
// =============================================================================

Status SkeletonHydrator::HydrateVertex(
    uint64_t vertex_id,
    LsmEngine* engine,
    VertexSkeleton* out_skeleton) {
    
    if (!engine || !out_skeleton) {
        return Status::InvalidArgument("SkeletonHydrator", "null pointer");
    }
    
    // 1. 扫描出边
    auto out_entries = engine->ScanEdgesWithFolding(
        vertex_id, EntityType::EdgeOut, 0xFFFF, Timestamp::Max());
    
    // 2. 扫描入边
    auto in_entries = engine->ScanEdgesWithFolding(
        vertex_id, EntityType::EdgeIn, 0xFFFF, Timestamp::Max());
    
    if (out_entries.empty() && in_entries.empty()) {
        // 该节点无边数据
        out_skeleton->SetStatus(2);  // Deleted 或不存在
        return Status::NotFound("SkeletonHydrator", "no edges found");
    }
    
    // 3. 计算基础时间戳（取最早边的时间）
    uint64_t min_ts = UINT64_MAX;
    for (const auto& entry : out_entries) {
        min_ts = std::min(min_ts, entry.timestamp.value());
    }
    for (const auto& entry : in_entries) {
        min_ts = std::min(min_ts, entry.timestamp.value());
    }
    out_skeleton->base_timestamp = static_cast<uint32_t>(min_ts);
    
    // 4. 压缩出边
    if (!out_entries.empty()) {
        out_skeleton->out_count = out_entries.size();
        out_skeleton->out_edges = new EdgeEntry12B[out_entries.size()];
        
        for (size_t i = 0; i < out_entries.size(); ++i) {
            const auto& entry = out_entries[i];
            // 构造临时 CedarKey 用于压缩
            CedarKey key = CedarKey::EdgeOut(vertex_id, entry.target_id, 
                                              EdgeTypeId(entry.edge_type), entry.timestamp);
            out_skeleton->out_edges[i] = EdgeEntry12B(key, out_skeleton->base_timestamp);
        }
    }
    
    // 5. 压缩入边
    if (!in_entries.empty()) {
        out_skeleton->in_count = in_entries.size();
        out_skeleton->in_edges = new EdgeEntry12B[in_entries.size()];
        
        for (size_t i = 0; i < in_entries.size(); ++i) {
            const auto& entry = in_entries[i];
            CedarKey key = CedarKey::EdgeIn(vertex_id, entry.target_id, 
                                             EdgeTypeId(entry.edge_type), entry.timestamp);
            out_skeleton->in_edges[i] = EdgeEntry12B(key, out_skeleton->base_timestamp);
        }
    }
    
    out_skeleton->capacity = out_skeleton->out_count + out_skeleton->in_count;
    out_skeleton->SetStatus(1);  // Active
    out_skeleton->version.fetch_add(1);
    
    return Status::OK();
}

Status SkeletonHydrator::BatchHydrate(
    const std::vector<uint64_t>& vertex_ids,
    LsmEngine* engine,
    ShardedSkeletonCache* cache) {
    
    if (!engine || !cache) {
        return Status::InvalidArgument("SkeletonHydrator", "null pointer");
    }
    
    for (uint64_t vertex_id : vertex_ids) {
        // 检查是否已在缓存中
        auto [existing, hit] = cache->Get(vertex_id);
        if (hit) {
            continue;  // 已存在，跳过
        }
        
        // 物化
        VertexSkeleton skeleton;
        Status s = HydrateVertex(vertex_id, engine, &skeleton);
        if (s.ok()) {
            cache->Put(vertex_id, std::move(skeleton));
        }
        // 失败继续处理下一个
    }
    
    return Status::OK();
}

}  // namespace cedar
