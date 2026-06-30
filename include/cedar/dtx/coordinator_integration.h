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
// DTx Coordinator Integration - 协调器与 MetaD 集成
// =============================================================================

#ifndef CEDAR_DTX_COORDINATOR_INTEGRATION_H_
#define CEDAR_DTX_COORDINATOR_INTEGRATION_H_

#include <memory>
#include <unordered_map>
#include <shared_mutex>
#include <condition_variable>

#include "cedar/core/status.h"
#include "cedar/types/descriptor.h"
#include "cedar/dtx/types.h"
#include "cedar/dtx/meta_service.h"
#include "cedar/dtx/txn_context.h"
#include "cedar/dtx/lsm_native_occ.h"

namespace cedar {
namespace dtx {

// =============================================================================
// 分区路由缓存
// =============================================================================

/**
 * @brief 分区路由信息
 */
struct PartitionRoute {
    PartitionID partition_id;
    NodeID leader_node;
    std::string leader_address;  // gRPC 地址
    uint64_t version{0};
    std::chrono::steady_clock::time_point cached_at;
    
    bool IsExpired(uint64_t ttl_sec = 60) const {
        auto now = std::chrono::steady_clock::now();
        return std::chrono::duration_cast<std::chrono::seconds>(
            now - cached_at).count() > static_cast<int64_t>(ttl_sec);
    }
};

/**
 * @brief 分区路由缓存
 * 
 * 本地缓存分区路由信息，减少对 MetaD 的查询
 */
class PartitionRouteCache {
public:
    PartitionRouteCache();
    ~PartitionRouteCache() = default;
    
    // 获取路由（带缓存）
    StatusOr<PartitionRoute> GetRoute(const std::string& space_name, 
                                       PartitionID partition_id);
    
    // 更新路由
    void UpdateRoute(const std::string& space_name, 
                     const PartitionRoute& route);
    
    // 使缓存失效（当收到 Watch 通知时）
    void Invalidate(const std::string& space_name, PartitionID partition_id);
    void InvalidateSpace(const std::string& space_name);
    void InvalidateAll();
    
    // 预加载 Space 的所有分区路由
    Status PreloadSpace(const std::string& space_name, 
                        MetaServiceClient* meta_client);

private:
    struct SpaceCache {
        std::unordered_map<PartitionID, PartitionRoute> routes;
        uint64_t version{0};
    };
    
    mutable std::shared_mutex cache_mutex_;
    std::unordered_map<std::string, SpaceCache> cache_;
};

// =============================================================================
// 提交结果
// =============================================================================

struct CommitResult {
    bool success{false};
    uint64_t commit_ts{0};
    std::string error_msg;
};

// =============================================================================
// 分区连接池
// =============================================================================

/**
 * @brief StorageD 连接
 */
struct StorageNodeConnection {
    NodeID node_id;
    std::string address;
    // std::unique_ptr<StorageService::Stub> stub;  // 实际使用时启用
    std::chrono::steady_clock::time_point last_used;
    std::atomic<bool> healthy_{true};
};

/**
 * @brief StorageD 连接池
 * 
 * 管理与 StorageD 节点的连接
 */
class StorageConnectionPool {
public:
    StorageConnectionPool();
    ~StorageConnectionPool();
    
    // 获取或创建连接
    // StatusOr<StorageService::Stub*> GetConnection(NodeID node_id, 
    //                                                const std::string& address);
    
    // 标记连接为不健康
    void MarkUnhealthy(NodeID node_id);
    
    // 关闭所有连接
    void CloseAll();
    
    // 获取健康检查统计
    struct HealthStats {
        size_t total_connections{0};
        size_t healthy_connections{0};
        size_t unhealthy_connections{0};
    };
    HealthStats GetHealthStats() const;

private:
    // 健康检查循环
    void HealthCheckLoop();
    
    mutable std::shared_mutex pool_mutex_;
    std::unordered_map<NodeID, std::unique_ptr<StorageNodeConnection>> connections_;
    
    std::atomic<bool> running_{false};
    std::thread health_check_thread_;
    std::condition_variable health_check_cv_;
    std::mutex health_check_cv_mutex_;
};

// =============================================================================
// 集成协调器
// =============================================================================

/**
 * @brief 集成配置
 */
struct IntegratedCoordinatorConfig {
    // MetaD 配置
    std::vector<std::string> meta_addresses;
    std::string space_name{"default"};
    uint64_t route_cache_ttl_sec{60};
    bool preload_all_routes{true};
    
    // 超时配置
    std::chrono::milliseconds rpc_timeout{5000};
    std::chrono::milliseconds connect_timeout{3000};
};

/**
 * @brief 集成协调器
 * 
 * 将 DTx 协调器与 MetaD 集成，提供完整的分布式事务能力
 */
class IntegratedCoordinator {
public:
    IntegratedCoordinator();
    ~IntegratedCoordinator();
    
    // 初始化
    Status Initialize(const IntegratedCoordinatorConfig& config);
    
    // 关闭
    Status Shutdown();
    
    // ===== 事务接口 =====
    
    // 开始事务
    StatusOr<TxnID> BeginTransaction(const DistributedTxnOptions& options);
    
    // 执行读操作 - 自动路由到正确的 Partition Leader
    StatusOr<Descriptor> Read(TxnID txn_id, const CedarKey& key);
    
    // 执行写操作（缓冲）
    Status Write(TxnID txn_id, const CedarKey& key, const Descriptor& value);
    
    // 提交事务
    StatusOr<CommitResult> Commit(TxnID txn_id);
    
    // 回滚事务
    Status Abort(TxnID txn_id);
    
    // ===== 路由接口 =====
    
    // 获取 Key 的路由信息
    StatusOr<PartitionRoute> GetKeyRoute(const CedarKey& key);
    
    // 批量获取路由
    std::vector<PartitionRoute> GetKeysRoutes(const std::vector<CedarKey>& keys);
    
    // ===== 管理接口 =====
    
    // 刷新路由缓存
    void RefreshRouteCache();
    
    // 获取缓存统计
    struct CacheStats {
        size_t cached_spaces{0};
        size_t cached_partitions{0};
        size_t cache_hits{0};
        size_t cache_misses{0};
    };
    CacheStats GetCacheStats() const;

private:
    // 初始化 MetaD 客户端
    Status InitializeMetaClient();
    
    // 初始化路由缓存
    Status InitializeRouteCache();
    
    // 处理分区变更通知
    void OnPartitionChange(const PartitionMapChange& change);
    
    // 获取事务上下文
    DistributedTxnContext* GetTxnContext(TxnID txn_id);
    
    // 路由 Key 到分区
    StatusOr<PartitionID> RouteKeyToPartition(const CedarKey& key);
    
    IntegratedCoordinatorConfig config_;
    
    // MetaD 客户端
    std::unique_ptr<MetaServiceClient> meta_client_;
    
    // 路由缓存
    PartitionRouteCache route_cache_;
    
    // StorageD 连接池
    StorageConnectionPool connection_pool_;
    
    // 基础协调器（Layer 1/2/3 提交）
    std::unique_ptr<LndOccEngine> lnd_engine_;
    
    // 活跃事务表
    mutable std::shared_mutex txns_mutex_;
    std::unordered_map<TxnID, std::unique_ptr<DistributedTxnContext>> active_txns_;
    std::atomic<TxnID> next_txn_id_{1};
    
    // 统计
    mutable std::atomic<size_t> cache_hits_{0};
    mutable std::atomic<size_t> cache_misses_{0};
    
    bool initialized_{false};
};

// =============================================================================
// 使用示例
// =============================================================================

/**
 * @brief 快速开始示例
 * 
 * ```cpp
 * // 1. 启动 MetaD（在 3 个节点上）
 * MetadataService meta_service;
 * meta_service.Initialize(config);
 * MetaServiceGrpcServer grpc_server;
 * grpc_server.Start("0.0.0.0:2379", &meta_service);
 * 
 * // 2. 客户端使用集成协调器
 * IntegratedCoordinator coordinator;
 * IntegratedCoordinatorConfig config;
 * config.meta_addresses = {"10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"};
 * coordinator.Initialize(config);
 * 
 * // 3. 执行事务
 * auto txn_id = coordinator.BeginTransaction(options);
 * coordinator.Write(txn_id, key1, value1);
 * coordinator.Write(txn_id, key2, value2);
 * auto result = coordinator.Commit(txn_id);
 * ```
 */

} // namespace dtx
} // namespace cedar

#endif // CEDAR_DTX_COORDINATOR_INTEGRATION_H_
