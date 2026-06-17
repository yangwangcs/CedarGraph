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
// Meta Service - 元数据服务（参考 NebulaGraph MetaD）
// =============================================================================

#ifndef CEDAR_DTX_META_SERVICE_H_
#define CEDAR_DTX_META_SERVICE_H_

#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <functional>
#include <chrono>
#include <shared_mutex>
#include <thread>
#include <mutex>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"
#include "cedar/dtx/raft/braft_node.h"

// Forward declaration for braft integration
struct RaftCommand;

namespace cedar {
namespace dtx {

// Forward declaration (defined in meta_service_impl.h)
struct MetaCommand;

// Inline raft types (migrated from deleted raft_interface.h)
using LogIndex = uint64_t;
using LogTerm = uint64_t;

struct LogEntry {
    LogTerm term{0};
    LogIndex index{0};
    std::string data;
    LogEntry() = default;
    LogEntry(LogTerm t, LogIndex i, std::string d)
        : term(t), index(i), data(std::move(d)) {}
};

struct Snapshot {
    LogIndex last_included_index{0};
    LogTerm last_included_term{0};
    std::string data;
    bool IsEmpty() const { return data.empty(); }
};

class StateMachine {
public:
    virtual ~StateMachine() = default;
    virtual void Apply(const LogEntry& entry) = 0;
    virtual Snapshot CreateSnapshot() = 0;
    virtual Status RestoreSnapshot(const Snapshot& snapshot) = 0;
    virtual LogIndex GetLastAppliedIndex() const = 0;
};

// =============================================================================
// 基础类型定义
// =============================================================================

/**
 * @brief Space（图空间）定义 - 类似数据库
 */
struct SpaceDef {
    std::string name;
    uint32_t partition_num{128};
    uint32_t replica_factor{3};
    std::chrono::system_clock::time_point created_at;
    
    std::string Serialize() const;
    static StatusOr<SpaceDef> Deserialize(const std::string& data);
};

// =============================================================================
// 分区分配 - 核心数据结构
// =============================================================================

/**
 * @brief 分区分配信息
 */
struct PartitionAssignment {
    PartitionID partition_id{kInvalidPartitionID};
    std::string space_name;
    NodeID leader_node{kInvalidNodeID};
    std::vector<NodeID> follower_nodes;
    uint64_t version{0};
    std::chrono::system_clock::time_point last_updated;
    
    enum class State : uint8_t {
        kNormal = 0,
        kMigrating = 1,
        kOffline = 2,
    };
    State state{State::kNormal};
    
    bool IsValid() const {
        return partition_id != kInvalidPartitionID && leader_node != kInvalidNodeID;
    }
    
    bool IsLeaderOn(NodeID node_id) const {
        return leader_node == node_id;
    }
    
    bool IsReplicaOn(NodeID node_id) const {
        if (leader_node == node_id) return true;
        for (auto& nid : follower_nodes) {
            if (nid == node_id) return true;
        }
        return false;
    }
    
    std::string Serialize() const;
    static StatusOr<PartitionAssignment> Deserialize(const std::string& data);
};

/**
 * @brief Space 的分区映射
 */
struct SpacePartitionMap {
    std::string space_name;
    uint32_t num_partitions{0};
    uint32_t replication_factor{0};
    std::unordered_map<PartitionID, PartitionAssignment> assignments;
    uint64_t version{0};
    std::chrono::system_clock::time_point updated_at;
    
    PartitionID GetPartitionForKey(const CedarKey& key) const;
    NodeID GetLeader(PartitionID pid) const;
    StatusOr<PartitionAssignment> GetAssignment(PartitionID pid) const;
    
    std::string Serialize() const;
    static StatusOr<SpacePartitionMap> Deserialize(const std::string& data);
};

// ============================================================================
// Schema Types
// ============================================================================

struct PropertyDef {
    std::string name;
    std::string type;
    bool nullable = true;
    bool indexed = false;

    std::string Serialize() const;
    static StatusOr<PropertyDef> Deserialize(const std::string& data);
};

struct LabelSchema {
    std::string name;
    std::vector<PropertyDef> properties;
    std::vector<std::string> indexes;

    std::string Serialize() const;
    static StatusOr<LabelSchema> Deserialize(const std::string& data);
};

struct IndexDef {
    std::string name;
    std::string label_name;
    std::vector<std::string> properties;
    std::string space_name;
    bool unique = false;
};

// =============================================================================
// 节点管理
// =============================================================================

/**
 * @brief 节点信息
 */
struct NodeInfo {
    NodeID node_id{kInvalidNodeID};
    std::string address;
    std::string data_path;
    uint32_t num_cpu_cores{0};
    uint64_t total_memory_bytes{0};
    uint64_t total_disk_bytes{0};
    std::chrono::system_clock::time_point registered_at;
    std::chrono::system_clock::time_point last_heartbeat;
    
    enum class State : uint8_t {
        kOnline = 0,
        kOffline = 1,
        kSuspected = 2,
    };
    State state{State::kOnline};
    
    bool IsOnline() const { return state == State::kOnline; }
    
    std::string Serialize() const;
    static StatusOr<NodeInfo> Deserialize(const std::string& data);
};

/**
 * @brief 节点状态（心跳上报）
 */
struct NodeStatus {
    NodeID node_id{kInvalidNodeID};
    double cpu_usage_percent{0.0};
    double memory_usage_percent{0.0};
    double disk_usage_percent{0.0};
    uint64_t qps{0};
    uint64_t latency_ms{0};
    std::vector<PartitionID> leader_partitions;
    std::vector<PartitionID> follower_partitions;
    std::chrono::system_clock::time_point timestamp;
    
    std::string Serialize() const;
    static StatusOr<NodeStatus> Deserialize(const std::string& data);
};

// =============================================================================
// 变更通知
// =============================================================================

enum class PartitionChangeType : uint8_t {
    kLeaderChanged = 0,
    kReplicaAdded = 1,
    kReplicaRemoved = 2,
    kPartitionMigrated = 3,
};

struct PartitionMapChange {
    std::string space_name;
    PartitionID partition_id{kInvalidPartitionID};
    PartitionChangeType change_type;
    NodeID old_leader{kInvalidNodeID};
    NodeID new_leader{kInvalidNodeID};
    uint64_t version{0};
    std::chrono::system_clock::time_point timestamp;
};

enum class NodeChangeType : uint8_t {
    kNodeJoined = 0,
    kNodeLeft = 1,
    kNodeSuspected = 2,
    kNodeRecovered = 3,
};

struct NodeChange {
    NodeID node_id{kInvalidNodeID};
    NodeChangeType change_type;
    NodeInfo node_info;
    std::chrono::system_clock::time_point timestamp;
};

// =============================================================================
// 元数据服务配置
// =============================================================================

struct MetaServiceConfig {
    // 节点标识
    NodeID node_id{0};
    std::string listen_address{"0.0.0.0:2379"};
    std::string advertise_address;
    
    // Raft 配置
    std::vector<std::pair<NodeID, std::string>> peers;
    uint64_t election_timeout_ms{1000};
    uint64_t heartbeat_interval_ms{100};
    
    // 存储路径
    std::string data_dir{"./meta_data"};
    
    // 心跳检测
    uint64_t heartbeat_timeout_sec{10};
    uint64_t heartbeat_check_interval_sec{5};

    // 测试模式：跳过 braft 初始化，直接操作状态机
    bool test_mode{false};
};

// =============================================================================
// 元数据服务 - 核心类
// =============================================================================

class MetadataService {
public:
    MetadataService();
    ~MetadataService();
    
    // 禁止拷贝
    MetadataService(const MetadataService&) = delete;
    MetadataService& operator=(const MetadataService&) = delete;
    
    // 初始化并启动
    Status Initialize(const MetaServiceConfig& config);
    
    // 关闭
    Status Shutdown();
    
    // ===== Schema 管理 =====
    
    Status CreateSpace(const SpaceDef& space);
    Status DropSpace(const std::string& space_name);
    StatusOr<SpaceDef> GetSpace(const std::string& space_name) const;
    std::vector<std::string> ListSpaces() const;
    
    // ===== 分区管理（核心）=====
    
    StatusOr<PartitionAssignment> GetPartitionAssignment(
        const std::string& space_name, PartitionID partition_id) const;
    
    StatusOr<std::vector<PartitionAssignment>> GetPartitionAssignments(
        const std::string& space_name, const std::vector<PartitionID>& partitions) const;
    
    StatusOr<SpacePartitionMap> GetSpacePartitionMap(const std::string& space_name) const;
    
    // 更新分区 Leader（由 StorageD 上报）
    Status UpdatePartitionLeader(const std::string& space_name,
                                  PartitionID partition_id,
                                  NodeID new_leader);

    // 更新完整分区 Assignment（由迁移协调器调用）
    Status UpdatePartitionAssignment(const PartitionAssignment& assignment);
    
    // ===== 节点管理 =====
    
    Status RegisterNode(const NodeInfo& info);
    virtual Status Heartbeat(const NodeStatus& status);
    StatusOr<NodeInfo> GetNode(NodeID node_id) const;
    std::vector<NodeInfo> GetAliveNodes() const;
    std::vector<NodeInfo> GetAllNodes() const;
    
    // ===== Schema 管理 =====
    
    Status CreateLabelSchema(const std::string& space_name, const LabelSchema& schema);
    std::vector<LabelSchema> GetSchema(const std::string& space_name,
                                        const std::vector<std::string>& labels) const;
    
    // ===== 索引管理 =====
    
    Status CreateIndex(const std::string& space_name, const IndexDef& index);
    Status DropIndex(const std::string& space_name, const std::string& index_name);
    std::vector<IndexDef> ListIndexes(const std::string& space_name,
                                       const std::string& label_name = "") const;
    
    // ===== 订阅/通知 =====
    
    using PartitionChangeCallback = std::function<void(const PartitionMapChange&)>;
    using NodeChangeCallback = std::function<void(const NodeChange&)>;
    
    void WatchPartitionMap(const std::string& space_name, PartitionChangeCallback callback);
    void WatchNodes(NodeChangeCallback callback);
    
    // ===== 内部状态 =====
    
    bool IsLeader() const;
    NodeID GetLeader() const;
    std::string GetLeaderAddress() const;
    
    // ===== braft 集成接口 =====
    
    // 应用 Raft 命令（由 StateMachine 调用）
    bool ApplyRaftCommand(const struct RaftCommand& cmd);
    
    // 序列化/反序列化状态（用于快照）
    std::string SerializeState() const;
    bool DeserializeState(const std::string& data);
    
    // 领导力变更回调
    void OnBecomeLeader();
    void OnStepDown();
    
private:
    // Raft 状态机实现
    class MetadataStateMachine : public StateMachine {
    public:
        void Apply(const LogEntry& entry) override;
        Snapshot CreateSnapshot() override;
        Status RestoreSnapshot(const Snapshot& snapshot) override;
        LogIndex GetLastAppliedIndex() const override;
        
        // 内部方法（由 MetadataService 调用）
        void ApplyCommand(const MetaCommand& cmd);
        void ApplyCreateSpace(const SpaceDef& space);
        void ApplyDropSpace(const std::string& space_name);
        void ApplyRegisterNode(const NodeInfo& info);
        void ApplyUpdateNodeStatus(const NodeStatus& status);
        std::pair<uint64_t, NodeID> ApplyUpdatePartitionLeader(
            const std::string& space_name,
            PartitionID partition_id,
            NodeID new_leader);
        std::pair<uint64_t, NodeID> ApplyUpdatePartitionAssignment(
            const PartitionAssignment& assignment);
        
        // 查询方法
        StatusOr<SpaceDef> GetSpace(const std::string& name) const;
        StatusOr<PartitionAssignment> GetPartitionAssignment(
            const std::string& space_name, PartitionID pid) const;
        StatusOr<SpacePartitionMap> GetSpacePartitionMap(
            const std::string& space_name) const;
        StatusOr<NodeInfo> GetNode(NodeID node_id) const;
        std::vector<NodeInfo> GetAliveNodes(uint64_t timeout_sec) const;
        std::vector<std::string> ListSpaces() const;
        std::vector<NodeInfo> GetAllNodes() const;
        
        // Schema
        Status CreateLabelSchema(const std::string& space_name, const LabelSchema& schema);
        std::vector<LabelSchema> GetSchema(const std::string& space_name,
                                            const std::vector<std::string>& labels) const;
        
        // Indexes
        Status CreateIndex(const std::string& space_name, const IndexDef& index);
        Status DropIndex(const std::string& space_name, const std::string& index_name);
        std::vector<IndexDef> ListIndexes(const std::string& space_name,
                                           const std::string& label_name) const;
        
        // Heartbeat / failure detection
        std::vector<NodeID> CheckNodeHeartbeats(uint64_t timeout_sec) const;
        bool MarkNodeOffline(NodeID node_id);
        
        // 序列化/反序列化
        std::string Serialize() const;
        Status Deserialize(const std::string& data);
        
    private:
        mutable std::shared_mutex mutex_;
        
        // Spaces
        std::unordered_map<std::string, SpaceDef> spaces_;
        
        // Schema: space_name -> label_name -> LabelSchema
        std::unordered_map<std::string, std::unordered_map<std::string, LabelSchema>> schemas_;
        
        // Indexes: space_name -> index_name -> IndexDef
        std::unordered_map<std::string, std::unordered_map<std::string, IndexDef>> indexes_;
        
        // 分区映射
        std::unordered_map<std::string, SpacePartitionMap> partition_maps_;
        
        // 节点信息
        std::unordered_map<NodeID, NodeInfo> nodes_;
        std::unordered_map<NodeID, NodeStatus> node_statuses_;
        
        std::atomic<LogIndex> last_applied_index_{0};
    };
    
    // 心跳检查线程
    void HeartbeatCheckLoop();
    
    // 通知客户端
    void NotifyPartitionChange(const PartitionMapChange& change);
    void NotifyPartitionLeaderChange(const std::string& space_name, PartitionID pid,
                                      NodeID old_leader, NodeID new_leader, uint64_t version);
    void NotifyNodeChange(const NodeChange& change);
    
    MetaServiceConfig config_;
    
    std::unique_ptr<BRaftNode> raft_node_;
    MetadataStateMachine state_machine_;
    
    // 回调列表
    std::mutex callbacks_mutex_;
    std::vector<std::pair<std::string, PartitionChangeCallback>> partition_callbacks_;
    std::vector<NodeChangeCallback> node_callbacks_;
    
    // 心跳检查
    std::atomic<bool> running_{false};
    std::thread heartbeat_thread_;
    std::mutex shutdown_mutex_;
    std::condition_variable shutdown_cv_;
    
    // Heartbeat rate limiting (token bucket per node)
    mutable std::mutex heartbeat_tokens_mutex_;
    std::unordered_map<NodeID, std::pair<std::chrono::steady_clock::time_point, uint32_t>> heartbeat_tokens_;
    static constexpr uint32_t kMaxHeartbeatsPerSecond = 10;

    std::atomic<bool> initialized_{false};
};

// =============================================================================
// MetaD 客户端
// =============================================================================

/**
 * @brief MetaD 客户端（供 StorageD 和 Client 使用）
 */
class MetaServiceClient {
public:
    MetaServiceClient();
    virtual ~MetaServiceClient();
    
    // 连接到 MetaD 集群
    virtual Status Connect(const std::vector<std::string>& meta_addresses) {
        (void)meta_addresses;
        return Status::OK();
    }
    
    // ===== 缓存接口（常用）=====
    
    // 获取分区 Leader（带本地缓存）
    virtual StatusOr<NodeID> GetPartitionLeader(const std::string& space_name, 
                                         PartitionID partition_id) {
        (void)space_name;
        (void)partition_id;
        return Status::NotSupported("GetPartitionLeader not implemented");
    }
    
    // 获取 Key 应该路由到的节点
    virtual StatusOr<NodeID> GetRouteForKey(const std::string& space_name, 
                                     const CedarKey& key) {
        (void)space_name;
        (void)key;
        return Status::NotSupported("GetRouteForKey not implemented");
    }
    
    // 刷新缓存
    virtual void RefreshCache(const std::string& space_name) {
        (void)space_name;
    }
    
    // ===== 直接查询接口 =====
    
    virtual StatusOr<PartitionAssignment> GetPartitionAssignment(
        const std::string& space_name, PartitionID partition_id) {
        (void)space_name;
        (void)partition_id;
        return Status::NotSupported("GetPartitionAssignment not implemented");
    }
    
    virtual StatusOr<SpacePartitionMap> GetSpacePartitionMap(const std::string& space_name) {
        (void)space_name;
        return Status::NotSupported("GetSpacePartitionMap not implemented");
    }
    
    virtual StatusOr<NodeInfo> GetNode(NodeID node_id) {
        (void)node_id;
        return Status::NotSupported("GetNode not implemented");
    }
    
    // ===== 节点操作（StorageD 使用）=====
    
    virtual Status RegisterNode(const NodeInfo& info) {
        (void)info;
        return Status::NotSupported("RegisterNode not implemented");
    }
    virtual Status Heartbeat(const NodeStatus& status) {
        (void)status;
        return Status::NotSupported("Heartbeat not implemented");
    }
    
    // ===== 订阅接口 =====
    
    virtual void WatchPartitionMap(const std::string& space_name,
                           std::function<void(const PartitionMapChange&)> callback) {
        (void)space_name;
        (void)callback;
    }
    
private:
    // 尝试连接到一个 MetaD 节点
    Status TryConnect(const std::string& address);
    
    // 获取当前 Leader
    StatusOr<std::string> GetCurrentLeader();
    
    // 处理变更通知
    void OnPartitionChange(const PartitionMapChange& change);
    
    std::vector<std::string> meta_addresses_;
    std::string current_leader_;
    mutable std::shared_mutex leader_mutex_;
    
    // 本地缓存
    struct CachedPartitionMap {
        SpacePartitionMap map;
        std::chrono::steady_clock::time_point cached_at;
        
        bool IsExpired(uint64_t ttl_sec = 60) const {
            auto now = std::chrono::steady_clock::now();
            return std::chrono::duration_cast<std::chrono::seconds>(
                now - cached_at).count() > ttl_sec;
        }
    };
    
    mutable std::shared_mutex cache_mutex_;
    std::unordered_map<std::string, CachedPartitionMap> partition_map_cache_;
    
    // gRPC stubs
    // std::unique_ptr<MetaService::Stub> stub_;  // 实际实现时使用
};

} // namespace dtx
} // namespace cedar

#endif // CEDAR_DTX_META_SERVICE_H_
