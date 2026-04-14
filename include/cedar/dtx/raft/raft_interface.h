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
// Raft Interface - 抽象 Raft 共识层
// =============================================================================

#ifndef CEDAR_DTX_RAFT_INTERFACE_H_
#define CEDAR_DTX_RAFT_INTERFACE_H_

#include <cstdint>
#include <string>
#include <vector>
#include <functional>
#include <memory>
#include <mutex>
#include <atomic>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"

namespace cedar {
namespace dtx {
namespace raft {

// 前向声明
class RaftNode;

// 日志条目类型
using LogIndex = uint64_t;
using LogTerm = uint64_t;
// NodeID is defined in cedar/dtx/types.h

/**
 * @brief 日志条目
 */
struct LogEntry {
    LogTerm term{0};
    LogIndex index{0};
    std::string data;  // 序列化的命令
    
    LogEntry() = default;
    LogEntry(LogTerm t, LogIndex i, std::string d)
        : term(t), index(i), data(std::move(d)) {}
};

/**
 * @brief 快照接口
 */
struct Snapshot {
    LogIndex last_included_index{0};
    LogTerm last_included_term{0};
    std::string data;  // 序列化的状态
    
    bool IsEmpty() const {
        return data.empty();
    }
};

/**
 * @brief Raft 状态机接口
 * 
 * 应用层需要实现此接口来处理已提交的日志条目
 */
class StateMachine {
public:
    virtual ~StateMachine() = default;
    
    // 应用已提交的日志条目（必须由实现类保证线程安全）
    virtual void Apply(const LogEntry& entry) = 0;
    
    // 创建快照
    virtual Snapshot CreateSnapshot() = 0;
    
    // 从快照恢复
    virtual Status RestoreSnapshot(const Snapshot& snapshot) = 0;
    
    // 获取最后应用的日志索引
    virtual LogIndex GetLastAppliedIndex() const = 0;
};

/**
 * @brief Raft 配置
 */
struct RaftConfig {
    // 本节点信息
    NodeID node_id{0};
    std::string listen_address;  // 如 "0.0.0.0:50051"
    std::string advertise_address;  // 如 "10.0.0.1:50051"
    
    // 集群成员
    std::vector<std::pair<NodeID, std::string>> peers;  // 其他节点列表
    
    // 超时配置（毫秒）
    uint64_t election_timeout_ms{1000};
    uint64_t heartbeat_interval_ms{100};
    
    // 快照配置
    uint64_t snapshot_interval_sec{3600};
    uint64_t max_log_entries{100000};
    
    // 存储路径
    std::string wal_dir;
    std::string snapshot_dir;
};

/**
 * @brief Raft 节点状态
 */
enum class RaftState : uint8_t {
    kFollower = 0,
    kCandidate = 1,
    kLeader = 2,
};

/**
 * @brief Raft 节点接口
 * 
 * 底层实现可以是 etcd/raft、braft 或自研
 */
class RaftNode {
public:
    virtual ~RaftNode() = default;
    
    // 初始化并启动 Raft 节点
    virtual Status Initialize(const RaftConfig& config, StateMachine* state_machine) = 0;
    
    // 关闭节点
    virtual Status Shutdown() = 0;
    
    // 提议条目（仅 Leader 能成功）
    // 返回提议的日志索引，失败返回错误
    virtual StatusOr<LogIndex> Propose(const std::string& data) = 0;
    
    // 检查是否是 Leader
    virtual bool IsLeader() const = 0;
    
    // 获取当前 Leader 的节点 ID
    virtual NodeID GetLeader() const = 0;
    
    // 获取当前状态
    virtual RaftState GetState() const = 0;
    
    // 获取当前任期
    virtual LogTerm GetTerm() const = 0;
    
    // 添加新节点到集群（需要 Leader 权限）
    virtual Status AddNode(NodeID node_id, const std::string& address) = 0;
    
    // 从集群移除节点（需要 Leader 权限）
    virtual Status RemoveNode(NodeID node_id) = 0;
    
    // 获取集群成员列表
    virtual std::vector<std::pair<NodeID, std::string>> GetMembers() const = 0;
    
    // 手动触发快照
    virtual Status TriggerSnapshot() = 0;
    
    // 注册状态变更回调
    virtual void RegisterStateCallback(
        std::function<void(RaftState old_state, RaftState new_state)> callback) = 0;
    
    // 注册 Leader 变更回调
    virtual void RegisterLeaderCallback(
        std::function<void(NodeID old_leader, NodeID new_leader)> callback) = 0;
};

/**
 * @brief Raft 工厂
 * 
 * 创建具体的 Raft 实现
 */
class RaftFactory {
public:
    // 创建 Raft 节点（使用默认实现）
    static std::unique_ptr<RaftNode> CreateNode();
    
    // 设置自定义实现
    static void RegisterImplementation(
        std::function<std::unique_ptr<RaftNode>()> factory);
};

// =============================================================================
// 简化版 Raft 实现（基于内存，用于测试）
// =============================================================================

/**
 * @brief 内存版 Raft（用于单元测试，不保证真正的共识）
 */
class MemoryRaftNode : public RaftNode {
public:
    Status Initialize(const RaftConfig& config, StateMachine* state_machine) override;
    Status Shutdown() override;
    StatusOr<LogIndex> Propose(const std::string& data) override;
    bool IsLeader() const override;
    NodeID GetLeader() const override;
    RaftState GetState() const override;
    LogTerm GetTerm() const override;
    Status AddNode(NodeID node_id, const std::string& address) override;
    Status RemoveNode(NodeID node_id) override;
    std::vector<std::pair<NodeID, std::string>> GetMembers() const override;
    Status TriggerSnapshot() override;
    void RegisterStateCallback(
        std::function<void(RaftState, RaftState)> callback) override;
    void RegisterLeaderCallback(
        std::function<void(NodeID, NodeID)> callback) override;

private:
    RaftConfig config_;
    StateMachine* state_machine_{nullptr};
    
    std::atomic<bool> is_leader_{true};
    std::atomic<NodeID> leader_id_{0};
    std::atomic<LogTerm> current_term_{1};
    std::atomic<LogIndex> next_index_{1};
    
    mutable std::mutex mutex_;
    std::vector<std::pair<NodeID, std::string>> members_;
    
    std::vector<std::function<void(RaftState, RaftState)>> state_callbacks_;
    std::vector<std::function<void(NodeID, NodeID)>> leader_callbacks_;
};

} // namespace raft
} // namespace dtx
} // namespace cedar

#endif // CEDAR_DTX_RAFT_INTERFACE_H_
