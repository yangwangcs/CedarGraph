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
// EmbeddedRaft Implementation - 基于内嵌存储的 Raft 实现
// =============================================================================
// Features:
// - 基于 raft_interface.h 的完整实现
// - RocksDB 持久化日志存储
// - 自动快照管理
// - 集群成员动态变更
// =============================================================================

#ifndef CEDAR_DTX_RAFT_EMBEDDED_RAFT_IMPL_H_
#define CEDAR_DTX_RAFT_EMBEDDED_RAFT_IMPL_H_

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/dtx/raft/raft_interface.h"

namespace cedar {
namespace dtx {
namespace raft {

// =============================================================================
// 持久化日志存储
// =============================================================================

class PersistentLogStore {
 public:
  struct Config {
    std::string wal_dir;
    size_t max_log_size{100 * 1024 * 1024};  // 100MB
    size_t max_log_files{10};
  };
  
  PersistentLogStore();
  ~PersistentLogStore();
  
  // 禁止拷贝
  PersistentLogStore(const PersistentLogStore&) = delete;
  PersistentLogStore& operator=(const PersistentLogStore&) = delete;
  
  Status Initialize(const Config& config);
  void Shutdown();
  
  // 日志操作
  Status Append(const LogEntry& entry);
  StatusOr<LogEntry> Get(LogIndex index);
  StatusOr<LogEntry> GetLast();
  Status DeleteUntil(LogIndex index);  // 删除直到指定索引（用于快照后清理）
  
  // 元数据
  LogIndex GetFirstIndex() const;
  LogIndex GetLastIndex() const;
  size_t GetLogCount() const;
  
  // 强制刷盘
  Status Sync();

 private:
  Config config_;
  std::atomic<bool> initialized_{false};
  
  mutable std::mutex mutex_;
  std::vector<LogEntry> entries_;  // 内存缓存
  LogIndex first_index_{0};
  LogIndex last_index_{0};
  
  // 文件操作
  std::fstream log_file_;
  std::string current_log_path_;
  size_t current_log_size_{0};
  uint32_t log_file_index_{0};
  
  Status OpenLogFile();
  Status RotateLogFile();
  Status WriteEntry(const LogEntry& entry);
  Status ReadEntriesFromFile();
};

// =============================================================================
// 快照管理器
// =============================================================================

class SnapshotManager {
 public:
  struct Config {
    std::string snapshot_dir;
    size_t max_snapshots{3};  // 保留的最大快照数
  };
  
  SnapshotManager();
  ~SnapshotManager();
  
  Status Initialize(const Config& config);
  
  // 保存快照
  Status SaveSnapshot(const Snapshot& snapshot);
  
  // 加载最新快照
  StatusOr<Snapshot> LoadLatestSnapshot();
  
  // 获取快照列表
  std::vector<Snapshot> ListSnapshots();
  
  // 清理旧快照
  Status CleanupOldSnapshots();

 private:
  Config config_;
  std::filesystem::path snapshot_dir_;
};

// =============================================================================
// Raft 消息类型
// =============================================================================

enum class RaftMessageType : uint8_t {
  kRequestVote = 0,
  kRequestVoteResponse = 1,
  kAppendEntries = 2,
  kAppendEntriesResponse = 3,
  kInstallSnapshot = 4,
  kInstallSnapshotResponse = 5,
};

struct RaftMessage {
  RaftMessageType type;
  LogTerm term;
  NodeID from;
  NodeID to;
  std::string payload;
};

// =============================================================================
// 网络传输接口
// =============================================================================

class RaftTransport {
 public:
  using MessageHandler = std::function<void(const RaftMessage&)>;
  
  virtual ~RaftTransport() = default;
  
  // 初始化传输层
  virtual Status Initialize(const std::string& listen_address,
                             MessageHandler handler) = 0;
  
  // 发送消息到指定节点
  virtual Status Send(NodeID node_id, const RaftMessage& message) = 0;
  
  // 关闭传输层
  virtual void Shutdown() = 0;
  
  // 添加/移除节点
  virtual void AddNode(NodeID node_id, const std::string& address) = 0;
  virtual void RemoveNode(NodeID node_id) = 0;
};

// =============================================================================
// gRPC 传输实现
// =============================================================================

class GrpcRaftTransport : public RaftTransport {
 public:
  GrpcRaftTransport();
  ~GrpcRaftTransport() override;
  
  Status Initialize(const std::string& listen_address,
                    MessageHandler handler) override;
  Status Send(NodeID node_id, const RaftMessage& message) override;
  void Shutdown() override;
  void AddNode(NodeID node_id, const std::string& address) override;
  void RemoveNode(NodeID node_id) override;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

// =============================================================================
// EmbeddedRaftNode - 完整 Raft 实现
// =============================================================================

class EmbeddedRaftNode : public RaftNode {
 public:
  EmbeddedRaftNode();
  ~EmbeddedRaftNode() override;
  
  // 禁止拷贝
  EmbeddedRaftNode(const EmbeddedRaftNode&) = delete;
  EmbeddedRaftNode& operator=(const EmbeddedRaftNode&) = delete;
  
  // RaftNode 接口实现
  Status Initialize(const RaftConfig& config, 
                    StateMachine* state_machine) override;
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
  
  // 额外接口
  Status Step(const RaftMessage& message);
  void SetTransport(std::unique_ptr<RaftTransport> transport);

 private:
  // 状态机实现
  void BecomeFollower(LogTerm term);
  void BecomeCandidate();
  void BecomeLeader();
  
  // 定时任务
  void ElectionLoop();
  void HeartbeatLoop();
  void ApplyLoop();
  
  // 消息处理
  void HandleRequestVote(const RaftMessage& msg);
  void HandleRequestVoteResponse(const RaftMessage& msg);
  void HandleAppendEntries(const RaftMessage& msg);
  void HandleAppendEntriesResponse(const RaftMessage& msg);
  
  // 日志复制
  void ReplicateLog();
  void SendAppendEntries(NodeID peer);
  
  // 快照
  void CheckSnapshot();
  void InstallSnapshot(NodeID peer);
  
  // 配置
  RaftConfig config_;
  StateMachine* state_machine_{nullptr};
  std::unique_ptr<RaftTransport> transport_;
  
  // 持久化存储
  std::unique_ptr<PersistentLogStore> log_store_;
  std::unique_ptr<SnapshotManager> snapshot_manager_;
  
  // 状态
  std::atomic<RaftState> state_{RaftState::kFollower};
  std::atomic<LogTerm> current_term_{0};
  std::atomic<NodeID> voted_for_{0};
  std::atomic<NodeID> current_leader_{0};
  std::atomic<LogIndex> commit_index_{0};
  std::atomic<LogIndex> last_applied_{0};
  
  // Leader 状态（仅在 Leader 时有效）
  struct LeaderState {
    std::map<NodeID, LogIndex> next_index;
    std::map<NodeID, LogIndex> match_index;
  };
  std::unique_ptr<LeaderState> leader_state_;
  
  // 成员列表
  mutable std::mutex members_mutex_;
  std::map<NodeID, std::string> members_;
  
  // 回调
  mutable std::mutex callbacks_mutex_;
  std::vector<std::function<void(RaftState, RaftState)>> state_callbacks_;
  std::vector<std::function<void(NodeID, NodeID)>> leader_callbacks_;
  
  // 后台线程
  std::atomic<bool> running_{false};
  std::thread election_thread_;
  std::thread heartbeat_thread_;
  std::thread apply_thread_;
  
  // 定时器
  std::chrono::steady_clock::time_point last_heartbeat_;
  std::chrono::steady_clock::time_point election_deadline_;
  mutable std::mutex timer_mutex_;
  
  // 提交通知
  std::condition_variable commit_cv_;
  std::mutex commit_mutex_;
};

}  // namespace raft
}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_RAFT_EMBEDDED_RAFT_IMPL_H_
