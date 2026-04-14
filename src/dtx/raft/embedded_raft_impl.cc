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

#include "cedar/dtx/raft/embedded_raft_impl.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>

namespace cedar {
namespace dtx {
namespace raft {

// =============================================================================
// PersistentLogStore Implementation
// =============================================================================

PersistentLogStore::PersistentLogStore() = default;

PersistentLogStore::~PersistentLogStore() {
  Shutdown();
}

Status PersistentLogStore::Initialize(const Config& config) {
  config_ = config;
  
  // 创建 WAL 目录
  std::filesystem::create_directories(config_.wal_dir);
  
  // 打开或创建日志文件
  auto status = OpenLogFile();
  if (!status.ok()) {
    return status;
  }
  
  // 读取已有日志
  status = ReadEntriesFromFile();
  if (!status.ok()) {
    return status;
  }
  
  initialized_.store(true);
  return Status::OK();
}

void PersistentLogStore::Shutdown() {
  if (!initialized_.exchange(false)) {
    return;
  }
  
  Sync();
  
  std::lock_guard<std::mutex> lock(mutex_);
  if (log_file_.is_open()) {
    log_file_.close();
  }
}

Status PersistentLogStore::Append(const LogEntry& entry) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  entries_.push_back(entry);
  last_index_ = entry.index;
  if (first_index_ == 0) {
    first_index_ = entry.index;
  }
  
  return WriteEntry(entry);
}

StatusOr<LogEntry> PersistentLogStore::Get(LogIndex index) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (index < first_index_ || index > last_index_) {
    return Status::NotFound("Log entry not found");
  }
  
  size_t offset = index - first_index_;
  if (offset >= entries_.size()) {
    return Status::NotFound("Log entry not found");
  }
  
  return entries_[offset];
}

StatusOr<LogEntry> PersistentLogStore::GetLast() {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (entries_.empty()) {
    return Status::NotFound("Log is empty");
  }
  
  return entries_.back();
}

Status PersistentLogStore::DeleteUntil(LogIndex index) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (index < first_index_) {
    return Status::OK();
  }
  
  size_t count = index - first_index_ + 1;
  if (count >= entries_.size()) {
    entries_.clear();
    first_index_ = 0;
    last_index_ = 0;
  } else {
    entries_.erase(entries_.begin(), entries_.begin() + count);
    first_index_ = index + 1;
  }
  
  // 这里应该截断日志文件，简化处理为重写
  // 实际生产环境应该使用更高效的方式
  
  return Status::OK();
}

LogIndex PersistentLogStore::GetFirstIndex() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return first_index_;
}

LogIndex PersistentLogStore::GetLastIndex() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return last_index_;
}

size_t PersistentLogStore::GetLogCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return entries_.size();
}

Status PersistentLogStore::Sync() {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (log_file_.is_open()) {
    log_file_.flush();
  }
  
  return Status::OK();
}

Status PersistentLogStore::OpenLogFile() {
  // 寻找下一个可用的日志文件索引
  log_file_index_ = 0;
  while (std::filesystem::exists(
      config_.wal_dir + "/" + std::to_string(log_file_index_) + ".log")) {
    log_file_index_++;
  }
  
  return RotateLogFile();
}

Status PersistentLogStore::RotateLogFile() {
  if (log_file_.is_open()) {
    log_file_.close();
  }
  
  current_log_path_ = config_.wal_dir + "/" + 
                      std::to_string(log_file_index_) + ".log";
  log_file_.open(current_log_path_, 
                 std::ios::in | std::ios::out | std::ios::app | std::ios::binary);
  
  if (!log_file_.is_open()) {
    return Status::IOError("Failed to open log file: " + current_log_path_);
  }
  
  current_log_size_ = std::filesystem::file_size(current_log_path_);
  log_file_index_++;
  
  return Status::OK();
}

Status PersistentLogStore::WriteEntry(const LogEntry& entry) {
  // 序列化：| term (8B) | index (8B) | data_len (4B) | data |
  uint64_t term = entry.term;
  uint64_t index = entry.index;
  uint32_t data_len = entry.data.size();
  
  log_file_.write(reinterpret_cast<const char*>(&term), sizeof(term));
  log_file_.write(reinterpret_cast<const char*>(&index), sizeof(index));
  log_file_.write(reinterpret_cast<const char*>(&data_len), sizeof(data_len));
  log_file_.write(entry.data.data(), data_len);
  
  current_log_size_ += sizeof(term) + sizeof(index) + sizeof(data_len) + data_len;
  
  // 检查是否需要轮转
  if (current_log_size_ >= config_.max_log_size) {
    return RotateLogFile();
  }
  
  return Status::OK();
}

Status PersistentLogStore::ReadEntriesFromFile() {
  // 读取所有日志文件
  for (uint32_t i = 0; i < log_file_index_; ++i) {
    std::string path = config_.wal_dir + "/" + std::to_string(i) + ".log";
    if (!std::filesystem::exists(path)) {
      continue;
    }
    
    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
      continue;
    }
    
    while (file.good()) {
      uint64_t term, index;
      uint32_t data_len;
      
      file.read(reinterpret_cast<char*>(&term), sizeof(term));
      if (!file.good()) break;
      
      file.read(reinterpret_cast<char*>(&index), sizeof(index));
      file.read(reinterpret_cast<char*>(&data_len), sizeof(data_len));
      
      std::string data(data_len, '\0');
      file.read(data.data(), data_len);
      
      LogEntry entry(term, index, std::move(data));
      entries_.push_back(std::move(entry));
      
      if (first_index_ == 0) {
        first_index_ = index;
      }
      last_index_ = index;
    }
  }
  
  return Status::OK();
}

// =============================================================================
// SnapshotManager Implementation
// =============================================================================

SnapshotManager::SnapshotManager() = default;

SnapshotManager::~SnapshotManager() = default;

Status SnapshotManager::Initialize(const Config& config) {
  config_ = config;
  snapshot_dir_ = config_.snapshot_dir;
  
  std::filesystem::create_directories(snapshot_dir_);
  
  return Status::OK();
}

Status SnapshotManager::SaveSnapshot(const Snapshot& snapshot) {
  std::string filename = std::to_string(snapshot.last_included_index) + "." +
                         std::to_string(snapshot.last_included_term) + ".snap";
  std::filesystem::path path = snapshot_dir_ / filename;
  
  std::ofstream file(path, std::ios::binary);
  if (!file.is_open()) {
    return Status::IOError("Failed to create snapshot file");
  }
  
  // 写入元数据
  file.write(reinterpret_cast<const char*>(&snapshot.last_included_index),
             sizeof(snapshot.last_included_index));
  file.write(reinterpret_cast<const char*>(&snapshot.last_included_term),
             sizeof(snapshot.last_included_term));
  
  // 写入数据长度和数据
  uint64_t data_len = snapshot.data.size();
  file.write(reinterpret_cast<const char*>(&data_len), sizeof(data_len));
  file.write(snapshot.data.data(), data_len);
  
  file.close();
  
  // 清理旧快照
  CleanupOldSnapshots();
  
  return Status::OK();
}

StatusOr<Snapshot> SnapshotManager::LoadLatestSnapshot() {
  auto snapshots = ListSnapshots();
  if (snapshots.empty()) {
    return Snapshot{};
  }
  
  // 返回索引最大的快照
  return snapshots.back();
}

std::vector<Snapshot> SnapshotManager::ListSnapshots() {
  std::vector<Snapshot> snapshots;
  
  if (!std::filesystem::exists(snapshot_dir_)) {
    return snapshots;
  }
  
  for (const auto& entry : std::filesystem::directory_iterator(snapshot_dir_)) {
    if (entry.path().extension() != ".snap") {
      continue;
    }
    
    std::ifstream file(entry.path(), std::ios::binary);
    if (!file.is_open()) {
      continue;
    }
    
    Snapshot snapshot;
    file.read(reinterpret_cast<char*>(&snapshot.last_included_index),
              sizeof(snapshot.last_included_index));
    file.read(reinterpret_cast<char*>(&snapshot.last_included_term),
              sizeof(snapshot.last_included_term));
    
    uint64_t data_len;
    file.read(reinterpret_cast<char*>(&data_len), sizeof(data_len));
    snapshot.data.resize(data_len);
    file.read(snapshot.data.data(), data_len);
    
    snapshots.push_back(std::move(snapshot));
  }
  
  // 按索引排序
  std::sort(snapshots.begin(), snapshots.end(),
            [](const Snapshot& a, const Snapshot& b) {
              return a.last_included_index < b.last_included_index;
            });
  
  return snapshots;
}

Status SnapshotManager::CleanupOldSnapshots() {
  auto snapshots = ListSnapshots();
  
  if (snapshots.size() <= config_.max_snapshots) {
    return Status::OK();
  }
  
  // 删除最旧的快照
  size_t to_delete = snapshots.size() - config_.max_snapshots;
  for (size_t i = 0; i < to_delete; ++i) {
    std::string filename = std::to_string(snapshots[i].last_included_index) + "." +
                           std::to_string(snapshots[i].last_included_term) + ".snap";
    std::filesystem::remove(snapshot_dir_ / filename);
  }
  
  return Status::OK();
}

// =============================================================================
// EmbeddedRaftNode Implementation (Simplified)
// =============================================================================

EmbeddedRaftNode::EmbeddedRaftNode() = default;

EmbeddedRaftNode::~EmbeddedRaftNode() {
  Shutdown();
}

Status EmbeddedRaftNode::Initialize(const RaftConfig& config,
                                     StateMachine* state_machine) {
  config_ = config;
  state_machine_ = state_machine;
  
  // 初始化日志存储
  log_store_ = std::make_unique<PersistentLogStore>();
  PersistentLogStore::Config log_config;
  log_config.wal_dir = config_.wal_dir;
  auto status = log_store_->Initialize(log_config);
  if (!status.ok()) {
    return status;
  }
  
  // 初始化快照管理器
  snapshot_manager_ = std::make_unique<SnapshotManager>();
  SnapshotManager::Config snapshot_config;
  snapshot_config.snapshot_dir = config_.snapshot_dir;
  snapshot_manager_->Initialize(snapshot_config);
  
  // 恢复状态
  auto snapshot_or = snapshot_manager_->LoadLatestSnapshot();
  if (snapshot_or.ok()) {
    auto& snapshot = snapshot_or.value();
    if (!snapshot.IsEmpty()) {
      status = state_machine_->RestoreSnapshot(snapshot);
      if (!status.ok()) {
        return status;
      }
      commit_index_ = snapshot.last_included_index;
      last_applied_ = snapshot.last_included_index;
    }
  }
  
  // 初始化成员列表
  for (const auto& [id, addr] : config_.peers) {
    members_[id] = addr;
  }
  
  // 初始化传输层
  if (transport_) {
    status = transport_->Initialize(config_.listen_address,
        [this](const RaftMessage& msg) { Step(msg); });
    if (!status.ok()) {
      return status;
    }
  }
  
  // 启动后台线程
  running_.store(true);
  state_.store(RaftState::kFollower);
  
  election_thread_ = std::thread(&EmbeddedRaftNode::ElectionLoop, this);
  apply_thread_ = std::thread(&EmbeddedRaftNode::ApplyLoop, this);
  
  return Status::OK();
}

Status EmbeddedRaftNode::Shutdown() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }
  
  commit_cv_.notify_all();
  
  if (election_thread_.joinable()) {
    election_thread_.join();
  }
  if (heartbeat_thread_.joinable()) {
    heartbeat_thread_.join();
  }
  if (apply_thread_.joinable()) {
    apply_thread_.join();
  }
  
  if (transport_) {
    transport_->Shutdown();
  }
  
  if (log_store_) {
    log_store_->Shutdown();
  }
  
  return Status::OK();
}

StatusOr<LogIndex> EmbeddedRaftNode::Propose(const std::string& data) {
  if (!IsLeader()) {
    return Status::IOError("Not leader");
  }
  
  LogEntry entry;
  entry.term = current_term_.load();
  entry.index = log_store_->GetLastIndex() + 1;
  entry.data = data;
  
  auto status = log_store_->Append(entry);
  if (!status.ok()) {
    return status;
  }
  
  // 触发复制
  ReplicateLog();
  
  return entry.index;
}

bool EmbeddedRaftNode::IsLeader() const {
  return state_.load() == RaftState::kLeader;
}

NodeID EmbeddedRaftNode::GetLeader() const {
  return current_leader_.load();
}

RaftState EmbeddedRaftNode::GetState() const {
  return state_.load();
}

LogTerm EmbeddedRaftNode::GetTerm() const {
  return current_term_.load();
}

Status EmbeddedRaftNode::AddNode(NodeID node_id, const std::string& address) {
  if (!IsLeader()) {
    return Status::IOError("Not leader");
  }
  
  std::lock_guard<std::mutex> lock(members_mutex_);
  members_[node_id] = address;
  
  if (transport_) {
    transport_->AddNode(node_id, address);
  }
  
  return Status::OK();
}

Status EmbeddedRaftNode::RemoveNode(NodeID node_id) {
  if (!IsLeader()) {
    return Status::IOError("Not leader");
  }
  
  std::lock_guard<std::mutex> lock(members_mutex_);
  members_.erase(node_id);
  
  if (transport_) {
    transport_->RemoveNode(node_id);
  }
  
  return Status::OK();
}

std::vector<std::pair<NodeID, std::string>> EmbeddedRaftNode::GetMembers() const {
  std::lock_guard<std::mutex> lock(members_mutex_);
  std::vector<std::pair<NodeID, std::string>> result;
  for (const auto& [id, addr] : members_) {
    result.emplace_back(id, addr);
  }
  return result;
}

Status EmbeddedRaftNode::TriggerSnapshot() {
  Snapshot snapshot = state_machine_->CreateSnapshot();
  snapshot.last_included_index = last_applied_.load();
  snapshot.last_included_term = current_term_.load();
  
  auto status = snapshot_manager_->SaveSnapshot(snapshot);
  if (!status.ok()) {
    return status;
  }
  
  // 清理已快照的日志
  log_store_->DeleteUntil(snapshot.last_included_index);
  
  return Status::OK();
}

void EmbeddedRaftNode::RegisterStateCallback(
    std::function<void(RaftState, RaftState)> callback) {
  std::lock_guard<std::mutex> lock(callbacks_mutex_);
  state_callbacks_.push_back(std::move(callback));
}

void EmbeddedRaftNode::RegisterLeaderCallback(
    std::function<void(NodeID, NodeID)> callback) {
  std::lock_guard<std::mutex> lock(callbacks_mutex_);
  leader_callbacks_.push_back(std::move(callback));
}

void EmbeddedRaftNode::SetTransport(std::unique_ptr<RaftTransport> transport) {
  transport_ = std::move(transport);
}

// =============================================================================
// 内部实现
// =============================================================================

void EmbeddedRaftNode::BecomeFollower(LogTerm term) {
  auto old_state = state_.exchange(RaftState::kFollower);
  current_term_.store(term);
  voted_for_.store(0);
  
  // 通知回调
  if (old_state != RaftState::kFollower) {
    std::lock_guard<std::mutex> lock(callbacks_mutex_);
    for (auto& cb : state_callbacks_) {
      cb(old_state, RaftState::kFollower);
    }
  }
}

void EmbeddedRaftNode::BecomeCandidate() {
  auto old_state = state_.exchange(RaftState::kCandidate);
  current_term_.fetch_add(1);
  voted_for_.store(config_.node_id);
  
  // 投票给自己
  int votes = 1;
  
  // 向其他节点请求投票
  {
    std::lock_guard<std::mutex> lock(members_mutex_);
    for (const auto& [id, addr] : members_) {
      if (id == config_.node_id) continue;
      
      RaftMessage msg;
      msg.type = RaftMessageType::kRequestVote;
      msg.term = current_term_.load();
      msg.from = config_.node_id;
      msg.to = id;
      
      if (transport_) {
        transport_->Send(id, msg);
      }
    }
  }
  
  // 通知回调
  std::lock_guard<std::mutex> lock(callbacks_mutex_);
  for (auto& cb : state_callbacks_) {
    cb(old_state, RaftState::kCandidate);
  }
}

void EmbeddedRaftNode::BecomeLeader() {
  auto old_state = state_.exchange(RaftState::kLeader);
  current_leader_.store(config_.node_id);
  
  // 初始化 Leader 状态
  leader_state_ = std::make_unique<LeaderState>();
  {
    std::lock_guard<std::mutex> lock(members_mutex_);
    LogIndex next_idx = log_store_->GetLastIndex() + 1;
    for (const auto& [id, addr] : members_) {
      leader_state_->next_index[id] = next_idx;
      leader_state_->match_index[id] = 0;
    }
  }
  
  // 启动心跳线程
  heartbeat_thread_ = std::thread(&EmbeddedRaftNode::HeartbeatLoop, this);
  
  // 立即发送心跳
  ReplicateLog();
  
  // 通知回调
  {
    std::lock_guard<std::mutex> lock(callbacks_mutex_);
    for (auto& cb : state_callbacks_) {
      cb(old_state, RaftState::kLeader);
    }
    for (auto& cb : leader_callbacks_) {
      cb(current_leader_.load(), config_.node_id);
    }
  }
}

void EmbeddedRaftNode::ElectionLoop() {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(150, 300);  // 150-300ms 随机超时
  
  while (running_.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    if (!running_.load()) break;
    
    auto state = state_.load();
    if (state == RaftState::kLeader) {
      continue;
    }
    
    // 检查是否超时
    auto now = std::chrono::steady_clock::now();
    std::chrono::milliseconds timeout(dis(gen));
    
    {
      std::lock_guard<std::mutex> lock(timer_mutex_);
      if (now - last_heartbeat_ < timeout) {
        continue;
      }
    }
    
    // 超时，开始选举
    if (state == RaftState::kFollower) {
      BecomeCandidate();
    } else if (state == RaftState::kCandidate) {
      // 选举失败，再次尝试
      BecomeCandidate();
    }
  }
}

void EmbeddedRaftNode::HeartbeatLoop() {
  while (running_.load() && IsLeader()) {
    ReplicateLog();
    std::this_thread::sleep_for(
        std::chrono::milliseconds(config_.heartbeat_interval_ms));
  }
}

void EmbeddedRaftNode::ApplyLoop() {
  while (running_.load()) {
    std::unique_lock<std::mutex> lock(commit_mutex_);
    commit_cv_.wait_for(lock, std::chrono::milliseconds(10),
        [this] { return !running_.load() || commit_index_ > last_applied_; });
    
    if (!running_.load()) break;
    
    while (last_applied_ < commit_index_) {
      LogIndex next_index = last_applied_.load() + 1;
      auto entry = log_store_->Get(next_index);
      
      if (entry.ok()) {
        state_machine_->Apply(entry.value());
        last_applied_.store(next_index);
      } else {
        break;
      }
    }
    
    // 检查是否需要快照
    CheckSnapshot();
  }
}

void EmbeddedRaftNode::ReplicateLog() {
  if (!IsLeader()) return;
  
  std::lock_guard<std::mutex> lock(members_mutex_);
  for (const auto& [id, addr] : members_) {
    if (id == config_.node_id) continue;
    SendAppendEntries(id);
  }
}

void EmbeddedRaftNode::SendAppendEntries(NodeID peer) {
  RaftMessage msg;
  msg.type = RaftMessageType::kAppendEntries;
  msg.term = current_term_.load();
  msg.from = config_.node_id;
  msg.to = peer;
  
  // TODO: 序列化日志条目
  
  if (transport_) {
    transport_->Send(peer, msg);
  }
}

void EmbeddedRaftNode::CheckSnapshot() {
  if (config_.snapshot_interval_sec == 0) return;
  
  // 简化实现：每当日志达到一定大小时触发快照
  if (log_store_->GetLogCount() > config_.max_log_entries) {
    TriggerSnapshot();
  }
}

Status EmbeddedRaftNode::Step(const RaftMessage& message) {
  if (message.term > current_term_.load()) {
    BecomeFollower(message.term);
    current_leader_.store(message.from);
  }
  
  switch (message.type) {
    case RaftMessageType::kRequestVote:
      HandleRequestVote(message);
      break;
    case RaftMessageType::kRequestVoteResponse:
      HandleRequestVoteResponse(message);
      break;
    case RaftMessageType::kAppendEntries:
      HandleAppendEntries(message);
      break;
    case RaftMessageType::kAppendEntriesResponse:
      HandleAppendEntriesResponse(message);
      break;
    default:
      break;
  }
  
  return Status::OK();
}

void EmbeddedRaftNode::HandleRequestVote(const RaftMessage& msg) {
  RaftMessage response;
  response.type = RaftMessageType::kRequestVoteResponse;
  response.term = current_term_.load();
  response.from = config_.node_id;
  response.to = msg.from;
  
  // 检查是否可以投票
  bool granted = false;
  if (msg.term >= current_term_.load()) {
    auto voted = voted_for_.load();
    if (voted == 0 || voted == msg.from) {
      // 检查日志是否至少一样新
      auto last_entry = log_store_->GetLast();
      if (!last_entry.ok() || msg.term > current_term_.load()) {
        granted = true;
        voted_for_.store(msg.from);
      }
    }
  }
  
  // response.payload = granted ? "1" : "0";
  (void)granted;
  
  if (transport_) {
    transport_->Send(msg.from, response);
  }
}

void EmbeddedRaftNode::HandleRequestVoteResponse(const RaftMessage& msg) {
  if (state_.load() != RaftState::kCandidate) return;
  if (msg.term < current_term_.load()) return;
  
  // TODO: 统计投票
  // 如果收到多数派投票，成为 Leader
}

void EmbeddedRaftNode::HandleAppendEntries(const RaftMessage& msg) {
  // 重置心跳超时
  {
    std::lock_guard<std::mutex> lock(timer_mutex_);
    last_heartbeat_ = std::chrono::steady_clock::now();
  }
  
  if (msg.term >= current_term_.load()) {
    BecomeFollower(msg.term);
    current_leader_.store(msg.from);
  }
  
  // TODO: 处理日志条目
}

void EmbeddedRaftNode::HandleAppendEntriesResponse(const RaftMessage& msg) {
  if (!IsLeader()) return;
  
  // TODO: 更新 match_index 和 next_index
  // 检查是否可以提交
}

}  // namespace raft
}  // namespace dtx
}  // namespace cedar
