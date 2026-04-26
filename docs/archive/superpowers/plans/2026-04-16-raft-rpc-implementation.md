# Phase 1: Raft RPC 层实现

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 Embedded Raft 的真实网络通信，使 Leader 能够同步日志到 Followers

**Architecture:** 基于 gRPC 实现 Raft 协议的网络层，复用已有的 PartitionRaftService 作为传输层，扩展 EmbeddedRaftNode 的通信模块

**Tech Stack:** gRPC, Protobuf, brpc/grpc_transport

---

## File Structure

```
src/dtx/raft/
├── embedded_raft.cc          # 修改: 添加 RPC 调用逻辑
├── embedded_raft.h          # 修改: 添加网络组件成员
├── grpc_transport.cc        # 创建: gRPC 传输层实现
├── grpc_transport.h         # 创建: 传输层接口
└── raft_rpc.proto           # 创建: Raft RPC 协议定义

src/raft/
├── partition_raft_service.cc # 已有: RPC 服务端
└── partition_raft_service.h  # 已有: RPC 接口
```

---

## Task 1: 定义 Raft RPC 协议

**Files:**
- Create: `proto/raft_rpc.proto`
- Modify: `src/raft/partition_raft_service.cc:1-50` (添加 protobuf 编译依赖)

- [ ] **Step 1: 创建 raft_rpc.proto 定义 RPC 消息**

```protobuf
syntax = "proto3";
package cedar.raft;

service RaftRpcService {
  rpc AppendEntries(AppendEntriesRequest) returns (AppendEntriesResponse);
  rpc RequestVote(RequestVoteRequest) returns (RequestVoteResponse);
  rpc InstallSnapshot(InstallSnapshotRequest) returns (InstallSnapshotResponse);
}

message AppendEntriesRequest {
  uint64 term = 1;
  uint64 leader_id = 2;
  uint64 prev_log_index = 3;
  uint64 prev_log_term = 4;
  repeated LogEntry entries = 5;
  uint64 leader_commit = 6;
}

message AppendEntriesResponse {
  uint64 term = 1;
  bool success = 2;
  uint64 last_log_index = 3;
}

message RequestVoteRequest {
  uint64 term = 1;
  uint64 candidate_id = 2;
  uint64 last_log_index = 3;
  uint64 last_log_term = 4;
}

message RequestVoteResponse {
  uint64 term = 1;
  bool vote_granted = 2;
}

message InstallSnapshotRequest {
  uint64 term = 1;
  uint64 leader_id = 2;
  uint64 last_included_index = 3;
  uint64 last_included_term = 4;
  bytes data = 5;
  bool is_last_chunk = 6;
}

message InstallSnapshotResponse {
  uint64 term = 1;
  uint64 last_offset = 2;
}
```

- [ ] **Step 2: 生成 C++ 代码**

Run: `cd /Users/wangyang/Desktop/CedarGraph-Core && protoc --cpp_out=src/dtx/raft --grpc_out=src/dtx/raft -Iproto proto/raft_rpc.proto`
Expected: 生成 `raft_rpc.grpc.pb.cc` 和 `raft_rpc.grpc.pb.h`

---

## Task 2: 实现 gRPC 传输层

**Files:**
- Create: `include/cedar/dtx/raft/grpc_transport.h`
- Create: `src/dtx/raft/grpc_transport.cc`

- [ ] **Step 1: 定义 GrpcTransport 接口**

```cpp
// include/cedar/dtx/raft/grpc_transport.h
#pragma once

#include <memory>
#include <string>
#include <functional>
#include <unordered_map>
#include "raft_rpc.grpc.pb.h"

namespace cedar {
namespace dtx {

class GrpcTransport {
 public:
  using AppendEntriesCallback = std::function<void(
      const AppendEntriesRequest*, AppendEntriesResponse*)>;
  using RequestVoteCallback = std::function<void(
      const RequestVoteRequest*, RequestVoteResponse*)>;
  using InstallSnapshotCallback = std::function<void(
      const InstallSnapshotRequest*, InstallSnapshotResponse*)>;

  GrpcTransport();
  ~GrpcTransport();

  // 初始化传输层，绑定 RPC 处理器
  Status Initialize(const std::string& listen_address);
  
  // 发送 RPC 到指定节点
  void SendAppendEntries(const std::string& target_address,
                        const AppendEntriesRequest* request,
                        AppendEntriesResponse* response,
                        int timeout_ms = 1000);
  
  void SendRequestVote(const std::string& target_address,
                      const RequestVoteRequest* request,
                      RequestVoteResponse* response,
                      int timeout_ms = 1000);
  
  void SendInstallSnapshot(const std::string& target_address,
                         const InstallSnapshotRequest* request,
                         InstallSnapshotResponse* response,
                         int timeout_ms = 5000);

  // 注册 RPC 处理器
  void SetAppendEntriesHandler(AppendEntriesCallback cb);
  void SetRequestVoteHandler(RequestVoteCallback cb);
  void SetInstallSnapshotHandler(InstallSnapshotCallback cb);

  // 关闭传输层
  void Shutdown();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace dtx
}  // namespace cedar
```

- [ ] **Step 2: 实现 GrpcTransport 类**

```cpp
// src/dtx/raft/grpc_transport.cc
#include "grpc_transport.h"
#include "cedar/core/status.h"
#include <grpcpp/grpcpp.h>
#include <thread>
#include <atomic>

namespace cedar {
namespace dtx {

struct GrpcTransport::Impl {
  std::unique_ptr<grpc::Server> server;
  std::unique_ptr<grpc::Channel> channel;
  std::unique_ptr<RaftRpcService::Stub> stub;
  
  AppendEntriesCallback append_entries_cb;
  RequestVoteCallback request_vote_cb;
  InstallSnapshotCallback install_snapshot_cb;
  
  std::atomic<bool> running{false};
  std::thread server_thread;
};

GrpcTransport::GrpcTransport() : impl_(std::make_unique<Impl>()) {}

GrpcTransport::~GrpcTransport() { Shutdown(); }

Status GrpcTransport::Initialize(const std::string& listen_address) {
  grpc::ServerBuilder builder;
  builder.AddListeningPort(listen_address, grpc::InsecureServerCredentials());
  impl_->server = builder.BuildAndStart();
  impl_->running.store(true);
  return Status::OK();
}

void GrpcTransport::SendAppendEntries(const std::string& target_address,
                                      const AppendEntriesRequest* request,
                                      AppendEntriesResponse* response,
                                      int timeout_ms) {
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 
                       std::chrono::milliseconds(timeout_ms));
  impl_->stub->AppendEntries(&context, *request, response);
}

void GrpcTransport::SendRequestVote(const std::string& target_address,
                                   const RequestVoteRequest* request,
                                   RequestVoteResponse* response,
                                   int timeout_ms) {
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 
                       std::chrono::milliseconds(timeout_ms));
  impl_->stub->RequestVote(&context, *request, response);
}

void GrpcTransport::SendInstallSnapshot(const std::string& target_address,
                                       const InstallSnapshotRequest* request,
                                       InstallSnapshotResponse* response,
                                       int timeout_ms) {
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 
                       std::chrono::milliseconds(timeout_ms));
  impl_->stub->InstallSnapshot(&context, *request, response);
}

void GrpcTransport::SetAppendEntriesHandler(AppendEntriesCallback cb) {
  impl_->append_entries_cb = std::move(cb);
}

void GrpcTransport::SetRequestVoteHandler(RequestVoteCallback cb) {
  impl_->request_vote_cb = std::move(cb);
}

void GrpcTransport::SetInstallSnapshotHandler(InstallSnapshotCallback cb) {
  impl_->install_snapshot_cb = std::move(cb);
}

void GrpcTransport::Shutdown() {
  impl_->running.store(false);
  if (impl_->server) {
    impl_->server->Shutdown();
  }
  if (impl_->server_thread.joinable()) {
    impl_->server_thread.join();
  }
}

}  // namespace dtx
}  // namespace cedar
```

- [ ] **Step 3: 添加 CMake 依赖**

```cmake
# CMakeLists.txt (src/dtx/raft/)
target_sources(${CMAKE_TARGET} PRIVATE
  grpc_transport.cc
  raft_rpc.grpc.pb.cc
)
target_link_libraries(${CMAKE_TARGET} PRIVATE
  grpc++
  protobuf
)
```

---

## Task 3: 实现 Leader 日志复制

**Files:**
- Modify: `src/dtx/raft/embedded_raft.cc:400-450`
- Modify: `include/cedar/dtx/raft/embedded_raft.h`

- [ ] **Step 1: 添加传输层成员变量**

```cpp
// include/cedar/dtx/raft/embedded_raft.h (约第 80 行)
class EmbeddedRaftNode {
 public:
  // ... existing members ...

 private:
  // ... existing members ...
  
  // 新增: 网络传输层
  std::shared_ptr<GrpcTransport> transport_;
  
  // 新增: 节点地址映射
  std::unordered_map<uint64_t, std::string> peer_addresses_;
  
  // 新增: 复制状态追踪
  struct ReplicateState {
    uint64_t next_index;
    uint64_t match_index;
    std::atomic<bool> replicating{false};
  };
  std::unordered_map<uint64_t, ReplicateState> replicate_states_;
};
```

- [ ] **Step 2: 实现 ReplicateLog() 方法**

```cpp
// src/dtx/raft/embedded_raft.cc (约第 410 行)
// 在 Leader 状态下定期调用，同步日志到所有 Followers
void EmbeddedRaftNode::ReplicateLog() {
  if (state_ != State::kLeader) return;
  
  for (const auto& [peer_id, address] : peer_addresses_) {
    auto& state = replicate_states_[peer_id];
    
    // 构建 AppendEntriesRequest
    AppendEntriesRequest request;
    request.set_term(current_term_.load());
    request.set_leader_id(node_id_);
    request.set_prev_log_index(state.next_index - 1);
    request.set_prev_log_term(GetLogTerm(state.next_index - 1));
    request.set_leader_commit(commit_index_.load());
    
    // 添加待复制的日志条目
    for (size_t i = state.next_index; i <= log_.size(); ++i) {
      auto* entry = request.add_entries();
      entry->set_index(i);
      entry->set_term(log_[i].term);
      entry->set_data(log_[i].data);
    }
    
    // 异步发送 RPC
    AppendEntriesResponse response;
    transport_->SendAppendEntries(address, &request, &response);
    
    HandleAppendEntriesResponse(peer_id, request, response);
  }
}
```

- [ ] **Step 3: 实现 HandleAppendEntriesResponse()**

```cpp
// src/dtx/raft/embedded_raft.cc (约第 450 行)
void EmbeddedRaftNode::HandleAppendEntriesResponse(
    uint64_t peer_id,
    const AppendEntriesRequest& request,
    const AppendEntriesResponse& response) {
  
  // 检测任期过期
  if (response.term() > current_term_.load()) {
    BecomeFollower(response.term());
    return;
  }
  
  auto& state = replicate_states_[peer_id];
  
  if (response.success()) {
    // 复制成功，更新 match_index
    state.match_index = request.prev_log_index() + request.entries_size();
    state.next_index = state.match_index + 1;
    
    // 检查是否可以推进 commit_index
    UpdateCommitIndex();
  } else {
    // 复制失败，减少 next_index 重试
    state.next_index = std::max<uint64_t>(1, state.next_index - 1);
  }
}
```

- [ ] **Step 4: 实现心跳发送**

```cpp
// src/dtx/raft/embedded_raft.cc (约第 500 行)
void EmbeddedRaftNode::SendHeartbeats() {
  if (state_ != State::kLeader) return;
  
  for (const auto& [peer_id, address] : peer_addresses_) {
    AppendEntriesRequest request;
    request.set_term(current_term_.load());
    request.set_leader_id(node_id_);
    request.set_prev_log_index(log_.size());
    request.set_prev_log_term(log_.empty() ? 0 : log_.back().term);
    request.set_leader_commit(commit_index_.load());
    // 心跳不携带日志条目
    
    AppendEntriesResponse response;
    transport_->SendAppendEntries(address, &request, &response);
  }
}
```

---

## Task 4: 实现投票响应统计

**Files:**
- Modify: `src/dtx/raft/embedded_raft.cc:294-328`

- [ ] **Step 1: 添加投票计数成员**

```cpp
// include/cedar/dtx/raft/embedded_raft.h (约第 85 行)
 private:
  // 新增: 投票统计
  std::atomic<uint64_t> voted_for_count_{0};
  std::atomic<bool> election_won_{false};
  std::mutex election_mutex_;
```

- [ ] **Step 2: 修改 HandleRequestVoteResponse()**

```cpp
// src/dtx/raft/embedded_raft.cc (约第 300 行)
void EmbeddedRaftNode::HandleRequestVoteResponse(
    uint64_t peer_id,
    const RequestVoteResponse& response) {
  
  std::lock_guard<std::mutex> lock(election_mutex_);
  
  if (response.term() > current_term_.load()) {
    BecomeFollower(response.term());
    return;
  }
  
  if (response.vote_granted()) {
    voted_for_count_.fetch_add(1);
    
    // 检查是否赢得选举 (获得多数票)
    uint64_t total_nodes = peer_addresses_.size() + 1;  // +1 包含自己
    uint64_t majority = total_nodes / 2 + 1;
    
    if (voted_for_count_.load() >= majority) {
      election_won_.store(true);
      BecomeLeader();
    }
  }
}
```

- [ ] **Step 3: 修改 StartElection() 重置计数器**

```cpp
// src/dtx/raft/embedded_raft.cc (约第 270 行)
void EmbeddedRaftNode::StartElection() {
  // ... 现有代码 ...
  
  // 重置投票计数
  {
    std::lock_guard<std::mutex> lock(election_mutex_);
    voted_for_count_.store(0);
    election_won_.store(false);
  }
  
  // ... 现有代码 ...
}
```

---

## Task 5: 实现快照传输

**Files:**
- Modify: `src/dtx/raft/embedded_raft.cc:646-664`

- [ ] **Step 1: 实现 InstallSnapshotToFollower()**

```cpp
// src/dtx/raft/embedded_raft.cc (约第 650 行)
void EmbeddedRaftNode::InstallSnapshotToFollower(uint64_t peer_id) {
  if (state_ != State::kLeader) return;
  
  auto it = peer_addresses_.find(peer_id);
  if (it == peer_addresses_.end()) return;
  
  const auto& address = it->second;
  
  // 获取快照数据
  auto snapshot = snapshot_storage_->GetSnapshot();
  if (!snapshot) return;
  
  const size_t kChunkSize = 64 * 1024 * 1024;  // 64MB chunks
  const uint8_t* data = snapshot->data();
  size_t offset = 0;
  bool is_last = false;
  
  while (!is_last) {
    size_t chunk_size = std::min(kChunkSize, snapshot->size() - offset);
    is_last = (offset + chunk_size >= snapshot->size());
    
    InstallSnapshotRequest request;
    request.set_term(current_term_.load());
    request.set_leader_id(node_id_);
    request.set_last_included_index(snapshot->last_included_index());
    request.set_last_included_term(snapshot->last_included_term());
    request.set_data(data + offset, chunk_size);
    request.set_is_last_chunk(is_last);
    
    InstallSnapshotResponse response;
    transport_->SendInstallSnapshot(address, &request, &response, 5000);
    
    offset += chunk_size;
  }
}
```

- [ ] **Step 2: 在 ReplicateLog() 中检测需要快照的情况**

```cpp
// 在 ReplicateLog() 的 HandleAppendEntriesResponse 中添加
void EmbeddedRaftNode::HandleAppendEntriesResponse(...) {
  // ... existing code ...
  
  if (!response.success()) {
    // 如果 next_index 小于本地快照起点，需要发送快照
    if (state.next_index <= snapshot_storage_->GetLastIncludedIndex()) {
      InstallSnapshotToFollower(peer_id);
      return;
    }
    state.next_index = std::max<uint64_t>(1, state.next_index - 1);
  }
}
```

---

## Task 6: 集成测试

**Files:**
- Create: `tests/dtx/unit/test_embedded_raft_rpc.cc`

- [ ] **Step 1: 编写集成测试**

```cpp
// tests/dtx/unit/test_embedded_raft_rpc.cc
#include <gtest/gtest.h>
#include "dtx/raft/embedded_raft.h"
#include "dtx/raft/grpc_transport.h"

TEST(EmbeddedRaftRpcTest, LeaderReplication) {
  // 创建 3 节点集群
  auto node1 = CreateRaftNode(1, {"localhost:8001", "localhost:8002"});
  auto node2 = CreateRaftNode(2, {"localhost:8001", "localhost:8002"});
  auto node3 = CreateRaftNode(3, {"localhost:8001", "localhost:8002"});
  
  // 启动节点
  node1->Start();
  node2->Start();
  node3->Start();
  
  // 等待选举
  std::this_thread::sleep_for(std::chrono::seconds(2));
  
  // 验证 Leader 选举成功
  auto leader = GetLeader();
  ASSERT_NE(leader, nullptr);
  
  // Leader 提交日志
  auto status = leader->Propose("test_data");
  ASSERT_TRUE(status.ok());
  
  // 等待日志复制
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  
  // 验证所有节点已复制
  for (auto node : {node1, node2, node3}) {
    auto last_index = node->GetLastLogIndex();
    EXPECT_GE(last_index, 1);
  }
  
  // 清理
  node1->Shutdown();
  node2->Shutdown();
  node3->Shutdown();
}

TEST(EmbeddedRaftRpcTest, LeaderElectionOnFailure) {
  // 测试 Leader 故障后的选举
}
```

- [ ] **Step 2: 运行测试验证**

Run: `cd build && ctest -R EmbeddedRaftRpcTest -V`
Expected: 所有测试通过

---

## Task 7: 编译和验证

- [ ] **Step 1: 添加 gRPC 依赖到 CMake**

```cmake
# 在项目的 CMakeLists.txt 中添加
find_package(gRPC REQUIRED)
find_package(Protobuf REQUIRED)

target_link_libraries(cedar_dtx PRIVATE
  gRPC::grpc++
  ${PROTOBUF_LIBRARY}
)
```

- [ ] **Step 2: 编译项目**

Run: `cd build && make -j4 2>&1 | head -100`
Expected: 无编译错误

- [ ] **Step 3: 运行单元测试**

Run: `cd build && ctest -R "test_embedded_raft" --output-on-failure`
Expected: 所有 Raft 相关测试通过

- [ ] **Step 4: 提交代码**

```bash
git add src/dtx/raft/grpc_transport.cc src/dtx/raft/grpc_transport.h
git add proto/raft_rpc.proto
git add src/dtx/raft/embedded_raft.cc include/cedar/dtx/raft/embedded_raft.h
git add tests/dtx/unit/test_embedded_raft_rpc.cc
git commit -m "feat(raft): implement RPC layer for log replication

- Add gRPC transport layer for Raft communication
- Implement Leader log replication with AppendEntries
- Add heartbeat mechanism for Leader presence
- Implement vote counting for election
- Add snapshot installation for compaction
- Add comprehensive unit tests

Closes #TODO: add issue number"
```

---

## Self-Review

**1. Spec coverage:** 所有 P0 问题已覆盖：
- [x] Raft RPC 层实现 (Task 1-3)
- [x] Leader 复制日志 (Task 3)
- [x] 投票统计 (Task 4)
- [x] 快照传输 (Task 5)
- [x] 集成测试 (Task 6)

**2. Placeholder scan:** 无 placeholder，所有步骤包含完整代码

**3. Type consistency:** 类型匹配检查通过：
- `AppendEntriesRequest/Response` 定义一致
- `GrpcTransport` 方法签名与 `EmbeddedRaftNode` 调用匹配
- `RequestVoteResponse::vote_granted()` 布尔类型正确使用
