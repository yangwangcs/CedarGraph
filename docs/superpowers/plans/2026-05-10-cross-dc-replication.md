# CrossDC Replication 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 CrossDCReplicator 的真实跨 DC 复制传输，使 replication logs 能通过网络发送到远程 DC 并被应用。

**Architecture:** 采用"gRPC 双向流 + 异步队列 + 冲突解决"策略。在现有 `dtx_protocol.proto` 中添加 `Replicate`/`ApplyReplication` RPC，实现轻量级 gRPC 服务器端接收 replication logs 并写入本地存储。利用 `CrossDCReplicator` 已有的队列、批处理和重试框架。

**Tech Stack:** C++17, gRPC, protobuf, 异步队列, 时间戳冲突解决

---

## 文件结构

```
proto/dtx_protocol.proto                          # 添加 Replicate/ApplyReplication RPC
src/dtx/cross_dc_replicator.cc                   # 实现 SendToRemoteDC/SyncWithDC
include/cedar/dtx/cross_dc_replicator.h           # 添加 remote endpoint 配置
src/dtx/dtx_service_impl.cc                       # 新增 DTXService gRPC 服务器实现
include/cedar/dtx/dtx_service_impl.h              # DTXServiceImpl 声明
src/dtx/transaction_state.cc                      # 集成 replication 钩子
```

---

## 第一阶段：Proto 定义与生成

### Task 1: 在 `dtx_protocol.proto` 中添加 Replication RPC

**Files:**
- Modify: `proto/dtx_protocol.proto`

**问题:** 当前 proto 没有 replication 相关的 message 或 RPC。

- [ ] **Step 1: 添加 ReplicationLog message 和 RPC**

在 `proto/dtx_protocol.proto` 末尾添加：

```protobuf
// Cross-DC Replication Messages
message ReplicationLogEntry {
  uint64 sequence_num = 1;
  bytes key = 2;           // Serialized CedarKey
  bytes value = 3;         // Serialized Descriptor
  uint64 timestamp = 4;    // Microseconds since epoch
  string source_dc = 5;
  repeated string target_dcs = 6;
}

message ReplicateRequest {
  string target_dc = 1;
  repeated ReplicationLogEntry logs = 2;
}

message ReplicateResponse {
  bool success = 1;
  uint64 last_applied_sequence = 2;
  string error_msg = 3;
}

message ApplyReplicationRequest {
  string source_dc = 1;
  repeated ReplicationLogEntry logs = 2;
}

message ApplyReplicationResponse {
  bool success = 1;
  uint64 last_applied_sequence = 2;
  string error_msg = 3;
}

// Extend DTXService with replication methods
service DTXService {
  // ... existing methods ...
  rpc Replicate(ReplicateRequest) returns (ReplicateResponse);
  rpc ApplyReplication(stream ApplyReplicationRequest) returns (ApplyReplicationResponse);
}
```

- [ ] **Step 2: 确保 CMake 生成新 proto**

验证 `CMakeLists.txt` 中 `dtx_protocol.proto` 在 protobuf 生成列表中。应该已经存在，因为 `DTXRpcClient` 已经在使用它。

- [ ] **Step 3: 重新生成 proto 并编译**

```bash
cd build && cmake .. && make cedar -j4
```

- [ ] **Step 4: Commit**

```bash
git add proto/dtx_protocol.proto
git commit -m "feat(crossdc): add ReplicationLogEntry and Replicate/ApplyReplication RPCs"
```

---

## 第二阶段：DTXService gRPC 服务器实现

### Task 2: 实现 `DTXServiceImpl` 处理 `ApplyReplication`

**Files:**
- Create: `src/dtx/dtx_service_impl.cc`
- Create: `include/cedar/dtx/dtx_service_impl.h`

**问题:** 当前没有 `DTXService` 的服务器实现，`DTXRpcClient` 是孤儿客户端。

- [ ] **Step 1: 创建 DTXServiceImpl 头文件**

```cpp
// include/cedar/dtx/dtx_service_impl.h
#ifndef CEDAR_DTX_SERVICE_IMPL_H_
#define CEDAR_DTX_SERVICE_IMPL_H_

#include "dtx_protocol.grpc.pb.h"
#include "cedar/storage/cedar_graph_storage.h"
#include <grpcpp/grpcpp.h>

namespace cedar {
namespace dtx {

class DTXServiceImpl final : public cedar::dtx::DTXService::Service {
 public:
  explicit DTXServiceImpl(cedar::CedarGraphStorage* storage);

  ::grpc::Status Prepare(::grpc::ServerContext* context,
                         const cedar::dtx::PrepareRequest* request,
                         cedar::dtx::PrepareResponse* response) override;
  ::grpc::Status Commit(::grpc::ServerContext* context,
                        const cedar::dtx::CommitRequest* request,
                        cedar::dtx::CommitResponse* response) override;
  ::grpc::Status Abort(::grpc::ServerContext* context,
                       const cedar::dtx::AbortRequest* request,
                       cedar::dtx::AbortResponse* response) override;
  ::grpc::Status Inquire(::grpc::ServerContext* context,
                         const cedar::dtx::InquireRequest* request,
                         cedar::dtx::InquireResponse* response) override;
  ::grpc::Status Replicate(::grpc::ServerContext* context,
                           const cedar::dtx::ReplicateRequest* request,
                           cedar::dtx::ReplicateResponse* response) override;
  ::grpc::Status ApplyReplication(
      ::grpc::ServerContext* context,
      ::grpc::ServerReader<cedar::dtx::ApplyReplicationRequest>* reader,
      cedar::dtx::ApplyReplicationResponse* response) override;

 private:
  cedar::CedarGraphStorage* storage_;

  Status ApplySingleLog(const cedar::dtx::ReplicationLogEntry& log_entry);
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_SERVICE_IMPL_H_
```

- [ ] **Step 2: 实现 ApplyReplication 和 Replicate**

```cpp
// src/dtx/dtx_service_impl.cc
#include "cedar/dtx/dtx_service_impl.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include <iostream>

namespace cedar {
namespace dtx {

DTXServiceImpl::DTXServiceImpl(cedar::CedarGraphStorage* storage)
    : storage_(storage) {}

::grpc::Status DTXServiceImpl::Replicate(
    ::grpc::ServerContext* context,
    const cedar::dtx::ReplicateRequest* request,
    cedar::dtx::ReplicateResponse* response) {
  (void)context;
  uint64_t last_sequence = 0;
  for (const auto& log : request->logs()) {
    auto s = ApplySingleLog(log);
    if (!s.ok()) {
      response->set_success(false);
      response->set_error_msg(s.ToString());
      return ::grpc::Status::OK;
    }
    last_sequence = log.sequence_num();
  }
  response->set_success(true);
  response->set_last_applied_sequence(last_sequence);
  return ::grpc::Status::OK;
}

::grpc::Status DTXServiceImpl::ApplyReplication(
    ::grpc::ServerContext* context,
    ::grpc::ServerReader<cedar::dtx::ApplyReplicationRequest>* reader,
    cedar::dtx::ApplyReplicationResponse* response) {
  (void)context;
  cedar::dtx::ApplyReplicationRequest request;
  uint64_t last_sequence = 0;
  while (reader->Read(&request)) {
    for (const auto& log : request.logs()) {
      auto s = ApplySingleLog(log);
      if (!s.ok()) {
        response->set_success(false);
        response->set_error_msg(s.ToString());
        return ::grpc::Status::OK;
      }
      last_sequence = log.sequence_num();
    }
  }
  response->set_success(true);
  response->set_last_applied_sequence(last_sequence);
  return ::grpc::Status::OK;
}

Status DTXServiceImpl::ApplySingleLog(const cedar::dtx::ReplicationLogEntry& log_entry) {
  if (!storage_) {
    return Status::IOError("Storage not available");
  }

  // Deserialize CedarKey from bytes
  if (log_entry.key().size() != sizeof(CedarKey)) {
    return Status::InvalidArgument("Invalid key size in replication log");
  }
  CedarKey key;
  std::memcpy(&key, log_entry.key().data(), sizeof(CedarKey));

  // Deserialize Descriptor from bytes
  if (log_entry.value().size() != sizeof(uint64_t)) {
    return Status::InvalidArgument("Invalid descriptor size in replication log");
  }
  uint64_t desc_raw;
  std::memcpy(&desc_raw, log_entry.value().data(), sizeof(uint64_t));
  Descriptor desc(desc_raw);

  Timestamp ts(log_entry.timestamp());

  // Apply to local storage
  auto s = storage_->Put(key.entity_id(), key.timestamp().value(), desc, ts);
  if (!s.ok()) {
    return Status::IOError("Storage Put failed: " + s.ToString());
  }
  return Status::OK();
}

// Stub implementations for 2PC methods (full implementation can reuse StorageServiceImpl)
::grpc::Status DTXServiceImpl::Prepare(::grpc::ServerContext* context,
                                       const cedar::dtx::PrepareRequest* request,
                                       cedar::dtx::PrepareResponse* response) {
  (void)context; (void)request;
  response->set_success(false);
  response->set_error_msg("Prepare not implemented in DTXService; use StorageService");
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "Use StorageService");
}

::grpc::Status DTXServiceImpl::Commit(::grpc::ServerContext* context,
                                      const cedar::dtx::CommitRequest* request,
                                      cedar::dtx::CommitResponse* response) {
  (void)context; (void)request;
  response->set_success(false);
  response->set_error_msg("Commit not implemented in DTXService; use StorageService");
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "Use StorageService");
}

::grpc::Status DTXServiceImpl::Abort(::grpc::ServerContext* context,
                                     const cedar::dtx::AbortRequest* request,
                                     cedar::dtx::AbortResponse* response) {
  (void)context; (void)request;
  response->set_success(false);
  response->set_error_msg("Abort not implemented in DTXService; use StorageService");
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "Use StorageService");
}

::grpc::Status DTXServiceImpl::Inquire(::grpc::ServerContext* context,
                                       const cedar::dtx::InquireRequest* request,
                                       cedar::dtx::InquireResponse* response) {
  (void)context; (void)request;
  response->set_state(cedar::dtx::TransactionState::UNKNOWN);
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "Use StorageService");
}

}  // namespace dtx
}  // namespace cedar
```

- [ ] **Step 3: 添加到 CMakeLists.txt**

在 `CMakeLists.txt` 的 cedar 源文件列表中添加：

```cmake
src/dtx/dtx_service_impl.cc
```

- [ ] **Step 4: 编译验证**

```bash
cd build && cmake .. && make cedar -j4
```

- [ ] **Step 5: Commit**

```bash
git add include/cedar/dtx/dtx_service_impl.h src/dtx/dtx_service_impl.cc CMakeLists.txt
git commit -m "feat(crossdc): implement DTXServiceImpl with ApplyReplication handler"
```

---

## 第三阶段：CrossDCReplicator 发送端实现

### Task 3: 实现 `CrossDCReplicator::SendToRemoteDC`

**Files:**
- Modify: `src/dtx/cross_dc_replicator.cc:253-258`
- Modify: `include/cedar/dtx/cross_dc_replicator.h:42-49`
- Modify: `src/dtx/cross_dc_replicator.cc:29-74`

**问题:** `SendToRemoteDC` 立即返回 OK 不做任何网络 I/O。

- [ ] **Step 1: 在 DCReplicationConfig 中添加 remote endpoints**

```cpp
// include/cedar/dtx/cross_dc_replicator.h
struct DCReplicationConfig {
  ReplicationMode mode = ReplicationMode::kAsync;
  std::chrono::milliseconds replication_timeout{5000};
  uint32_t max_retry_attempts = 3;
  std::chrono::milliseconds retry_delay{1000};
  bool enable_compression = true;
  uint32_t batch_size = 100;
  // NEW: remote DC endpoint mapping
  std::map<std::string, std::string> remote_dc_endpoints;
};
```

- [ ] **Step 2: 在 Initialize 中创建 gRPC stubs**

```cpp
// src/dtx/cross_dc_replicator.cc
Status CrossDCReplicator::Initialize(
    const DCReplicationConfig& config,
    const std::string& local_dc_id,
    const std::vector<std::string>& peer_dcs) {
  config_ = config;
  local_dc_id_ = local_dc_id;
  peer_dcs_ = peer_dcs;

  // Create gRPC channels to remote DCs
  for (const auto& dc : peer_dcs) {
    auto it = config.remote_dc_endpoints.find(dc);
    if (it != config.remote_dc_endpoints.end()) {
      auto channel = grpc::CreateChannel(it->second,
          grpc::InsecureChannelCredentials());
      dc_stubs_[dc] = cedar::dtx::DTXService::NewStub(channel);
    }
  }

  for (const auto& dc : peer_dcs) {
    dc_statuses_[dc] = ReplicationStatus{};
  }
  return Status::OK();
}
```

- [ ] **Step 3: 实现 SendToRemoteDC**

```cpp
Status CrossDCReplicator::SendToRemoteDC(
    const ReplicationLog& log, const std::string& dc_id) {
  auto it = dc_stubs_.find(dc_id);
  if (it == dc_stubs_.end()) {
    return Status::IOError("No gRPC stub for DC: " + dc_id);
  }

  cedar::dtx::ReplicateRequest request;
  request.set_target_dc(dc_id);

  auto* entry = request.add_logs();
  entry->set_sequence_num(log.sequence_num);
  entry->set_key(reinterpret_cast<const char*>(&log.key), sizeof(log.key));
  entry->set_value(reinterpret_cast<const char*>(&log.value), sizeof(uint64_t));
  entry->set_timestamp(log.timestamp.value());
  entry->set_source_dc(log.source_dc);
  for (const auto& target : log.target_dcs) {
    entry->add_target_dcs(target);
  }

  cedar::dtx::ReplicateResponse response;
  ::grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                        std::chrono::milliseconds(config_.replication_timeout));

  auto status = it->second->Replicate(&context, request, &response);
  if (!status.ok()) {
    return Status::IOError("Replicate RPC failed: " + status.error_message());
  }
  if (!response.success()) {
    return Status::IOError("Remote DC rejected replication: " + response.error_msg());
  }
  return Status::OK();
}
```

- [ ] **Step 4: 在头文件中添加 dc_stubs_ 成员**

```cpp
// include/cedar/dtx/cross_dc_replicator.h
private:
  std::map<std::string, std::unique_ptr<cedar::dtx::DTXService::Stub>> dc_stubs_;
```

- [ ] **Step 5: 编译验证**

- [ ] **Step 6: Commit**

```bash
git commit -m "feat(crossdc): implement SendToRemoteDC with gRPC Replicate RPC"
```

---

## 第四阶段：SyncWithDC 和 ReceiveReplication

### Task 4: 实现 `SyncWithDC` 和 `ReceiveReplication` 数据应用

**Files:**
- Modify: `src/dtx/cross_dc_replicator.cc:149-152` (SyncWithDC)
- Modify: `src/dtx/cross_dc_replicator.cc:117-131` (ReceiveReplication)

- [ ] **Step 1: 实现 SyncWithDC**

```cpp
Status CrossDCReplicator::SyncWithDC(const std::string& dc_id) {
  auto it = dc_stubs_.find(dc_id);
  if (it == dc_stubs_.end()) {
    return Status::IOError("No gRPC stub for DC: " + dc_id);
  }

  // Send a sync ping to check health and get last applied sequence
  cedar::dtx::ReplicateRequest request;
  request.set_target_dc(dc_id);
  // Empty log list = heartbeat/sync check

  cedar::dtx::ReplicateResponse response;
  ::grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                        std::chrono::milliseconds(config_.replication_timeout));

  auto status = it->second->Replicate(&context, request, &response);
  if (!status.ok()) {
    return Status::IOError("Sync RPC failed: " + status.error_message());
  }

  // Update local tracking
  auto& dc_status = dc_statuses_[dc_id];
  dc_status.last_sequence = response.last_applied_sequence();
  dc_status.is_healthy = response.success();

  return Status::OK();
}
```

- [ ] **Step 2: 实现 ReceiveReplication 数据应用**

`ReceiveReplication` 需要接收 replication log 并写入本地存储。但目前 `CrossDCReplicator` 没有存储指针。需要添加注入：

```cpp
// include/cedar/dtx/cross_dc_replicator.h
public:
  void SetStorage(cedar::CedarGraphStorage* storage) { storage_ = storage; }

private:
  cedar::CedarGraphStorage* storage_ = nullptr;
```

然后实现 `ReceiveReplication`：

```cpp
Status CrossDCReplicator::ReceiveReplication(const ReplicationLog& log) {
  if (log.source_dc == local_dc_id_) {
    return Status::InvalidArgument("Cannot receive replication from local DC");
  }

  if (!storage_) {
    return Status::IOError("Storage not set for replication application");
  }

  // Apply the log to local storage
  auto s = storage_->Put(log.key.entity_id(), log.timestamp.value(),
                          log.value, log.timestamp);
  if (!s.ok()) {
    return Status::IOError("Storage Put failed in ReceiveReplication: " + s.ToString());
  }

  // Update sequence tracking
  auto& dc_status = dc_statuses_[log.source_dc];
  dc_status.last_sequence = log.sequence_num;
  dc_status.replicated_count++;

  return Status::OK();
}
```

- [ ] **Step 3: 实现 ResolveConflict 的数据应用**

```cpp
Status CrossDCReplicator::ResolveConflict(
    const std::string& key,
    const std::vector<ReplicationLog>& conflicting_logs) {
  if (conflicting_logs.empty()) return Status::OK();

  // Use last-write-wins based on Timestamp
  const ReplicationLog* winner = &conflicting_logs[0];
  for (const auto& log : conflicting_logs) {
    if (log.timestamp.value() > winner->timestamp.value()) {
      winner = &log;
    }
  }

  // Apply the winner
  return ReceiveReplication(*winner);
}
```

- [ ] **Step 4: 编译验证**

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(crossdc): implement SyncWithDC, ReceiveReplication, and ResolveConflict"
```

---

## 第五阶段：集成与测试

### Task 5: 在 StorageServer 初始化时启动 DTXService 和 CrossDCReplicator

**Files:**
- Modify: `src/dtx/storage_impl/storage_server.cc`

- [ ] **Step 1: 在 StorageServer 中创建 DTXService gRPC 服务器**

在 `StorageServer::Initialize` 的 gRPC 服务器创建部分：

```cpp
// After creating the main StorageService server
auto dtx_service_impl = std::make_unique<cedar::dtx::DTXServiceImpl>(
    partition_manager_->GetSharedStorage());

::grpc::ServerBuilder dtx_builder;
dtx_builder.AddListeningPort("0.0.0.0:" + std::to_string(dtx_port_),
                              grpc::InsecureServerCredentials());
dtx_builder.RegisterService(dtx_service_impl.get());
auto dtx_server = dtx_builder.BuildAndStart();
```

- [ ] **Step 2: 配置并启动 CrossDCReplicator**

```cpp
DCReplicationConfig dc_config;
dc_config.remote_dc_endpoints = config_.remote_dc_endpoints;
cross_dc_replicator_ = std::make_unique<CrossDCReplicator>();
cross_dc_replicator_->Initialize(dc_config, config_.local_dc_id, config_.peer_dcs);
cross_dc_replicator_->SetStorage(partition_manager_->GetSharedStorage());
cross_dc_replicator_->Start();
```

- [ ] **Step 3: 编译验证**

- [ ] **Step 4: Commit**

```bash
git commit -m "feat(crossdc): wire DTXService and CrossDCReplicator into StorageServer"
```

---

### Task 6: 更新 CrossDC 测试

**Files:**
- Modify: `tests/cluster/test_cross_dc_replication.cc`

- [ ] **Step 1: 更新测试预期**

由于 `SendToRemoteDC` 现在会尝试真实 RPC（如果没有 endpoint 配置则返回 IOError），需要更新测试：

```cpp
TEST(CrossDCReplication, BasicReplicationWithoutEndpoint) {
  CrossDCReplicator replicator;
  DCReplicationConfig config;
  // No remote_dc_endpoints configured
  auto s = replicator.Initialize(config, "dc1", {"dc2"});
  ASSERT_TRUE(s.ok());

  ReplicationLog log;
  log.sequence_num = 1;
  log.key = CedarKey::MakeTestKey(100);
  log.value = Descriptor::InlineInt(0, 42);
  log.timestamp = Timestamp(12345);
  log.source_dc = "dc1";
  log.target_dcs = {"dc2"};

  s = replicator.Replicate(log);
  // Without endpoint, should fail with IOError (not NotSupported)
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsIOError());
}
```

- [ ] **Step 2: 运行测试**

```bash
cd build && ctest -R CrossDC -V
```

- [ ] **Step 3: Commit**

```bash
git commit -m "test(crossdc): update test expectations for real SendToRemoteDC"
```

---

## Self-Review

### 1. Spec Coverage

| 审计发现 | 对应任务 |
|---------|---------|
| SendToRemoteDC 空桩 | Task 3 |
| SyncWithDC 空桩 | Task 4 |
| ReceiveReplication 不应用数据 | Task 4 |
| ResolveConflict 不应用结果 | Task 4 |
| 无 replication proto | Task 1 |
| 无 DTXService 服务器 | Task 2 |
| 无 endpoint 配置 | Task 3 |

### 2. Placeholder Scan

- 无 TBD/TODO/"implement later"
- 所有代码步骤包含具体代码块

### 3. Type Consistency

- `ReplicationLog`、`Timestamp`、`Descriptor`、`CedarKey` 类型一致
- `Status::OK()`、`Status::IOError` 使用一致

---

**Plan complete and saved to `docs/superpowers/plans/2026-05-10-cross-dc-replication.md`.**

**Two execution options:**

**1. Subagent-Driven (recommended)** — Dispatch a fresh subagent per task.

**2. Inline Execution** — Execute tasks in this session.

**Which approach?**
