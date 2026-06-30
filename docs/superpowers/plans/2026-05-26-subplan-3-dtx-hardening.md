# DTX Distributed Transaction Hardening — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Harden three critical gaps in CedarGraph-Core's distributed transaction layer: implement `DTXServiceImpl::RegisterParticipant` with in-memory registry and decision-log persistence; replace `ClusterInitializer::RegisterStorageNodes` hardcoded success with real MetaService gRPC calls; and implement `BraftPartitionNode::ReadIndex` via a safe log-appending barrier.

**Architecture:** Each fix is independent and targets a single subsystem. `RegisterParticipant` adds a mutex-protected participant map to `DTXServiceImpl` with an append-only text log for coordinator recovery. `ClusterInitializer` injects a `MetaServiceGrpcClient` to call `RegisterNode` for each discovered `StorageNodeInfo`. `ReadIndex` introduces a new `kReadIndex` log entry type that the state machine treats as a no-op barrier, providing linearizable reads without native braft `read_index` API support.

**Tech Stack:** C++17, CMake, gRPC, protobuf, braft (vendored), googletest, Apple Clang 17 / Linux GCC 11+

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `include/cedar/dtx/dtx_service_impl.h` | Modify | Add `ParticipantRecord`, participant map, mutex, and log path members to `DTXServiceImpl` |
| `src/dtx/dtx_service_impl.cc` | Modify | Implement `RegisterParticipant` with validation, in-memory registry, and `PersistParticipantRegistration` |
| `tests/dtx/test_register_participant.cc` | Create | TDD unit tests for RegisterParticipant (success, duplicate, persistence round-trip) |
| `src/dtx/service_discovery.h` | Modify | Add `#include "cedar/dtx/meta_service_grpc.h"` and `std::unique_ptr<MetaServiceGrpcClient>` to `ClusterInitializer` |
| `src/dtx/service_discovery.cc` | Modify | Replace hardcoded `success = true` in `RegisterStorageNodes` with real `RegisterNode` RPC via `MetaServiceGrpcClient` |
| `tests/dtx/test_cluster_initializer_registration.cc` | Create | TDD unit tests that spin up a real `MetaServiceGrpcServer` and verify nodes are actually registered |
| `include/cedar/dtx/storage/braft_partition_raft.h` | Modify | Add `kReadIndex = 7` to `StorageLogEntry::Type` |
| `src/dtx/storage/braft_partition_raft.cc` | Modify | Update `Serialize`/`Deserialize` for `kReadIndex`; update `on_apply` to handle `kReadIndex` as no-op before storage null-check; implement `ReadIndex` via log-appending barrier |
| `tests/dtx/test_braft_read_index.cc` | Create | TDD unit tests for `ReadIndex` on a single-node braft cluster |
| `tests/CMakeLists.txt` | Modify | Add the three new test executables and link targets |

---

## Task 1: Implement DTXServiceImpl::RegisterParticipant

**Files:**
- Modify: `include/cedar/dtx/dtx_service_impl.h`
- Modify: `src/dtx/dtx_service_impl.cc`
- Create: `tests/dtx/test_register_participant.cc`
- Modify: `tests/CMakeLists.txt`

---

- [ ] **Step 1: Write the failing test**

Create `tests/dtx/test_register_participant.cc`:

```cpp
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

#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>

#include "cedar/dtx/dtx_service_impl.h"

using namespace cedar;
using namespace cedar::dtx;

class RegisterParticipantTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = "/tmp/cedar_test_register_participant_" +
                std::to_string(getpid()) + "_" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(test_dir_);
  }

  void TearDown() override {
    std::filesystem::remove_all(test_dir_);
  }

  std::string test_dir_;
};

TEST_F(RegisterParticipantTest, BasicRegistrationReturnsSuccess) {
  DTXServiceImpl service(nullptr, nullptr);
  service.SetParticipantLogPath(test_dir_);

  grpc::ServerContext context;
  cedar::dtx::RegisterRequest request;
  request.set_txn_id("txn-123");
  request.set_participant_id("part-1");
  request.set_endpoint("127.0.0.1:50051");
  request.set_role(cedar::dtx::RegisterRequest::PARTICIPANT);

  cedar::dtx::RegisterResponse response;
  auto grpc_status = service.RegisterParticipant(&context, &request, &response);

  EXPECT_TRUE(grpc_status.ok()) << grpc_status.error_message();
  EXPECT_TRUE(response.success());
  EXPECT_EQ(response.error_msg(), "");
  EXPECT_EQ(response.assigned_id(), "part-1");
}

TEST_F(RegisterParticipantTest, DuplicateRegistrationAllowed) {
  DTXServiceImpl service(nullptr, nullptr);
  service.SetParticipantLogPath(test_dir_);

  grpc::ServerContext context;
  cedar::dtx::RegisterRequest request;
  request.set_txn_id("txn-456");
  request.set_participant_id("part-A");
  request.set_endpoint("127.0.0.1:50052");
  request.set_role(cedar::dtx::RegisterRequest::PARTICIPANT);

  cedar::dtx::RegisterResponse response1;
  EXPECT_TRUE(service.RegisterParticipant(&context, &request, &response1).ok());
  EXPECT_TRUE(response1.success());

  cedar::dtx::RegisterResponse response2;
  EXPECT_TRUE(service.RegisterParticipant(&context, &request, &response2).ok());
  EXPECT_TRUE(response2.success());
}

TEST_F(RegisterParticipantTest, MissingTxnIdReturnsInvalidArgument) {
  DTXServiceImpl service(nullptr, nullptr);

  grpc::ServerContext context;
  cedar::dtx::RegisterRequest request;
  request.set_participant_id("part-1");
  request.set_endpoint("127.0.0.1:50051");

  cedar::dtx::RegisterResponse response;
  auto grpc_status = service.RegisterParticipant(&context, &request, &response);

  EXPECT_EQ(grpc_status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  EXPECT_FALSE(response.success());
}

TEST_F(RegisterParticipantTest, RegistrationPersistedToLogFile) {
  DTXServiceImpl service(nullptr, nullptr);
  service.SetParticipantLogPath(test_dir_);

  grpc::ServerContext context;
  cedar::dtx::RegisterRequest request;
  request.set_txn_id("txn-789");
  request.set_participant_id("part-X");
  request.set_endpoint("127.0.0.1:50053");
  request.set_role(cedar::dtx::RegisterRequest::COORDINATOR);

  cedar::dtx::RegisterResponse response;
  ASSERT_TRUE(service.RegisterParticipant(&context, &request, &response).ok());

  // Verify log file was created and contains the record
  std::string log_path = test_dir_ + "/participant_registry.log";
  ASSERT_TRUE(std::filesystem::exists(log_path));

  std::ifstream ifs(log_path);
  ASSERT_TRUE(ifs);
  std::string line;
  ASSERT_TRUE(std::getline(ifs, line));
  EXPECT_NE(line.find("txn-789"), std::string::npos);
  EXPECT_NE(line.find("part-X"), std::string::npos);
  EXPECT_NE(line.find("127.0.0.1:50053"), std::string::npos);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
```

Add to `tests/CMakeLists.txt` (append near the other DTX tests):

```cmake
add_executable(test_register_participant dtx/test_register_participant.cc)
target_link_libraries(test_register_participant ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_register_participant)
```

- [ ] **Step 2: Run the failing test**

```bash
cd build && cmake --build . --target test_register_participant -j$(sysctl -n hw.ncpu)
./tests/test_register_participant
```

**Expected output:**
```
[  FAILED  ] RegisterParticipantTest.BasicRegistrationReturnsSuccess
... error: 'class cedar::dtx::DTXServiceImpl' has no member named 'SetParticipantLogPath'
[  FAILED  ] RegisterParticipantTest.RegistrationPersistedToLogFile
... same error
```

- [ ] **Step 3: Add participant registry to DTXServiceImpl header**

Modify `include/cedar/dtx/dtx_service_impl.h` — add includes and members:

```cpp
#ifndef CEDAR_DTX_SERVICE_IMPL_H_
#define CEDAR_DTX_SERVICE_IMPL_H_

#include "cedar/dtx/cross_dc_replicator.h"
#include "dtx_protocol.grpc.pb.h"
#include "cedar/storage/cedar_graph_storage.h"
#include <grpcpp/grpcpp.h>

// NEW includes for participant registry
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace cedar {
namespace dtx {

// Forward declaration to avoid circular include
class StorageServiceImpl;

// NEW: participant record for in-memory registry
struct ParticipantRecord {
  std::string participant_id;
  std::string endpoint;
  cedar::dtx::RegisterRequest::Role role;
};

class DTXServiceImpl final : public cedar::dtx::DTXService::Service {
 public:
  explicit DTXServiceImpl(cedar::CedarGraphStorage* storage,
                         cedar::dtx::StorageServiceImpl* storage_service = nullptr);

  void SetCrossDCReplicator(CrossDCReplicator* replicator) {
    cross_dc_replicator_ = replicator;
  }

  // NEW: set the directory where participant registration logs are persisted
  void SetParticipantLogPath(const std::string& path) {
    participant_log_path_ = path;
  }

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
  ::grpc::Status RegisterParticipant(::grpc::ServerContext* context,
                                     const cedar::dtx::RegisterRequest* request,
                                     cedar::dtx::RegisterResponse* response) override;
  ::grpc::Status Replicate(::grpc::ServerContext* context,
                           const cedar::dtx::ReplicateRequest* request,
                           cedar::dtx::ReplicateResponse* response) override;
  ::grpc::Status ApplyReplication(
      ::grpc::ServerContext* context,
      ::grpc::ServerReader<cedar::dtx::ApplyReplicationRequest>* reader,
      cedar::dtx::ApplyReplicationResponse* response) override;

 private:
  cedar::CedarGraphStorage* storage_;
  cedar::dtx::StorageServiceImpl* storage_service_ = nullptr;
  CrossDCReplicator* cross_dc_replicator_ = nullptr;

  Status ApplySingleLog(const cedar::dtx::ReplicationLogEntry& log_entry);

  // NEW: participant registry
  mutable std::mutex participants_mutex_;
  std::unordered_map<std::string, std::vector<ParticipantRecord>> participants_;
  std::string participant_log_path_;

  Status PersistParticipantRegistration(const cedar::dtx::RegisterRequest& request);
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_SERVICE_IMPL_H_
```

- [ ] **Step 4: Implement RegisterParticipant and persistence**

Replace the `RegisterParticipant` stub in `src/dtx/dtx_service_impl.cc` (lines 318–325) and add `PersistParticipantRegistration`:

```cpp
::grpc::Status DTXServiceImpl::RegisterParticipant(::grpc::ServerContext* context,
                                                   const cedar::dtx::RegisterRequest* request,
                                                   cedar::dtx::RegisterResponse* response) {
  (void)context;

  // Validate required fields
  if (request->txn_id().empty()) {
    response->set_success(false);
    response->set_error_msg("txn_id is required");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "txn_id is required");
  }
  if (request->participant_id().empty()) {
    response->set_success(false);
    response->set_error_msg("participant_id is required");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "participant_id is required");
  }

  // Build record
  ParticipantRecord record;
  record.participant_id = request->participant_id();
  record.endpoint = request->endpoint();
  record.role = request->role();

  // Add to in-memory registry
  {
    std::lock_guard<std::mutex> lock(participants_mutex_);
    participants_[request->txn_id()].push_back(record);
  }

  // Persist to decision log (best-effort; do not fail RPC on log write failure)
  auto persist_status = PersistParticipantRegistration(*request);
  if (!persist_status.ok()) {
    std::cerr << "[DTXServiceImpl] Failed to persist participant registration for txn="
              << request->txn_id() << ": " << persist_status.ToString() << std::endl;
  }

  response->set_success(true);
  response->set_assigned_id(request->participant_id());
  return ::grpc::Status::OK;
}

Status DTXServiceImpl::PersistParticipantRegistration(
    const cedar::dtx::RegisterRequest& request) {
  if (participant_log_path_.empty()) {
    return Status::OK();  // persistence disabled
  }

  std::string log_file = participant_log_path_ + "/participant_registry.log";
  std::ofstream ofs(log_file, std::ios::app);
  if (!ofs) {
    return Status::IOError("Cannot open participant log: " + log_file);
  }

  // Format: txn_id|participant_id|endpoint|role|timestamp_us
  auto now_us = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count();
  ofs << request.txn_id() << "|"
      << request.participant_id() << "|"
      << request.endpoint() << "|"
      << static_cast<int>(request.role()) << "|"
      << now_us << "\n";
  ofs.flush();
  if (!ofs) {
    return Status::IOError("Failed to write participant log");
  }
  return Status::OK();
}
```

Make sure the new includes are present at the top of `src/dtx/dtx_service_impl.cc`:

```cpp
#include "cedar/dtx/storage_service_impl.h"
#include "cedar/dtx/dtx_service_impl.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include <iostream>
#include "storage_service.pb.h"

#include <functional>
#include <string>

// NEW includes for participant persistence
#include <chrono>
#include <filesystem>
#include <fstream>
#include <mutex>
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cd build && cmake --build . --target test_register_participant -j$(sysctl -n hw.ncpu)
./tests/test_register_participant
```

**Expected output:**
```
[==========] Running 4 tests from 1 test suite
[----------] Global test environment set-up
[----------] 4 tests from RegisterParticipantTest
[ RUN      ] RegisterParticipantTest.BasicRegistrationReturnsSuccess
[       OK ] RegisterParticipantTest.BasicRegistrationReturnsSuccess (0 ms)
[ RUN      ] RegisterParticipantTest.DuplicateRegistrationAllowed
[       OK ] RegisterParticipantTest.DuplicateRegistrationAllowed (0 ms)
[ RUN      ] RegisterParticipantTest.MissingTxnIdReturnsInvalidArgument
[       OK ] RegisterParticipantTest.MissingTxnIdReturnsInvalidArgument (0 ms)
[ RUN      ] RegisterParticipantTest.RegistrationPersistedToLogFile
[       OK ] RegisterParticipantTest.RegistrationPersistedToLogFile (0 ms)
[==========] 4 tests from 1 test suite ran (2 ms total)
[  PASSED  ] 4 tests.
```

- [ ] **Step 6: Commit**

```bash
git add include/cedar/dtx/dtx_service_impl.h src/dtx/dtx_service_impl.cc \
       tests/dtx/test_register_participant.cc tests/CMakeLists.txt
git commit -m "feat(dtx): implement RegisterParticipant with registry and decision log persistence

- Add mutex-protected participant map to DTXServiceImpl
- Persist registrations to append-only text log
- Validate required fields (txn_id, participant_id)
- Add unit tests for success, duplicate, missing fields, and persistence"
```

---

## Task 2: Replace ClusterInitializer Hardcoded Success with Real MetaService RPC

**Files:**
- Modify: `src/dtx/service_discovery.h`
- Modify: `src/dtx/service_discovery.cc`
- Create: `tests/dtx/test_cluster_initializer_registration.cc`
- Modify: `tests/CMakeLists.txt`

---

- [ ] **Step 1: Write the failing test**

Create `tests/dtx/test_cluster_initializer_registration.cc`:

```cpp
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

#include <gtest/gtest.h>
#include "cedar/dtx/service_discovery.h"
#include "cedar/dtx/meta_service.h"
#include "cedar/dtx/meta_service_grpc.h"

using namespace cedar;
using namespace cedar::dtx;

class ClusterInitializerRegistrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    MetaServiceConfig config;
    config.node_id = 1;
    config.listen_address = "127.0.0.1:19560";
    config.advertise_address = "127.0.0.1:19560";
    config.test_mode = true;

    auto status = meta_service_.Initialize(config);
    ASSERT_TRUE(status.ok()) << status.ToString();

    auto grpc_status = grpc_server_.Start("127.0.0.1:19560", &meta_service_);
    ASSERT_TRUE(grpc_status.ok()) << grpc_status.ToString();
  }

  void TearDown() override {
    grpc_server_.Stop();
    meta_service_.Shutdown();
  }

  MetadataService meta_service_;
  MetaServiceGrpcServer grpc_server_;
};

TEST_F(ClusterInitializerRegistrationTest, RegisterStorageNodesViaRealRpc) {
  ClusterInitializer::Config init_config;
  init_config.meta_servers = {"127.0.0.1:19560"};
  init_config.auto_discover_storaged = false;

  ClusterInitializer initializer(init_config);

  // Manually construct some storage nodes to register
  std::vector<StorageNodeInfo> nodes;
  StorageNodeInfo n1;
  n1.host = "10.0.0.1";
  n1.port = 9779;
  n1.ip_address = "10.0.0.1";
  n1.is_healthy = true;
  nodes.push_back(n1);

  StorageNodeInfo n2;
  n2.host = "10.0.0.2";
  n2.port = 9779;
  n2.ip_address = "10.0.0.2";
  n2.is_healthy = true;
  nodes.push_back(n2);

  auto status = initializer.RegisterStorageNodes(nodes);
  EXPECT_TRUE(status.ok()) << status.ToString();

  // Verify nodes were actually registered by querying MetaService
  auto alive = meta_service_.GetAliveNodes();
  EXPECT_EQ(alive.size(), 2u) << "Expected 2 nodes registered in MetaService";
}

TEST_F(ClusterInitializerRegistrationTest, RegisterEmptyNodesReturnsError) {
  ClusterInitializer::Config init_config;
  init_config.meta_servers = {"127.0.0.1:19560"};
  init_config.auto_discover_storaged = false;

  ClusterInitializer initializer(init_config);

  std::vector<StorageNodeInfo> nodes;
  auto status = initializer.RegisterStorageNodes(nodes);
  EXPECT_FALSE(status.ok()) << "Expected error for empty node list";
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
```

Add to `tests/CMakeLists.txt`:

```cmake
add_executable(test_cluster_initializer_registration dtx/test_cluster_initializer_registration.cc)
target_link_libraries(test_cluster_initializer_registration ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_cluster_initializer_registration)
```

- [ ] **Step 2: Run the failing test**

```bash
cd build && cmake --build . --target test_cluster_initializer_registration -j$(sysctl -n hw.ncpu)
./tests/test_cluster_initializer_registration
```

**Expected output:**
```
[  FAILED  ] ClusterInitializerRegistrationTest.RegisterStorageNodesViaRealRpc
Expected equality of these values:
  alive.size()
    Which is: 0
  2u
    Which is: 2
Expected 2 nodes registered in MetaService
```

The hardcoded `success = true` does not actually register nodes.

- [ ] **Step 3: Add MetaServiceGrpcClient to ClusterInitializer header**

Modify `src/dtx/service_discovery.h`:

1. Add the include after `#include "cedar/core/status.h"`:

```cpp
#include "cedar/core/status.h"
#include "cedar/dtx/meta_service_grpc.h"  // NEW
```

2. Add the member to `ClusterInitializer` private section:

```cpp
 private:
  Config config_;
  std::unique_ptr<ServiceDiscovery> service_discovery_;
  std::unique_ptr<MetaServiceGrpcClient> meta_client_;  // NEW
};
```

- [ ] **Step 4: Implement real RegisterNode RPC in RegisterStorageNodes**

Replace the body of `ClusterInitializer::RegisterStorageNodes` in `src/dtx/service_discovery.cc` (lines 595–630) with:

```cpp
Status ClusterInitializer::RegisterStorageNodes(const std::vector<StorageNodeInfo>& nodes) {
  std::cerr << "[ClusterInitializer] Registering storage nodes..." << std::endl;

  if (nodes.empty()) {
    return Status::InvalidArgument("No storage nodes to register");
  }

  // Lazily initialize MetaServiceGrpcClient on first registration attempt
  if (!meta_client_ && !config_.meta_servers.empty()) {
    meta_client_ = std::make_unique<MetaServiceGrpcClient>();
    auto connect_status = meta_client_->Connect(config_.meta_servers);
    if (!connect_status.ok()) {
      meta_client_.reset();
      return Status::IOError("Failed to connect to MetaD: " + connect_status.ToString());
    }
  }

  int success_count = 0;
  for (const auto& node : nodes) {
    std::cerr << "  Registering: " << node.GetEndpoint() << " ... " << std::flush;

    if (!meta_client_) {
      std::cerr << "SKIPPED (no MetaD connection)" << std::endl;
      continue;
    }

    // Convert StorageNodeInfo -> NodeInfo for MetaService RPC
    NodeInfo info;
    info.node_id = static_cast<NodeID>(std::hash<std::string>{}(node.GetEndpoint()) & 0x7FFFFFFF);
    info.address = node.ip_address.empty() ? node.GetEndpoint() : node.ip_address;
    info.data_path = "/data/cedar";
    info.state = NodeInfo::State::kOnline;

    auto status = meta_client_->RegisterNode(info);
    if (status.ok()) {
      std::cerr << "OK" << std::endl;
      success_count++;
    } else {
      std::cerr << "FAILED: " << status.ToString() << std::endl;
    }
  }

  std::cerr << "[ClusterInitializer] Registered " << success_count << "/"
            << nodes.size() << " nodes" << std::endl;

  return success_count > 0 ? Status::OK()
                           : Status::IOError("Failed to register any nodes");
}
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cd build && cmake --build . --target test_cluster_initializer_registration -j$(sysctl -n hw.ncpu)
./tests/test_cluster_initializer_registration
```

**Expected output:**
```
[==========] Running 2 tests from 1 test suite
[----------] 2 tests from ClusterInitializerRegistrationTest
[ RUN      ] ClusterInitializerRegistrationTest.RegisterStorageNodesViaRealRpc
[       OK ] ClusterInitializerRegistrationTest.RegisterStorageNodesViaRealRpc (45 ms)
[ RUN      ] ClusterInitializerRegistrationTest.RegisterEmptyNodesReturnsError
[       OK ] ClusterInitializerRegistrationTest.RegisterEmptyNodesReturnsError (0 ms)
[==========] 2 tests ran (48 ms total)
[  PASSED  ] 2 tests.
```

- [ ] **Step 6: Commit**

```bash
git add src/dtx/service_discovery.h src/dtx/service_discovery.cc \
       tests/dtx/test_cluster_initializer_registration.cc tests/CMakeLists.txt
git commit -m "feat(dtx): replace ClusterInitializer simulated registration with real MetaService RPC

- Add MetaServiceGrpcClient to ClusterInitializer
- Convert StorageNodeInfo to NodeInfo and call RegisterNode per node
- Return error when no nodes are provided or all registrations fail
- Add unit tests against real MetaServiceGrpcServer"
```

---

## Task 3: Implement BraftPartitionNode::ReadIndex via Log-Appending Barrier

**Files:**
- Modify: `include/cedar/dtx/storage/braft_partition_raft.h`
- Modify: `src/dtx/storage/braft_partition_raft.cc`
- Create: `tests/dtx/test_braft_read_index.cc`
- Modify: `tests/CMakeLists.txt`

---

- [ ] **Step 1: Write the failing test**

Create `tests/dtx/test_braft_read_index.cc`:

```cpp
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

#include <gtest/gtest.h>
#include <filesystem>
#include <thread>

#include "cedar/dtx/storage/braft_partition_raft.h"

using namespace cedar;
using namespace cedar::dtx;

class BraftReadIndexTest : public ::testing::Test {
 protected:
  std::string test_dir_;

  void SetUp() override {
    test_dir_ = "/tmp/cedar_test_read_index_" + std::to_string(getpid()) + "_" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::remove_all(test_dir_);
    std::filesystem::create_directories(test_dir_);
  }

  void TearDown() override {
    std::filesystem::remove_all(test_dir_);
  }
};

TEST_F(BraftReadIndexTest, ReadIndexReturnsCommittedIndexOnLeader) {
  BraftPartitionNode node;
  BraftPartitionNode::Options options;
  options.partition_id = 999;
  options.node_id = 1;
  options.listen_address = "127.0.0.1:14444";
  options.data_path = test_dir_;
  options.initial_peers = {"127.0.0.1:14444"};
  options.peer_node_ids = {{"127.0.0.1:14444", 1}};
  options.election_timeout_ms = 300;

  auto status = node.Init(options, nullptr);
  ASSERT_TRUE(status.ok()) << status.ToString();

  // Wait for leader election (single-node cluster)
  bool became_leader = false;
  for (int i = 0; i < 50; ++i) {
    if (node.IsLeader()) {
      became_leader = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_TRUE(became_leader) << "Node did not become leader in time";

  // Call ReadIndex — should return a positive committed index
  auto result = node.ReadIndex(std::chrono::seconds(5));
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_GT(result.value(), 0u) << "ReadIndex should return a positive committed index";

  node.Shutdown();
}

TEST_F(BraftReadIndexTest, ReadIndexOnNonLeaderReturnsNotLeader) {
  // We cannot easily test a non-leader without a multi-node cluster,
  // but we can at least verify the API returns an error when the node
  // is not initialized or not leader.
  BraftPartitionNode node;
  auto result = node.ReadIndex(std::chrono::seconds(1));
  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsNotLeader() || result.status().IsIOError())
      << "Expected NotLeader or IOError, got: " << result.status().ToString();
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
```

Add to `tests/CMakeLists.txt`:

```cmake
add_executable(test_braft_read_index dtx/test_braft_read_index.cc)
target_link_libraries(test_braft_read_index ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_braft_read_index)
```

- [ ] **Step 2: Run the failing test**

```bash
cd build && cmake --build . --target test_braft_read_index -j$(sysctl -n hw.ncpu)
./tests/test_braft_read_index
```

**Expected output:**
```
[  FAILED  ] BraftReadIndexTest.ReadIndexReturnsCommittedIndexOnLeader
... value of: result.ok()
  actual: false
expected: true
ReadIndex not supported in this braft version
```

- [ ] **Step 3: Add kReadIndex to StorageLogEntry::Type and update serialization**

Modify `include/cedar/dtx/storage/braft_partition_raft.h` — add `kReadIndex`:

```cpp
struct StorageLogEntry {
  enum class Type : uint8_t {
    kPut = 1,
    kDelete = 2,
    kBatch = 3,
    kPrepare = 4,
    kCommit = 5,
    kAbort = 6,
    kReadIndex = 7,  // NEW: no-op barrier for linearizable reads
  };
  // ... rest unchanged
};
```

Modify `src/dtx/storage/braft_partition_raft.cc`:

1. Update `StorageLogEntry::Serialize` — add `kReadIndex` minimal serialization after the type byte:

```cpp
std::string StorageLogEntry::Serialize() const {
  std::string data;
  // type: 1 byte
  data.push_back(static_cast<char>(type));

  // NEW: kReadIndex is a minimal no-op barrier
  if (type == Type::kReadIndex) {
    return data;
  }

  // 2PC entries use different serialization
  if (type == Type::kPrepare || type == Type::kCommit || type == Type::kAbort) {
    // ... existing 2PC serialization unchanged
  }
  // ... rest of existing serialization unchanged
}
```

2. Update `StorageLogEntry::Deserialize` — add `kReadIndex` minimal deserialization after reading the type byte:

```cpp
StatusOr<StorageLogEntry> StorageLogEntry::Deserialize(
    const std::string& data) {
  if (data.size() < 1) {
    return Status::InvalidArgument("Empty log entry");
  }

  StorageLogEntry entry;
  size_t pos = 0;

  // type
  entry.type = static_cast<Type>(data[pos++]);

  // NEW: kReadIndex is a minimal no-op barrier
  if (entry.type == Type::kReadIndex) {
    return entry;
  }

  // 2PC entries
  if (entry.type == Type::kPrepare || entry.type == Type::kCommit || entry.type == Type::kAbort) {
    // ... existing 2PC deserialization unchanged
  }
  // ... rest of existing deserialization unchanged
}
```

- [ ] **Step 4: Update state machine to handle kReadIndex as no-op**

In `src/dtx/storage/braft_partition_raft.cc`, modify `StoragePartitionStateMachine::on_apply` to handle `kReadIndex` **before** the `storage_` null check:

```cpp
void StoragePartitionStateMachine::on_apply(braft::Iterator& iter) {
  for (; iter.valid(); iter.next()) {
    braft::AsyncClosureGuard closure_guard(iter.done());

    std::string data = iter.data().to_string();
    if (data.size() < 1) {
      LOG(ERROR) << "Corrupt log entry: too small at index=" << iter.index()
                 << " — stepping down";
      iter.set_error_and_rollback();
      return;
    }

    auto entry_result = StorageLogEntry::Deserialize(data);
    if (!entry_result.ok()) {
      LOG(ERROR) << "Failed to deserialize log entry: "
                 << entry_result.status().ToString()
                 << " — stepping down";
      iter.set_error_and_rollback();
      return;
    }

    const auto& entry = entry_result.value();

    // NEW: Handle read-index barrier before storage null-check — it needs no storage
    if (entry.type == StorageLogEntry::Type::kReadIndex) {
      last_term_ = iter.term();
      continue;  // no-op barrier
    }

    if (!storage_) {
      LOG(ERROR) << "No storage available for apply at index=" << iter.index();
      iter.set_error_and_rollback();
      return;
    }
    if (entry.type == StorageLogEntry::Type::kPut) {
      // ... rest of existing on_apply unchanged
    }
    // ... rest unchanged
  }
}
```

- [ ] **Step 5: Implement ReadIndex via log-appending barrier**

Replace `BraftPartitionNode::Impl::ReadIndex` in `src/dtx/storage/braft_partition_raft.cc` (lines 797–802) with:

```cpp
StatusOr<uint64_t> ReadIndex(std::chrono::milliseconds timeout) {
  // Step 1: Validate leadership
  {
    std::lock_guard<std::mutex> lock(node_mutex_);
    if (!node_ || !node_->is_leader()) {
      return Status::NotLeader("Not leader");
    }
  }

  // Step 2: Propose a read-index barrier entry
  StorageLogEntry entry;
  entry.type = StorageLogEntry::Type::kReadIndex;

  auto propose_status = Propose(entry);
  if (!propose_status.ok()) {
    return propose_status;
  }

  // Step 3: Get the committed index after our barrier is committed
  uint64_t committed_index = 0;
  {
    std::lock_guard<std::mutex> lock(node_mutex_);
    if (!node_) {
      return Status::IOError("Node not initialized");
    }
    braft::NodeStatus node_status;
    node_->get_status(&node_status);
    committed_index = static_cast<uint64_t>(node_status.committed_index);
  }

  // Step 4: Wait for the local state machine to apply up to committed_index
  auto wait_status = WaitForApplied(committed_index, timeout);
  if (!wait_status.ok()) {
    return wait_status;
  }

  return committed_index;
}
```

- [ ] **Step 6: Run tests to verify they pass**

```bash
cd build && cmake --build . --target test_braft_read_index -j$(sysctl -n hw.ncpu)
./tests/test_braft_read_index
```

**Expected output:**
```
[==========] Running 2 tests from 1 test suite
[----------] 2 tests from BraftReadIndexTest
[ RUN      ] BraftReadIndexTest.ReadIndexReturnsCommittedIndexOnLeader
[       OK ] BraftReadIndexTest.ReadIndexReturnsCommittedIndexOnLeader (5203 ms)
[ RUN      ] BraftReadIndexTest.ReadIndexOnNonLeaderReturnsNotLeader
[       OK ] BraftReadIndexTest.ReadIndexOnNonLeaderReturnsNotLeader (0 ms)
[==========] 2 tests ran (5204 ms total)
[  PASSED  ] 2 tests.
```

- [ ] **Step 7: Commit**

```bash
git add include/cedar/dtx/storage/braft_partition_raft.h \
       src/dtx/storage/braft_partition_raft.cc \
       tests/dtx/test_braft_read_index.cc tests/CMakeLists.txt
git commit -m "feat(raft): implement BraftPartitionNode::ReadIndex via log-appending barrier

- Add kReadIndex log entry type (minimal serialization, no storage needed)
- Handle kReadIndex in StoragePartitionStateMachine::on_apply as no-op barrier
- Implement ReadIndex by proposing barrier, waiting for commit+apply
- Provides linearizable reads without native braft read_index API"
```

---

## Regression & Integration Verification

After all three tasks are committed, run the full DTX test suite to ensure no regressions.

- [ ] **Step 1: Build all DTX tests**

```bash
cd build && cmake --build . --target cedar_tests -j$(sysctl -n hw.ncpu)
```

- [ ] **Step 2: Run DTX-specific tests**

```bash
cd build && ctest --output-on-failure -R "test_register_participant|test_cluster_initializer_registration|test_braft_read_index|test_dtx_raft_critical|test_meta_service|test_meta_service_grpc_client"
```

**Expected output:**
```
Test project <repo-root>/build
    Start 1: test_register_participant
1/6   Test #1: test_register_participant .........................   Passed    0.02 sec
    Start 2: test_cluster_initializer_registration
2/6   Test #2: test_cluster_initializer_registration .............   Passed    0.05 sec
    Start 3: test_braft_read_index
3/6   Test #3: test_braft_read_index .............................   Passed    5.21 sec
    Start 4: test_dtx_raft_critical
4/6   Test #4: test_dtx_raft_critical ............................   Passed    0.01 sec
    Start 5: test_meta_service
5/6   Test #5: test_meta_service .................................   Passed    0.03 sec
    Start 6: test_meta_service_grpc_client
6/6   Test #6: test_meta_service_grpc_client .....................   Passed    0.04 sec

100% tests passed, 0 tests failed out of 6
```

- [ ] **Step 3: Full test suite**

```bash
cd build && ctest --output-on-failure
```

**Expected:** all previously passing tests still pass (763/763 baseline).

---

## Self-Review

**1. Spec coverage:**

| Requirement | Task |
|-------------|------|
| `DTXServiceImpl::RegisterParticipant` returns UNIMPLEMENTED → implement actual registration with participant map + decision log | Task 1 (Steps 3–4) |
| `ClusterInitializer::RegisterStorageNodes` hardcodes `success = true` → real MetaService gRPC | Task 2 (Steps 3–4) |
| `BraftPartitionNode::ReadIndex()` returns NotSupported → safe log-appending barrier | Task 3 (Steps 3–5) |

All three gaps are covered. No missing requirements.

**2. Placeholder scan:**

- No "TBD", "TODO", "implement later", or "fill in details" found.
- No vague descriptions like "add appropriate error handling" — every validation and error path is explicit in the code.
- No "Similar to Task N" shortcuts — each task repeats its complete code.
- All test commands include expected output.

**3. Type consistency:**

- `ParticipantRecord::role` uses `cedar::dtx::RegisterRequest::Role` consistently.
- `StorageLogEntry::Type::kReadIndex = 7` is used in serialize, deserialize, on_apply, and ReadIndex.
- `MetaServiceGrpcClient::RegisterNode` takes `NodeInfo` — conversion from `StorageNodeInfo` is explicit.
- Method names match across header and implementation: `SetParticipantLogPath`, `PersistParticipantRegistration`, `RegisterParticipant`.

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-05-26-subplan-3-dtx-hardening.md`.**

**Two execution options:**

**1. Subagent-Driven (recommended)** — Dispatch a fresh subagent per task, review between tasks, fast iteration. Use `superpowers:subagent-driven-development` skill.

**2. Inline Execution** — Execute tasks in this session using `superpowers:executing-plans` skill, batch execution with checkpoints for review.

**Which approach?**
