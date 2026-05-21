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
#include <brpc/server.h>
#include <braft/raft.h>
#include <butil/endpoint.h>
#include <butil/logging.h>

#include <chrono>
#include <filesystem>
#include <string>
#include <thread>

#include "cedar/dtx/storage/braft_partition_raft.h"
#include "cedar/dtx/raft/braft_node.h"

namespace cedar {
namespace dtx {

namespace fs = std::filesystem;

class CorruptLogRecoveryTest : public ::testing::Test {
 protected:
  std::string test_dir_ = "/tmp/test_corrupt_log_recovery";

  void SetUp() override {
    fs::remove_all(test_dir_);
    fs::create_directories(test_dir_);
  }

  void TearDown() override { fs::remove_all(test_dir_); }

  std::string LogUri(const std::string& subdir) {
    return "local://" + test_dir_ + "/" + subdir + "/log";
  }
  std::string MetaUri(const std::string& subdir) {
    return "local://" + test_dir_ + "/" + subdir + "/meta";
  }
  std::string SnapshotUri(const std::string& subdir) {
    return "local://" + test_dir_ + "/" + subdir + "/snapshot";
  }
};

// Helper: Poll until predicate is true or timeout.
template <typename Pred>
bool PollFor(std::chrono::milliseconds timeout, std::chrono::milliseconds interval,
             Pred pred) {
  auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (pred()) return true;
    std::this_thread::sleep_for(interval);
  }
  return pred();
}

TEST_F(CorruptLogRecoveryTest, StoragePartitionStateMachineStepsDown) {
  // Retry a few ports in case the first choice is transiently in use.
  std::unique_ptr<brpc::Server> server;
  std::string addr;
  bool ready = false;
  for (int attempt = 0; attempt < 10; ++attempt) {
    int port = 29999 + attempt;
    addr = "127.0.0.1:" + std::to_string(port);

    auto srv = std::make_unique<brpc::Server>();
    if (braft::add_service(srv.get(), addr.c_str()) != 0) {
      continue;
    }
    if (srv->Start(addr.c_str(), nullptr) != 0) {
      continue;
    }
    server = std::move(srv);
    ready = true;
    break;
  }
  ASSERT_TRUE(ready) << "Failed to start brpc server for braft";

  auto* fsm = new StoragePartitionStateMachine(nullptr);

  braft::NodeOptions options;
  options.election_timeout_ms = 300;
  options.fsm = fsm;
  options.node_owns_fsm = true;
  options.initial_conf.add_peer(braft::PeerId(addr));
  options.log_uri = LogUri("storage");
  options.raft_meta_uri = MetaUri("storage");
  options.snapshot_uri = SnapshotUri("storage");

  braft::Node node("test_storage_partition", braft::PeerId(addr));
  ASSERT_EQ(0, node.init(options));

  // Wait for the single-node cluster to elect itself leader.
  ASSERT_TRUE(PollFor(std::chrono::seconds(5), std::chrono::milliseconds(50),
                      [&]() { return node.is_leader(); }))
      << "Node did not become leader";

  // Apply a corrupt entry (empty data) that triggers the error path.
  butil::IOBuf data;
  braft::Task task;
  task.data = &data;
  node.apply(task);

  // Verify the node steps down instead of crashing.
  ASSERT_TRUE(PollFor(std::chrono::seconds(5), std::chrono::milliseconds(50),
                      [&]() { return !node.is_leader(); }))
      << "Node should have stepped down after corrupt log entry";

  node.shutdown(nullptr);
  node.join();
  server->Stop(0);
  server->Join();
}

TEST_F(CorruptLogRecoveryTest, MetaRaftStateMachineStepsDown) {
  std::unique_ptr<brpc::Server> server;
  std::string addr;
  bool ready = false;
  for (int attempt = 0; attempt < 10; ++attempt) {
    int port = 30099 + attempt;
    addr = "127.0.0.1:" + std::to_string(port);

    auto srv = std::make_unique<brpc::Server>();
    if (braft::add_service(srv.get(), addr.c_str()) != 0) {
      continue;
    }
    if (srv->Start(addr.c_str(), nullptr) != 0) {
      continue;
    }
    server = std::move(srv);
    ready = true;
    break;
  }
  ASSERT_TRUE(ready) << "Failed to start brpc server for braft";

  auto* fsm = new MetaRaftStateMachine(nullptr);

  braft::NodeOptions options;
  options.election_timeout_ms = 300;
  options.fsm = fsm;
  options.node_owns_fsm = true;
  options.initial_conf.add_peer(braft::PeerId(addr));
  options.log_uri = LogUri("meta");
  options.raft_meta_uri = MetaUri("meta");
  options.snapshot_uri = SnapshotUri("meta");

  braft::Node node("test_meta_service", braft::PeerId(addr));
  ASSERT_EQ(0, node.init(options));

  // Wait for the single-node cluster to elect itself leader.
  ASSERT_TRUE(PollFor(std::chrono::seconds(5), std::chrono::milliseconds(50),
                      [&]() { return node.is_leader(); }))
      << "Node did not become leader";

  // Apply a corrupt entry (1 byte — too small for meta command header).
  butil::IOBuf data;
  uint8_t byte = 0x01;
  data.append(&byte, 1);
  braft::Task task;
  task.data = &data;
  node.apply(task);

  // Verify the node steps down instead of crashing.
  ASSERT_TRUE(PollFor(std::chrono::seconds(5), std::chrono::milliseconds(50),
                      [&]() { return !node.is_leader(); }))
      << "Node should have stepped down after corrupt log entry";

  node.shutdown(nullptr);
  node.join();
  server->Stop(0);
  server->Join();
}

}  // namespace dtx
}  // namespace cedar
