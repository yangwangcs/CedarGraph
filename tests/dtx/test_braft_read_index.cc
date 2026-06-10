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

#include <brpc/server.h>
#include <braft/raft.h>

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
  // Setup a brpc server so braft can attach its service.
  std::unique_ptr<brpc::Server> server;
  bool ready = false;
  int port = 0;

  for (int attempt = 0; attempt < 10; ++attempt) {
    port = 14444 + attempt;
    std::string addr = "127.0.0.1:" + std::to_string(port);

    auto srv = std::make_unique<brpc::Server>();
    if (braft::add_service(srv.get(), addr.c_str()) != 0) {
      continue;
    }
    if (srv->Start(addr.c_str(), nullptr) != 0) {
      continue;
    }

    BraftPartitionNode node;
    BraftPartitionNode::Options options;
    options.partition_id = 999;
    options.node_id = 1;
    options.listen_address = addr;
    options.data_path = test_dir_;
    options.initial_peers = {addr};
    options.peer_node_ids = {{addr, 1}};
    options.election_timeout_ms = 300;

    auto status = node.Init(options, nullptr);
    if (!status.ok()) {
      srv->Stop(0);
      srv->Join();
      continue;
    }

    // Wait for leader election (single-node cluster)
    bool became_leader = false;
    for (int i = 0; i < 50; ++i) {
      if (node.IsLeader()) {
        became_leader = true;
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    if (!became_leader) {
      node.Shutdown();
      srv->Stop(0);
      srv->Join();
      continue;
    }

    // Call ReadIndex — should return a positive committed index
    auto result = node.ReadIndex(std::chrono::seconds(5));
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    EXPECT_GT(result.value(), 0u) << "ReadIndex should return a positive committed index";

    node.Shutdown();
    srv->Stop(0);
    srv->Join();
    ready = true;
    break;
  }

  ASSERT_TRUE(ready) << "Failed to set up braft node after 10 attempts";
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
