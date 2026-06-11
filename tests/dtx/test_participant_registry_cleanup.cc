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
#include <chrono>
#include <filesystem>
#include <fstream>
#include <unistd.h>

#include "cedar/dtx/dtx_service_impl.h"
#include "cedar/storage/cedar_graph_storage.h"

using namespace cedar;
using namespace cedar::dtx;

class ParticipantRegistryCleanupTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = "/tmp/cedar_test_registry_cleanup_" +
                std::to_string(getpid()) + "_" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(test_dir_);
  }

  void TearDown() override { std::filesystem::remove_all(test_dir_); }

  std::string test_dir_;
};

TEST_F(ParticipantRegistryCleanupTest, CommitRemovesParticipants) {
  // Arrange: create a DTXServiceImpl with no storage backend
  // We only need the participant registry, so nullptr is acceptable here.
  DTXServiceImpl service(nullptr, nullptr);
  service.SetParticipantLogPath(test_dir_);

  std::string txn_id = "txn-cleanup-001";

  // Register 3 participants
  for (int i = 0; i < 3; ++i) {
    cedar::dtx::RegisterRequest req;
    req.set_txn_id(txn_id);
    req.set_participant_id("p" + std::to_string(i));
    req.set_endpoint("127.0.0.1:" + std::to_string(9000 + i));
    req.set_role(cedar::dtx::RegisterRequest::COORDINATOR);

    cedar::dtx::RegisterResponse resp;
    ::grpc::ServerContext ctx;
    auto status = service.RegisterParticipant(&ctx, &req, &resp);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(resp.success());
  }

  EXPECT_EQ(service.ParticipantCountForTest(txn_id), 3u);

  // Act: call Commit
  cedar::dtx::CommitRequest commit_req;
  commit_req.set_txn_id(txn_id);
  cedar::dtx::CommitResponse commit_resp;
  ::grpc::ServerContext commit_ctx;
  // Commit will return UNIMPLEMENTED because storage_service_ is nullptr,
  // but we still expect the cleanup to run.
  (void)service.Commit(&commit_ctx, &commit_req, &commit_resp);

  // Assert: registry should be empty for this txn
  EXPECT_EQ(service.ParticipantCountForTest(txn_id), 0u)
      << "Commit should clean up the in-memory participant registry";

  // Verify the participant log still contains the original 3 registrations
  std::ifstream log_file(test_dir_ + "/participant_registry.log");
  ASSERT_TRUE(log_file.is_open());
  std::string line;
  int log_entries = 0;
  while (std::getline(log_file, line)) {
    if (!line.empty()) ++log_entries;
  }
  EXPECT_EQ(log_entries, 3);

  // Re-registering the same txn_id after commit should still succeed
  cedar::dtx::RegisterRequest req2;
  req2.set_txn_id(txn_id);
  req2.set_participant_id("p-post-commit");
  req2.set_endpoint("127.0.0.1:9999");
  req2.set_role(cedar::dtx::RegisterRequest::COORDINATOR);
  cedar::dtx::RegisterResponse resp2;
  ::grpc::ServerContext ctx2;
  auto st2 = service.RegisterParticipant(&ctx2, &req2, &resp2);
  EXPECT_TRUE(st2.ok());
  EXPECT_TRUE(resp2.success());
}

TEST_F(ParticipantRegistryCleanupTest, AbortRemovesParticipants) {
  DTXServiceImpl service(nullptr, nullptr);
  service.SetParticipantLogPath(test_dir_);

  std::string txn_id = "txn-abort-001";

  for (int i = 0; i < 2; ++i) {
    cedar::dtx::RegisterRequest req;
    req.set_txn_id(txn_id);
    req.set_participant_id("p" + std::to_string(i));
    req.set_endpoint("127.0.0.1:" + std::to_string(8000 + i));
    req.set_role(cedar::dtx::RegisterRequest::PARTICIPANT);

    cedar::dtx::RegisterResponse resp;
    ::grpc::ServerContext ctx;
    auto status = service.RegisterParticipant(&ctx, &req, &resp);
    EXPECT_TRUE(status.ok());
  }

  EXPECT_EQ(service.ParticipantCountForTest(txn_id), 2u);

  cedar::dtx::AbortRequest abort_req;
  abort_req.set_txn_id(txn_id);
  cedar::dtx::AbortResponse abort_resp;
  ::grpc::ServerContext abort_ctx;
  (void)service.Abort(&abort_ctx, &abort_req, &abort_resp);

  EXPECT_EQ(service.ParticipantCountForTest(txn_id), 0u)
      << "Abort should clean up the in-memory participant registry";

  // Re-register after abort should succeed (proves no stale state blocks it)
  cedar::dtx::RegisterRequest req2;
  req2.set_txn_id(txn_id);
  req2.set_participant_id("p-post-abort");
  req2.set_endpoint("127.0.0.1:7777");
  req2.set_role(cedar::dtx::RegisterRequest::PARTICIPANT);
  cedar::dtx::RegisterResponse resp2;
  ::grpc::ServerContext ctx2;
  auto st2 = service.RegisterParticipant(&ctx2, &req2, &resp2);
  EXPECT_TRUE(st2.ok());
  EXPECT_TRUE(resp2.success());
}
