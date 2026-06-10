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
