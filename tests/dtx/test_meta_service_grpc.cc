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
#include "cedar/dtx/meta_service.h"
#include "cedar/dtx/meta_service_grpc.h"
#include "meta_service.pb.h"

using namespace cedar::dtx;

class MockMetadataService : public MetadataService {
 public:
  cedar::Status Heartbeat(const NodeStatus& status) override {
    captured_status = status;
    return cedar::Status::OK();
  }

  NodeStatus captured_status;
};

TEST(MetaServiceGrpcImpl, HeartbeatForwardsAllFields) {
  MockMetadataService mock_meta;
  MetaServiceGrpcImpl impl(&mock_meta);

  cedar::meta::HeartbeatRequest request;
  auto* status = request.mutable_status();
  status->set_node_id(42);
  status->set_cpu_usage_percent(50.0);
  status->set_memory_usage_percent(60.0);
  status->set_disk_usage_percent(70.0);
  status->set_qps(1000);
  status->set_latency_ms(5);
  status->add_leader_partitions(1);
  status->add_leader_partitions(3);
  status->add_follower_partitions(2);
  status->add_follower_partitions(4);
  status->set_timestamp_unix(1715000000);

  grpc::ServerContext context;
  cedar::meta::HeartbeatResponse response;
  auto grpc_status = impl.Heartbeat(&context, &request, &response);

  EXPECT_TRUE(grpc_status.ok());
  EXPECT_TRUE(response.success());

  EXPECT_EQ(mock_meta.captured_status.node_id, 42);
  EXPECT_DOUBLE_EQ(mock_meta.captured_status.cpu_usage_percent, 50.0);
  EXPECT_DOUBLE_EQ(mock_meta.captured_status.memory_usage_percent, 60.0);
  EXPECT_DOUBLE_EQ(mock_meta.captured_status.disk_usage_percent, 70.0);
  EXPECT_EQ(mock_meta.captured_status.qps, 1000);
  EXPECT_EQ(mock_meta.captured_status.latency_ms, 5);
  EXPECT_EQ(mock_meta.captured_status.leader_partitions.size(), 2);
  EXPECT_EQ(mock_meta.captured_status.leader_partitions[0], 1);
  EXPECT_EQ(mock_meta.captured_status.leader_partitions[1], 3);
  EXPECT_EQ(mock_meta.captured_status.follower_partitions.size(), 2);
  EXPECT_EQ(mock_meta.captured_status.follower_partitions[0], 2);
  EXPECT_EQ(mock_meta.captured_status.follower_partitions[1], 4);
  EXPECT_EQ(std::chrono::system_clock::to_time_t(mock_meta.captured_status.timestamp), 1715000000);
}

TEST(MetaServiceGrpcImpl, HeartbeatForwardsEmptyPartitionLists) {
  MockMetadataService mock_meta;
  MetaServiceGrpcImpl impl(&mock_meta);

  cedar::meta::HeartbeatRequest request;
  auto* status = request.mutable_status();
  status->set_node_id(7);
  status->set_cpu_usage_percent(10.0);
  status->set_memory_usage_percent(20.0);
  status->set_disk_usage_percent(30.0);
  status->set_qps(100);
  status->set_latency_ms(1);
  status->set_timestamp_unix(1715000001);

  grpc::ServerContext context;
  cedar::meta::HeartbeatResponse response;
  auto grpc_status = impl.Heartbeat(&context, &request, &response);

  EXPECT_TRUE(grpc_status.ok());
  EXPECT_TRUE(response.success());

  EXPECT_EQ(mock_meta.captured_status.node_id, 7);
  EXPECT_TRUE(mock_meta.captured_status.leader_partitions.empty());
  EXPECT_TRUE(mock_meta.captured_status.follower_partitions.empty());
  EXPECT_EQ(std::chrono::system_clock::to_time_t(mock_meta.captured_status.timestamp), 1715000001);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
