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

#include <grpcpp/grpcpp.h>

#include "cedar/gcn/coordinator_client.h"

using namespace cedar::gcn;

TEST(CoordinatorClientTest, LocateReturnsWindow) {
  auto channel = grpc::CreateChannel("localhost:50051",
                                     grpc::InsecureChannelCredentials());
  CoordinatorClient client(channel);

  auto result = client.Locate(42, 1000);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->gcn_node_id, 7u);
  EXPECT_EQ(result->version, 3u);
}
