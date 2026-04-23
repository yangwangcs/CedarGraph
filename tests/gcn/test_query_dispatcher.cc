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

#include <limits>
#include <vector>

#include "cedar/gcn/query_dispatcher.h"
#include "cedar/gcn/tmv_engine.h"
#include "gcn_service.pb.h"

using namespace cedar::gcn;

TEST(QueryDispatcherTest, LocalHit) {
  TMVEngine engine(16);

  // Bootstrap vertex 42 with two outbound edges valid at t=1000
  std::vector<TMVEdge> edges;
  edges.push_back(
      {100, 500, std::numeric_limits<uint32_t>::max(), 0, 1, 0});
  edges.push_back(
      {200, 500, std::numeric_limits<uint32_t>::max(), 0, 1, 0});
  engine.BootstrapVertex(42, Direction::kOut, edges, false);

  QueryDispatcher dispatcher(&engine);

  TraversalRequest req;
  req.set_trace_id("trace-123");
  req.set_root_entity_id(42);
  req.set_query_time(1000);
  req.set_max_hops(3);

  TraversalResponse resp;
  grpc::Status status = dispatcher.DispatchTraversal(req, &resp);

  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(resp.success());
  EXPECT_EQ(resp.trace_id(), "trace-123");
  EXPECT_EQ(resp.visited_entity_ids_size(), 2);
  EXPECT_EQ(resp.visited_entity_ids(0), 100u);
  EXPECT_EQ(resp.visited_entity_ids(1), 200u);
}

TEST(QueryDispatcherTest, LocalMiss) {
  TMVEngine engine(16);
  QueryDispatcher dispatcher(&engine);

  TraversalRequest req;
  req.set_trace_id("trace-456");
  req.set_root_entity_id(99);
  req.set_query_time(1000);
  req.set_max_hops(3);

  TraversalResponse resp;
  grpc::Status status = dispatcher.DispatchTraversal(req, &resp);

  EXPECT_TRUE(status.ok());
  EXPECT_FALSE(resp.success());
  EXPECT_EQ(resp.visited_entity_ids_size(), 0);
}
