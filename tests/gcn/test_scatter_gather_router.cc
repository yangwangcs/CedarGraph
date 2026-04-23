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

#include "cedar/gcn/scatter_gather_router.h"
#include "gcn_service.pb.h"

using namespace cedar::gcn;

TEST(ScatterGatherRouterTest, SubQueryRequestBuildsCorrectly) {
  SubQueryRequest req;
  req.set_trace_id("trace-123");
  req.set_parent_gcn_id("gcn-1");
  req.set_root_entity_id(42);
  req.set_current_entity_id(43);
  req.set_query_time(1000);
  req.set_remaining_hops(3);
  req.add_visited_path(42);
  req.add_visited_path(43);
  req.set_algorithm_context("ctx");

  EXPECT_EQ(req.trace_id(), "trace-123");
  EXPECT_EQ(req.parent_gcn_id(), "gcn-1");
  EXPECT_EQ(req.root_entity_id(), 42u);
  EXPECT_EQ(req.current_entity_id(), 43u);
  EXPECT_EQ(req.query_time(), 1000u);
  EXPECT_EQ(req.remaining_hops(), 3u);
  EXPECT_EQ(req.visited_path_size(), 2);
  EXPECT_EQ(req.visited_path(0), 42u);
  EXPECT_EQ(req.visited_path(1), 43u);
  EXPECT_EQ(req.algorithm_context(), "ctx");
}

TEST(ScatterGatherRouterTest, GatherMergesResponses) {
  SubQueryResponse resp1;
  resp1.set_success(true);
  resp1.set_trace_id("trace-123");
  resp1.add_next_entity_ids(10);
  resp1.add_next_entity_ids(20);
  resp1.set_truncated(false);

  SubQueryResponse resp2;
  resp2.set_success(true);
  resp2.set_trace_id("trace-123");
  resp2.add_next_entity_ids(30);
  resp2.set_truncated(true);

  std::vector<SubQueryResponse> responses = {resp1, resp2};

  ScatterGatherRouter router;
  TraversalResponse merged = router.Gather(responses);

  EXPECT_EQ(merged.trace_id(), "trace-123");
  EXPECT_EQ(merged.visited_entity_ids_size(), 3);
  EXPECT_EQ(merged.visited_entity_ids(0), 10u);
  EXPECT_EQ(merged.visited_entity_ids(1), 20u);
  EXPECT_EQ(merged.visited_entity_ids(2), 30u);
  EXPECT_TRUE(merged.truncated());
  EXPECT_TRUE(merged.success());
}
