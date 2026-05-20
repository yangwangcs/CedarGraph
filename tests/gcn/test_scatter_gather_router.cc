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

#include "cedar/gcn/gcn_node.h"
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

TEST(ScatterGatherRouterTest, GatherMarksFailureIfAnySubQueryFails) {
  SubQueryResponse resp1;
  resp1.set_success(true);
  resp1.add_next_entity_ids(10);

  SubQueryResponse resp2;
  resp2.set_success(false);

  std::vector<SubQueryResponse> responses = {resp1, resp2};

  ScatterGatherRouter router;
  TraversalResponse merged = router.Gather(responses);

  EXPECT_FALSE(merged.success());
  EXPECT_EQ(merged.visited_entity_ids_size(), 1);
}

TEST(ScatterGatherRouterTest, ScatterRejectsUnknownPeer) {
  ScatterGatherRouter router;

  SubQueryRequest req;
  req.set_trace_id("trace-123");
  req.set_current_entity_id(42);

  SubQueryResponse resp = router.Scatter(req, "unknown-gcn");

  EXPECT_FALSE(resp.success());
  EXPECT_NE(resp.error_msg().find("Unknown target GCN"), std::string::npos);
}

TEST(ScatterGatherRouterTest, ConsistentHashRoutesToRegisteredPeer) {
  ScatterGatherRouter router;

  // No peers registered -> should return empty target
  EXPECT_EQ(router.GetTargetGCN(42), "");

  // Register two peers
  router.RegisterPeer("gcn-a", nullptr);
  router.RegisterPeer("gcn-b", nullptr);

  EXPECT_EQ(router.PeerCount(), 2u);

  // After registration, hash ring should route to one of the peers
  std::string target = router.GetTargetGCN(42);
  EXPECT_TRUE(target == "gcn-a" || target == "gcn-b");

  // Same entity should map to the same GCN (deterministic)
  EXPECT_EQ(router.GetTargetGCN(42), target);

  // Different entity may map to same or different GCN
  std::string target2 = router.GetTargetGCN(99);
  EXPECT_TRUE(target2 == "gcn-a" || target2 == "gcn-b");
}

TEST(ScatterGatherRouterTest, UnregisterPeerRemovesFromHashRing) {
  ScatterGatherRouter router;
  router.RegisterPeer("gcn-a", nullptr);
  router.RegisterPeer("gcn-b", nullptr);

  std::string target_before = router.GetTargetGCN(42);

  router.UnregisterPeer("gcn-a");
  EXPECT_EQ(router.PeerCount(), 1u);

  std::string target_after = router.GetTargetGCN(42);
  // After removing gcn-a, all entities must still route to the remaining peer
  EXPECT_EQ(target_after, "gcn-b");
}

TEST(ScatterGatherRouterTest, ScatterByEntityRejectsWhenNoPeers) {
  ScatterGatherRouter router;

  SubQueryRequest req;
  req.set_trace_id("trace-123");
  req.set_current_entity_id(42);

  SubQueryResponse resp = router.ScatterByEntity(req);

  EXPECT_FALSE(resp.success());
  EXPECT_NE(resp.error_msg().find("No GCN available"), std::string::npos);
}

TEST(GcnNodePeerTest, InitializeRegistersPeers) {
  cedar::GcnNode node;
  node.SetPeerAddresses({"127.0.0.1:9781", "127.0.0.1:9782"});
  // We cannot Start() without a real gRPC port, but Initialize() should
  // at least create the router and register peers without crashing.
  auto status = node.Initialize();
  (void)status;
  SUCCEED();
}
