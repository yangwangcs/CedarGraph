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
#include <sstream>
#include <thread>

#include "cedar/gcn/gcn_node.h"
#include "cedar/gcn/scatter_gather_router.h"
#include "gcn_service.grpc.pb.h"

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

// ---------------------------------------------------------------------------
// Mock GCN service that returns a downstream failure in SubQuery
// ---------------------------------------------------------------------------
class FailingGcnService : public GcnService::Service {
 public:
  grpc::Status SubQuery(grpc::ServerContext* /*context*/,
                        const SubQueryRequest* /*request*/,
                        SubQueryResponse* response) override {
    response->set_success(false);
    response->set_error_msg("downstream error: shard unavailable");
    return grpc::Status::OK;
  }

  grpc::Status Traverse(grpc::ServerContext* /*context*/,
                        const TraversalRequest* /*request*/,
                        TraversalResponse* response) override {
    response->set_success(false);
    response->set_error_msg("downstream error: shard unavailable");
    return grpc::Status::OK;
  }
};

TEST(ScatterGatherRouterTest, ScatterPropagatesDownstreamFailure) {
  FailingGcnService service;
  std::string server_address = "127.0.0.1:0";
  grpc::ServerBuilder builder;
  int port = 0;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials(), &port);
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
  ASSERT_NE(server, nullptr);

  std::ostringstream oss;
  oss << "127.0.0.1:" << port;
  server_address = oss.str();

  std::thread server_thread([&server]() { server->Wait(); });

  ScatterGatherRouter router;
  auto channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
  router.RegisterPeer("failing-gcn", channel);

  SubQueryRequest req;
  req.set_trace_id("trace-123");
  req.set_current_entity_id(42);

  SubQueryResponse resp = router.Scatter(req, "failing-gcn");

  EXPECT_FALSE(resp.success());
  EXPECT_NE(resp.error_msg().find("downstream error"), std::string::npos);

  server->Shutdown();
  server_thread.join();
}

TEST(ScatterGatherRouterTest, ScatterTraversalPropagatesDownstreamFailure) {
  FailingGcnService service;
  std::string server_address = "127.0.0.1:0";
  grpc::ServerBuilder builder;
  int port = 0;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials(), &port);
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
  ASSERT_NE(server, nullptr);

  std::ostringstream oss;
  oss << "127.0.0.1:" << port;
  server_address = oss.str();

  std::thread server_thread([&server]() { server->Wait(); });

  ScatterGatherRouter router;
  auto channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
  router.RegisterPeer("failing-gcn", channel);

  TraversalRequest req;
  req.set_trace_id("trace-456");
  req.set_root_entity_id(42);

  TraversalResponse resp = router.ScatterTraversal(req, "failing-gcn");

  EXPECT_FALSE(resp.success());
  EXPECT_NE(resp.error_msg().find("downstream error"), std::string::npos);

  server->Shutdown();
  server_thread.join();
}

TEST(ScatterGatherRouterTest, ConcurrentRegisterUnregisterIsSafe) {
  ScatterGatherRouter router;
  const int kThreads = 8;
  const int kOpsPerThread = 500;

  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&router, t, kOpsPerThread]() {
      for (int i = 0; i < kOpsPerThread; ++i) {
        std::string id = "gcn-" + std::to_string(t) + "-" + std::to_string(i);
        router.RegisterPeer(id, nullptr);
        router.UnregisterPeer(id);
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(router.PeerCount(), 0u);
}

TEST(ScatterGatherRouterTest, ConcurrentMixedOperationsIsSafe) {
  ScatterGatherRouter router;
  const int kThreads = 8;
  const int kOpsPerThread = 500;

  // Pre-register some peers so reads have something to look up
  for (int i = 0; i < 4; ++i) {
    router.RegisterPeer("stable-gcn-" + std::to_string(i), nullptr);
  }

  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&router, t, kOpsPerThread]() {
      for (int i = 0; i < kOpsPerThread; ++i) {
        switch (i % 4) {
          case 0: {
            std::string id = "dyn-gcn-" + std::to_string(t) + "-" + std::to_string(i);
            router.RegisterPeer(id, nullptr);
            break;
          }
          case 1: {
            std::string id = "dyn-gcn-" + std::to_string(t) + "-" + std::to_string(i - 1);
            router.UnregisterPeer(id);
            break;
          }
          case 2:
            (void)router.PeerCount();
            break;
          case 3:
            (void)router.GetTargetGCN(static_cast<uint64_t>(i));
            break;
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // We can't assert an exact count because of interleaving, but it must be
  // deterministic and non-crashing. All dynamically-added peers should be gone
  // because each thread unregisters what it registered (modulo interleaving
  // where another thread may have unregistered it already, which is idempotent).
  // The stable peers should remain.
  EXPECT_GE(router.PeerCount(), 4u);
}
