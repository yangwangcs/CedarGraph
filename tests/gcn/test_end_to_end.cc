// Copyright 2025 The Cedar Authors
//
// End-to-end integration test for the GCN traversal pipeline.
// Verifies: CDC ingestion -> TMVEngine storage -> gRPC query -> traversal results.

#include <gtest/gtest.h>

#include <grpcpp/grpcpp.h>
#include <set>
#include <sstream>
#include <thread>

#include "cedar/gcn/gcn_service.h"
#include "cedar/gcn/tmv_engine.h"
#include "gcn_service.grpc.pb.h"

using namespace cedar::gcn;

TEST(EndToEnd, FullTraversalPipeline) {
  // 1. Create TMVEngine
  TMVEngine engine(16);

  // 2. Bootstrap vertex 42 with outgoing edges to entities 100, 200, 300
  std::vector<TMVEdge> edges;
  edges.push_back(
      {100, 0, UINT32_MAX, 0, 1, 0});
  edges.push_back(
      {200, 0, UINT32_MAX, 0, 1, 0});
  edges.push_back(
      {300, 0, UINT32_MAX, 0, 1, 0});
  engine.BootstrapVertex(42, Direction::kOut, edges, false);

  // 3. Start gRPC server
  GcnServiceImpl service(&engine);
  std::string server_address("127.0.0.1:0");
  int port = 0;
  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials(), &port);
  builder.RegisterService(&service);
  auto server = builder.BuildAndStart();
  ASSERT_NE(server, nullptr);

  std::ostringstream oss;
  oss << "127.0.0.1:" << port;
  server_address = oss.str();

  std::thread server_thread([&server]() { server->Wait(); });

  // 4. Send TraversalRequest
  auto channel =
      grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
  auto stub = cedar::gcn::GcnService::NewStub(channel);

  cedar::gcn::TraversalRequest req;
  req.set_trace_id("e2e-test-1");
  req.set_root_entity_id(42);
  req.set_query_time(9999);
  req.set_max_hops(1);
  req.set_edge_type(1);
  req.set_required_version(0);

  cedar::gcn::TraversalResponse resp;
  grpc::ClientContext ctx;
  grpc::Status status = stub->Traverse(&ctx, req, &resp);

  // 5. Verify
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(resp.success());
  EXPECT_EQ(resp.trace_id(), "e2e-test-1");
  EXPECT_EQ(resp.visited_entity_ids_size(), 3);

  std::set<uint64_t> visited(resp.visited_entity_ids().begin(),
                             resp.visited_entity_ids().end());
  EXPECT_EQ(visited, (std::set<uint64_t>{100, 200, 300}));

  // Cleanup
  server->Shutdown();
  server_thread.join();
}
