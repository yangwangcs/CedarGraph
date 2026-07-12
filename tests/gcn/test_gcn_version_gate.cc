#include <gtest/gtest.h>

#include <limits>
#include <vector>

#include "cedar/gcn/gcn_service.h"
#include "cedar/gcn/query_dispatcher.h"
#include "cedar/gcn/scatter_gather_router.h"
#include "cedar/gcn/tmv_engine.h"
#include "gcn_service.pb.h"

namespace cedar::gcn {
namespace {

TEST(GcnVersionGateTest, RejectsRequestAheadOfAppliedVersionBeforeReadingTmv) {
  TMVEngine engine(16);
  std::vector<TMVEdge> edges;
  edges.push_back({100, 500, std::numeric_limits<uint32_t>::max(), 0, 1, 0});
  engine.BootstrapVertex(42, Direction::kOut, edges, false);

  QueryDispatcher dispatcher(&engine);
  dispatcher.SetPartitionProgress(3, /*epoch=*/4, /*applied_version=*/99,
                                  /*query_ready=*/true);

  TraversalRequest request;
  request.set_trace_id("lagging-read");
  request.set_root_entity_id(42);
  request.set_query_time(1000);
  request.set_required_version(100);
  request.set_partition_id(3);

  TraversalResponse response;
  grpc::Status status = dispatcher.DispatchTraversal(request, &response);

  EXPECT_TRUE(status.ok());
  EXPECT_FALSE(response.success());
  EXPECT_EQ(response.cache_status(), CACHE_STATUS_VERSION_LAG);
  EXPECT_EQ(response.partition_epoch(), 4u);
  EXPECT_EQ(response.served_version(), 99u);
  EXPECT_EQ(response.visited_entity_ids_size(), 0);
}

TEST(GcnVersionGateTest, AllowsRequestAtAppliedVersionAndReturnsMetadata) {
  TMVEngine engine(16);
  std::vector<TMVEdge> edges;
  edges.push_back({100, 500, std::numeric_limits<uint32_t>::max(), 0, 1, 0});
  engine.BootstrapVertex(42, Direction::kOut, edges, false);

  QueryDispatcher dispatcher(&engine);
  dispatcher.SetPartitionProgress(3, /*epoch=*/4, /*applied_version=*/100,
                                  /*query_ready=*/true);

  TraversalRequest request;
  request.set_trace_id("fresh-read");
  request.set_root_entity_id(42);
  request.set_query_time(1000);
  request.set_required_version(100);
  request.set_partition_id(3);

  TraversalResponse response;
  grpc::Status status = dispatcher.DispatchTraversal(request, &response);

  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(response.success());
  EXPECT_EQ(response.cache_status(), CACHE_STATUS_HIT);
  EXPECT_EQ(response.partition_epoch(), 4u);
  EXPECT_EQ(response.served_version(), 100u);
  ASSERT_EQ(response.visited_entity_ids_size(), 1);
  EXPECT_EQ(response.visited_entity_ids(0), 100u);
}

TEST(GcnVersionGateTest, GatesExplicitPartitionZero) {
  TMVEngine engine(16);
  std::vector<TMVEdge> edges;
  edges.push_back({100, 500, std::numeric_limits<uint32_t>::max(), 0, 1, 0});
  engine.BootstrapVertex(42, Direction::kOut, edges, false);

  QueryDispatcher dispatcher(&engine);
  dispatcher.SetPartitionProgress(0, /*epoch=*/4, /*applied_version=*/99,
                                  /*query_ready=*/true);

  TraversalRequest request;
  request.set_trace_id("partition-zero-lag");
  request.set_root_entity_id(42);
  request.set_query_time(1000);
  request.set_required_version(100);
  request.set_partition_id(0);

  TraversalResponse response;
  grpc::Status status = dispatcher.DispatchTraversal(request, &response);

  EXPECT_TRUE(status.ok());
  EXPECT_FALSE(response.success());
  EXPECT_EQ(response.cache_status(), CACHE_STATUS_VERSION_LAG);
  EXPECT_EQ(response.partition_epoch(), 4u);
  EXPECT_EQ(response.served_version(), 99u);
  EXPECT_EQ(response.visited_entity_ids_size(), 0);
}

TEST(GcnVersionGateTest, ServiceExposesPartitionProgressToDispatcher) {
  TMVEngine engine(16);
  std::vector<TMVEdge> edges;
  edges.push_back({200, 500, std::numeric_limits<uint32_t>::max(), 0, 1, 0});
  engine.BootstrapVertex(7, Direction::kOut, edges, false);

  GcnServiceImpl service(&engine);
  service.SetPartitionProgress(5, /*epoch=*/8, /*applied_version=*/77,
                               /*query_ready=*/true);

  TraversalRequest request;
  request.set_trace_id("service-gate");
  request.set_root_entity_id(7);
  request.set_query_time(1000);
  request.set_required_version(78);
  request.set_partition_id(5);

  TraversalResponse response;
  grpc::ServerContext context;
  grpc::Status status = service.Traverse(&context, &request, &response);

  EXPECT_TRUE(status.ok());
  EXPECT_FALSE(response.success());
  EXPECT_EQ(response.cache_status(), CACHE_STATUS_VERSION_LAG);
  EXPECT_EQ(response.partition_epoch(), 8u);
  EXPECT_EQ(response.served_version(), 77u);
  EXPECT_EQ(response.visited_entity_ids_size(), 0);
}

TEST(GcnVersionGateTest, ServiceDoesNotRouteVersionLagAsLocalMiss) {
  TMVEngine engine(16);
  std::vector<TMVEdge> edges;
  edges.push_back({200, 500, std::numeric_limits<uint32_t>::max(), 0, 1, 0});
  engine.BootstrapVertex(7, Direction::kOut, edges, false);

  GcnServiceImpl service(&engine);
  service.SetScatterGatherRouter(std::make_shared<ScatterGatherRouter>());
  service.SetPartitionProgress(5, /*epoch=*/8, /*applied_version=*/77,
                               /*query_ready=*/true);

  TraversalRequest request;
  request.set_trace_id("service-gate-with-router");
  request.set_root_entity_id(7);
  request.set_query_time(1000);
  request.set_required_version(78);
  request.set_partition_id(5);

  TraversalResponse response;
  grpc::ServerContext context;
  grpc::Status status = service.Traverse(&context, &request, &response);

  EXPECT_TRUE(status.ok());
  EXPECT_FALSE(response.success());
  EXPECT_EQ(response.cache_status(), CACHE_STATUS_VERSION_LAG);
  EXPECT_EQ(response.partition_epoch(), 8u);
  EXPECT_EQ(response.served_version(), 77u);
  EXPECT_EQ(response.error_msg(), "");
}

}  // namespace
}  // namespace cedar::gcn
