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

// =============================================================================
// MetaServiceGrpcImpl GCN Cache Handler Tests
// =============================================================================

#include <gtest/gtest.h>

#include "cedar/dtx/meta_service_grpc.h"
#include "meta_service.pb.h"

using namespace cedar::dtx;

class MetaServiceGcnCacheTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // MetaServiceGrpcImpl only uses meta_service_ for non-GCN handlers;
    // GCN handlers operate solely on location_table_. Passing nullptr is safe.
    impl_ = std::make_unique<MetaServiceGrpcImpl>(nullptr);
  }

  std::unique_ptr<MetaServiceGrpcImpl> impl_;
};

// ---------------------------------------------------------------------------
// LocateCache
// ---------------------------------------------------------------------------

TEST_F(MetaServiceGcnCacheTest, LocateCacheFound) {
  // First report a cache window via ReportCache
  cedar::meta::ReportCacheRequest report_req;
  auto* w = report_req.mutable_window();
  w->set_entity_id(42);
  w->set_cached_from(10);
  w->set_cached_to(100);
  w->set_gcn_node_id(7);
  w->set_version(1);
  w->set_expire_at(1000);

  cedar::meta::ReportCacheResponse report_resp;
  grpc::ServerContext ctx;
  auto status = impl_->ReportCache(&ctx, &report_req, &report_resp);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(report_resp.success());

  // Now locate it
  cedar::meta::LocateCacheRequest locate_req;
  locate_req.set_entity_id(42);
  locate_req.set_query_time(50);

  cedar::meta::LocateCacheResponse locate_resp;
  grpc::ServerContext ctx2;
  status = impl_->LocateCache(&ctx2, &locate_req, &locate_resp);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(locate_resp.found());
  EXPECT_EQ(locate_resp.window().entity_id(), 42);
  EXPECT_EQ(locate_resp.window().cached_from(), 10);
  EXPECT_EQ(locate_resp.window().cached_to(), 100);
  EXPECT_EQ(locate_resp.window().gcn_node_id(), 7);
}

TEST_F(MetaServiceGcnCacheTest, LocateCacheNotFound) {
  cedar::meta::LocateCacheRequest locate_req;
  locate_req.set_entity_id(99);
  locate_req.set_query_time(50);

  cedar::meta::LocateCacheResponse locate_resp;
  grpc::ServerContext ctx;
  auto status = impl_->LocateCache(&ctx, &locate_req, &locate_resp);
  EXPECT_TRUE(status.ok());
  EXPECT_FALSE(locate_resp.found());
}

TEST_F(MetaServiceGcnCacheTest, LocateCacheOutsideWindow) {
  cedar::meta::ReportCacheRequest report_req;
  auto* w = report_req.mutable_window();
  w->set_entity_id(42);
  w->set_cached_from(10);
  w->set_cached_to(100);
  w->set_gcn_node_id(7);
  w->set_version(1);
  w->set_expire_at(1000);

  cedar::meta::ReportCacheResponse report_resp;
  grpc::ServerContext ctx;
  impl_->ReportCache(&ctx, &report_req, &report_resp);

  cedar::meta::LocateCacheRequest locate_req;
  locate_req.set_entity_id(42);
  locate_req.set_query_time(200);  // outside window

  cedar::meta::LocateCacheResponse locate_resp;
  grpc::ServerContext ctx2;
  auto status = impl_->LocateCache(&ctx2, &locate_req, &locate_resp);
  EXPECT_TRUE(status.ok());
  EXPECT_FALSE(locate_resp.found());
}

// ---------------------------------------------------------------------------
// ReportCache
// ---------------------------------------------------------------------------

TEST_F(MetaServiceGcnCacheTest, ReportCacheSuccess) {
  cedar::meta::ReportCacheRequest req;
  auto* w = req.mutable_window();
  w->set_entity_id(42);
  w->set_cached_from(10);
  w->set_cached_to(100);
  w->set_gcn_node_id(7);
  w->set_version(1);
  w->set_expire_at(1000);

  cedar::meta::ReportCacheResponse resp;
  grpc::ServerContext ctx;
  auto status = impl_->ReportCache(&ctx, &req, &resp);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(resp.success());
}

// ---------------------------------------------------------------------------
// GcnHeartbeat
// ---------------------------------------------------------------------------

TEST_F(MetaServiceGcnCacheTest, GcnHeartbeatMultipleWindows) {
  cedar::meta::GcnHeartbeatRequest req;
  req.set_gcn_node_id(7);

  auto* w1 = req.add_windows();
  w1->set_entity_id(10);
  w1->set_cached_from(0);
  w1->set_cached_to(100);
  w1->set_gcn_node_id(7);
  w1->set_version(1);
  w1->set_expire_at(1000);

  auto* w2 = req.add_windows();
  w2->set_entity_id(20);
  w2->set_cached_from(50);
  w2->set_cached_to(150);
  w2->set_gcn_node_id(7);
  w2->set_version(1);
  w2->set_expire_at(1000);

  cedar::meta::GcnHeartbeatResponse resp;
  grpc::ServerContext ctx;
  auto status = impl_->GcnHeartbeat(&ctx, &req, &resp);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(resp.success());

  // Verify both windows are now locatable
  cedar::meta::LocateCacheRequest locate_req;
  locate_req.set_entity_id(10);
  locate_req.set_query_time(50);
  cedar::meta::LocateCacheResponse locate_resp;
  grpc::ServerContext ctx2;
  impl_->LocateCache(&ctx2, &locate_req, &locate_resp);
  EXPECT_TRUE(locate_resp.found());

  locate_req.set_entity_id(20);
  locate_req.set_query_time(100);
  cedar::meta::LocateCacheResponse locate_resp2;
  grpc::ServerContext ctx3;
  impl_->LocateCache(&ctx3, &locate_req, &locate_resp2);
  EXPECT_TRUE(locate_resp2.found());
}

TEST_F(MetaServiceGcnCacheTest, GcnHeartbeatEmptyIsOk) {
  cedar::meta::GcnHeartbeatRequest req;
  req.set_gcn_node_id(7);

  cedar::meta::GcnHeartbeatResponse resp;
  grpc::ServerContext ctx;
  auto status = impl_->GcnHeartbeat(&ctx, &req, &resp);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(resp.success());
}

TEST_F(MetaServiceGcnCacheTest, GcnHeartbeatUpdatesExistingWindow) {
  // Initial report
  cedar::meta::ReportCacheRequest report_req;
  auto* w = report_req.mutable_window();
  w->set_entity_id(42);
  w->set_cached_from(10);
  w->set_cached_to(100);
  w->set_gcn_node_id(7);
  w->set_version(1);
  w->set_expire_at(1000);

  cedar::meta::ReportCacheResponse report_resp;
  grpc::ServerContext ctx1;
  impl_->ReportCache(&ctx1, &report_req, &report_resp);

  // Heartbeat with higher version
  cedar::meta::GcnHeartbeatRequest hb_req;
  auto* hw = hb_req.add_windows();
  hw->set_entity_id(42);
  hw->set_cached_from(20);
  hw->set_cached_to(200);
  hw->set_gcn_node_id(8);
  hw->set_version(2);
  hw->set_expire_at(2000);

  cedar::meta::GcnHeartbeatResponse hb_resp;
  grpc::ServerContext ctx2;
  impl_->GcnHeartbeat(&ctx2, &hb_req, &hb_resp);

  // Verify updated
  cedar::meta::LocateCacheRequest locate_req;
  locate_req.set_entity_id(42);
  locate_req.set_query_time(150);
  cedar::meta::LocateCacheResponse locate_resp;
  grpc::ServerContext ctx3;
  impl_->LocateCache(&ctx3, &locate_req, &locate_resp);
  EXPECT_TRUE(locate_resp.found());
  EXPECT_EQ(locate_resp.window().gcn_node_id(), 8);
  EXPECT_EQ(locate_resp.window().cached_to(), 200);
}
