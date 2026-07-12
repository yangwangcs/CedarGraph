#include <gtest/gtest.h>

#include <atomic>
#include <cstdlib>
#include <cstdint>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <thread>

#include <grpcpp/grpcpp.h>

#define private public
#include "cedar/service/gcn_route_cache.h"
#include "cedar/service/graph_service_router.h"
#undef private

#include "cedar/dtx/security.h"
#include "gcn_service.grpc.pb.h"
#include "meta_service.grpc.pb.h"
#include "storage_service.grpc.pb.h"

namespace {

class TestGrpcServer {
 public:
  template <typename Service>
  explicit TestGrpcServer(Service* service) {
    grpc::ServerBuilder builder;
    int port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(service);
    server_ = builder.BuildAndStart();
    EXPECT_NE(server_, nullptr);
    std::ostringstream oss;
    oss << "127.0.0.1:" << port;
    address_ = oss.str();
    thread_ = std::thread([this]() { server_->Wait(); });
  }

  ~TestGrpcServer() {
    if (server_) {
      server_->Shutdown();
    }
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  const std::string& address() const { return address_; }

 private:
  std::unique_ptr<grpc::Server> server_;
  std::thread thread_;
  std::string address_;
};

class FakeGcnService final : public cedar::gcn::GcnService::Service {
 public:
  grpc::Status Traverse(grpc::ServerContext*,
                        const cedar::gcn::TraversalRequest* request,
                        cedar::gcn::TraversalResponse* response) override {
    calls.fetch_add(1);
    last_request = *request;
    *response = response_template;
    return grpc::Status::OK;
  }

  std::atomic<int> calls{0};
  cedar::gcn::TraversalRequest last_request;
  cedar::gcn::TraversalResponse response_template;
};

class FakeStorageService final : public cedar::storage::StorageService::Service {
 public:
  grpc::Status Scan(grpc::ServerContext*,
                    const cedar::storage::ScanRequest* request,
                    cedar::storage::ScanResponse* response) override {
    calls.fetch_add(1);
    last_request = *request;
    response->set_success(true);
    for (int i = 0; i < item_count; ++i) {
      response->add_items()->set_timestamp(100 + i);
    }
    return grpc::Status::OK;
  }

  std::atomic<int> calls{0};
  int item_count = 2;
  cedar::storage::ScanRequest last_request;
};

class GraphdGcnRoutingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    setenv("CEDAR_GRPC_ALLOW_INSECURE", "1", 1);
    auto* sm = cedar::dtx::security::SecurityManager::GetInstance();
    cedar::dtx::security::SecurityManager::Config cfg;
    cfg.enable_auth = false;
    auto status = sm->Initialize(cfg);
    ASSERT_TRUE(status.ok()) << status.ToString();

    router = std::make_unique<cedar::service::GraphServiceRouter>();
  }

  void TearDown() override {
    router.reset();
    cedar::dtx::security::SecurityManager::GetInstance()->Shutdown();
    unsetenv("CEDAR_GRPC_ALLOW_INSECURE");
  }

  void InstallStorageRoute(uint32_t partition_id, const std::string& storage_addr) {
    cedar::service::PartitionRoute route;
    route.partition_id = partition_id;
    route.leader_node = storage_addr;
    router->partition_cache_[partition_id] = route;
  }

  void InstallDynamicGcnRoute(const cedar::dtx::GcnRoute& route) {
    router->dynamic_gcn_routes_ = std::make_unique<cedar::service::GcnRouteCache>(
        [route](uint32_t partition_id, uint64_t required_version)
            -> cedar::StatusOr<cedar::dtx::GcnRoute> {
          if (partition_id != route.partition_id) {
            return cedar::Status::NotFound("wrong partition");
          }
          if (required_version > 0 && route.applied_version < required_version) {
            return cedar::Status::NotFound("stale gcn route");
          }
          return route;
        },
        [](const std::string& endpoint) {
          return grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials());
        });
  }

  std::unique_ptr<cedar::service::GraphServiceRouter> router;
};

TEST_F(GraphdGcnRoutingTest, DynamicLocateGcnSuccessUsesGcnWithoutStorage) {
  FakeGcnService gcn;
  gcn.response_template.set_success(true);
  gcn.response_template.set_served_version(123);
  gcn.response_template.set_partition_epoch(9);
  gcn.response_template.set_cache_status(cedar::gcn::CACHE_STATUS_HIT);
  gcn.response_template.add_visited_entity_ids(42);
  gcn.response_template.add_visited_entity_ids(43);
  TestGrpcServer gcn_server(&gcn);

  FakeStorageService storage;
  TestGrpcServer storage_server(&storage);

  cedar::query::TraverseRequest request;
  request.set_start_node_id(42);
  request.set_as_of_timestamp(100);
  request.set_max_depth(2);
  request.add_edge_types(7);
  const uint32_t partition_id = router->CalculatePartition(request.start_node_id());
  InstallStorageRoute(partition_id, storage_server.address());
  InstallDynamicGcnRoute({partition_id, 77, gcn_server.address(), 55, 123,
                          std::numeric_limits<uint64_t>::max()});

  cedar::query::TraverseResponse response;
  grpc::ServerContext context;
  auto status = router->Traverse(&context, &request, &response);

  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(response.success()) << response.error_msg();
  EXPECT_EQ(response.nodes_visited(), 2);
  EXPECT_EQ(gcn.calls.load(), 1);
  EXPECT_EQ(storage.calls.load(), 0);
  EXPECT_EQ(gcn.last_request.root_entity_id(), 42);
  EXPECT_TRUE(gcn.last_request.has_partition_id());
  EXPECT_EQ(gcn.last_request.partition_id(), partition_id);
  EXPECT_EQ(gcn.last_request.required_version(), 100);
  EXPECT_EQ(gcn.last_request.edge_type(), 7);
  EXPECT_EQ(gcn.last_request.max_hops(), 2);
  EXPECT_EQ(gcn.last_request.query_time(), 100);
}

TEST_F(GraphdGcnRoutingTest, LocateMissFallsBackToStorage) {
  FakeStorageService storage;
  TestGrpcServer storage_server(&storage);

  cedar::query::TraverseRequest request;
  request.set_start_node_id(99);
  const uint32_t partition_id = router->CalculatePartition(request.start_node_id());
  InstallStorageRoute(partition_id, storage_server.address());
  router->dynamic_gcn_routes_ = std::make_unique<cedar::service::GcnRouteCache>(
      [](uint32_t, uint64_t) -> cedar::StatusOr<cedar::dtx::GcnRoute> {
        return cedar::Status::NotFound("no gcn lease");
      },
      [](const std::string& endpoint) {
        return grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials());
      });

  cedar::query::TraverseResponse response;
  grpc::ServerContext context;
  auto status = router->Traverse(&context, &request, &response);

  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(response.success()) << response.error_msg();
  EXPECT_EQ(response.nodes_visited(), storage.item_count);
  EXPECT_EQ(storage.calls.load(), 1);
  EXPECT_EQ(storage.last_request.partition_id(), partition_id);
}

TEST_F(GraphdGcnRoutingTest, DynamicMissDoesNotBypassLeaseGateViaStaticGcn) {
  FakeGcnService static_gcn;
  static_gcn.response_template.set_success(true);
  static_gcn.response_template.set_served_version(999);
  static_gcn.response_template.set_partition_epoch(7);
  static_gcn.response_template.set_cache_status(cedar::gcn::CACHE_STATUS_HIT);
  static_gcn.response_template.add_visited_entity_ids(555);
  TestGrpcServer static_gcn_server(&static_gcn);

  FakeStorageService storage;
  TestGrpcServer storage_server(&storage);

  cedar::query::TraverseRequest request;
  request.set_start_node_id(100);
  const uint32_t partition_id =
      router->CalculatePartition(request.start_node_id());
  InstallStorageRoute(partition_id, storage_server.address());
  router->dynamic_gcn_routes_ =
      std::make_unique<cedar::service::GcnRouteCache>(
          [](uint32_t, uint64_t) -> cedar::StatusOr<cedar::dtx::GcnRoute> {
            return cedar::Status::NotFound("no gcn lease");
          },
          [](const std::string& endpoint) {
            return grpc::CreateChannel(endpoint,
                                       grpc::InsecureChannelCredentials());
          });
  router->gcn_router_ = std::make_shared<cedar::gcn::ScatterGatherRouter>();
  auto static_channel = grpc::CreateChannel(
      static_gcn_server.address(), grpc::InsecureChannelCredentials());
  router->gcn_router_->RegisterPeer(static_gcn_server.address(),
                                    static_channel);
  router->gcn_peer_addresses_.push_back(static_gcn_server.address());

  cedar::query::TraverseResponse response;
  grpc::ServerContext context;
  auto status = router->Traverse(&context, &request, &response);

  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(response.success()) << response.error_msg();
  EXPECT_EQ(static_gcn.calls.load(), 0);
  EXPECT_EQ(storage.calls.load(), 1);
  EXPECT_EQ(response.nodes_visited(), storage.item_count);
}

TEST_F(GraphdGcnRoutingTest, GcnVersionLagFallsBackToStorage) {
  FakeGcnService gcn;
  gcn.response_template.set_success(true);
  gcn.response_template.set_served_version(99);
  gcn.response_template.set_partition_epoch(8);
  gcn.response_template.set_cache_status(cedar::gcn::CACHE_STATUS_VERSION_LAG);
  TestGrpcServer gcn_server(&gcn);

  FakeStorageService storage;
  TestGrpcServer storage_server(&storage);

  cedar::query::TraverseRequest request;
  request.set_start_node_id(123);
  request.set_as_of_timestamp(100);
  const uint32_t partition_id = router->CalculatePartition(request.start_node_id());
  InstallStorageRoute(partition_id, storage_server.address());
  InstallDynamicGcnRoute({partition_id, 88, gcn_server.address(), 123456, 100,
                          std::numeric_limits<uint64_t>::max()});

  cedar::query::TraverseResponse response;
  grpc::ServerContext context;
  auto status = router->Traverse(&context, &request, &response);

  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(response.success()) << response.error_msg();
  EXPECT_EQ(gcn.calls.load(), 1);
  EXPECT_EQ(storage.calls.load(), 1);
}

TEST_F(GraphdGcnRoutingTest, DoesNotTreatLeaseEpochAsPartitionEpoch) {
  FakeGcnService gcn;
  gcn.response_template.set_success(true);
  gcn.response_template.set_served_version(100);
  gcn.response_template.set_partition_epoch(0);
  gcn.response_template.set_cache_status(cedar::gcn::CACHE_STATUS_HIT);
  gcn.response_template.add_visited_entity_ids(1000);
  TestGrpcServer gcn_server(&gcn);

  FakeStorageService storage;
  TestGrpcServer storage_server(&storage);

  cedar::query::TraverseRequest request;
  request.set_start_node_id(124);
  request.set_as_of_timestamp(100);
  const uint32_t partition_id = router->CalculatePartition(request.start_node_id());
  InstallStorageRoute(partition_id, storage_server.address());
  InstallDynamicGcnRoute({partition_id, 89, gcn_server.address(), 777, 100,
                          std::numeric_limits<uint64_t>::max()});

  cedar::query::TraverseResponse response;
  grpc::ServerContext context;
  auto status = router->Traverse(&context, &request, &response);

  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(response.success()) << response.error_msg();
  EXPECT_EQ(gcn.calls.load(), 1);
  EXPECT_EQ(storage.calls.load(), 1)
      << "A non-zero MetaD lease_epoch must not make a zero GCN partition_epoch valid";
}

}  // namespace
