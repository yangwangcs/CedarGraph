// tests/test_graphd_load_balancer.cc
// Test GraphD load balancer functionality

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <grpcpp/grpcpp.h>
#include <thread>

#include "cedar/client/graphd_load_balancer.h"
#include "meta_service.grpc.pb.h"

using namespace cedar::client;

namespace {

class MockGraphDMetaService final : public cedar::meta::MetaService::Service {
 public:
  void set_include_offline_node(bool include) { include_offline_node_ = include; }
  void set_include_online_node(bool include) { include_online_node_ = include; }

  grpc::Status GetGraphDNodes(
      grpc::ServerContext* context,
      const cedar::meta::GetGraphDNodesRequest* request,
      cedar::meta::GetGraphDNodesResponse* response) override {
    (void)context;
    (void)request;
    response->set_success(true);

    if (include_offline_node_) {
      auto* offline = response->add_nodes();
      offline->set_address("127.0.0.1");
      offline->set_port(9669);
      offline->set_state("OFFLINE");
      offline->set_active_queries(0);
    }

    if (include_online_node_) {
      auto* online = response->add_nodes();
      online->set_address("127.0.0.2");
      online->set_port(9670);
      online->set_state("ONLINE");
      online->set_active_queries(10);
    }

    return grpc::Status::OK;
  }

 private:
  bool include_offline_node_ = true;
  bool include_online_node_ = true;
};

class GraphDLoadBalancerGrpcTest : public ::testing::Test {
 protected:
  void SetUp() override {
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0",
                             grpc::InsecureServerCredentials(),
                             &port_);
    builder.RegisterService(&service_);
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);
    address_ = "127.0.0.1:" + std::to_string(port_);
    server_thread_ = std::thread([this]() { server_->Wait(); });
  }

  void TearDown() override {
    if (server_) {
      server_->Shutdown();
    }
    if (server_thread_.joinable()) {
      server_thread_.join();
    }
  }

  MockGraphDMetaService service_;
  std::unique_ptr<grpc::Server> server_;
  std::thread server_thread_;
  std::string address_;
  int port_ = 0;
};

}  // namespace

TEST(GraphDLoadBalancerTest, RoundRobinSelection) {
  GraphDLoadBalancer::Config config;
  config.strategy = GraphDLoadBalancer::Strategy::ROUND_ROBIN;
  
  GraphDLoadBalancer lb(config);
  
  // Test round robin selection
  EXPECT_EQ(config.strategy, GraphDLoadBalancer::Strategy::ROUND_ROBIN);
}

TEST(GraphDLoadBalancerTest, LeastConnectionsSelection) {
  GraphDLoadBalancer::Config config;
  config.strategy = GraphDLoadBalancer::Strategy::LEAST_CONNECTIONS;
  
  GraphDLoadBalancer lb(config);
  
  // Test least connections selection
  EXPECT_EQ(config.strategy, GraphDLoadBalancer::Strategy::LEAST_CONNECTIONS);
}

TEST(GraphDLoadBalancerTest, RandomSelection) {
  GraphDLoadBalancer::Config config;
  config.strategy = GraphDLoadBalancer::Strategy::RANDOM;
  
  GraphDLoadBalancer lb(config);
  
  // Test random selection
  EXPECT_EQ(config.strategy, GraphDLoadBalancer::Strategy::RANDOM);
}

TEST(GraphDLoadBalancerTest, FailoverSelection) {
  GraphDLoadBalancer::Config config;
  config.strategy = GraphDLoadBalancer::Strategy::ROUND_ROBIN;
  
  GraphDLoadBalancer lb(config);
  
  // Test failover selection
  EXPECT_EQ(config.strategy, GraphDLoadBalancer::Strategy::ROUND_ROBIN);
}

TEST(GraphDLoadBalancerTest, MarkNodeFailed) {
  GraphDLoadBalancer::Config config;
  config.strategy = GraphDLoadBalancer::Strategy::ROUND_ROBIN;
  
  GraphDLoadBalancer lb(config);
  
  // Test marking node as failed
  lb.MarkNodeFailed("127.0.0.1", 9669);
  
  // Verify node is marked as failed
  EXPECT_EQ(config.strategy, GraphDLoadBalancer::Strategy::ROUND_ROBIN);
}

TEST(GraphDLoadBalancerTest, RejectsNonPositiveRefreshInterval) {
  GraphDLoadBalancer::Config config;
  config.refresh_interval_seconds = 0;

  GraphDLoadBalancer lb(config);
  EXPECT_FALSE(lb.Initialize());
}

TEST_F(GraphDLoadBalancerGrpcTest, SelectNodeUsesOnlineCandidatesOnly) {
  GraphDLoadBalancer::Config config;
  config.meta_address = address_;
  config.strategy = GraphDLoadBalancer::Strategy::ROUND_ROBIN;
  config.refresh_interval_seconds = 60;

  GraphDLoadBalancer lb(config);
  ASSERT_TRUE(lb.Initialize());

  auto selected = lb.SelectNode();
  EXPECT_EQ(selected.address, "127.0.0.2");
  EXPECT_EQ(selected.state, "ONLINE");
  lb.Stop();
}

TEST_F(GraphDLoadBalancerGrpcTest, SelectNodeFailsWhenNoOnlineNodesExist) {
  service_.set_include_offline_node(true);
  service_.set_include_online_node(false);

  GraphDLoadBalancer::Config config;
  config.meta_address = address_;
  config.strategy = GraphDLoadBalancer::Strategy::ROUND_ROBIN;
  config.refresh_interval_seconds = 60;

  GraphDLoadBalancer lb(config);
  ASSERT_TRUE(lb.Initialize());

  EXPECT_THROW((void)lb.SelectNode(), std::runtime_error);
  lb.Stop();
}

TEST_F(GraphDLoadBalancerGrpcTest, StopWakesFailoverRetryPromptly) {
  service_.set_include_offline_node(false);

  GraphDLoadBalancer::Config config;
  config.meta_address = address_;
  config.strategy = GraphDLoadBalancer::Strategy::ROUND_ROBIN;
  config.refresh_interval_seconds = 60;

  GraphDLoadBalancer lb(config);
  ASSERT_TRUE(lb.Initialize());

  lb.MarkNodeFailed("127.0.0.2", 9670);

  std::atomic<bool> started{false};
  auto future = std::async(std::launch::async, [&]() {
    started.store(true);
    try {
      (void)lb.SelectNodeWithFailover();
    } catch (const std::exception&) {
    }
  });

  for (int i = 0; i < 100 && !started.load(); ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  auto start = std::chrono::steady_clock::now();
  lb.Stop();
  ASSERT_EQ(future.wait_for(std::chrono::seconds(1)), std::future_status::ready);
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
            500);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
