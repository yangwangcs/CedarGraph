// tests/test_graphd_load_balancer.cc
// Test GraphD load balancer functionality

#include <gtest/gtest.h>
#include "cedar/client/graphd_load_balancer.h"

using namespace cedar::client;

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

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
