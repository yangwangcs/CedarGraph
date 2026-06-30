#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>
#define private public
#include "cedar/service/graph_service_router.h"
#undef private
#include "cedar/dtx/security.h"

using namespace cedar;
using cedar::service::GraphServiceRouter;

class GraphServiceRouterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto* sm = cedar::dtx::security::SecurityManager::GetInstance();
    cedar::dtx::security::SecurityManager::Config cfg;
    cfg.enable_auth = false;
    auto s = sm->Initialize(cfg);
    EXPECT_TRUE(s.ok()) << s.ToString();

    router_ = std::make_unique<GraphServiceRouter>();
  }

  void TearDown() override {
    router_.reset();
    cedar::dtx::security::SecurityManager::GetInstance()->Shutdown();
  }

  std::unique_ptr<GraphServiceRouter> router_;
};

TEST_F(GraphServiceRouterTest, GetSchemaReturnsEmptyWhenNoMetaD) {
  // Without initialization, GetSchema should gracefully fail
  grpc::ServerContext context;
  context.AddInitialMetadata("authorization", "Bearer test-token");
  cedar::query::GetSchemaRequest request;
  cedar::query::GetSchemaResponse response;

  auto status = router_->GetSchema(&context, &request, &response);
  EXPECT_TRUE(status.ok());
  EXPECT_FALSE(response.success());
  EXPECT_FALSE(response.error_msg().empty());
}

TEST_F(GraphServiceRouterTest, StopWakesPartitionRefreshBackoffPromptly) {
  router_->partition_refresh_interval_ = std::chrono::seconds(1);

  auto start_status = router_->Start();
  ASSERT_TRUE(start_status.ok()) << start_status.ToString();

  std::this_thread::sleep_for(std::chrono::milliseconds(1100));

  auto start = std::chrono::steady_clock::now();
  auto stop_status = router_->Stop();
  auto elapsed = std::chrono::steady_clock::now() - start;

  ASSERT_TRUE(stop_status.ok()) << stop_status.ToString();
  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(), 500);
}
