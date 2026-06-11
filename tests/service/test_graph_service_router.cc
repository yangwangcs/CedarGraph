#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>
#include "cedar/service/graph_service_router.h"
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
