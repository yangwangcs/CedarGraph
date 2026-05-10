#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>
#include "cedar/service/graph_service_router.h"

using namespace cedar;
using cedar::service::GraphServiceRouter;

class GraphServiceRouterTest : public ::testing::Test {
protected:
    void SetUp() override {
        router_ = std::make_unique<GraphServiceRouter>();
    }

    std::unique_ptr<GraphServiceRouter> router_;
};

TEST_F(GraphServiceRouterTest, GetSchemaReturnsEmptyWhenNoMetaD) {
    // Without initialization, GetSchema should gracefully fail
    grpc::ServerContext context;
    cedar::query::GetSchemaRequest request;
    cedar::query::GetSchemaResponse response;

    auto status = router_->GetSchema(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_FALSE(response.success());
    EXPECT_FALSE(response.error_msg().empty());
}
