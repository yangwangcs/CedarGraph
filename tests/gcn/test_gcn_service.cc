#include <gtest/gtest.h>

#include <grpcpp/grpcpp.h>
#include <thread>

#include "cedar/gcn/gcn_service.h"
#include "gcn_service.grpc.pb.h"

using namespace cedar::gcn;

class GcnServiceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    service_ = std::make_unique<GcnServiceImpl>();
    server_address_ = "127.0.0.1:0";

    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials(), &port_);
    builder.RegisterService(service_.get());
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);

    std::ostringstream oss;
    oss << "127.0.0.1:" << port_;
    server_address_ = oss.str();

    server_thread_ = std::thread([this]() { server_->Wait(); });

    auto channel = grpc::CreateChannel(server_address_, grpc::InsecureChannelCredentials());
    stub_ = GcnService::NewStub(channel);
  }

  void TearDown() override {
    if (server_) {
      server_->Shutdown();
    }
    if (server_thread_.joinable()) {
      server_thread_.join();
    }
  }

  std::unique_ptr<GcnServiceImpl> service_;
  std::unique_ptr<grpc::Server> server_;
  std::thread server_thread_;
  std::string server_address_;
  int port_ = 0;
  std::unique_ptr<GcnService::Stub> stub_;
};

TEST_F(GcnServiceTest, TraverseReturnsDefaults) {
  TraversalRequest request;
  request.set_trace_id("test-trace-123");
  request.set_root_entity_id(42);
  request.set_max_hops(3);

  TraversalResponse response;
  grpc::ClientContext context;
  grpc::Status status = stub_->Traverse(&context, request, &response);

  EXPECT_TRUE(status.ok());
  EXPECT_EQ(response.served_version(), 0u);
  EXPECT_EQ(response.visited_entity_ids_size(), 0);
  EXPECT_EQ(response.success(), false);
  EXPECT_EQ(response.error_msg(), "Dispatcher not available");
  EXPECT_EQ(response.trace_id(), "");
}

TEST_F(GcnServiceTest, SubQueryReturnsDefaults) {
  SubQueryRequest request;
  request.set_trace_id("test-trace-456");
  request.set_root_entity_id(42);

  SubQueryResponse response;
  grpc::ClientContext context;
  grpc::Status status = stub_->SubQuery(&context, request, &response);

  EXPECT_TRUE(status.ok());
  EXPECT_EQ(response.success(), false);
  EXPECT_EQ(response.error_msg(), "Dispatcher not available");
  EXPECT_EQ(response.trace_id(), "test-trace-456");
  EXPECT_EQ(response.next_entity_ids_size(), 0);
}

TEST_F(GcnServiceTest, OnCacheInvalidateReturnsEmpty) {
  CacheInvalidateNotice request;
  request.set_entity_id(42);
  request.set_version(1);

  Empty response;
  grpc::ClientContext context;
  grpc::Status status = stub_->OnCacheInvalidate(&context, request, &response);

  EXPECT_TRUE(status.ok());
}
