// Copyright 2026 CedarGraph Authors
//
// Licensed under the Apache License, Version 2.0

#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server_builder.h>
#include <memory>

#include "cedar/service/graph_service_router.h"
#include "cedar/dtx/security.h"

using namespace cedar;
using cedar::service::GraphServiceRouter;
using cedar::dtx::security::SecurityManager;

class GraphServiceRouterAuthTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto* sm = SecurityManager::GetInstance();
    cedar::dtx::security::SecurityManager::Config cfg;
    cfg.enable_auth = true;
    cfg.auth.jwt_secret = "test-secret-key-with-at-least-32-bytes!!";
    cfg.auth.accounts.push_back({"writer", "writerpass", {"readwrite"}});
    auto s = sm->Initialize(cfg);
    EXPECT_TRUE(s.ok()) << s.ToString();

    router_ = std::make_unique<GraphServiceRouter>();

    grpc::ServerBuilder builder;
    int port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(static_cast<cedar::query::QueryService::Service*>(router_.get()));
    builder.RegisterService(static_cast<cedargrpc::CedarGraphService::Service*>(router_.get()));
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);
    ASSERT_GT(port, 0);

    channel_ = grpc::CreateChannel("127.0.0.1:" + std::to_string(port),
                                   grpc::InsecureChannelCredentials());
    query_stub_ = cedar::query::QueryService::NewStub(channel_);
    graph_stub_ = cedargrpc::CedarGraphService::NewStub(channel_);
  }

  void TearDown() override {
    query_stub_.reset();
    graph_stub_.reset();
    channel_.reset();
    if (server_) {
      server_->Shutdown();
      server_->Wait();
      server_.reset();
    }
    router_.reset();
    SecurityManager::GetInstance()->Shutdown();
  }

  std::string GetValidToken() {
    auto* auth = SecurityManager::GetInstance()->GetAuthenticator();
    auto result = auth->Authenticate("writer", "writerpass");
    EXPECT_TRUE(result.ok());
    return auth->GenerateJWT(result.value());
  }

  std::unique_ptr<GraphServiceRouter> router_;
  std::unique_ptr<grpc::Server> server_;
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<cedar::query::QueryService::Stub> query_stub_;
  std::unique_ptr<cedargrpc::CedarGraphService::Stub> graph_stub_;
};

TEST_F(GraphServiceRouterAuthTest, ExecuteQueryMissingTokenRejected) {
  grpc::ClientContext ctx;
  cedar::query::ExecuteQueryRequest req;
  req.set_query("MATCH (n) RETURN n LIMIT 1");
  cedar::query::ExecuteQueryResponse resp;
  auto status = query_stub_->ExecuteQuery(&ctx, req, &resp);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
}

TEST_F(GraphServiceRouterAuthTest, ExecuteQueryValidTokenAccepted) {
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer " + GetValidToken());
  cedar::query::ExecuteQueryRequest req;
  req.set_query("MATCH (n) RETURN n LIMIT 1");
  cedar::query::ExecuteQueryResponse resp;
  auto status = query_stub_->ExecuteQuery(&ctx, req, &resp);
  EXPECT_NE(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
  EXPECT_NE(status.error_code(), grpc::StatusCode::PERMISSION_DENIED);
}

TEST_F(GraphServiceRouterAuthTest, BeginTransactionMissingTokenRejected) {
  grpc::ClientContext ctx;
  cedargrpc::BeginTransactionRequest req;
  cedargrpc::Transaction resp;
  auto status = graph_stub_->BeginTransaction(&ctx, req, &resp);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
}

TEST_F(GraphServiceRouterAuthTest, BeginTransactionValidTokenAccepted) {
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer " + GetValidToken());
  cedargrpc::BeginTransactionRequest req;
  cedargrpc::Transaction resp;
  auto status = graph_stub_->BeginTransaction(&ctx, req, &resp);
  EXPECT_NE(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
  EXPECT_NE(status.error_code(), grpc::StatusCode::PERMISSION_DENIED);
}
