// Copyright 2026 CedarGraph Authors
//
// Licensed under the Apache License, Version 2.0

#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server_builder.h>

#include "cedar/dtx/security.h"
#include "cedar/dtx/storage_service_impl.h"
#include "cedar/storage/cedar_graph_storage.h"

using namespace cedar;
using namespace cedar::dtx;
using namespace cedar::dtx::security;

class StorageServiceAuthTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto* sm = SecurityManager::GetInstance();
    SecurityManager::Config cfg;
    cfg.enable_auth = true;
    cfg.auth.jwt_secret = "test-secret-key-with-at-least-32-bytes!!";
    cfg.auth.accounts.push_back({"admin", "adminpass", {"admin"}});
    auto s = sm->Initialize(cfg);
    EXPECT_TRUE(s.ok()) << s.ToString();

    pm_ = std::make_unique<StoragePartitionManager>();
    StoragePartitionManager::PartitionConfig pcfg;
    pcfg.data_root = "/tmp/cedar_test_storage_auth";
    pcfg.max_partitions = 4;
    s = pm_->Initialize(pcfg);
    EXPECT_TRUE(s.ok()) << s.ToString();
    pm_->AddPartition(0);

    service_ = std::make_unique<StorageServiceImpl>(pm_.get());

    grpc::ServerBuilder builder;
    int port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(service_.get());
    server_ = builder.BuildAndStart();
    EXPECT_NE(server_, nullptr);
    EXPECT_GT(port, 0);

    auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(port),
                                       grpc::InsecureChannelCredentials());
    stub_ = cedar::storage::StorageService::NewStub(channel);
  }

  void TearDown() override {
    stub_.reset();
    if (server_) {
      server_->Shutdown();
      server_->Wait();
      server_.reset();
    }
    service_.reset();
    pm_->Shutdown();
    pm_.reset();
    SecurityManager::GetInstance()->Shutdown();
  }

  std::unique_ptr<grpc::ServerContext> MakeContext(const std::string& bearer_token = "") {
    auto ctx = std::make_unique<grpc::ServerContext>();
    if (!bearer_token.empty()) {
      ctx->AddInitialMetadata("authorization", "Bearer " + bearer_token);
    }
    return ctx;
  }

  std::string GetValidToken() {
    auto* auth = SecurityManager::GetInstance()->GetAuthenticator();
    auto result = auth->Authenticate("admin", "adminpass");
    EXPECT_TRUE(result.ok());
    return auth->GenerateJWT(result.value());
  }

  std::unique_ptr<StoragePartitionManager> pm_;
  std::unique_ptr<StorageServiceImpl> service_;
  std::unique_ptr<grpc::Server> server_;
  std::unique_ptr<cedar::storage::StorageService::Stub> stub_;
};

// =============================================================================
// Missing-token rejection tests for every unary StorageService handler.
// =============================================================================

TEST_F(StorageServiceAuthTest, PutMissingTokenRejected) {
  auto ctx = MakeContext();
  cedar::storage::PutRequest req;
  cedar::storage::PutResponse resp;
  auto status = service_->Put(ctx.get(), &req, &resp);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
}

TEST_F(StorageServiceAuthTest, GetMissingTokenRejected) {
  auto ctx = MakeContext();
  cedar::storage::GetRequest req;
  cedar::storage::GetResponse resp;
  auto status = service_->Get(ctx.get(), &req, &resp);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
}

TEST_F(StorageServiceAuthTest, DeleteMissingTokenRejected) {
  auto ctx = MakeContext();
  cedar::storage::DeleteRequest req;
  cedar::storage::DeleteResponse resp;
  auto status = service_->Delete(ctx.get(), &req, &resp);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
}

TEST_F(StorageServiceAuthTest, ScanMissingTokenRejected) {
  auto ctx = MakeContext();
  cedar::storage::ScanRequest req;
  cedar::storage::ScanResponse resp;
  auto status = service_->Scan(ctx.get(), &req, &resp);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
}

TEST_F(StorageServiceAuthTest, ScanNodeV2MissingTokenRejected) {
  auto ctx = MakeContext();
  cedar::storage::ScanNodeRequestV2 req;
  cedar::storage::ScanResponse resp;
  auto status = service_->ScanNodeV2(ctx.get(), &req, &resp);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
}

TEST_F(StorageServiceAuthTest, ScanEdgeV2MissingTokenRejected) {
  auto ctx = MakeContext();
  cedar::storage::ScanEdgeRequestV2 req;
  cedar::storage::ScanResponse resp;
  auto status = service_->ScanEdgeV2(ctx.get(), &req, &resp);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
}

TEST_F(StorageServiceAuthTest, BatchPutMissingTokenRejected) {
  auto ctx = MakeContext();
  cedar::storage::BatchPutRequest req;
  cedar::storage::BatchPutResponse resp;
  auto status = service_->BatchPut(ctx.get(), &req, &resp);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
}

TEST_F(StorageServiceAuthTest, BatchGetMissingTokenRejected) {
  auto ctx = MakeContext();
  cedar::storage::BatchGetRequest req;
  cedar::storage::BatchGetResponse resp;
  auto status = service_->BatchGet(ctx.get(), &req, &resp);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
}

TEST_F(StorageServiceAuthTest, PrepareMissingTokenRejected) {
  auto ctx = MakeContext();
  cedar::storage::PrepareRequest req;
  cedar::storage::PrepareResponse resp;
  auto status = service_->Prepare(ctx.get(), &req, &resp);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
}

TEST_F(StorageServiceAuthTest, CommitMissingTokenRejected) {
  auto ctx = MakeContext();
  cedar::storage::CommitRequest req;
  cedar::storage::CommitResponse resp;
  auto status = service_->Commit(ctx.get(), &req, &resp);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
}

TEST_F(StorageServiceAuthTest, AbortMissingTokenRejected) {
  auto ctx = MakeContext();
  cedar::storage::AbortRequest req;
  cedar::storage::AbortResponse resp;
  auto status = service_->Abort(ctx.get(), &req, &resp);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
}

TEST_F(StorageServiceAuthTest, InquireMissingTokenRejected) {
  auto ctx = MakeContext();
  cedar::storage::InquireRequest req;
  cedar::storage::InquireResponse resp;
  auto status = service_->Inquire(ctx.get(), &req, &resp);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
}

TEST_F(StorageServiceAuthTest, GetRangeForComputeMissingTokenRejected) {
  auto ctx = MakeContext();
  cedar::storage::GetRangeForComputeRequest req;
  cedar::storage::GetRangeForComputeResponse resp;
  auto status = service_->GetRangeForCompute(ctx.get(), &req, &resp);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
}

TEST_F(StorageServiceAuthTest, GetCommittedVersionMissingTokenRejected) {
  auto ctx = MakeContext();
  cedar::storage::GetCommittedVersionRequest req;
  cedar::storage::GetCommittedVersionResponse resp;
  auto status = service_->GetCommittedVersion(ctx.get(), &req, &resp);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
}

TEST_F(StorageServiceAuthTest, GetPartitionInfoMissingTokenRejected) {
  auto ctx = MakeContext();
  cedar::storage::GetPartitionInfoRequest req;
  cedar::storage::GetPartitionInfoResponse resp;
  auto status = service_->GetPartitionInfo(ctx.get(), &req, &resp);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
}

TEST_F(StorageServiceAuthTest, FlushMissingTokenRejected) {
  auto ctx = MakeContext();
  cedar::storage::FlushRequest req;
  cedar::storage::FlushResponse resp;
  auto status = service_->Flush(ctx.get(), &req, &resp);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
}

// =============================================================================
// Streaming handlers also reject unauthenticated callers before touching the
// stream.  A null stream pointer is safe because the auth gate returns early.
// =============================================================================

TEST_F(StorageServiceAuthTest, HeartbeatMissingTokenRejected) {
  auto ctx = MakeContext();
  auto status = service_->Heartbeat(ctx.get(), nullptr);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
}

TEST_F(StorageServiceAuthTest, ExecuteSubQueryMissingTokenRejected) {
  auto ctx = MakeContext();
  cedar::storage::ExecuteSubQueryRequest req;
  auto status = service_->ExecuteSubQuery(ctx.get(), &req, nullptr);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
}

// =============================================================================
// Invalid and valid token tests for representative handlers.
// =============================================================================

TEST_F(StorageServiceAuthTest, PutInvalidTokenRejected) {
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer bad-token");
  cedar::storage::PutRequest req;
  cedar::storage::PutResponse resp;
  auto status = stub_->Put(&ctx, req, &resp);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::PERMISSION_DENIED);
}

TEST_F(StorageServiceAuthTest, PutValidTokenAccepted) {
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer " + GetValidToken());
  cedar::storage::PutRequest req;
  auto* key = req.mutable_key();
  key->set_entity_id(1);
  key->set_partition_id(0);
  key->set_timestamp(1);
  req.mutable_txn_version()->set_value(1);
  req.set_txn_id(1);
  cedar::storage::PutResponse resp;
  auto status = stub_->Put(&ctx, req, &resp);
  EXPECT_NE(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
  EXPECT_NE(status.error_code(), grpc::StatusCode::PERMISSION_DENIED);
}

TEST_F(StorageServiceAuthTest, GetValidTokenAccepted) {
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer " + GetValidToken());
  cedar::storage::GetRequest req;
  req.mutable_key()->set_partition_id(0);
  req.mutable_key()->set_entity_id(1);
  req.mutable_key()->set_timestamp(1);
  cedar::storage::GetResponse resp;
  auto status = stub_->Get(&ctx, req, &resp);
  EXPECT_NE(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
  EXPECT_NE(status.error_code(), grpc::StatusCode::PERMISSION_DENIED);
}

TEST_F(StorageServiceAuthTest, GetPartitionInfoValidTokenAccepted) {
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer " + GetValidToken());
  cedar::storage::GetPartitionInfoRequest req;
  cedar::storage::GetPartitionInfoResponse resp;
  auto status = stub_->GetPartitionInfo(&ctx, req, &resp);
  EXPECT_NE(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
  EXPECT_NE(status.error_code(), grpc::StatusCode::PERMISSION_DENIED);
}

TEST_F(StorageServiceAuthTest, FlushValidTokenAccepted) {
  grpc::ClientContext ctx;
  ctx.AddMetadata("authorization", "Bearer " + GetValidToken());
  cedar::storage::FlushRequest req;
  cedar::storage::FlushResponse resp;
  auto status = stub_->Flush(&ctx, req, &resp);
  EXPECT_NE(status.error_code(), grpc::StatusCode::UNAUTHENTICATED);
  EXPECT_NE(status.error_code(), grpc::StatusCode::PERMISSION_DENIED);
}
