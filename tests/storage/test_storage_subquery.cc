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
// StorageServiceImpl::ExecuteSubQuery End-to-End Test
// =============================================================================

#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>
#include <thread>

#include "cedar/dtx/storage_service_impl.h"
#include "storage_service.grpc.pb.h"
#include "storage_service.pb.h"

using namespace cedar::dtx;

class StorageSubqueryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create StorageServiceImpl with no partition manager
    // This means cypher_engine_ is null, so ExecuteSubQuery returns empty batch
    service_impl_ = std::make_unique<StorageServiceImpl>(nullptr, nullptr);

    // Start in-process gRPC server
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port_);
    builder.RegisterService(service_impl_.get());
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);
    ASSERT_GT(port_, 0);

    // Create client stub
    std::string target = "127.0.0.1:" + std::to_string(port_);
    channel_ = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
    stub_ = cedar::storage::StorageService::NewStub(channel_);
  }

  void TearDown() override {
    stub_.reset();
    channel_.reset();
    if (server_) {
      server_->Shutdown();
      server_->Wait();
    }
    service_impl_.reset();
  }

  int port_ = 0;
  std::unique_ptr<StorageServiceImpl> service_impl_;
  std::unique_ptr<grpc::Server> server_;
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<cedar::storage::StorageService::Stub> stub_;
};

// ---------------------------------------------------------------------------
// ExecuteSubQuery with no CypherEngine (null partition_manager)
// ---------------------------------------------------------------------------

TEST_F(StorageSubqueryTest, ExecuteSubQueryNoEngineReturnsEmptyBatch) {
  cedar::storage::ExecuteSubQueryRequest request;
  request.set_partition_id(1);
  request.set_query_fragment("MATCH (n) RETURN n");

  grpc::ClientContext context;
  auto reader = stub_->ExecuteSubQuery(&context, request);

  cedar::storage::SubQueryResultBatch batch;
  bool got_batch = reader->Read(&batch);
  EXPECT_TRUE(got_batch);
  EXPECT_TRUE(batch.is_last());
  EXPECT_EQ(batch.columns_size(), 0);
  EXPECT_EQ(batch.records_size(), 0);

  grpc::Status status = reader->Finish();
  EXPECT_TRUE(status.ok());
}

TEST_F(StorageSubqueryTest, ExecuteSubQueryWithParametersNoEngine) {
  cedar::storage::ExecuteSubQueryRequest request;
  request.set_partition_id(1);
  request.set_query_fragment("MATCH (n {id: $id}) RETURN n");

  auto* params = request.mutable_parameters();
  cedar::storage::QueryValue qv;
  qv.set_int_val(42);
  (*params)["id"] = qv;

  grpc::ClientContext context;
  auto reader = stub_->ExecuteSubQuery(&context, request);

  cedar::storage::SubQueryResultBatch batch;
  bool got_batch = reader->Read(&batch);
  EXPECT_TRUE(got_batch);
  EXPECT_TRUE(batch.is_last());

  grpc::Status status = reader->Finish();
  EXPECT_TRUE(status.ok());
}

TEST_F(StorageSubqueryTest, ExecuteSubQueryCancelledContext) {
  cedar::storage::ExecuteSubQueryRequest request;
  request.set_partition_id(1);
  request.set_query_fragment("MATCH (n) RETURN n");

  grpc::ClientContext context;
  context.TryCancel();
  auto reader = stub_->ExecuteSubQuery(&context, request);

  cedar::storage::SubQueryResultBatch batch;
  // Server may return CANCELLED immediately
  grpc::Status status = reader->Finish();
  // Status can be CANCELLED or OK depending on timing
  EXPECT_TRUE(status.ok() || status.error_code() == grpc::StatusCode::CANCELLED);
}
