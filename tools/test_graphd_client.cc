// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

// Simple test client for GraphD query routing

#include <iostream>
#include <memory>
#include <string>
#include <chrono>

#include <grpcpp/grpcpp.h>
#include "query_service.grpc.pb.h"
#include "meta_service.grpc.pb.h"
#include "storage_service.grpc.pb.h"

void TestMetaDPartitionAssignment(std::shared_ptr<grpc::Channel> channel) {
  std::cout << "=== Testing MetaD Partition Assignment ===" << std::endl;
  
  auto stub = cedar::meta::MetaService::NewStub(channel);
  
  cedar::meta::GetPartitionAssignmentRequest request;
  request.set_space_name("default");
  request.set_partition_id(123);
  
  cedar::meta::GetPartitionAssignmentResponse response;
  grpc::ClientContext context;
  
  auto status = stub->GetPartitionAssignment(&context, request, &response);
  
  if (status.ok() && response.success()) {
    std::cout << "✓ Partition 123 assigned to node " << response.assignment().leader_node() << std::endl;
    std::cout << "  Version: " << response.assignment().version() << std::endl;
    std::cout << "  Followers: " << response.assignment().follower_nodes_size() << std::endl;
  } else {
    std::cout << "✗ Failed: " << (status.ok() ? response.error_msg() : status.error_message()) << std::endl;
  }
  std::cout << std::endl;
}

void TestGraphDHealth(std::shared_ptr<grpc::Channel> channel) {
  std::cout << "=== Testing GraphD Health ===" << std::endl;
  
  auto stub = cedar::query::QueryService::NewStub(channel);
  
  cedar::query::HealthRequest request;
  request.set_detailed(true);
  
  cedar::query::HealthResponse response;
  grpc::ClientContext context;
  
  auto status = stub->Health(&context, request, &response);
  
  if (status.ok()) {
    std::cout << "✓ GraphD Health: " << (response.healthy() ? "HEALTHY" : "DEGRADED") << std::endl;
    std::cout << "  Status: " << response.status() << std::endl;
    std::cout << "  Active Queries: " << response.active_queries() << std::endl;
  } else {
    std::cout << "✗ Health check failed: " << status.error_message() << std::endl;
  }
  std::cout << std::endl;
}

void TestGraphDQuery(std::shared_ptr<grpc::Channel> channel) {
  std::cout << "=== Testing GraphD Query Routing ===" << std::endl;
  
  auto stub = cedar::query::QueryService::NewStub(channel);
  
  cedar::query::ExecuteQueryRequest request;
  request.set_query("MATCH (n) WHERE id(n) = 12345 RETURN n");
  request.set_explain_only(true);
  
  cedar::query::ExecuteQueryResponse response;
  grpc::ClientContext context;
  
  auto start = std::chrono::steady_clock::now();
  auto status = stub->ExecuteQuery(&context, request, &response);
  auto end = std::chrono::steady_clock::now();
  
  auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  
  if (status.ok() && response.success()) {
    std::cout << "✓ Query executed successfully" << std::endl;
    std::cout << "  Query ID: " << response.query_id() << std::endl;
    std::cout << "  Latency: " << latency << "ms" << std::endl;
    if (response.has_stats()) {
      std::cout << "  Execution Time: " << response.stats().execution_time_us() << " us" << std::endl;
    }
    if (!response.execution_plan().empty()) {
      std::cout << "  Execution Plan:\n" << response.execution_plan() << std::endl;
    }
  } else {
    std::cout << "✗ Query failed: " << (status.ok() ? response.error_msg() : status.error_message()) << std::endl;
  }
  std::cout << std::endl;
}

void TestWriteData(const std::string& storage_addr) {
  std::cout << "=== Writing Test Data to StorageD ===" << std::endl;
  
  auto channel = grpc::CreateChannel(storage_addr, grpc::InsecureChannelCredentials());
  auto stub = cedar::storage::StorageService::NewStub(channel);
  
  auto now = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
  
  // Helper: create a simple descriptor (8-byte big-endian uint64)
  auto MakeDescriptor = [](uint64_t raw_value) -> std::string {
    std::string buf(8, '\0');
    uint64_t be = __builtin_bswap64(raw_value);  // big-endian
    memcpy(&buf[0], &be, 8);
    return buf;
  };
  
  // Write vertex 12345
  {
    cedar::storage::PutRequest req;
    auto* key = req.mutable_key();
    key->set_entity_id(12345);
    key->set_timestamp(now);
    key->set_target_id(0);
    key->set_column_id(0);
    key->set_sequence(0);
    key->set_type_flags(0);
    key->set_partition_id(12345 % 32768);
    key->set_entity_type(0);  // Vertex
    req.mutable_value_descriptor()->set_data(MakeDescriptor(42));
    req.mutable_txn_version()->set_value(1);
    req.set_txn_id(1);
    
    cedar::storage::PutResponse resp;
    grpc::ClientContext ctx;
    auto status = stub->Put(&ctx, req, &resp);
    if (status.ok() && resp.success()) {
      std::cout << "✓ Written vertex 12345" << std::endl;
    } else {
      std::cout << "✗ Failed to write vertex: " 
                << (status.ok() ? resp.error_msg() : status.error_message()) << std::endl;
    }
  }
  
  // Write edge 12345 -> 67890
  {
    cedar::storage::PutRequest req;
    auto* key = req.mutable_key();
    key->set_entity_id(12345);
    key->set_timestamp(now + 1);
    key->set_target_id(67890);
    key->set_column_id(1);  // edge type
    key->set_sequence(0);
    key->set_type_flags(0);
    key->set_partition_id(12345 % 32768);
    key->set_entity_type(1);  // EdgeOut
    req.mutable_value_descriptor()->set_data(MakeDescriptor(100));
    req.mutable_txn_version()->set_value(1);
    req.set_txn_id(1);
    
    cedar::storage::PutResponse resp;
    grpc::ClientContext ctx;
    auto status = stub->Put(&ctx, req, &resp);
    if (status.ok() && resp.success()) {
      std::cout << "✓ Written edge 12345 -> 67890" << std::endl;
    } else {
      std::cout << "✗ Failed to write edge: " 
                << (status.ok() ? resp.error_msg() : status.error_message()) << std::endl;
    }
  }
  
  std::cout << std::endl;
}

void TestGraphDTraverse(std::shared_ptr<grpc::Channel> channel) {
  std::cout << "=== Testing GraphD Traverse ===" << std::endl;
  
  auto stub = cedar::query::QueryService::NewStub(channel);
  
  cedar::query::TraverseRequest request;
  request.set_start_node_id(12345);
  request.set_direction(cedar::query::TraverseRequest::OUTGOING);
  request.set_max_depth(2);
  
  cedar::query::TraverseResponse response;
  grpc::ClientContext context;
  
  auto status = stub->Traverse(&context, request, &response);
  
  if (status.ok() && response.success()) {
    std::cout << "✓ Traverse query successful" << std::endl;
    std::cout << "  Nodes visited: " << response.nodes_visited() << std::endl;
    std::cout << "  Paths returned: " << response.paths_size() << std::endl;
  } else {
    std::cout << "✗ Traverse failed: " << (status.ok() ? response.error_msg() : status.error_message()) << std::endl;
  }
  std::cout << std::endl;
}

int main(int argc, char* argv[]) {
  std::string metad_addr = "127.0.0.1:10559";
  std::string graphd_addr = "127.0.0.1:9669";
  
  if (argc > 1) graphd_addr = argv[1];
  if (argc > 2) metad_addr = argv[2];
  
  std::cout << R"(
   ____                 _     _              
  / ___|_ __ __ _ _ __ | |__ (_)_ __   __ _  
 | |  _| '__/ _` | '_ \| '_ \| | '_ \ / _` | 
 | |_| | | | (_| | |_) | | | | | | | | (_| | 
  \____|_|  \__,_| .__/|_| |_|_|_| |_|\__, | 
                 |_|                  |___/  
         GraphD Query Router Test Client
)" << std::endl;
  
  std::cout << "MetaD: " << metad_addr << std::endl;
  std::cout << "GraphD: " << graphd_addr << std::endl;
  std::cout << std::endl;
  
  auto metad_channel = grpc::CreateChannel(metad_addr, grpc::InsecureChannelCredentials());
  auto graphd_channel = grpc::CreateChannel(graphd_addr, grpc::InsecureChannelCredentials());
  
  // 等待连接就绪
  std::cout << "Waiting for connections..." << std::endl;
  auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(5);
  if (!metad_channel->WaitForConnected(deadline)) {
    std::cerr << "Failed to connect to MetaD" << std::endl;
    return 1;
  }
  if (!graphd_channel->WaitForConnected(deadline)) {
    std::cerr << "Failed to connect to GraphD" << std::endl;
    return 1;
  }
  std::cout << "Connected!" << std::endl << std::endl;
  
  // 运行测试
  TestMetaDPartitionAssignment(metad_channel);
  TestGraphDHealth(graphd_channel);
  TestGraphDQuery(graphd_channel);
  TestWriteData("127.0.0.1:9779");
  TestGraphDTraverse(graphd_channel);
  
  std::cout << "=== All tests completed ===" << std::endl;
  
  return 0;
}
