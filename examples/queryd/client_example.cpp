// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Example client for cedar-queryd

#include <iostream>
#include <string>

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include "query_service.pb.h"
#include "query_service.grpc.pb.h"

using cedar::query::QueryService;
using cedar::query::ExecuteQueryRequest;
using cedar::query::ExecuteQueryResponse;
using cedar::query::TraverseRequest;
using cedar::query::TraverseResponse;
using cedar::query::TemporalQueryRequest;
using cedar::query::TemporalQueryResponse;
using cedar::query::HealthRequest;
using cedar::query::HealthResponse;

class QueryClient {
 public:
  QueryClient(std::shared_ptr<grpc::Channel> channel)
      : stub_(QueryService::NewStub(channel)) {}

  // Execute a Cypher query
  bool ExecuteQuery(const std::string& query,
                    const std::map<std::string, std::string>& params,
                    ExecuteQueryResponse* response) {
    ExecuteQueryRequest request;
    request.set_query(query);
    
    // Add parameters
    for (const auto& [key, value] : params) {
      (*request.mutable_parameters()->mutable_params())[key].set_string_val(value);
    }
    
    request.set_consistency(cedar::query::ConsistencyLevel::READ_YOUR_WRITES);
    request.set_timeout_ms(30000);
    
    grpc::ClientContext context;
    auto status = stub_->ExecuteQuery(&context, request, response);
    
    return status.ok();
  }

  // Traverse from a starting node
  bool Traverse(uint64_t start_node_id,
                TraverseRequest::Direction direction,
                uint32_t max_depth,
                TraverseResponse* response) {
    TraverseRequest request;
    request.set_start_node_id(start_node_id);
    request.set_direction(direction);
    request.set_max_depth(max_depth);
    request.set_max_branch(100);
    
    grpc::ClientContext context;
    auto status = stub_->Traverse(&context, request, response);
    
    return status.ok();
  }

  // Query entity at specific time
  bool TemporalQuery(uint64_t entity_id,
                     TemporalQueryRequest::EntityType entity_type,
                     uint64_t timestamp,
                     TemporalQueryResponse* response) {
    TemporalQueryRequest request;
    request.set_entity_id(entity_id);
    request.set_entity_type(entity_type);
    request.set_query_type(TemporalQueryRequest::AS_OF);
    request.set_timestamp(timestamp);
    
    grpc::ClientContext context;
    auto status = stub_->TemporalQuery(&context, request, response);
    
    return status.ok();
  }

  // Health check
  bool HealthCheck(bool detailed, HealthResponse* response) {
    HealthRequest request;
    request.set_detailed(detailed);
    
    grpc::ClientContext context;
    auto status = stub_->Health(&context, request, response);
    
    return status.ok();
  }

 private:
  std::unique_ptr<QueryService::Stub> stub_;
};

void PrintResult(const ExecuteQueryResponse& response) {
  if (!response.success()) {
    std::cout << "Query failed: " << response.error_msg() << std::endl;
    return;
  }
  
  std::cout << "Query ID: " << response.query_id() << std::endl;
  std::cout << "Execution time: " << response.stats().execution_time_us() 
            << " us" << std::endl;
  std::cout << "Rows scanned: " << response.stats().rows_scanned() << std::endl;
  std::cout << "Rows returned: " << response.stats().rows_returned() << std::endl;
  
  const auto& result = response.result_set();
  std::cout << "\nColumns: ";
  for (const auto& col : result.columns()) {
    std::cout << col << " ";
  }
  std::cout << std::endl;
  
  std::cout << "\nResults (" << result.rows_size() << " rows):" << std::endl;
  for (const auto& row : result.rows()) {
    std::cout << "  [";
    for (int i = 0; i < row.values_size(); ++i) {
      const auto& val = row.values(i);
      switch (val.value_type_case()) {
        case cedar::query::Value::kStringVal:
          std::cout << "\"" << val.string_val() << "\"";
          break;
        case cedar::query::Value::kIntVal:
          std::cout << val.int_val();
          break;
        case cedar::query::Value::kFloatVal:
          std::cout << val.float_val();
          break;
        case cedar::query::Value::kBoolVal:
          std::cout << (val.bool_val() ? "true" : "false");
          break;
        case cedar::query::Value::kNullVal:
          std::cout << "null";
          break;
        default:
          std::cout << "?";
      }
      if (i < row.values_size() - 1) {
        std::cout << ", ";
      }
    }
    std::cout << "]" << std::endl;
  }
}

int main(int argc, char** argv) {
  std::string server_address = "localhost:9669";
  if (argc > 1) {
    server_address = argv[1];
  }
  
  std::cout << "Connecting to cedar-queryd at " << server_address << std::endl;
  
  // Create client
  QueryClient client(
      grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials()));
  
  // Health check
  HealthResponse health;
  if (client.HealthCheck(true, &health)) {
    std::cout << "\n=== Health Check ===" << std::endl;
    std::cout << "Status: " << (health.healthy() ? "healthy" : "degraded") 
              << std::endl;
    std::cout << "Parser: " << (health.parser_healthy() ? "OK" : "FAIL") 
              << std::endl;
    std::cout << "Executor: " << (health.executor_healthy() ? "OK" : "FAIL")
              << std::endl;
    std::cout << "Storage Client: " 
              << (health.storage_client_healthy() ? "OK" : "FAIL") << std::endl;
    std::cout << "Active queries: " << health.active_queries() << std::endl;
  }
  
  // Example 1: Simple query
  std::cout << "\n=== Example 1: Match All Persons ===" << std::endl;
  ExecuteQueryResponse response1;
  if (client.ExecuteQuery("MATCH (n:Person) RETURN n.name, n.age LIMIT 10", 
                          {}, &response1)) {
    PrintResult(response1);
  }
  
  // Example 2: Query with parameters
  std::cout << "\n=== Example 2: Query with Parameters ===" << std::endl;
  ExecuteQueryResponse response2;
  std::map<std::string, std::string> params = {{"name", "Alice"}};
  if (client.ExecuteQuery(
          "MATCH (n:Person {name: $name}) RETURN n", params, &response2)) {
    PrintResult(response2);
  }
  
  // Example 3: Graph traversal
  std::cout << "\n=== Example 3: Graph Traversal ===" << std::endl;
  TraverseResponse traverse_response;
  if (client.Traverse(1, TraverseRequest::OUTGOING, 2, &traverse_response)) {
    std::cout << "Traverse success: " << traverse_response.success() << std::endl;
    std::cout << "Nodes visited: " << traverse_response.nodes_visited() << std::endl;
    std::cout << "Paths found: " << traverse_response.paths_size() << std::endl;
  }
  
  // Example 4: Temporal query (AS OF)
  std::cout << "\n=== Example 4: Temporal Query (AS OF) ===" << std::endl;
  TemporalQueryResponse temporal_response;
  if (client.TemporalQuery(1, TemporalQueryRequest::NODE, 
                           1704067200000,  // 2024-01-01 00:00:00 UTC
                           &temporal_response)) {
    std::cout << "Temporal query success: " << temporal_response.success() 
              << std::endl;
    std::cout << "Versions found: " << temporal_response.versions_size() 
              << std::endl;
    for (const auto& version : temporal_response.versions()) {
      std::cout << "  Version at " << version.timestamp() 
                << (version.is_deleted() ? " [DELETED]" : "") << std::endl;
    }
  }
  
  std::cout << "\n=== Done ===" << std::endl;
  return 0;
}
