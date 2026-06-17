// Test inserting and querying data from CedarGraph
// Uses point lookup pattern which is known to work
#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include "query_service.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using cedar::query::QueryService;
using cedar::query::ExecuteQueryRequest;
using cedar::query::ExecuteQueryResponse;

class CedarGraphClient {
 public:
  CedarGraphClient(std::shared_ptr<Channel> channel)
      : stub_(QueryService::NewStub(channel)) {}

  void ExecuteQuery(const std::string& query, const std::string& description) {
    std::cout << "=== " << description << " ===" << std::endl;
    std::cout << "Query: " << query << std::endl;
    
    ExecuteQueryRequest request;
    request.set_query(query);
    
    ExecuteQueryResponse response;
    ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
    
    Status status = stub_->ExecuteQuery(&context, request, &response);
    
    if (!status.ok()) {
      std::cout << "  RPC Error: " << status.error_message() << std::endl << std::endl;
      return;
    }
    
    if (!response.success()) {
      std::cout << "  Error: " << response.error_msg() << std::endl << std::endl;
      return;
    }
    
    // Parse result set
    if (response.has_result_set()) {
      const auto& rs = response.result_set();
      
      // Print columns
      if (rs.columns_size() > 0) {
        std::cout << "  Columns: ";
        for (int i = 0; i < rs.columns_size(); ++i) {
          if (i > 0) std::cout << ", ";
          std::cout << rs.columns(i);
        }
        std::cout << std::endl;
      }
      
      // Print rows
      std::cout << "  Rows: " << rs.rows_size() << std::endl;
      for (const auto& row : rs.rows()) {
        std::cout << "  [";
        for (int i = 0; i < row.values_size(); ++i) {
          if (i > 0) std::cout << ", ";
          const auto& val = row.values(i);
          if (val.has_int_val()) {
            std::cout << val.int_val();
          } else if (val.has_float_val()) {
            std::cout << val.float_val();
          } else if (val.has_string_val()) {
            std::cout << "\"" << val.string_val() << "\"";
          } else if (val.has_bool_val()) {
            std::cout << (val.bool_val() ? "true" : "false");
          } else {
            std::cout << "null";
          }
        }
        std::cout << "]" << std::endl;
      }
    } else {
      std::cout << "  (no result set)" << std::endl;
    }
    
    // Print stats
    std::cout << "  Execution time: " << response.stats().execution_time_us() << " us" << std::endl;
    std::cout << "  Rows returned: " << response.stats().rows_returned() << std::endl;
    std::cout << std::endl;
  }

 private:
  std::unique_ptr<QueryService::Stub> stub_;
};

int main() {
  std::string server_address = "127.0.0.1:9669";
  auto channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
  CedarGraphClient client(channel);
  
  std::cout << "=== CedarGraph Data Test ===" << std::endl;
  std::cout << "Connected to: " << server_address << std::endl << std::endl;
  
  // Test 1: CREATE with explicit ID (point lookup pattern)
  client.ExecuteQuery(
    "CREATE (n:Person {id: 100, name: 'Alice', age: 30})",
    "CREATE Alice with ID 100");
  
  // Test 2: CREATE another node with ID
  client.ExecuteQuery(
    "CREATE (n:Person {id: 200, name: 'Bob', age: 25})",
    "CREATE Bob with ID 200");
  
  // Test 3: Point lookup returning specific properties
  client.ExecuteQuery(
    "MATCH (n:Person {id: 100}) RETURN n.id, n.name, n.age",
    "Point lookup Alice - return properties");
  
  // Test 4: Point lookup for Bob
  client.ExecuteQuery(
    "MATCH (n:Person {id: 200}) RETURN n.id, n.name, n.age",
    "Point lookup Bob - return properties");
  
  // Test 5: Cross-partition query
  client.ExecuteQuery(
    "MATCH (n:Person) RETURN n.id, n.name, n.age",
    "Cross-partition MATCH all Person nodes");
  
  return 0;
}
