// Simple gRPC client to test CedarGraph queries
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

  std::string ExecuteQuery(const std::string& query) {
    ExecuteQueryRequest request;
    request.set_query(query);
    
    ExecuteQueryResponse response;
    ClientContext context;
    
    Status status = stub_->ExecuteQuery(&context, request, &response);
    
    if (status.ok()) {
      if (response.success()) {
        std::string result;
        if (response.has_result_set()) {
          const auto& result_set = response.result_set();
          
          // Print column names
          for (const auto& col : result_set.columns()) {
            result += col + "\t";
          }
          result += "\n";
          
          // Print rows
          for (const auto& row : result_set.rows()) {
            for (const auto& val : row.values()) {
              if (val.has_int_val()) {
                result += std::to_string(val.int_val()) + "\t";
              } else if (val.has_string_val()) {
                result += val.string_val() + "\t";
              } else if (val.has_float_val()) {
                result += std::to_string(val.float_val()) + "\t";
              } else if (val.has_bool_val()) {
                result += std::string(val.bool_val() ? "true" : "false") + "\t";
              } else {
                result += "null\t";
              }
            }
            result += "\n";
          }
        }
        return result;
      } else {
        return "Error: " + response.error_msg();
      }
    } else {
      return "RPC Error: " + status.error_message();
    }
  }

 private:
  std::unique_ptr<QueryService::Stub> stub_;
};

int main(int argc, char** argv) {
  std::string server_address = "127.0.0.1:9669";
  
  auto channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
  CedarGraphClient client(channel);
  
  std::cout << "=== CedarGraph Query Client ===" << std::endl;
  std::cout << "Connected to: " << server_address << std::endl;
  std::cout << std::endl;
  
  // Test 1: SHOW SPACES
  std::cout << "=== Test 1: SHOW SPACES ===" << std::endl;
  std::string result = client.ExecuteQuery("SHOW SPACES");
  std::cout << result << std::endl;
  
  // Test 2: MATCH query
  std::cout << "=== Test 2: MATCH (n) RETURN n LIMIT 5 ===" << std::endl;
  result = client.ExecuteQuery("MATCH (n) RETURN n LIMIT 5");
  std::cout << result << std::endl;
  
  // Test 3: CREATE node
  std::cout << "=== Test 3: CREATE (n:Person {name: 'Alice', age: 30}) ===" << std::endl;
  result = client.ExecuteQuery("CREATE (n:Person {name: 'Alice', age: 30})");
  std::cout << result << std::endl;
  
  // Test 4: MATCH with WHERE
  std::cout << "=== Test 4: MATCH (n:Person) WHERE n.age > 25 RETURN n.name ===" << std::endl;
  result = client.ExecuteQuery("MATCH (n:Person) WHERE n.age > 25 RETURN n.name");
  std::cout << result << std::endl;
  
  return 0;
}
