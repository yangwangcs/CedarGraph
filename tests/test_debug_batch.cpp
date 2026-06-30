// Debug test for batch neighbor queries
#include <iostream>
#include <thread>
#include <chrono>
#include <grpcpp/grpcpp.h>
#include "storage_service.pb.h"
#include "storage_service.grpc.pb.h"

using namespace std;

int main() {
  auto channel = grpc::CreateChannel("127.0.0.1:7001", grpc::InsecureChannelCredentials());
  auto stub = cedar::storage::StorageService::NewStub(channel);
  
  cout << "=== Debug Batch Neighbor Query ===" << endl << endl;
  
  // Test 1: Write single vertex with multiple neighbors
  cout << "[1] Writing vertex 100 with 5 neighbors..." << endl;
  for (int e = 0; e < 5; ++e) {
    cedar::storage::PutRequest req;
    auto* key = req.mutable_key();
    key->set_entity_id(100);  // Vertex ID
    key->set_timestamp(5000000);  // Fixed timestamp
    key->set_column_id(100 + e);  // Edge column: 100, 101, 102, 103, 104
    key->set_type_flags(0 << 16);  // Vertex type
    key->set_partition_id(0);
    
    int32_t neighbor_id = 200 + e;  // Neighbor: 200, 201, 202, 203, 204
    req.mutable_value_descriptor()->set_data(
        reinterpret_cast<const char*>(&neighbor_id), sizeof(neighbor_id));
    req.mutable_txn_version()->set_value(5000000);
    
    cedar::storage::PutResponse resp;
    grpc::ClientContext ctx;
    auto status = stub->Put(&ctx, req, &resp);
    
    cout << "  Edge " << e << " (col=" << (100+e) << "): " 
         << (status.ok() && resp.success() ? "OK" : "FAIL") << endl;
  }
  
  // Small delay
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  
  // Test 2: Read single neighbor
  cout << endl << "[2] Reading single neighbor (col=100)..." << endl;
  {
    cedar::storage::GetRequest req;
    req.mutable_key()->set_entity_id(100);
    req.mutable_key()->set_timestamp(5000000);
    req.mutable_key()->set_column_id(100);
    req.mutable_key()->set_type_flags(0 << 16);
    req.mutable_key()->set_partition_id(0);
    
    cedar::storage::GetResponse resp;
    grpc::ClientContext ctx;
    auto status = stub->Get(&ctx, req, &resp);
    
    cout << "  Status: " << (status.ok() ? "OK" : "FAIL") << endl;
    cout << "  Success: " << resp.success() << endl;
    cout << "  Found: " << resp.found() << endl;
    
    if (resp.found() && resp.has_value_descriptor() && resp.value_descriptor().data().size() >= 4) {
      int32_t val = *reinterpret_cast<const int32_t*>(resp.value_descriptor().data().data());
      cout << "  Value: " << val << " (expected: 200)" << endl;
    }
  }
  
  // Test 3: Batch read all neighbors
  cout << endl << "[3] Batch reading all 5 neighbors..." << endl;
  {
    cedar::storage::BatchGetRequest req;
    for (int e = 0; e < 5; ++e) {
      auto* key = req.add_keys();
      key->set_entity_id(100);
      key->set_timestamp(5000000);
      key->set_column_id(100 + e);
      key->set_type_flags(0 << 16);
      key->set_partition_id(0);
    }
    
    cedar::storage::BatchGetResponse resp;
    grpc::ClientContext ctx;
    auto status = stub->BatchGet(&ctx, req, &resp);
    
    cout << "  Status: " << (status.ok() ? "OK" : "FAIL") << endl;
    cout << "  Success: " << resp.success() << endl;
    cout << "  Results count: " << resp.found_size() << endl;
    
    for (int i = 0; i < resp.found_size(); ++i) {
      cout << "  [" << i << "] Found: " << resp.found(i);
      if (resp.found(i) && resp.descriptors(i).data().size() >= 4) {
        int32_t val = *reinterpret_cast<const int32_t*>(resp.descriptors(i).data().data());
        cout << " Value: " << val;
      }
      cout << endl;
    }
  }
  
  // Test 4: Individual reads for comparison
  cout << endl << "[4] Individual reads for comparison..." << endl;
  for (int e = 0; e < 5; ++e) {
    cedar::storage::GetRequest req;
    req.mutable_key()->set_entity_id(100);
    req.mutable_key()->set_timestamp(5000000);
    req.mutable_key()->set_column_id(100 + e);
    req.mutable_key()->set_type_flags(0 << 16);
    req.mutable_key()->set_partition_id(0);
    
    cedar::storage::GetResponse resp;
    grpc::ClientContext ctx;
    auto status = stub->Get(&ctx, req, &resp);
    
    cout << "  col=" << (100+e) << ": " 
         << (status.ok() && resp.found() ? "FOUND" : "NOT FOUND");
    if (resp.found() && resp.has_value_descriptor() && resp.value_descriptor().data().size() >= 4) {
      int32_t val = *reinterpret_cast<const int32_t*>(resp.value_descriptor().data().data());
      cout << " -> " << val;
    }
    cout << endl;
  }
  
  cout << endl << "=== Debug Complete ===" << endl;
  return 0;
}
