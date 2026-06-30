// Debug timestamp issue
#include <iostream>
#include <thread>
#include <chrono>
#include <grpcpp/grpcpp.h>
#include "storage_service.pb.h"
#include "storage_service.grpc.pb.h"
#include "cedar/types/descriptor.h"

int main() {
  auto channel = grpc::CreateChannel("127.0.0.1:9779", grpc::InsecureChannelCredentials());
  auto stub = cedar::storage::StorageService::NewStub(channel);
  
  // Test with timestamp = 2000000 (like Test 1)
  std::cout << "=== Test timestamp = 2000000 ===" << std::endl;
  {
    cedar::storage::PutRequest req;
    req.mutable_key()->set_entity_id(100);
    req.mutable_key()->set_timestamp(2000000);
    req.mutable_key()->set_column_id(1);
    req.mutable_key()->set_type_flags(0 << 16);
    req.mutable_key()->set_partition_id(0);
    cedar::Descriptor desc = cedar::Descriptor::InlineInt(1, 42);
    auto encoded = desc.Encode();
    req.mutable_value_descriptor()->set_data(encoded.data(), encoded.size());
    req.mutable_txn_version()->set_value(2000000);
    
    cedar::storage::PutResponse resp;
    grpc::ClientContext ctx;
    auto status = stub->Put(&ctx, req, &resp);
    std::cout << "Write: " << (status.ok() && resp.success() ? "OK" : "FAIL") << std::endl;
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    cedar::storage::GetRequest get_req;
    get_req.mutable_key()->set_entity_id(100);
    get_req.mutable_key()->set_timestamp(2000000);
    get_req.mutable_key()->set_column_id(1);
    get_req.mutable_key()->set_type_flags(0 << 16);
    get_req.mutable_key()->set_partition_id(0);
    
    cedar::storage::GetResponse get_resp;
    grpc::ClientContext get_ctx;
    auto get_status = stub->Get(&get_ctx, get_req, &get_resp);
    std::cout << "Read: " << (get_status.ok() && get_resp.found() ? "OK" : "FAIL") << std::endl;
  }
  
  // Test with timestamp = 5000000 (like Test 5)
  std::cout << std::endl << "=== Test timestamp = 5000000 ===" << std::endl;
  {
    cedar::storage::PutRequest req;
    req.mutable_key()->set_entity_id(101);
    req.mutable_key()->set_timestamp(5000000);
    req.mutable_key()->set_column_id(1);
    req.mutable_key()->set_type_flags(0 << 16);
    req.mutable_key()->set_partition_id(0);
    cedar::Descriptor desc = cedar::Descriptor::InlineInt(1, 43);
    auto encoded = desc.Encode();
    req.mutable_value_descriptor()->set_data(encoded.data(), encoded.size());
    req.mutable_txn_version()->set_value(5000000);
    
    cedar::storage::PutResponse resp;
    grpc::ClientContext ctx;
    auto status = stub->Put(&ctx, req, &resp);
    std::cout << "Write: " << (status.ok() && resp.success() ? "OK" : "FAIL") << std::endl;
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    cedar::storage::GetRequest get_req;
    get_req.mutable_key()->set_entity_id(101);
    get_req.mutable_key()->set_timestamp(5000000);
    get_req.mutable_key()->set_column_id(1);
    get_req.mutable_key()->set_type_flags(0 << 16);
    get_req.mutable_key()->set_partition_id(0);
    
    cedar::storage::GetResponse get_resp;
    grpc::ClientContext get_ctx;
    auto get_status = stub->Get(&get_ctx, get_req, &get_resp);
    std::cout << "Read: " << (get_status.ok() && get_resp.found() ? "OK" : "FAIL") << std::endl;
  }
  
  return 0;
}
