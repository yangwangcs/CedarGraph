// Standalone neighbor query test
#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <random>
#include <grpcpp/grpcpp.h>
#include "storage_service.pb.h"
#include "storage_service.grpc.pb.h"
#include "cedar/types/descriptor.h"

class NeighborTestClient {
 public:
  NeighborTestClient(const std::string& endpoint) {
    channel_ = grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials());
    stub_ = cedar::storage::StorageService::NewStub(channel_);
  }
  
  bool WriteVertex(uint64_t vertex_id, uint16_t col_id, int32_t value, uint64_t timestamp) {
    cedar::storage::PutRequest request;
    auto* key = request.mutable_key();
    key->set_entity_id(vertex_id);
    key->set_timestamp(timestamp);
    key->set_column_id(col_id);
    key->set_type_flags((0 << 16));
    key->set_partition_id(0);
    
    auto* desc = request.mutable_value_descriptor();
    cedar::Descriptor d = cedar::Descriptor::InlineInt(col_id, value);
    auto encoded = d.Encode();
    desc->set_data(encoded.data(), encoded.size());
    request.mutable_txn_version()->set_value(timestamp);
    
    cedar::storage::PutResponse response;
    grpc::ClientContext context;
    auto status = stub_->Put(&context, request, &response);
    return status.ok() && response.success();
  }
  
  std::optional<int32_t> ReadVertex(uint64_t vertex_id, uint16_t col_id, uint64_t timestamp) {
    cedar::storage::GetRequest request;
    auto* key = request.mutable_key();
    key->set_entity_id(vertex_id);
    key->set_timestamp(timestamp);
    key->set_column_id(col_id);
    key->set_type_flags((0 << 16));
    key->set_partition_id(0);
    
    cedar::storage::GetResponse response;
    grpc::ClientContext context;
    auto status = stub_->Get(&context, request, &response);
    
    if (status.ok() && response.success() && response.found() && 
        response.has_value_descriptor() && response.value_descriptor().data().size() >= 8) {
      auto opt_desc = cedar::Descriptor::Decode(
          cedar::Slice(response.value_descriptor().data().data(), response.value_descriptor().data().size()));
      if (opt_desc.has_value()) {
        return opt_desc.value().AsInlineInt();
      }
    }
    return std::nullopt;
  }
  
  std::vector<std::optional<int32_t>> BatchRead(
      const std::vector<std::tuple<uint64_t, uint16_t, uint64_t>>& keys) {
    cedar::storage::BatchGetRequest request;
    
    for (const auto& [entity_id, col_id, timestamp] : keys) {
      auto* key = request.add_keys();
      key->set_entity_id(entity_id);
      key->set_timestamp(timestamp);
      key->set_column_id(col_id);
      key->set_type_flags((0 << 16));
      key->set_partition_id(0);
    }
    
    cedar::storage::BatchGetResponse response;
    grpc::ClientContext context;
    auto status = stub_->BatchGet(&context, request, &response);
    
    std::vector<std::optional<int32_t>> results;
    if (!status.ok() || !response.success()) {
      std::cerr << "BatchGet failed: " 
                << (status.ok() ? response.error_msg() : status.error_message()) 
                << std::endl;
      return results;  // Return empty on failure
    }
    
    for (int i = 0; i < response.found_size(); ++i) {
      if (response.found(i) && response.descriptors(i).data().size() >= 8) {
        auto opt_desc = cedar::Descriptor::Decode(
            cedar::Slice(response.descriptors(i).data().data(),
                         response.descriptors(i).data().size()));
        if (opt_desc.has_value()) {
          results.push_back(opt_desc.value().AsInlineInt());
        } else {
          results.push_back(std::nullopt);
        }
      } else {
        results.push_back(std::nullopt);
      }
    }
    return results;
  }
  
 private:
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<cedar::storage::StorageService::Stub> stub_;
};

int main() {
  std::cout << "╔════════════════════════════════════════════════════════════╗" << std::endl;
  std::cout << "║     Standalone Neighbor Query Test                         ║" << std::endl;
  std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
  
  NeighborTestClient client("127.0.0.1:9779");
  
  const int num_vertices = 100;
  const int edges_per_vertex = 10;
  const uint64_t timestamp = 6000000;
  
  std::cout << std::endl << "[1] Writing " << (num_vertices * edges_per_vertex) << " edges..." << std::endl;
  int write_ok = 0;
  for (int v = 0; v < num_vertices; ++v) {
    for (int e = 0; e < edges_per_vertex; ++e) {
      int32_t dst = (v + e + 1) % num_vertices;
      if (client.WriteVertex(v, 1 + e, dst, timestamp)) {
        write_ok++;
      }
    }
  }
  std::cout << "    Written: " << write_ok << "/" << (num_vertices * edges_per_vertex) << std::endl;
  
  // Immediate check
  std::cout << std::endl << "[2] Immediate check (vertex 0)..." << std::endl;
  int immediate_ok = 0;
  for (int e = 0; e < edges_per_vertex; ++e) {
    auto result = client.ReadVertex(0, 1 + e, timestamp);
    if (result.has_value()) {
      immediate_ok++;
      std::cout << "    edge " << e << ": " << result.value() << std::endl;
    }
  }
  std::cout << "    Found: " << immediate_ok << "/" << edges_per_vertex << std::endl;
  
  // Wait and check again
  std::cout << std::endl << "[3] After 1 second delay..." << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(1));
  
  int delayed_ok = 0;
  for (int v = 0; v < 10; ++v) {
    auto result = client.ReadVertex(v, 1, timestamp);
    if (result.has_value()) delayed_ok++;
  }
  std::cout << "    Sample found: " << delayed_ok << "/10" << std::endl;
  
  // Batch query
  std::cout << std::endl << "[4] Batch query test (100 queries)..." << std::endl;
  std::mt19937 rng(42);
  int batch_success = 0;
  int total_neighbors = 0;
  
  for (int q = 0; q < 100; ++q) {
    int vid = rng() % num_vertices;
    
    std::vector<std::tuple<uint64_t, uint16_t, uint64_t>> keys;
    for (int e = 0; e < edges_per_vertex; ++e) {
      keys.push_back({vid, static_cast<uint16_t>(1 + e), timestamp});
    }
    
    auto results = client.BatchRead(keys);
    int found = 0;
    for (const auto& r : results) {
      if (r.has_value()) found++;
    }
    
    if (found > 0) {
      batch_success++;
      total_neighbors += found;
    }
  }
  
  std::cout << "    Queries: " << batch_success << "/100" << std::endl;
  std::cout << "    Avg neighbors: " << (batch_success > 0 ? (double)total_neighbors / batch_success : 0) << std::endl;
  std::cout << "    Success rate: " << batch_success << "%" << std::endl;
  
  std::cout << std::endl << "=== Test Complete ===" << std::endl;
  return 0;
}
