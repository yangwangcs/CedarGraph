// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// 分布式写入测试 - 验证数据分布和落盘

#include <iostream>
#include <random>
#include <chrono>
#include <thread>
#include <vector>
#include <sstream>
#include <iomanip>

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include "query_service.pb.h"
#include "query_service.grpc.pb.h"

using cedar::query::QueryService;
using cedar::query::ExecuteQueryRequest;
using cedar::query::ExecuteQueryResponse;
using cedar::query::HealthRequest;
using cedar::query::HealthResponse;
using cedar::query::QueryStatsRequest;
using cedar::query::QueryStatsResponse;

class DistributedWriteTest {
 public:
  DistributedWriteTest(const std::string& server_address)
      : stub_(QueryService::NewStub(
            grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials()))),
        rng_(std::random_device{}()),
        dist_(1, 1000000) {}
  
  const std::vector<uint64_t>& GetWrittenIds() const { return written_ids_; }

  // 执行写入测试
  bool RunWriteTest(int num_writes) {
    std::cout << "=== 分布式写入测试 ===" << std::endl;
    std::cout << "写入数量: " << num_writes << std::endl;
    
    int success_count = 0;
    int fail_count = 0;
    
    auto start = std::chrono::steady_clock::now();
    
    for (int i = 0; i < num_writes; i++) {
      // 生成不同的 entity_id 来测试分布
      uint64_t entity_id = dist_(rng_);
      
      if (WriteNode(entity_id, i)) {
        success_count++;
        written_ids_.push_back(entity_id);
      } else {
        fail_count++;
      }
      
      if ((i + 1) % 100 == 0) {
        std::cout << "进度: " << (i + 1) << "/" << num_writes 
                  << " (成功: " << success_count << ", 失败: " << fail_count << ")" << std::endl;
      }
    }
    
    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    std::cout << "\n写入完成:" << std::endl;
    std::cout << "  总时间: " << duration.count() << " ms" << std::endl;
    std::cout << "  成功: " << success_count << std::endl;
    std::cout << "  失败: " << fail_count << std::endl;
    std::cout << "  QPS: " << (num_writes * 1000 / (duration.count() + 1)) << std::endl;
    
    return fail_count == 0;
  }
  
  // 检查数据分布
  bool CheckDataDistribution() {
    std::cout << "\n=== 检查数据分布 ===" << std::endl;
    
    grpc::ClientContext context;
    QueryStatsRequest request;
    QueryStatsResponse response;
    
    auto status = stub_->GetStats(&context, request, &response);
    
    if (!status.ok()) {
      std::cout << "获取统计信息失败: " << status.error_message() << std::endl;
      return false;
    }
    
    std::cout << "系统统计:" << std::endl;
    std::cout << "  总查询数: " << response.total_queries() << std::endl;
    std::cout << "  失败查询: " << response.failed_queries() << std::endl;
    std::cout << "  平均延迟: " << response.avg_latency_us() << " us" << std::endl;
    std::cout << "  QPS: " << response.queries_per_second() << std::endl;
    
    return true;
  }
  
  // 读取测试 - 验证数据可读取
  bool RunReadTest(int num_reads) {
    std::cout << "\n=== 读取测试 ===" << std::endl;
    std::cout << "读取数量: " << num_reads << std::endl;
    
    int found_count = 0;
    int not_found_count = 0;
    
    if (written_ids_.empty()) {
      std::cout << "没有写入的数据可供读取" << std::endl;
      return false;
    }
    
    std::uniform_int_distribution<size_t> idx_dist(0, written_ids_.size() - 1);
    for (int i = 0; i < num_reads; i++) {
      uint64_t entity_id = written_ids_[idx_dist(rng_)];
      
      if (ReadNode(entity_id)) {
        found_count++;
      } else {
        not_found_count++;
      }
    }
    
    std::cout << "读取结果:" << std::endl;
    std::cout << "  找到: " << found_count << std::endl;
    std::cout << "  未找到: " << not_found_count << std::endl;
    
    return true;
  }

 private:
  // 写入节点
  bool WriteNode(uint64_t entity_id, int seq) {
    grpc::ClientContext context;
    ExecuteQueryRequest request;
    ExecuteQueryResponse response;
    
    // 构建 CREATE 查询
    std::stringstream query;
    query << "CREATE (n:Person {id: " << entity_id 
          << ", seq: " << seq 
          << ", data: 'test_data_" << seq << "'})";
    
    request.set_query(query.str());
    request.mutable_parameters()->Clear();
    
    auto status = stub_->ExecuteQuery(&context, request, &response);
    
    if (!status.ok()) {
      std::cerr << "写入失败: " << status.error_message() << std::endl;
      return false;
    }
    
    return response.success();
  }
  
  // 读取节点
  bool ReadNode(uint64_t entity_id) {
    grpc::ClientContext context;
    ExecuteQueryRequest request;
    ExecuteQueryResponse response;
    
    // 构建 MATCH 查询
    std::stringstream query;
    query << "MATCH (n:Person {id: " << entity_id << "}) RETURN n";
    
    request.set_query(query.str());
    
    auto status = stub_->ExecuteQuery(&context, request, &response);
    
    if (!status.ok()) {
      return false;
    }
    
    return response.success() && response.stats().rows_returned() > 0;
  }
  
  std::unique_ptr<QueryService::Stub> stub_;
  std::mt19937 rng_;
  std::uniform_int_distribution<uint64_t> dist_;
  std::vector<uint64_t> written_ids_;
};

int main(int argc, char** argv) {
  std::string server_address = "localhost:9669";
  if (argc > 1) {
    server_address = argv[1];
  }
  
  std::cout << "连接到: " << server_address << std::endl;
  
  DistributedWriteTest test(server_address);
  
  // 健康检查
  std::cout << "=== 健康检查 ===" << std::endl;
  grpc::ClientContext context;
  HealthRequest health_req;
  HealthResponse health_resp;
  
  auto channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
  auto stub = QueryService::NewStub(channel);
  auto status = stub->Health(&context, health_req, &health_resp);
  
  if (!status.ok() || !health_resp.healthy()) {
    std::cerr << "服务不健康，退出测试" << std::endl;
    return 1;
  }
  
  std::cout << "服务健康，开始测试" << std::endl;
  
  // 运行写入测试
  bool write_ok = test.RunWriteTest(500);
  
  // 检查分布
  test.CheckDataDistribution();
  
  // 运行读取测试
  test.RunReadTest(100);
  
  std::cout << "\n=== 测试完成 ===" << std::endl;
  
  return write_ok ? 0 : 1;
}
