// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Query Service - gRPC Service Implementation

#ifndef CEDAR_QUERYD_QUERY_SERVICE_H_
#define CEDAR_QUERYD_QUERY_SERVICE_H_

#include <atomic>
#include <chrono>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <grpc/grpc.h>
#include <grpcpp/server.h>

#include "cedar/core/status.h"
#include "cedar/cypher/value.h"

// Protobuf generated headers
#include "query_service.pb.h"
#include "query_service.grpc.pb.h"

namespace cedar {
namespace queryd {

// Forward declarations
class DistributedExecutor;
class QueryStorageClient;
class QueryMetaClient;
class QueryPlanCache;

// ============================================================================
// Query Context
// ============================================================================

struct ActiveQuery {
  std::string query_id;
  std::string query_text;
  std::chrono::steady_clock::time_point start_time;
  std::string client_address;
  bool is_streaming = false;
};

// ============================================================================
// Query Service Implementation
// ============================================================================

class QueryServiceImpl final : public cedar::query::QueryService::Service {
 public:
  struct Options {
    // Executor options
    size_t executor_workers = 16;
    
    // Cache options
    size_t plan_cache_size = 1000;
    
    // Query limits
    uint32_t max_concurrent_queries = 1000;
    uint32_t max_query_timeout_ms = 300000;  // 5 minutes
    
    // Streaming options
    uint32_t default_batch_size = 1000;
    uint32_t max_streaming_memory_mb = 1024;
  };

  QueryServiceImpl(
      std::shared_ptr<QueryStorageClient> storage_client,
      std::shared_ptr<QueryMetaClient> meta_client,
      const Options& options);
  ~QueryServiceImpl();

  // Initialize service
  Status Init();

  // gRPC methods
  grpc::Status ExecuteQuery(
      grpc::ServerContext* context,
      const cedar::query::ExecuteQueryRequest* request,
      cedar::query::ExecuteQueryResponse* response) override;

  grpc::Status StreamQuery(
      grpc::ServerContext* context,
      const cedar::query::StreamQueryRequest* request,
      grpc::ServerWriter<cedar::query::StreamQueryResponse>* writer) override;

  grpc::Status Traverse(
      grpc::ServerContext* context,
      const cedar::query::TraverseRequest* request,
      cedar::query::TraverseResponse* response) override;

  grpc::Status TemporalQuery(
      grpc::ServerContext* context,
      const cedar::query::TemporalQueryRequest* request,
      cedar::query::TemporalQueryResponse* response) override;

  grpc::Status BatchQuery(
      grpc::ServerContext* context,
      const cedar::query::BatchQueryRequest* request,
      cedar::query::BatchQueryResponse* response) override;

  grpc::Status GetSchema(
      grpc::ServerContext* context,
      const cedar::query::GetSchemaRequest* request,
      cedar::query::GetSchemaResponse* response) override;

  grpc::Status Health(
      grpc::ServerContext* context,
      const cedar::query::HealthRequest* request,
      cedar::query::HealthResponse* response) override;

  grpc::Status GetStats(
      grpc::ServerContext* context,
      const cedar::query::QueryStatsRequest* request,
      cedar::query::QueryStatsResponse* response) override;

  // Get service stats
  struct ServiceStats {
    uint64_t total_queries;
    uint64_t failed_queries;
    uint64_t active_queries;
    uint64_t queued_queries;
    double avg_latency_ms;
    size_t cache_size;
    double cache_hit_rate;
  };
  ServiceStats GetStats() const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

// ============================================================================
// Query Server
// ============================================================================

class QueryServer {
 public:
  struct Options {
    std::string listen_address = "0.0.0.0:9669";
    std::string meta_service_address = "127.0.0.1:9559";
    
    // gRPC options
    int max_concurrent_streams = 100;
    int max_message_size_mb = 64;
    
    // Service options
    QueryServiceImpl::Options service_options;
  };

  explicit QueryServer(const Options& options);
  ~QueryServer();

  // Initialize and start
  Status Init();
  Status Start();
  
  // Stop server
  Status Stop();
  
  // Wait for shutdown
  void Wait();
  
  // Get server info
  std::string GetListenAddress() const;
  bool IsRunning() const;

 private:
  Options options_;
  
  std::shared_ptr<QueryStorageClient> storage_client_;
  std::shared_ptr<QueryMetaClient> meta_client_;
  std::unique_ptr<QueryServiceImpl> service_impl_;
  
  std::unique_ptr<grpc::Server> grpc_server_;
  std::atomic<bool> running_{false};
};

}  // namespace queryd
}  // namespace cedar

#endif  // CEDAR_QUERYD_QUERY_SERVICE_H_
