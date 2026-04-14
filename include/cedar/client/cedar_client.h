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
// CedarGraph Client SDK - C++ API
// =============================================================================

#ifndef CEDAR_CLIENT_CEDAR_CLIENT_H_
#define CEDAR_CLIENT_CEDAR_CLIENT_H_

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <functional>
#include <future>

#include "cedar/core/status.h"
#include "cedar/types/descriptor.h"
#include "cedar/types/cedar_key.h"
#include "cedar/dtx/types.h"

namespace cedar {
namespace client {

// =============================================================================
// Client Configuration
// =============================================================================

struct ClientConfig {
  // MetaD endpoints
  std::vector<std::string> metad_endpoints;
  
  // Connection settings
  uint32_t connection_timeout_ms = 5000;
  uint32_t request_timeout_ms = 10000;
  
  // Pool settings
  uint32_t max_connections_per_storage = 4;
  uint32_t max_pending_requests = 1000;
  
  // Retry settings
  uint32_t max_retries = 3;
  uint32_t retry_delay_ms = 100;
  
  // Consistency
  bool read_from_follower = false;  // true = allow stale reads
  bool require_leader_read = false;
};

// =============================================================================
// Graph Data Types
// =============================================================================

struct Vertex {
  CedarKey id;
  std::string label;
  std::unordered_map<std::string, std::string> properties;
  Timestamp timestamp;
};

struct Edge {
  CedarKey id;
  CedarKey from_vertex;
  CedarKey to_vertex;
  std::string label;
  std::unordered_map<std::string, std::string> properties;
  Timestamp timestamp;
};

struct Path {
  std::vector<Vertex> vertices;
  std::vector<Edge> edges;
};

// =============================================================================
// Query Builder
// =============================================================================

class QueryBuilder {
 public:
  QueryBuilder& Match(const std::string& pattern);
  QueryBuilder& Where(const std::string& condition);
  QueryBuilder& Return(const std::vector<std::string>& fields);
  QueryBuilder& Limit(uint32_t n);
  QueryBuilder& OrderBy(const std::string& field, bool ascending = true);
  
  std::string Build() const;

 private:
  std::string match_clause_;
  std::string where_clause_;
  std::string return_clause_;
  std::string limit_clause_;
  std::string order_clause_;
};

// =============================================================================
// Transaction
// =============================================================================

class Transaction {
 public:
  explicit Transaction(uint64_t txn_id);
  ~Transaction();
  
  // Vertex operations
  Status CreateVertex(const Vertex& vertex);
  Status UpdateVertex(const CedarKey& id, 
                      const std::unordered_map<std::string, std::string>& props);
  Status DeleteVertex(const CedarKey& id);
  StatusOr<Vertex> GetVertex(const CedarKey& id);
  
  // Edge operations
  Status CreateEdge(const Edge& edge);
  Status DeleteEdge(const CedarKey& id);
  StatusOr<Edge> GetEdge(const CedarKey& id);
  
  // Query
  StatusOr<std::vector<Vertex>> QueryVertices(const QueryBuilder& query);
  StatusOr<std::vector<Edge>> QueryEdges(const QueryBuilder& query);
  StatusOr<std::vector<Path>> QueryPaths(const CedarKey& from, 
                                          const CedarKey& to,
                                          const std::string& edge_type,
                                          uint32_t max_depth);
  
  // Transaction control
  Status Commit();
  Status Rollback();
  
  uint64_t GetTxnId() const { return txn_id_; }
  bool IsActive() const { return active_; }

 private:
  uint64_t txn_id_;
  bool active_ = true;
  std::vector<std::string> operations_;  // WAL for rollback
};

// =============================================================================
// CedarClient - Main API
// =============================================================================

class CedarClient {
 public:
  explicit CedarClient(const ClientConfig& config);
  ~CedarClient();
  
  // Lifecycle
  Status Connect();
  Status Disconnect();
  bool IsConnected() const;
  
  // Space management
  Status CreateSpace(const std::string& space_name, 
                     uint32_t partition_num = 128,
                     uint32_t replica_factor = 3);
  Status DropSpace(const std::string& space_name);
  Status UseSpace(const std::string& space_name);
  std::vector<std::string> ListSpaces();
  
  // Transaction
  StatusOr<std::unique_ptr<Transaction>> BeginTransaction();
  Status ExecuteInTransaction(std::function<Status(Transaction*)> func);
  
  // Simple API (auto-transaction)
  Status CreateVertex(const Vertex& vertex);
  Status CreateEdge(const Edge& edge);
  StatusOr<Vertex> GetVertex(const CedarKey& id);
  StatusOr<Edge> GetEdge(const CedarKey& id);
  StatusOr<std::vector<Vertex>> QueryVertices(const QueryBuilder& query);
  
  // Async API
  std::future<Status> CreateVertexAsync(const Vertex& vertex);
  std::future<StatusOr<Vertex>> GetVertexAsync(const CedarKey& id);
  
  // Batch operations
  Status BatchCreateVertices(const std::vector<Vertex>& vertices);
  Status BatchCreateEdges(const std::vector<Edge>& edges);
  
  // Admin
  struct ClusterStatus {
    bool is_healthy;
    uint32_t node_count;
    uint32_t leader_count;
    std::vector<std::string> spaces;
  };
  StatusOr<ClusterStatus> GetClusterStatus();
  
  // Callbacks
  using StatusCallback = std::function<void(Status)>;
  void SetOnDisconnect(StatusCallback callback);

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace client
}  // namespace cedar

#endif  // CEDAR_CLIENT_CEDAR_CLIENT_H_
