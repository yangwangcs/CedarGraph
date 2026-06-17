// Copyright 2025 The Cedar Authors
//
// Query Router for routing queries to appropriate nodes

#ifndef CEDAR_CLIENT_QUERY_ROUTER_H_
#define CEDAR_CLIENT_QUERY_ROUTER_H_

#include <memory>
#include <string>
#include <vector>

#include "cedar/client/load_balancer.h"
#include "cedar/client/service_discovery.h"

namespace cedar {
namespace client {

// Query type
enum class QueryType {
  READ,       // Read-only queries (MATCH, RETURN)
  WRITE,      // Write queries (CREATE, SET, DELETE)
  DDL,        // DDL queries (CREATE SPACE, CREATE TAG, etc.)
  ADMIN       // Admin queries (SHOW, USE)
};

// Query route result
struct QueryRouteResult {
  std::string host;
  int port;
  bool success;
  std::string error_message;
};

// Query Router
class QueryRouter {
 public:
  QueryRouter(std::shared_ptr<ServiceDiscovery> service_discovery,
              std::shared_ptr<LoadBalancer> load_balancer);
  ~QueryRouter();

  // Route a query to appropriate node
  QueryRouteResult RouteQuery(const std::string& query, 
                               const std::string& space_name);

  // Route based on query type
  QueryRouteResult RouteByType(QueryType type, const std::string& space_name);

  // Route to specific partition
  QueryRouteResult RouteToPartition(const std::string& space_name,
                                     int partition_id);

  // Get query type from query string
  QueryType GetQueryType(const std::string& query) const;

  // Get partition ID for a query
  int GetPartitionId(const std::string& query, const std::string& space_name) const;

 private:
  std::shared_ptr<ServiceDiscovery> service_discovery_;
  std::shared_ptr<LoadBalancer> load_balancer_;

  // Parse query to determine type
  QueryType ParseQueryType(const std::string& query) const;

  // Calculate partition from query
  int CalculatePartition(const std::string& query, const std::string& space_name) const;
};

}  // namespace client
}  // namespace cedar

#endif  // CEDAR_CLIENT_QUERY_ROUTER_H_
