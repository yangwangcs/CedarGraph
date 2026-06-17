// Copyright 2025 The Cedar Authors
//
// Query Router implementation

#include "cedar/client/query_router.h"

#include <algorithm>
#include <cctype>
#include <functional>

namespace cedar {
namespace client {

QueryRouter::QueryRouter(std::shared_ptr<ServiceDiscovery> service_discovery,
                           std::shared_ptr<LoadBalancer> load_balancer)
    : service_discovery_(service_discovery),
      load_balancer_(load_balancer) {}

QueryRouter::~QueryRouter() = default;

QueryRouteResult QueryRouter::RouteQuery(const std::string& query,
                                           const std::string& space_name) {
  QueryType type = GetQueryType(query);
  
  // DDL and ADMIN queries go to GraphD
  if (type == QueryType::DDL || type == QueryType::ADMIN) {
    return RouteByType(type, space_name);
  }
  
  // READ and WRITE queries need partition routing
  int partition_id = GetPartitionId(query, space_name);
  return RouteToPartition(space_name, partition_id);
}

QueryRouteResult QueryRouter::RouteByType(QueryType type, const std::string& space_name) {
  QueryRouteResult result;
  result.success = false;

  // For now, route all queries to GraphD
  // TODO: Implement proper routing based on query type
  auto node = load_balancer_->SelectNode();
  if (node.host.empty()) {
    result.error_message = "No available nodes";
    return result;
  }

  result.host = node.host;
  result.port = node.port;
  result.success = true;

  return result;
}

QueryRouteResult QueryRouter::RouteToPartition(const std::string& space_name,
                                                 int partition_id) {
  QueryRouteResult result;
  result.success = false;

  // Get partition leader from service discovery
  auto node = service_discovery_->GetPartitionLeader(space_name, partition_id);
  if (node.host.empty()) {
    // Fallback to load balancer
    auto lb_node = load_balancer_->SelectNode();
    if (lb_node.host.empty()) {
      result.error_message = "No available nodes";
      return result;
    }
    result.host = lb_node.host;
    result.port = lb_node.port;
  } else {
    result.host = node.host;
    result.port = node.port;
  }

  result.success = true;
  return result;
}

QueryType QueryRouter::GetQueryType(const std::string& query) const {
  return ParseQueryType(query);
}

int QueryRouter::GetPartitionId(const std::string& query,
                                  const std::string& space_name) const {
  return CalculatePartition(query, space_name);
}

QueryType QueryRouter::ParseQueryType(const std::string& query) const {
  // Convert to uppercase for comparison
  std::string upper_query = query;
  std::transform(upper_query.begin(), upper_query.end(), upper_query.begin(), ::toupper);

  // Remove leading whitespace
  size_t start = upper_query.find_first_not_of(" \t\n\r");
  if (start != std::string::npos) {
    upper_query = upper_query.substr(start);
  }

  // Check for DDL keywords
  if (upper_query.find("CREATE") == 0 || 
      upper_query.find("DROP") == 0 ||
      upper_query.find("ALTER") == 0) {
    return QueryType::DDL;
  }

  // Check for admin keywords
  if (upper_query.find("SHOW") == 0 || 
      upper_query.find("USE") == 0 ||
      upper_query.find("EXPLAIN") == 0 ||
      upper_query.find("PROFILE") == 0) {
    return QueryType::ADMIN;
  }

  // Check for write keywords
  if (upper_query.find("CREATE") == 0 || 
      upper_query.find("SET") == 0 ||
      upper_query.find("DELETE") == 0 ||
      upper_query.find("MERGE") == 0) {
    return QueryType::WRITE;
  }

  // Default to read
  return QueryType::READ;
}

int QueryRouter::CalculatePartition(const std::string& query,
                                      const std::string& space_name) const {
  // Simple hash-based partitioning
  // TODO: Extract entity ID from query for better partitioning
  std::hash<std::string> hasher;
  size_t hash = hasher(query + space_name);
  return hash % 16;  // Assuming 16 partitions
}

}  // namespace client
}  // namespace cedar
