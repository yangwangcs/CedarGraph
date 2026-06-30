// Copyright 2025 The Cedar Authors
//
// Query Router implementation

#include "cedar/client/query_router.h"

#include <algorithm>
#include <cctype>
#include <functional>
#include <sstream>

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
  std::transform(upper_query.begin(), upper_query.end(), upper_query.begin(),
                 [](unsigned char c) { return static_cast<char>(std::toupper(c)); });

  // Remove leading whitespace
  size_t start = upper_query.find_first_not_of(" \t\n\r");
  if (start != std::string::npos) {
    upper_query = upper_query.substr(start);
  }

  std::istringstream stream(upper_query);
  std::string first;
  std::string second;
  stream >> first >> second;

  // Check for admin keywords
  if (first == "SHOW" || first == "USE" || first == "EXPLAIN" ||
      first == "PROFILE") {
    return QueryType::ADMIN;
  }

  // DDL uses explicit schema/object keywords. Plain Cypher CREATE is a write.
  if ((first == "CREATE" &&
       (second == "SPACE" || second == "TAG" || second == "EDGE" ||
        second == "INDEX" || second == "LABEL")) ||
      (first == "DROP" &&
       (second == "SPACE" || second == "TAG" || second == "EDGE" ||
        second == "INDEX" || second == "LABEL")) ||
      (first == "ALTER" &&
       (second == "SPACE" || second == "TAG" || second == "EDGE" ||
        second == "INDEX" || second == "LABEL"))) {
    return QueryType::DDL;
  }

  // Check for write keywords
  if (first == "CREATE" || first == "SET" || first == "DELETE" ||
      first == "MERGE") {
    return QueryType::WRITE;
  }

  // Default to read
  return QueryType::READ;
}

int QueryRouter::CalculatePartition(const std::string& query,
                                      const std::string& space_name) const {
  // Use FNV-1a hash for stability across processes
  uint64_t hash = 14695981039346656037ULL;  // FNV offset basis
  for (char c : query) {
    hash ^= static_cast<uint64_t>(c);
    hash *= 1099511628211ULL;  // FNV prime
  }
  for (char c : space_name) {
    hash ^= static_cast<uint64_t>(c);
    hash *= 1099511628211ULL;
  }
  return static_cast<int>(hash % 65536);  // Default partition count
}

}  // namespace client
}  // namespace cedar
