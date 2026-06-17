// Copyright 2025 The Cedar Authors
//
// Common types shared across client components

#ifndef CEDAR_CLIENT_TYPES_H_
#define CEDAR_CLIENT_TYPES_H_

#include <string>
#include <vector>

namespace cedar {
namespace client {

// Query result
struct QueryResult {
  bool success = false;
  std::string error_message;
  std::vector<std::vector<std::string>> rows;
  int64_t execution_time_ms = 0;
};

// DDL result
struct DDLResult {
  bool success = false;
  std::string error_message;
};

// Client statistics
struct ClientStats {
  int total_queries = 0;
  int successful_queries = 0;
  int failed_queries = 0;
  int64_t total_execution_time_ms = 0;
  int active_connections = 0;
};

}  // namespace client
}  // namespace cedar

#endif  // CEDAR_CLIENT_TYPES_H_
