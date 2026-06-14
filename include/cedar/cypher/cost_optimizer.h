// Copyright 2025 The Cedar Authors
//
// Cost-Based Optimizer for Cypher Query Execution
// Estimates costs for alternative plans and selects the optimal one

#ifndef CEDAR_CYPHER_COST_OPTIMIZER_H_
#define CEDAR_CYPHER_COST_OPTIMIZER_H_

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "cedar/types/cedar_types.h"

namespace cedar {
namespace cypher {

// Forward declarations
class PhysicalOperator;

// =============================================================================
// Statistics collected from storage layer
// =============================================================================

struct TableStatistics {
  uint64_t row_count = 0;           // Total rows in table
  uint64_t avg_row_size = 0;        // Average row size in bytes
  double selectivity = 1.0;         // Default selectivity (1.0 = all rows)
};

struct ColumnStatistics {
  uint64_t distinct_count = 0;      // Number of distinct values
  uint64_t null_count = 0;          // Number of null values
  double selectivity = 1.0;         // Selectivity for equality predicate
};

struct EdgeStatistics {
  uint64_t edge_count = 0;          // Total edges of this type
  double avg_degree = 0.0;          // Average out-degree
  double selectivity = 1.0;         // Default selectivity
};

// =============================================================================
// Cost Model
// =============================================================================

struct CostEstimate {
  double io_cost = 0.0;             // I/O cost (SST reads, disk access)
  double cpu_cost = 0.0;            // CPU cost (comparison, serialization)
  double memory_cost = 0.0;         // Memory cost (buffer, cache)
  double total_cost = 0.0;          // Total estimated cost
  uint64_t estimated_rows = 0;      // Estimated output rows
  
  CostEstimate() = default;
  CostEstimate(double io, double cpu, double mem, uint64_t rows)
      : io_cost(io), cpu_cost(cpu), memory_cost(mem), 
        total_cost(io + cpu + mem), estimated_rows(rows) {}
};

// =============================================================================
// Cost-Based Optimizer
// =============================================================================

class CostOptimizer {
 public:
  CostOptimizer();
  ~CostOptimizer();
  
  // Load statistics from storage layer
  void LoadStatistics(const std::string& db_path);
  
  // Estimate cost of an operator
  CostEstimate EstimateCost(const PhysicalOperator* op) const;
  
  // Estimate cost of a full plan
  CostEstimate EstimatePlanCost(const PhysicalOperator* root) const;
  
  // Select optimal plan from alternatives
  std::shared_ptr<PhysicalOperator> SelectOptimal(
      const std::vector<std::shared_ptr<PhysicalOperator>>& alternatives) const;
  
  // Optimize a plan (reorder joins, push down predicates, etc.)
  std::shared_ptr<PhysicalOperator> Optimize(
      std::shared_ptr<PhysicalOperator> root) const;
  
  // Get statistics
  const TableStatistics& GetTableStats(const std::string& table_name) const;
  const ColumnStatistics& GetColumnStats(const std::string& table_name,
                                          const std::string& column_name) const;
  const EdgeStatistics& GetEdgeStats(const std::string& edge_type) const;
  
  // Update statistics (called after data changes)
  void UpdateTableStats(const std::string& table_name, const TableStatistics& stats);
  void UpdateColumnStats(const std::string& table_name, const std::string& column_name,
                          const ColumnStatistics& stats);
  void UpdateEdgeStats(const std::string& edge_type, const EdgeStatistics& stats);

 private:
  // Cost estimation helpers
  double EstimateScanCost(uint64_t row_count, double selectivity) const;
  double EstimateJoinCost(uint64_t left_rows, uint64_t right_rows) const;
  double EstimateFilterCost(uint64_t input_rows, double selectivity) const;
  double EstimateProjectCost(uint64_t input_rows) const;
  double EstimateSortCost(uint64_t input_rows) const;
  
  // Statistics storage
  std::unordered_map<std::string, TableStatistics> table_stats_;
  std::unordered_map<std::string, ColumnStatistics> column_stats_;
  std::unordered_map<std::string, EdgeStatistics> edge_stats_;
  
  // Cost weights
  static constexpr double kIOWeight = 1.0;
  static constexpr double kCPUWeight = 0.1;
  static constexpr double kMemoryWeight = 0.01;
};

}  // namespace cypher
}  // namespace cedar

#endif  // CEDAR_CYPHER_COST_OPTIMIZER_H_
