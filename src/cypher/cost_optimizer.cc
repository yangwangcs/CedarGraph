// Copyright 2025 The Cedar Authors
//
// Cost-Based Optimizer for Cypher Query Execution

#include "cedar/cypher/cost_optimizer.h"

#include <algorithm>
#include <cmath>
#include <limits>

namespace cedar {
namespace cypher {

CostOptimizer::CostOptimizer() = default;
CostOptimizer::~CostOptimizer() = default;

// =============================================================================
// Statistics Management
// =============================================================================

void CostOptimizer::LoadStatistics([[maybe_unused]] const std::string& db_path) {
  // TODO: Load statistics from persistent storage
  // For now, use default statistics
}

const TableStatistics& CostOptimizer::GetTableStats(
    const std::string& table_name) const {
  static const TableStatistics kDefaultStats;
  auto it = table_stats_.find(table_name);
  return it != table_stats_.end() ? it->second : kDefaultStats;
}

const ColumnStatistics& CostOptimizer::GetColumnStats(
    const std::string& table_name, const std::string& column_name) const {
  static const ColumnStatistics kDefaultStats;
  std::string key = table_name + "." + column_name;
  auto it = column_stats_.find(key);
  return it != column_stats_.end() ? it->second : kDefaultStats;
}

const EdgeStatistics& CostOptimizer::GetEdgeStats(
    const std::string& edge_type) const {
  static const EdgeStatistics kDefaultStats;
  auto it = edge_stats_.find(edge_type);
  return it != edge_stats_.end() ? it->second : kDefaultStats;
}

void CostOptimizer::UpdateTableStats(const std::string& table_name,
                                      const TableStatistics& stats) {
  table_stats_[table_name] = stats;
}

void CostOptimizer::UpdateColumnStats(const std::string& table_name,
                                       const std::string& column_name,
                                       const ColumnStatistics& stats) {
  column_stats_[table_name + "." + column_name] = stats;
}

void CostOptimizer::UpdateEdgeStats(const std::string& edge_type,
                                     const EdgeStatistics& stats) {
  edge_stats_[edge_type] = stats;
}

// =============================================================================
// Cost Estimation
// =============================================================================

double CostOptimizer::EstimateScanCost(uint64_t row_count,
                                        double selectivity) const {
  // Scan cost: proportional to rows read
  double rows_read = row_count * selectivity;
  return rows_read * kIOWeight + rows_read * kCPUWeight;
}

double CostOptimizer::EstimateJoinCost(uint64_t left_rows,
                                        uint64_t right_rows) const {
  // Nested loop join cost: left_rows * right_rows
  // Hash join cost: left_rows + right_rows (build + probe)
  // Use hash join estimate
  return (left_rows + right_rows) * kIOWeight + 
         (left_rows + right_rows) * kCPUWeight;
}

double CostOptimizer::EstimateFilterCost(uint64_t input_rows,
                                          double selectivity) const {
  // Filter cost: read all rows, output selectivity fraction
  return input_rows * kCPUWeight;
}

double CostOptimizer::EstimateProjectCost(uint64_t input_rows) const {
  // Project cost: read all rows, output all rows
  return input_rows * kCPUWeight;
}

double CostOptimizer::EstimateSortCost(uint64_t input_rows) const {
  // Sort cost: O(N log N)
  if (input_rows == 0) return 0.0;
  return input_rows * std::log2(input_rows) * kCPUWeight;
}

CostEstimate CostOptimizer::EstimateCost(const PhysicalOperator* op) const {
  if (!op) return CostEstimate();
  
  std::string op_name = op->GetName();
  
  // NodeScan: scan all nodes of a type
  if (op_name == "NodeScan") {
    const auto& details = op->GetDetails();
    auto it = details.find("label");
    std::string label = it != details.end() ? it->second : "default";
    const auto& stats = GetTableStats(label);
    double io = EstimateScanCost(stats.row_count, 1.0);
    return CostEstimate(io, stats.row_count * kCPUWeight, 0, stats.row_count);
  }
  
  // IndexScan: index lookup (much cheaper)
  if (op_name == "IndexScan") {
    // Index lookup is O(log N) + result_size
    constexpr uint64_t kDefaultIndexLookup = 10;
    return CostEstimate(kDefaultIndexLookup * kIOWeight, 
                        kDefaultIndexLookup * kCPUWeight, 0, kDefaultIndexLookup);
  }
  
  // Filter: reduce rows by selectivity
  if (op_name == "Filter") {
    // Default selectivity: 10% for simple predicates
    constexpr double kDefaultFilterSelectivity = 0.1;
    // Need input rows - use a default for now
    constexpr uint64_t kDefaultInputRows = 1000;
    uint64_t output_rows = static_cast<uint64_t>(kDefaultInputRows * kDefaultFilterSelectivity);
    double cpu = EstimateFilterCost(kDefaultInputRows, kDefaultFilterSelectivity);
    return CostEstimate(0, cpu, 0, output_rows);
  }
  
  // Project: no row reduction
  if (op_name == "Project") {
    constexpr uint64_t kDefaultRows = 1000;
    return CostEstimate(0, EstimateProjectCost(kDefaultRows), 0, kDefaultRows);
  }
  
  // Sort: O(N log N)
  if (op_name == "Sort") {
    constexpr uint64_t kDefaultRows = 1000;
    return CostEstimate(0, EstimateSortCost(kDefaultRows), 0, kDefaultRows);
  }
  
  // TopN: O(N log K) where K is limit
  if (op_name == "TopN") {
    constexpr uint64_t kDefaultRows = 1000;
    constexpr uint64_t kDefaultLimit = 10;
    double cpu = kDefaultRows * std::log2(kDefaultLimit) * kCPUWeight;
    return CostEstimate(0, cpu, 0, kDefaultLimit);
  }
  
  // ProduceResults: pass-through
  if (op_name == "ProduceResults") {
    constexpr uint64_t kDefaultRows = 1000;
    return CostEstimate(0, kDefaultRows * kCPUWeight, 0, kDefaultRows);
  }
  
  // Default: estimate based on child costs
  CostEstimate total;
  for (size_t i = 0; i < op->GetChildren().size(); ++i) {
    CostEstimate child_cost = EstimateCost(op->GetChildren()[i]);
    total.io_cost += child_cost.io_cost;
    total.cpu_cost += child_cost.cpu_cost;
    total.memory_cost += child_cost.memory_cost;
    total.estimated_rows = std::max(total.estimated_rows, child_cost.estimated_rows);
  }
  total.total_cost = total.io_cost + total.cpu_cost + total.memory_cost;
  return total;
}

CostEstimate CostOptimizer::EstimatePlanCost(const PhysicalOperator* root) const {
  return EstimateCost(root);
}

// =============================================================================
// Plan Optimization
// =============================================================================

std::shared_ptr<PhysicalOperator> CostOptimizer::SelectOptimal(
    const std::vector<std::shared_ptr<PhysicalOperator>>& alternatives) const {
  if (alternatives.empty()) return nullptr;
  if (alternatives.size() == 1) return alternatives[0];
  
  // Find the plan with lowest estimated cost
  std::shared_ptr<PhysicalOperator> best_plan = alternatives[0];
  CostEstimate best_cost = EstimatePlanCost(best_plan.get());
  
  for (size_t i = 1; i < alternatives.size(); ++i) {
    CostEstimate cost = EstimatePlanCost(alternatives[i].get());
    if (cost.total_cost < best_cost.total_cost) {
      best_plan = alternatives[i];
      best_cost = cost;
    }
  }
  
  return best_plan;
}

std::shared_ptr<PhysicalOperator> CostOptimizer::Optimize(
    std::shared_ptr<PhysicalOperator> root) const {
  if (!root) return nullptr;
  
  // For now, return the original plan
  // TODO: Implement plan transformations:
  // 1. Join reordering
  // 2. Predicate pushdown
  // 3. Index selection
  // 4. Subquery unnesting
  
  return root;
}

}  // namespace cypher
}  // namespace cedar
