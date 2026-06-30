// Copyright 2025 The Cedar Authors. All rights reserved.
// Cost-Based Optimizer (CBO) for CedarGraph

#pragma once

#include <memory>
#include <mutex>
#include <vector>
#include <unordered_map>
#include <string>

namespace cedar {
namespace cypher {

// Forward declarations
class PhysicalOperator;
class ExecutionContext;

// Statistics for a single entity type
struct EntityStatistics {
  uint64_t entity_count = 0;
  uint64_t edge_count = 0;
  std::unordered_map<std::string, uint64_t> property_counts;
  std::unordered_map<std::string, double> property_selectivity;
};

// Cost estimation for a single operation
struct OperationCost {
  double cpu_cost = 0.0;
  double io_cost = 0.0;
  double memory_cost = 0.0;
  double network_cost = 0.0;
  
  double TotalCost() const {
    return cpu_cost + io_cost + memory_cost + network_cost;
  }
};

// Cost-Based Optimizer
class CostBasedOptimizer {
 public:
  struct Config {
    bool enable_statistics;
    bool enable_plan_selection;
    double selectivity_threshold;
    
    Config() : enable_statistics(true), enable_plan_selection(true), selectivity_threshold(0.1) {}
  };

  explicit CostBasedOptimizer(const Config& config = Config());
  ~CostBasedOptimizer() = default;

  // Estimate cost for a single operation
  OperationCost EstimateCost(const PhysicalOperator* op, 
                             const ExecutionContext* ctx) const;

  // Estimate cost for a complete plan
  OperationCost EstimatePlanCost(const PhysicalOperator* root,
                                 const ExecutionContext* ctx) const;

  // Select the best plan from alternatives
  PhysicalOperator* SelectBestPlan(const std::vector<PhysicalOperator*>& plans,
                                   const ExecutionContext* ctx) const;

  // Update statistics for an entity type
  void UpdateStatistics(const std::string& label, const EntityStatistics& stats);

  // Get statistics for an entity type
  EntityStatistics GetStatistics(const std::string& label) const;

 private:
  Config config_;
  std::unordered_map<std::string, EntityStatistics> statistics_;
  mutable std::mutex statistics_mutex_;
};

}  // namespace cypher
}  // namespace cedar
