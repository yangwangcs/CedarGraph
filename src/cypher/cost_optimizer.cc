// Copyright 2025 The Cedar Authors. All rights reserved.
// Cost-Based Optimizer (CBO) implementation

#include "cedar/cypher/cost_optimizer.h"
#include "cedar/cypher/execution_plan.h"

namespace cedar {
namespace cypher {

CostBasedOptimizer::CostBasedOptimizer(const Config& config) : config_(config) {}

OperationCost CostBasedOptimizer::EstimateCost(const PhysicalOperator* op,
                                               const ExecutionContext* ctx) const {
  OperationCost cost;
  
  if (!op) return cost;
  
  // Estimate based on operator type
  // This is a simplified estimation - in production, use actual statistics
  
  // NodeScan: O(N) where N is number of entities
  if (dynamic_cast<const NodeScan*>(op)) {
    cost.cpu_cost = 100.0;  // Base cost for scanning
    cost.io_cost = 50.0;    // I/O cost for reading from storage
  }
  // IndexScan: O(log N) with index
  else if (dynamic_cast<const IndexScan*>(op)) {
    cost.cpu_cost = 10.0;   // Much cheaper with index
    cost.io_cost = 5.0;
  }
  // Expand: O(E) where E is number of edges
  else if (dynamic_cast<const Expand*>(op)) {
    cost.cpu_cost = 50.0;
    cost.io_cost = 30.0;
  }
  // Filter: O(1) per record
  else if (dynamic_cast<const Filter*>(op)) {
    cost.cpu_cost = 1.0;
  }
  // ProduceResults: O(1)
  else if (dynamic_cast<const ProduceResults*>(op)) {
    cost.cpu_cost = 0.1;
  }
  
  return cost;
}

OperationCost CostBasedOptimizer::EstimatePlanCost(const PhysicalOperator* root,
                                                   const ExecutionContext* ctx) const {
  OperationCost total_cost;
  
  if (!root) return total_cost;
  
  // Estimate cost for this operator
  OperationCost op_cost = EstimateCost(root, ctx);
  total_cost.cpu_cost += op_cost.cpu_cost;
  total_cost.io_cost += op_cost.io_cost;
  total_cost.memory_cost += op_cost.memory_cost;
  total_cost.network_cost += op_cost.network_cost;
  
  // Recursively estimate cost for children
  for (const auto& child : root->GetChildren()) {
    OperationCost child_cost = EstimatePlanCost(child.get(), ctx);
    total_cost.cpu_cost += child_cost.cpu_cost;
    total_cost.io_cost += child_cost.io_cost;
    total_cost.memory_cost += child_cost.memory_cost;
    total_cost.network_cost += child_cost.network_cost;
  }
  
  return total_cost;
}

PhysicalOperator* CostBasedOptimizer::SelectBestPlan(
    const std::vector<PhysicalOperator*>& plans,
    const ExecutionContext* ctx) const {
  
  if (plans.empty()) return nullptr;
  
  PhysicalOperator* best_plan = plans[0];
  OperationCost best_cost = EstimatePlanCost(plans[0], ctx);
  
  for (size_t i = 1; i < plans.size(); ++i) {
    OperationCost cost = EstimatePlanCost(plans[i], ctx);
    if (cost.TotalCost() < best_cost.TotalCost()) {
      best_cost = cost;
      best_plan = plans[i];
    }
  }
  
  return best_plan;
}

void CostBasedOptimizer::UpdateStatistics(const std::string& label, 
                                          const EntityStatistics& stats) {
  std::lock_guard<std::mutex> lock(statistics_mutex_);
  statistics_[label] = stats;
}

EntityStatistics CostBasedOptimizer::GetStatistics(const std::string& label) const {
  std::lock_guard<std::mutex> lock(statistics_mutex_);
  auto it = statistics_.find(label);
  if (it != statistics_.end()) {
    return it->second;
  }
  return EntityStatistics{};
}

}  // namespace cypher
}  // namespace cedar
