// Copyright (c) 2025 The Cedar Authors. All rights reserved.

#include "cedar/cypher/cypher_engine.h"
#include "cedar/cypher/parser.h"
#include "cedar/cypher/fingerprint.h"
#include "cedar/storage/cedar_graph_storage.h"
#include <chrono>

namespace cedar {
namespace cypher {

CypherEngine::CypherEngine(CedarGraphStorage* storage) : storage_(storage) {}

CypherEngine::~CypherEngine() = default;

ResultSet CypherEngine::Execute(const std::string& query) {
  // Compute fingerprint for cache key
  auto fingerprint = ComputeFingerprint(query);

  // Check cache first
  if (auto cached = GetCachedPlan(fingerprint)) {
    ExecutionContext ctx;
    ctx.gcn_traversal_callback = gcn_traversal_callback_;
    return cached->Execute(&ctx);
  }
  
  // Parse and create new plan
  auto plan = ParseAndPlan(query);
  if (!plan) {
    ResultSet result;
    result.SetError(last_error_);
    return result;
  }
  
  // Execute
  ExecutionContext ctx;
  ctx.gcn_traversal_callback = gcn_traversal_callback_;
  auto* raw_plan = plan.get();
  CachePlan(fingerprint, std::move(plan));
  return raw_plan->Execute(&ctx);
}

ResultSet CypherEngine::Execute(const std::string& query,
                                 const std::map<std::string, Value>& parameters) {
  // Compute fingerprint for cache key
  auto fingerprint = ComputeFingerprint(query);

  // Check cache first
  if (auto cached = GetCachedPlan(fingerprint)) {
    ExecutionContext ctx;
    ctx.gcn_traversal_callback = gcn_traversal_callback_;
    for (const auto& [k, v] : parameters) {
      ctx.SetVariable(k, v);
    }
    return cached->Execute(&ctx);
  }
  
  // Parse and create new plan
  auto plan = ParseAndPlan(query);
  if (!plan) {
    ResultSet result;
    result.SetError(last_error_);
    return result;
  }
  
  // Execute with parameters bound to context variables
  ExecutionContext ctx;
  ctx.gcn_traversal_callback = gcn_traversal_callback_;
  for (const auto& [k, v] : parameters) {
    ctx.SetVariable(k, v);
  }
  auto* raw_plan = plan.get();
  CachePlan(fingerprint, std::move(plan));
  return raw_plan->Execute(&ctx);
}

bool CypherEngine::IsValid(const std::string& query) {
  CypherParser parser(query);
  return parser.IsValid();
}

std::string CypherEngine::Explain(const std::string& query) {
  auto plan = ParseAndPlan(query);
  if (!plan) {
    return "Error: " + last_error_;
  }
  return plan->Explain();
}

void CypherEngine::SetGcnTraversalCallback(
    std::function<std::vector<uint64_t>(uint64_t entity_id, uint32_t edge_type, uint64_t query_time)> callback) {
  gcn_traversal_callback_ = std::move(callback);
}

void CypherEngine::ClearCache() {
  plan_cache_.clear();
}

size_t CypherEngine::GetCacheSize() const {
  return plan_cache_.size();
}

std::unique_ptr<ExecutionPlan> CypherEngine::ParseAndPlan(const std::string& query) {
  CypherParser parser(query);
  auto stmt = parser.ParseStatement();
  
  if (!stmt) {
    last_error_ = parser.GetError();
    return nullptr;
  }
  
  auto temporal_clause = parser.GetTemporalClause();
  auto root = ExecutionPlanBuilder::Build(stmt, temporal_clause);
  
  if (!root) {
    last_error_ = "Failed to build execution plan";
    return nullptr;
  }
  
  return std::make_unique<ExecutionPlan>(root);
}

ExecutionPlan* CypherEngine::GetCachedPlan(const std::string& fingerprint) {
  auto it = plan_cache_.find(fingerprint);
  if (it != plan_cache_.end()) {
    return it->second.get();
  }
  return nullptr;
}

void CypherEngine::CachePlan(const std::string& fingerprint, 
                             std::unique_ptr<ExecutionPlan> plan) {
  plan_cache_[fingerprint] = std::move(plan);
}

}  // namespace cypher
}  // namespace cedar
