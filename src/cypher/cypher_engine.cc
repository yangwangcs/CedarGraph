// Copyright (c) 2025 The Cedar Authors. All rights reserved.

#include "cedar/cypher/cypher_engine.h"
#include "cedar/cypher/parser.h"
#include "cedar/cypher/fingerprint.h"
#include "cedar/metrics/metrics_registry.h"
#include "cedar/storage/cedar_graph_storage.h"
#include <chrono>

namespace cedar {
namespace cypher {

CypherEngine::CypherEngine(CedarGraphStorage* storage) : storage_(storage) {
  if (storage_) {
    graph_ = std::make_unique<CedarGraph>(storage_);
  }
}

CypherEngine::~CypherEngine() = default;

ResultSet CypherEngine::Execute(const std::string& query) {
  auto start = std::chrono::steady_clock::now();

  auto do_execute = [&]() -> ResultSet {
    // Compute fingerprint for cache key
    auto fingerprint = ComputeFingerprint(query);

    // Check cache first
    if (auto cached = GetCachedPlan(fingerprint)) {
      ExecutionContext ctx;
      ctx.graph = graph_.get();
      ctx.storage = storage_;
      ctx.gcn_traversal_callback = gcn_traversal_callback_;
      return cached->Clone()->Execute(&ctx);
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
    ctx.graph = graph_.get();
    ctx.storage = storage_;
    ctx.gcn_traversal_callback = gcn_traversal_callback_;
    auto* raw_plan = plan.get();
    CachePlan(fingerprint, std::move(plan));
    return raw_plan->Execute(&ctx);
  };

  ResultSet result = do_execute();

  auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now() - start).count();
  using cedar::metrics::MetricsRegistry;
  MetricsRegistry::Instance().GetOrCreateCounter(
      "cypher_queries_total", "Total Cypher queries executed")
      ->Increment();
  MetricsRegistry::Instance().GetOrCreateHistogram(
      "cypher_query_latency_us", "Query latency in microseconds",
      {1000, 5000, 10000, 50000, 100000, 500000, 1000000})
      ->Observe(static_cast<double>(elapsed_us));

  return result;
}

ResultSet CypherEngine::Execute(const std::string& query,
                                 const std::map<std::string, Value>& parameters) {
  auto start = std::chrono::steady_clock::now();

  auto do_execute = [&]() -> ResultSet {
    // Parse and create new plan (cache disabled for debugging)
    auto plan = ParseAndPlan(query);
    if (!plan) {
      ResultSet result;
      result.SetError(last_error_);
      return result;
    }
    
    // Execute with parameters bound to context variables
    ExecutionContext ctx;
    ctx.graph = graph_.get();
    ctx.storage = storage_;
    ctx.gcn_traversal_callback = gcn_traversal_callback_;
    for (const auto& [k, v] : parameters) {
      ctx.SetVariable(k, v);
    }
    
    // Execute plan
    return plan->Execute(&ctx);
  };

  ResultSet result = do_execute();

  auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now() - start).count();
  using cedar::metrics::MetricsRegistry;
  MetricsRegistry::Instance().GetOrCreateCounter(
      "cypher_queries_total", "Total Cypher queries executed")
      ->Increment();
  MetricsRegistry::Instance().GetOrCreateHistogram(
      "cypher_query_latency_us", "Query latency in microseconds",
      {1000, 5000, 10000, 50000, 100000, 500000, 1000000})
      ->Observe(static_cast<double>(elapsed_us));

  return result;
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
  std::unique_lock<std::shared_mutex> lock(plan_cache_mutex_);
  plan_cache_.clear();
}

size_t CypherEngine::GetCacheSize() const {
  std::shared_lock<std::shared_mutex> lock(plan_cache_mutex_);
  return plan_cache_.size();
}

std::unique_ptr<ExecutionPlan> CypherEngine::ParseAndPlan(const std::string& query) {
  CypherParser parser(query);
  auto stmt = parser.ParseStatement();
  
  if (!stmt) {
    last_error_ = parser.GetError();
    return nullptr;
  }
  
  if (validator_) {
    auto validation = validator_->Validate(*stmt);
    if (!validation.ok()) {
      last_error_ = validation.ToString();
      return nullptr;
    }
  }
  
  auto temporal_clause = parser.GetTemporalClause();
  auto root = ExecutionPlanBuilder::Build(stmt, temporal_clause);
  
  if (!root) {
    last_error_ = "Failed to build execution plan";
    return nullptr;
  }
  
  return std::make_unique<ExecutionPlan>(root);
}

std::shared_ptr<ExecutionPlan> CypherEngine::GetCachedPlan(
    const std::string& fingerprint) {
  std::shared_lock<std::shared_mutex> lock(plan_cache_mutex_);
  auto it = plan_cache_.find(fingerprint);
  if (it != plan_cache_.end()) {
    return it->second;
  }
  return nullptr;
}

static constexpr size_t kMaxPlanCacheSize = 1000;

void CypherEngine::CachePlan(const std::string& fingerprint,
                             std::unique_ptr<ExecutionPlan> plan) {
  std::unique_lock<std::shared_mutex> lock(plan_cache_mutex_);
  if (plan_cache_.size() >= kMaxPlanCacheSize) {
    if (!plan_cache_.empty()) {
      plan_cache_.erase(plan_cache_.begin());
    }
  }
  plan_cache_[fingerprint] = std::shared_ptr<ExecutionPlan>(plan.release());
}

}  // namespace cypher
}  // namespace cedar
