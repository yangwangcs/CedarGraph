// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Cypher Query Engine - Main API

#ifndef CEDAR_CYPHER_ENGINE_H_
#define CEDAR_CYPHER_ENGINE_H_

#include <string>
#include <memory>
#include <map>
#include <functional>
#include <vector>

#include "cedar/cypher/value.h"
#include "cedar/cypher/execution_plan.h"
#include "cedar/core/status.h"
#include "cedar/graph/cedar_graph.h"

// Forward declaration for query validator
#include "cedar/cypher/validator.h"

namespace cedar {

class CedarGraphStorage;

namespace cypher {

// 查询计划缓存项
struct CachedPlan {
  std::unique_ptr<ExecutionPlan> plan;
  size_t use_count = 0;
};

// Cypher 查询引擎
class CypherEngine {
 public:
  explicit CypherEngine(CedarGraphStorage* storage);
  ~CypherEngine();

  // 禁止拷贝
  CypherEngine(const CypherEngine&) = delete;
  CypherEngine& operator=(const CypherEngine&) = delete;

  // 执行 Cypher 查询
  ResultSet Execute(const std::string& query);
  
  // 执行带参数的查询
  ResultSet Execute(const std::string& query, 
                    const std::map<std::string, Value>& parameters);
  
  // 获取查询执行计划（用于调试和分析）
  std::string Explain(const std::string& query);
  
  // 验证查询语法
  bool IsValid(const std::string& query);
  
  // 获取上次错误信息
  std::string GetLastError() const { return last_error_; }
  
  // 清空查询计划缓存
  void ClearCache();
  
  // 获取缓存统计
  size_t GetCacheSize() const;
  
  // 获取缓存计划 (thread-safe, returns shared_ptr to keep plan alive)
  std::shared_ptr<ExecutionPlan> GetCachedPlan(const std::string& fingerprint);
  
  // 缓存计划 (thread-safe)
  void CachePlan(const std::string& fingerprint, std::unique_ptr<ExecutionPlan> plan);
  
  // Set GCN traversal callback for routing edge expansions to GCN
  void SetGcnTraversalCallback(
      std::function<std::vector<uint64_t>(uint64_t entity_id, uint32_t edge_type, uint64_t query_time)> callback);
  
  // Set query validator for semantic validation
  void SetValidator(std::unique_ptr<QueryValidator> validator) {
    validator_ = std::move(validator);
  }

 private:
  CedarGraphStorage* storage_;
  std::unique_ptr<CedarGraph> graph_;
  std::string last_error_;
  
  // GCN traversal callback
  std::function<std::vector<uint64_t>(uint64_t entity_id, uint32_t edge_type, uint64_t query_time)> gcn_traversal_callback_;
  
  // Query validator
  std::unique_ptr<QueryValidator> validator_;
  
  // 查询计划缓存
  mutable std::shared_mutex plan_cache_mutex_;
  std::unordered_map<std::string, std::shared_ptr<ExecutionPlan>> plan_cache_;
  
  // 解析和生成执行计划
  std::unique_ptr<ExecutionPlan> ParseAndPlan(const std::string& query);
  
};

}  // namespace cypher
}  // namespace cedar

#endif  // FERN_CYPHER_ENGINE_H_
