// Copyright (c) 2024 CedarGraph Project
// Licensed under the MIT License.

#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "cedar/types/cedar_types.h"
#include "cedar/cypher/ast.h"
#include "cedar/cypher/temporal_dialect.h"
#include "cedar/cypher/value.h"
#include "cedar/graph/cedar_graph.h"

namespace cedar {

class CedarGraph;

namespace gcn {
  class TMVEngine;
}

namespace cypher {

// Forward declarations
class PhysicalOperator;

/**
 * @brief Execution context for query execution
 */
struct ExecutionContext {
  CedarGraph* graph = nullptr;
  CedarGraphStorage* storage = nullptr;
  std::unordered_map<std::string, Value> variables;
  std::unordered_map<std::string, std::shared_ptr<Record>> variable_records;
  
  // Temporal context
  std::shared_ptr<TemporalClause> temporal_clause;
  Timestamp query_timestamp = 0;
  std::pair<Timestamp, Timestamp> time_range{0, Timestamp::Max()};
  
  // GCN traversal callback - routes edge expansion to GCN when available
  std::function<std::vector<uint64_t>(uint64_t entity_id, uint32_t edge_type, uint64_t query_time)> gcn_traversal_callback;
  
  // Storage-backed alternatives when CedarGraph is not available (e.g., QueryD sub-queries)
  std::function<std::vector<uint64_t>(uint64_t min_id, uint64_t max_id, uint64_t step)> get_all_entities_fn;
  std::function<std::vector<cedar::Neighbor>(uint64_t vertex_id, uint16_t edge_type, cedar::Timestamp start, cedar::Timestamp end)> get_out_neighbors_fn;
  std::function<std::vector<cedar::Neighbor>(uint64_t vertex_id, uint16_t edge_type, cedar::Timestamp start, cedar::Timestamp end)> get_in_neighbors_fn;
  
  // Partition restriction for sub-query execution
  std::optional<uint16_t> partition_id;
  
  // TMV engine for temporal queries
  cedar::gcn::TMVEngine* tmv_engine = nullptr;
  
  void SetVariable(const std::string& name, const Value& val);
  std::optional<Value> GetVariable(const std::string& name) const;
};

/**
 * @brief Base class for physical operators
 */
class PhysicalOperator {
 public:
  virtual ~PhysicalOperator() = default;
  
  /**
   * @brief Initialize the operator
   * @return true on success
   */
  virtual bool Init(ExecutionContext* ctx) = 0;
  
  /**
   * @brief Get next record from this operator
   * @return Record or nullptr if exhausted
   */
  virtual std::shared_ptr<Record> Next() = 0;
  
  /**
   * @brief Reset operator state
   */
  virtual void Reset() {}
  
  /**
   * @brief Get operator name for EXPLAIN
   */
  virtual std::string GetName() const = 0;
  
  /**
   * @brief Get operator details for EXPLAIN
    */
  virtual std::string GetDetails() const { return ""; }
  
  /**
   * @brief Format for EXPLAIN output
   */
  virtual std::string Explain(int indent = 0) const;
  
  /**
   * @brief Clone the operator (deep copy with reset state)
   */
  virtual std::unique_ptr<PhysicalOperator> Clone() const = 0;
  
  /**
   * @brief Returns true if this operator requires a graph or GCN callback
   */
  virtual bool RequiresGraph() const { return false; }
  
  /**
   * @brief Returns true if this operator strictly requires a GCN callback
   */
  virtual bool RequiresGcnCallback() const { return false; }
  
  /**
   * @brief Add child operator
   */
  void AddChild(std::shared_ptr<PhysicalOperator> child) {
    children_.push_back(child);
  }
  
  /**
   * @brief Get child operators
   */
  const std::vector<std::shared_ptr<PhysicalOperator>>& GetChildren() const {
    return children_;
  }
  
 protected:
  ExecutionContext* context_ = nullptr;
  std::vector<std::shared_ptr<PhysicalOperator>> children_;
};

// ============================================================================
// Standard Operators
// ============================================================================

/**
 * @brief Node scan operator
 */
class NodeScan : public PhysicalOperator {
 public:
  NodeScan(std::string variable, std::optional<std::string> label = std::nullopt);
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  std::string GetName() const override { return "NodeScan"; }
  std::string GetDetails() const override;
  std::unique_ptr<PhysicalOperator> Clone() const override;
  
 private:
  std::string variable_;
  std::optional<std::string> label_;
  size_t current_index_ = 0;
  std::vector<uint64_t> node_ids_;
};

/**
 * @brief Relationship expand operator
 */
class Expand : public PhysicalOperator {
 public:
  Expand(std::string from_variable,
         std::string rel_variable,
         std::string to_variable,
         Direction direction,
         std::optional<std::string> rel_type = std::nullopt);
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  void Reset() override;
  std::string GetName() const override { return "Expand"; }
  std::string GetDetails() const override;
  std::unique_ptr<PhysicalOperator> Clone() const override;
  
 private:
  std::string from_variable_;
  std::string rel_variable_;
  std::string to_variable_;
  Direction direction_;
  std::optional<std::string> rel_type_;
  
  std::shared_ptr<Record> current_record_;
  size_t neighbor_index_ = 0;
  std::vector<std::pair<uint64_t, uint64_t>> neighbors_;  // (rel_id, target_id)
};

/**
 * @brief Filter operator
 */
class Filter : public PhysicalOperator {
 public:
  explicit Filter(std::shared_ptr<Expression> predicate);
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  std::string GetName() const override { return "Filter"; }
  std::string GetDetails() const override;
  std::unique_ptr<PhysicalOperator> Clone() const override;
  
 private:
  std::shared_ptr<Expression> predicate_;
  
  bool EvaluatePredicate(const Record& record);
};

/**
 * @brief Project operator
 */
class Project : public PhysicalOperator {
 public:
  explicit Project(std::vector<std::pair<std::string, std::shared_ptr<Expression>>> projections);
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  std::string GetName() const override { return "Project"; }
  std::string GetDetails() const override;
  std::unique_ptr<PhysicalOperator> Clone() const override;
  
 private:
  std::vector<std::pair<std::string, std::shared_ptr<Expression>>> projections_;
};

/**
 * @brief Produce results operator (root)
 */
class ProduceResults : public PhysicalOperator {
 public:
  explicit ProduceResults(std::vector<std::string> columns);
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  std::string GetName() const override { return "ProduceResults"; }
  
  ResultSet GetResultSet();
  std::unique_ptr<PhysicalOperator> Clone() const override;
  
 private:
  std::vector<std::string> columns_;
  ResultSet result_set_;
};

/**
 * @brief Sort operator
 */
class Sort : public PhysicalOperator {
 public:
  Sort(std::vector<std::pair<std::shared_ptr<Expression>, bool>> sort_items);
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  std::string GetName() const override { return "Sort"; }
  std::unique_ptr<PhysicalOperator> Clone() const override;
  
 private:
  std::vector<std::pair<std::shared_ptr<Expression>, bool>> sort_items_;
  std::vector<std::shared_ptr<Record>> buffered_records_;
  size_t current_index_ = 0;
  bool sorted_ = false;
  
  void DoSort();
};

/**
 * @brief Limit operator
 */
class Limit : public PhysicalOperator {
 public:
  explicit Limit(size_t limit);
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  std::string GetName() const override { return "Limit"; }
  std::string GetDetails() const override;
  std::unique_ptr<PhysicalOperator> Clone() const override;
  
 private:
  size_t limit_;
  size_t count_ = 0;
};

/**
 * @brief Skip operator
 */
class Skip : public PhysicalOperator {
 public:
  explicit Skip(size_t skip);
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  std::string GetName() const override { return "Skip"; }
  std::string GetDetails() const override;
  std::unique_ptr<PhysicalOperator> Clone() const override;
  
 private:
  size_t skip_;
  size_t skipped_ = 0;
  bool initialized_ = false;
};

/**
 * @brief Key used by Distinct to detect duplicates with hash+value equality.
 */
struct DistinctKey {
  size_t hash;
  std::vector<Value> values;
  bool operator==(const DistinctKey& o) const {
    return hash == o.hash && values == o.values;
  }
};

/**
 * @brief Hash functor for DistinctKey (uses precomputed hash).
 */
struct KeyHash {
  size_t operator()(const DistinctKey& k) const { return k.hash; }
};

/**
 * @brief Distinct operator
 */
class Distinct : public PhysicalOperator {
 public:
  explicit Distinct(std::vector<std::shared_ptr<Expression>> keys);
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  std::string GetName() const override { return "Distinct"; }
  std::unique_ptr<PhysicalOperator> Clone() const override;
  
 private:
  std::vector<std::shared_ptr<Expression>> keys_;
  std::unordered_set<DistinctKey, KeyHash> seen_keys_;
};

/**
 * @brief Aggregation operator
 */
class Aggregate : public PhysicalOperator {
 public:
  enum class AggregationFunc {
    kCount, kSum, kAvg, kMin, kMax, kCollect
  };
  
  struct AggregationItem {
    std::string output_name;
    AggregationFunc func;
    std::shared_ptr<Expression> expression;
    std::optional<std::string> group_by_key;
  };
  
  explicit Aggregate(std::vector<AggregationItem> items);
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  std::string GetName() const override { return "Aggregate"; }
  std::unique_ptr<PhysicalOperator> Clone() const override;
  
 private:
  std::vector<AggregationItem> items_;
  std::vector<std::shared_ptr<Record>> buffered_records_;
  size_t current_index_ = 0;
  bool aggregated_ = false;
  
  void DoAggregate();
};

// ============================================================================
// Temporal Operators
// ============================================================================

/**
 * @brief Temporal node scan operator
 * 
 * Scans nodes with temporal filtering based on the query's temporal clause.
 * Uses Cedar's MVCC version chain for efficient historical queries.
 */
class TemporalNodeScan : public PhysicalOperator {
 public:
  TemporalNodeScan(std::string variable,
                   std::optional<std::string> label,
                   TemporalModifierType modifier,
                   std::optional<TimestampExpression> start_time = std::nullopt,
                   std::optional<TimestampExpression> end_time = std::nullopt,
                   std::optional<uint64_t> version_number = std::nullopt);
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  std::string GetName() const override { return "TemporalNodeScan"; }
  std::string GetDetails() const override;
  std::unique_ptr<PhysicalOperator> Clone() const override;
  
 private:
  std::string variable_;
  std::optional<std::string> label_;
  TemporalModifierType modifier_;
  std::optional<TimestampExpression> start_time_;
  std::optional<TimestampExpression> end_time_;
  std::optional<uint64_t> version_number_;
  
  // Execution state
  size_t current_index_ = 0;
  std::vector<uint64_t> node_ids_;
  Timestamp query_start_ = 0;
  Timestamp query_end_ = Timestamp::Max();
  
  // Temporal filtering methods
  bool MatchesTemporalConstraint(const Node& node) const;
  bool MatchesAsOf(const Node& node) const;
  bool MatchesBetween(const Node& node) const;
  bool MatchesContainedIn(const Node& node) const;
  bool MatchesVersion(const Node& node) const;
};

/**
 * @brief Temporal expand operator
 * 
 * Expands relationships with temporal constraints.
 * Supports different temporal path semantics (snapshot, continuous, pairwise).
 */
class TemporalExpand : public PhysicalOperator {
 public:
  TemporalExpand(std::string from_variable,
                 std::string rel_variable,
                 std::string to_variable,
                 Direction direction,
                 TemporalModifierType modifier,
                 std::optional<std::string> rel_type = std::nullopt,
                 TemporalPathSemantics path_semantics = TemporalPathSemantics::kSnapshot);
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  void Reset() override;
  std::string GetName() const override { return "TemporalExpand"; }
  std::string GetDetails() const override;
  std::unique_ptr<PhysicalOperator> Clone() const override;
  
 private:
  std::string from_variable_;
  std::string rel_variable_;
  std::string to_variable_;
  Direction direction_;
  TemporalModifierType modifier_;
  std::optional<std::string> rel_type_;
  TemporalPathSemantics path_semantics_;
  
  // Execution state
  std::shared_ptr<Record> current_record_;
  size_t neighbor_index_ = 0;
  std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> neighbors_;  // (rel_id, target_id, source_id)
  
  Timestamp query_start_ = 0;
  Timestamp query_end_ = Timestamp::Max();
  
  // Temporal filtering
  bool RequiresGraph() const override { return true; }
  
  bool MatchesTemporalConstraint(const Relationship& rel) const;
  bool IsPathContinuous(const std::vector<Relationship>& path) const;
};

/**
 * @brief Snapshot scan operator
 * 
 * Optimized operator for AS OF queries - reads snapshot at specific version.
 */
class SnapshotScan : public PhysicalOperator {
 public:
  SnapshotScan(std::string variable,
               std::optional<std::string> label,
               Timestamp snapshot_time);
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  std::string GetName() const override { return "SnapshotScan"; }
  std::string GetDetails() const override;
  std::unique_ptr<PhysicalOperator> Clone() const override;
  
 private:
  std::string variable_;
  std::optional<std::string> label_;
  Timestamp snapshot_time_;
  
  size_t current_index_ = 0;
  std::vector<uint64_t> node_ids_;
};

/**
 * @brief Version scan operator
 * 
 * Scans all versions of nodes/relationships for ALL VERSIONS queries.
 */
class VersionScan : public PhysicalOperator {
 public:
  VersionScan(std::string variable,
              std::optional<std::string> label,
              std::optional<uint64_t> specific_version = std::nullopt);
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  std::string GetName() const override { return "VersionScan"; }
  std::string GetDetails() const override;
  std::unique_ptr<PhysicalOperator> Clone() const override;
  
 private:
  std::string variable_;
  std::optional<std::string> label_;
  std::optional<uint64_t> specific_version_;
  
  // Version chain iteration
  size_t current_node_index_ = 0;
  size_t current_version_index_ = 0;
  std::vector<uint64_t> node_ids_;
  std::vector<Node> current_versions_;
};

// ============================================================================
// Execution Plan Builder
// ============================================================================

/**
 * @brief Builds execution plan from AST
 */
class ExecutionPlanBuilder {
 public:
  /**
   * @brief Build physical execution plan from AST
   */
  static std::shared_ptr<PhysicalOperator> Build(
      std::shared_ptr<QueryStatement> query,
      std::shared_ptr<TemporalClause> temporal_clause = nullptr);
  
  /**
   * @brief Build plan with temporal optimization
   */
  static std::shared_ptr<PhysicalOperator> BuildTemporalPlan(
      std::shared_ptr<QueryStatement> query,
      std::shared_ptr<TemporalClause> temporal_clause);
  
 private:
  static std::shared_ptr<PhysicalOperator> BuildMatchPlan(
      std::shared_ptr<MatchClause> match,
      std::shared_ptr<TemporalClause> temporal_clause);
  
  static std::shared_ptr<PhysicalOperator> BuildCreatePlan(
      std::shared_ptr<CreateClause> create);
  
  static std::shared_ptr<PhysicalOperator> BuildScanForPattern(
      PathPattern pattern,
      std::shared_ptr<TemporalClause> temporal_clause);
  
  static std::shared_ptr<PhysicalOperator> CreateTemporalScan(
      std::string variable,
      std::optional<std::string> label,
      std::shared_ptr<TemporalClause> temporal_clause);
  
  static std::shared_ptr<PhysicalOperator> CreateTemporalExpand(
      std::string from_var,
      std::string rel_var,
      std::string to_var,
      Direction dir,
      std::optional<std::string> rel_type,
      std::shared_ptr<TemporalClause> temporal_clause);
};

/**
 * @brief Execution plan for a query
 */
class ExecutionPlan {
 public:
  explicit ExecutionPlan(std::shared_ptr<PhysicalOperator> root);
  
  /**
   * @brief Execute the plan
   */
  ResultSet Execute(ExecutionContext* ctx);
  
  /**
   * @brief Get EXPLAIN output
   */
  std::string Explain() const;
  
  /**
   * @brief Get root operator
   */
  std::shared_ptr<PhysicalOperator> GetRoot() const { return root_; }
  
  /**
   * @brief Clone the execution plan (deep copy with reset state)
   */
  std::unique_ptr<ExecutionPlan> Clone() const;
  
  /**
   * @brief Validate that the execution context satisfies operator dependencies
   */
  cedar::Status ValidateDependencies(const ExecutionContext& ctx) const;
  
 private:
  std::shared_ptr<PhysicalOperator> root_;
};

}  // namespace cypher
}  // namespace cedar
