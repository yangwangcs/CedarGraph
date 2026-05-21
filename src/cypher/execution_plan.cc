// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Execution Plan with Storage Layer Integration

#include "cedar/cypher/execution_plan.h"
#include "cedar/cypher/expression_evaluator.h"
#include "cedar/graph/cedar_graph.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/core/logging.h"
#include "cedar/core/status.h"
#include <algorithm>
#include <cstdlib>
#include <sstream>

namespace cedar {
namespace cypher {

// ============================================================================
// Record Implementation
// ============================================================================

std::optional<Value> Record::Get(const std::string& key) const {
  auto it = values.find(key);
  if (it != values.end()) {
    return it->second;
  }
  return std::nullopt;
}

// ============================================================================
// Helper: Convert Cedar Neighbor to Cypher Relationship
// ============================================================================

static Relationship NeighborToRelationship(const Neighbor& neighbor, 
                                           uint64_t src_id,
                                           uint16_t edge_type) {
  Relationship rel;
  rel.id = neighbor.id;  // Use neighbor id as relationship id
  rel.start_id = src_id;
  rel.end_id = neighbor.id;
  rel.type = std::to_string(edge_type);  // Convert edge_type to string
  
  // Add properties
  if (neighbor.value) {
    rel.properties["weight"] = Value(static_cast<int64_t>(*neighbor.value));
  }
  rel.properties["timestamp"] = Value(neighbor.timestamp);
  
  return rel;
}

// ============================================================================
// Helper: Convert Cedar Neighbor to Cypher Node
// ============================================================================

static Node NeighborToNode(const Neighbor& neighbor) {
  Node node;
  node.id = neighbor.id;
  node.labels.push_back("Node");  // Default label
  
  // Add properties
  if (neighbor.value) {
    node.properties["value"] = Value(static_cast<int64_t>(*neighbor.value));
  }
  node.properties["edge_type"] = Value(static_cast<int64_t>(neighbor.edge_type));
  
  return node;
}

// ============================================================================
// PhysicalOperator base
// ============================================================================

std::string PhysicalOperator::Explain(int indent) const {
  std::ostringstream oss;
  oss << std::string(indent * 2, ' ') << GetName();
  if (!GetDetails().empty()) {
    oss << " [" << GetDetails() << "]";
  }
  oss << "\n";
  for (const auto& child : children_) {
    oss << child->Explain(indent + 1);
  }
  return oss.str();
}

// ============================================================================
// ExecutionContext
// ============================================================================

void ExecutionContext::SetVariable(const std::string& name, const Value& val) {
  variables[name] = val;
}

std::optional<Value> ExecutionContext::GetVariable(const std::string& name) const {
  auto it = variables.find(name);
  if (it != variables.end()) {
    return it->second;
  }
  return std::nullopt;
}

// ============================================================================
// ExecutionPlan
// ============================================================================

ExecutionPlan::ExecutionPlan(std::shared_ptr<PhysicalOperator> root) 
    : root_(root) {}

ResultSet ExecutionPlan::Execute(ExecutionContext* ctx) {
  if (!ctx) {
    ResultSet result;
    result.SetError("Invalid execution context");
    return result;
  }
  
  auto status = ValidateDependencies(*ctx);
  if (!status.ok()) {
    ResultSet result;
    result.SetError(status.ToString());
    return result;
  }
  
  if (!root_->Init(ctx)) {
    ResultSet result;
    result.SetError("Failed to initialize execution plan");
    return result;
  }
  
  ResultSet result;
  auto produce_results = std::dynamic_pointer_cast<ProduceResults>(root_);
  
  while (auto record = root_->Next()) {
    if (produce_results) {
      // Root is ProduceResults, it handles result collection
    } else {
      // Collect records directly
      result.records.push_back(*record);
    }
  }
  
  if (produce_results) {
    result = produce_results->GetResultSet();
  }
  
  return result;
}

std::string ExecutionPlan::Explain() const {
  return root_->Explain(0);
}

std::unique_ptr<ExecutionPlan> ExecutionPlan::Clone() const {
  auto cloned_root = std::shared_ptr<PhysicalOperator>(root_->Clone());
  return std::make_unique<ExecutionPlan>(cloned_root);
}

 cedar::Status ExecutionPlan::ValidateDependencies(const ExecutionContext& ctx) const {
  std::vector<const PhysicalOperator*> stack;
  stack.push_back(root_.get());
  
  while (!stack.empty()) {
    const auto* op = stack.back();
    stack.pop_back();
    
    if (op->RequiresGraph() && !ctx.graph && !ctx.gcn_traversal_callback) {
      return cedar::Status::InvalidArgument("Operator requires graph or GCN callback");
    }
    if (op->RequiresGcnCallback() && !ctx.gcn_traversal_callback) {
      return cedar::Status::InvalidArgument("Operator requires GCN callback");
    }
    
    for (const auto& child : op->GetChildren()) {
      stack.push_back(child.get());
    }
  }
  
  return cedar::Status::OK();
}

// ============================================================================
// ProduceResults
// ============================================================================

ProduceResults::ProduceResults(std::vector<std::string> columns)
    : columns_(columns) {
  result_set_.columns = columns;
}

bool ProduceResults::Init(ExecutionContext* ctx) {
  context_ = ctx;
  if (!children_.empty()) {
    return children_[0]->Init(ctx);
  }
  return true;
}

std::shared_ptr<Record> ProduceResults::Next() {
  if (children_.empty()) {
    return nullptr;
  }
  
  auto record = children_[0]->Next();
  if (record) {
    result_set_.records.push_back(*record);
  }
  return record;
}

ResultSet ProduceResults::GetResultSet() {
  return result_set_;
}

std::unique_ptr<PhysicalOperator> ProduceResults::Clone() const {
  auto clone = std::make_unique<ProduceResults>(columns_);
  for (const auto& child : children_) {
    clone->AddChild(std::shared_ptr<PhysicalOperator>(child->Clone()));
  }
  clone->result_set_.records.clear();
  clone->result_set_.error.reset();
  clone->result_set_.rows_affected = 0;
  clone->result_set_.rows_returned = 0;
  clone->result_set_.execution_time_us = 0;
  return clone;
}

// ============================================================================
// NodeScan with Storage Integration
// ============================================================================

NodeScan::NodeScan(std::string variable, std::optional<std::string> label)
    : variable_(variable), label_(label), current_index_(0) {}

bool NodeScan::Init(ExecutionContext* ctx) {
  context_ = ctx;
  node_ids_.clear();
  
  // Generic node scan - iterate over a configurable entity range.
  // Range can be customized by setting the CEDAR_SCAN_MAX_ENTITIES env var.
  uint64_t min_entity_id = 1;
  uint64_t max_entity_id = 1000;
  const char* env_max = std::getenv("CEDAR_SCAN_MAX_ENTITIES");
  if (env_max) {
    max_entity_id = std::max(min_entity_id, static_cast<uint64_t>(std::strtoull(env_max, nullptr, 10)));
  }
  
  // Check if graph context provides entity enumeration
  if (ctx->get_all_entities_fn) {
    node_ids_ = ctx->get_all_entities_fn(min_entity_id, max_entity_id, 1);
  } else if (ctx->graph) {
    node_ids_ = ctx->graph->ScanVertices(ctx->time_range.first, ctx->time_range.second);
  } else {
    // Fallback: simple sequential range
    node_ids_.reserve(max_entity_id - min_entity_id + 1);
    for (uint64_t i = min_entity_id; i <= max_entity_id; ++i) {
      node_ids_.push_back(i);
    }
  }
  
  current_index_ = 0;
  return true;
}

std::shared_ptr<Record> NodeScan::Next() {
  if (current_index_ >= node_ids_.size()) {
    return nullptr;
  }
  
  uint64_t node_id = node_ids_[current_index_++];
  
  // Create a node value
  Node node;
  node.id = node_id;
  if (label_) {
    node.labels.push_back(*label_);
  } else {
    node.labels.push_back("Node");
  }
  node.properties["id"] = Value(static_cast<int64_t>(node_id));
  
  // Create record
  auto record = std::make_shared<Record>();
  record->Set(variable_, Value(node));
  
  return record;
}

std::string NodeScan::GetDetails() const {
  std::string details = variable_;
  if (label_) {
    details += ":" + *label_;
  }
  details += " (" + std::to_string(node_ids_.size()) + " nodes)";
  return details;
}

std::unique_ptr<PhysicalOperator> NodeScan::Clone() const {
  auto clone = std::make_unique<NodeScan>(variable_, label_);
  for (const auto& child : children_) {
    clone->AddChild(std::shared_ptr<PhysicalOperator>(child->Clone()));
  }
  clone->current_index_ = 0;
  clone->node_ids_.clear();
  return clone;
}

// ============================================================================
// Expand with Storage Integration
// ============================================================================

Expand::Expand(std::string from_variable,
               std::string rel_variable,
               std::string to_variable,
               Direction direction,
               std::optional<std::string> rel_type)
    : from_variable_(from_variable),
      rel_variable_(rel_variable),
      to_variable_(to_variable),
      direction_(direction),
      rel_type_(rel_type),
      neighbor_index_(0) {}

bool Expand::Init(ExecutionContext* ctx) {
  context_ = ctx;
  if (children_.empty()) {
    return false;
  }
  
  if (!children_[0]->Init(ctx)) {
    return false;
  }
  
  // Get first record from child
  current_record_ = children_[0]->Next();
  neighbor_index_ = 0;
  
  return true;
}

std::shared_ptr<Record> Expand::Next() {
  while (current_record_) {
    // If we exhausted all neighbors for the current record, advance to next input
    if (!neighbors_.empty() && neighbor_index_ >= neighbors_.size()) {
      current_record_ = children_[0]->Next();
      neighbors_.clear();
      neighbor_index_ = 0;
      continue;
    }
    
    // Get the source node from current record
    auto from_val = current_record_->Get(from_variable_);
    if (!from_val || !from_val->IsNode()) {
      // Move to next input record
      current_record_ = children_[0]->Next();
      neighbor_index_ = 0;
      continue;
    }
    
    uint64_t from_id = from_val->GetNode().id;
    
    // Get neighbors from storage or GCN callback
    if (neighbors_.empty()) {
      neighbors_.clear();
      
      uint16_t edge_type = 0;
      if (rel_type_ && !rel_type_->empty()) {
        char* end = nullptr;
        long parsed = std::strtol(rel_type_->c_str(), &end, 10);
        if (end != rel_type_->c_str() && *end == '\0') {
          edge_type = static_cast<uint16_t>(parsed);
        } else {
          CEDAR_LOG_WARN() << "Symbolic edge type '" << *rel_type_
                           << "' not resolved to numeric ID; using 0";
          edge_type = 0;
        }
      }
      
      if (context_->gcn_traversal_callback) {
        auto neighbor_ids = context_->gcn_traversal_callback(
            from_id, static_cast<uint32_t>(edge_type), context_->query_timestamp.value());
        for (uint64_t nid : neighbor_ids) {
          neighbors_.emplace_back(nid, Timestamp(0));
        }
      } else if (direction_ == Direction::INCOMING && context_->get_in_neighbors_fn) {
        auto neighbor_list = context_->get_in_neighbors_fn(
            from_id, edge_type, Timestamp(0), Timestamp::Max());
        for (const auto& n : neighbor_list) {
          neighbors_.emplace_back(n.id, n.timestamp);
        }
      } else if (direction_ != Direction::INCOMING && context_->get_out_neighbors_fn) {
        auto neighbor_list = context_->get_out_neighbors_fn(
            from_id, edge_type, Timestamp(0), Timestamp::Max());
        for (const auto& n : neighbor_list) {
          neighbors_.emplace_back(n.id, n.timestamp);
        }
      } else if (context_->graph) {
        if (direction_ == Direction::INCOMING) {
          auto neighbor_list = context_->graph->GetInNeighbors(
              from_id, edge_type, Timestamp(0), Timestamp::Max());
          for (const auto& n : neighbor_list) {
            neighbors_.emplace_back(n.id, n.timestamp);
          }
        } else if (direction_ == Direction::BOTH) {
          auto out_list = context_->graph->GetOutNeighbors(
              from_id, edge_type, Timestamp(0), Timestamp::Max());
          for (const auto& n : out_list) {
            neighbors_.emplace_back(n.id, n.timestamp);
          }
          auto in_list = context_->graph->GetInNeighbors(
              from_id, edge_type, Timestamp(0), Timestamp::Max());
          for (const auto& n : in_list) {
            neighbors_.emplace_back(n.id, n.timestamp);
          }
        } else {
          auto neighbor_list = context_->graph->GetOutNeighbors(
              from_id, edge_type, Timestamp(0), Timestamp::Max());
          for (const auto& n : neighbor_list) {
            neighbors_.emplace_back(n.id, n.timestamp);
          }
        }
      }
      
      neighbor_index_ = 0;
      
      if (neighbors_.empty()) {
        // No neighbors, move to next input record
        current_record_ = children_[0]->Next();
        continue;
      }
    }
    
    // Create result record
    auto record = std::make_shared<Record>(*current_record_);
    
    uint64_t target_id = neighbors_[neighbor_index_].first;
    Timestamp ts = neighbors_[neighbor_index_].second;
    
    // Add relationship
    Relationship rel;
    rel.id = std::hash<std::string>{}(
        std::to_string(from_id) + ":" + std::to_string(target_id) + ":" + std::to_string(ts.value()));
    rel.start_id = from_id;
    rel.end_id = target_id;
    rel.type = rel_type_.value_or("CONNECTED_TO");
    rel.properties["timestamp"] = Value(ts);
    record->Set(rel_variable_, Value(rel));
    
    // Add target node
    Node to_node;
    to_node.id = target_id;
    to_node.labels.push_back("Node");
    to_node.properties["id"] = Value(static_cast<int64_t>(target_id));
    record->Set(to_variable_, Value(to_node));
    
    neighbor_index_++;
    return record;
  }
  
  return nullptr;
}

void Expand::Reset() {
  neighbor_index_ = 0;
  neighbors_.clear();
  current_record_.reset();
}

std::string Expand::GetDetails() const {
  std::string details = "(" + from_variable_ + ")";
  details += (direction_ == Direction::INCOMING) ? "<-" : "-";
  details += "[" + rel_variable_;
  if (rel_type_) {
    details += ":" + *rel_type_;
  }
  details += "]";
  details += (direction_ == Direction::INCOMING) ? "-" : "->";
  details += "(" + to_variable_ + ")";
  return details;
}

std::unique_ptr<PhysicalOperator> Expand::Clone() const {
  auto clone = std::make_unique<Expand>(
      from_variable_, rel_variable_, to_variable_, direction_, rel_type_);
  for (const auto& child : children_) {
    clone->AddChild(std::shared_ptr<PhysicalOperator>(child->Clone()));
  }
  clone->current_record_.reset();
  clone->neighbor_index_ = 0;
  clone->neighbors_.clear();
  return clone;
}

// ============================================================================
// Filter Implementation
// ============================================================================

Filter::Filter(std::shared_ptr<Expression> predicate)
    : predicate_(predicate) {}

bool Filter::Init(ExecutionContext* ctx) {
  context_ = ctx;
  if (!children_.empty()) {
    return children_[0]->Init(ctx);
  }
  return true;
}

std::shared_ptr<Record> Filter::Next() {
  while (true) {
    auto record = children_[0]->Next();
    if (!record) {
      return nullptr;
    }
    
    if (EvaluatePredicate(*record)) {
      return record;
    }
  }
}

bool Filter::EvaluatePredicate(const Record& record) {
  if (!predicate_) return true;
  ExpressionEvaluator evaluator(context_);
  auto result = evaluator.Evaluate(*predicate_, record);
  return result.GetBool();
}

std::string Filter::GetDetails() const {
  return "predicate";
}

std::unique_ptr<PhysicalOperator> Filter::Clone() const {
  auto clone = std::make_unique<Filter>(predicate_);
  for (const auto& child : children_) {
    clone->AddChild(std::shared_ptr<PhysicalOperator>(child->Clone()));
  }
  return clone;
}

// ============================================================================
// Project Implementation
// ============================================================================

Project::Project(std::vector<std::pair<std::string, std::shared_ptr<Expression>>> projections)
    : projections_(projections) {}

bool Project::Init(ExecutionContext* ctx) {
  context_ = ctx;
  if (!children_.empty()) {
    return children_[0]->Init(ctx);
  }
  return true;
}

std::shared_ptr<Record> Project::Next() {
  auto record = children_[0]->Next();
  if (!record) {
    return nullptr;
  }
  
  // Create new record with projected values
  auto result = std::make_shared<Record>();
  ExpressionEvaluator evaluator(context_);
  
  for (const auto& [name, expr] : projections_) {
    if (expr) {
      auto val = evaluator.Evaluate(*expr, *record);
      result->Set(name, val);
    } else {
      // Fallback: copy variable value by name
      auto val = record->Get(name);
      if (val) {
        result->Set(name, *val);
      }
    }
  }
  
  return result;
}

std::string Project::GetDetails() const {
  return std::to_string(projections_.size()) + " projections";
}

std::unique_ptr<PhysicalOperator> Project::Clone() const {
  auto clone = std::make_unique<Project>(projections_);
  for (const auto& child : children_) {
    clone->AddChild(std::shared_ptr<PhysicalOperator>(child->Clone()));
  }
  return clone;
}

// ============================================================================
// ExecutionPlanBuilder
// ============================================================================

std::shared_ptr<PhysicalOperator> ExecutionPlanBuilder::Build(
    std::shared_ptr<QueryStatement> stmt,
    std::shared_ptr<TemporalClause> temporal_clause) {
  
  if (stmt->clauses.empty()) {
    return nullptr;
  }
  
  // Collect clauses by type
  std::shared_ptr<MatchClause> match_clause;
  std::shared_ptr<WhereClause> where_clause;
  std::shared_ptr<ReturnClause> return_clause;
  std::shared_ptr<OrderByClause> order_by_clause;
  std::shared_ptr<LimitClause> limit_clause;
  std::shared_ptr<SkipClause> skip_clause;
  
  for (const auto& clause : stmt->clauses) {
    switch (clause->clause_type) {
      case ClauseType::MATCH:
        match_clause = std::static_pointer_cast<MatchClause>(clause);
        break;
      case ClauseType::WHERE:
        where_clause = std::static_pointer_cast<WhereClause>(clause);
        break;
      case ClauseType::RETURN:
        return_clause = std::static_pointer_cast<ReturnClause>(clause);
        break;
      case ClauseType::ORDER_BY:
        order_by_clause = std::static_pointer_cast<OrderByClause>(clause);
        break;
      case ClauseType::LIMIT:
        limit_clause = std::static_pointer_cast<LimitClause>(clause);
        break;
      case ClauseType::SKIP:
        skip_clause = std::static_pointer_cast<SkipClause>(clause);
        break;
      default:
        break;
    }
  }
  
  // Build execution plan bottom-up
  std::shared_ptr<PhysicalOperator> root = nullptr;
  
  // 1. MATCH → Scan/Expand
  if (match_clause) {
    root = BuildMatchPlan(match_clause, temporal_clause);
  }
  
  // 2. WHERE → Filter
  if (where_clause && where_clause->condition && root) {
    auto filter = std::make_shared<Filter>(where_clause->condition);
    filter->AddChild(root);
    root = filter;
  }
  
  // 3. ORDER BY → Sort
  if (order_by_clause && !order_by_clause->items.empty() && root) {
    std::vector<std::pair<std::shared_ptr<Expression>, bool>> sort_items;
    for (const auto& item : order_by_clause->items) {
      sort_items.push_back({item.expression, item.ascending});
    }
    auto sort = std::make_shared<Sort>(sort_items);
    sort->AddChild(root);
    root = sort;
  }
  
  // 4. SKIP
  if (skip_clause && skip_clause->expression && root) {
    // Evaluate skip expression to get the count
    // For simplicity, assume it's a literal integer
    int64_t skip_count = 0;
    if (auto lit = std::dynamic_pointer_cast<LiteralExpr>(skip_clause->expression)) {
      if (lit->value.IsInt()) skip_count = lit->value.GetInt();
    }
    if (skip_count > 0) {
      auto skip_op = std::make_shared<Skip>(static_cast<size_t>(skip_count));
      skip_op->AddChild(root);
      root = skip_op;
    }
  }
  
  // 5. LIMIT
  if (limit_clause && limit_clause->expression && root) {
    int64_t limit_count = 0;
    if (auto lit = std::dynamic_pointer_cast<LiteralExpr>(limit_clause->expression)) {
      if (lit->value.IsInt()) limit_count = lit->value.GetInt();
    }
    if (limit_count > 0) {
      auto limit_op = std::make_shared<Limit>(static_cast<size_t>(limit_count));
      limit_op->AddChild(root);
      root = limit_op;
    }
  }
  
  // 6. RETURN → Project + Distinct (if needed) + ProduceResults
  if (return_clause) {
    std::vector<std::string> columns;
    std::vector<std::pair<std::string, std::shared_ptr<Expression>>> projections;
    
    for (const auto& item : return_clause->items) {
      std::string col_name = item.alias.value_or("column");
      columns.push_back(col_name);
      projections.push_back({col_name, item.expression});
    }
    
    // Project operator
    if (root) {
      auto project = std::make_shared<Project>(projections);
      project->AddChild(root);
      root = project;
    }
    
    // DISTINCT
    if (return_clause->distinct && root) {
      std::vector<std::shared_ptr<Expression>> distinct_keys;
      for (const auto& item : return_clause->items) {
        distinct_keys.push_back(item.expression);
      }
      auto distinct = std::make_shared<Distinct>(distinct_keys);
      distinct->AddChild(root);
      root = distinct;
    }
    
    // ProduceResults
    auto produce = std::make_shared<ProduceResults>(columns);
    if (root) {
      produce->AddChild(root);
    }
    root = produce;
  }
  
  return root;
}

std::shared_ptr<PhysicalOperator> ExecutionPlanBuilder::BuildMatchPlan(
    std::shared_ptr<MatchClause> match,
    std::shared_ptr<TemporalClause> temporal_clause) {
  
  if (match->patterns.empty()) {
    return nullptr;
  }
  
  // For now, just build scan for first pattern
  return BuildScanForPattern(match->patterns[0], temporal_clause);
}

std::shared_ptr<PhysicalOperator> ExecutionPlanBuilder::BuildScanForPattern(
    PathPattern pattern,
    std::shared_ptr<TemporalClause> temporal_clause) {
  
  if (pattern.elements.empty()) {
    return nullptr;
  }
  
  // Get first node pattern
  if (std::holds_alternative<NodePattern>(pattern.elements[0])) {
    const auto& node = std::get<NodePattern>(pattern.elements[0]);
    
    std::shared_ptr<PhysicalOperator> scan;
    
    if (temporal_clause && temporal_clause->HasTemporalConstraint()) {
      // Use temporal scan
      scan = std::make_shared<TemporalNodeScan>(
          node.variable,
          node.labels.empty() ? std::nullopt : std::optional(node.labels[0]),
          temporal_clause->modifier,
          temporal_clause->start_time,
          temporal_clause->end_time,
          temporal_clause->version_number);
    } else {
      // Regular scan
      scan = std::make_shared<NodeScan>(
          node.variable,
          node.labels.empty() ? std::nullopt : std::optional(node.labels[0]));
    }
    
    // Build expand chain for relationships
    std::shared_ptr<PhysicalOperator> current = scan;
    size_t i = 1;
    
    while (i < pattern.elements.size()) {
      if (std::holds_alternative<RelationshipPattern>(pattern.elements[i])) {
        const auto& rel = std::get<RelationshipPattern>(pattern.elements[i]);
        
        // Get the next node pattern
        if (i + 1 < pattern.elements.size() && 
            std::holds_alternative<NodePattern>(pattern.elements[i + 1])) {
          const auto& next_node = std::get<NodePattern>(pattern.elements[i + 1]);
          
          // Create expand operator
          auto expand = std::make_shared<Expand>(
              node.variable,  // From the first node
              rel.variable,
              next_node.variable,
              rel.direction,
              rel.types.empty() ? std::nullopt : std::optional(rel.types[0]));
          
          expand->AddChild(current);
          current = expand;
          
          i += 2;
        } else {
          break;
        }
      } else {
        break;
      }
    }
    
    return current;
  }
  
  return nullptr;
}

// ============================================================================
// Sort Implementation
// ============================================================================

Sort::Sort(std::vector<std::pair<std::shared_ptr<Expression>, bool>> sort_items)
    : sort_items_(std::move(sort_items)) {}

bool Sort::Init(ExecutionContext* ctx) {
  context_ = ctx;
  if (!children_.empty()) {
    return children_[0]->Init(ctx);
  }
  return true;
}

std::shared_ptr<Record> Sort::Next() {
  if (!sorted_) {
    DoSort();
    sorted_ = true;
  }
  
  if (current_index_ < buffered_records_.size()) {
    return buffered_records_[current_index_++];
  }
  return nullptr;
}

void Sort::DoSort() {
  // Drain all records from child
  while (auto record = children_[0]->Next()) {
    buffered_records_.push_back(record);
  }
  
  ExpressionEvaluator evaluator(context_);
  
  std::stable_sort(buffered_records_.begin(), buffered_records_.end(),
    [&](const std::shared_ptr<Record>& a, const std::shared_ptr<Record>& b) {
      for (const auto& [expr, ascending] : sort_items_) {
        if (!expr) continue;
        auto val_a = evaluator.Evaluate(*expr, *a);
        auto val_b = evaluator.Evaluate(*expr, *b);
        
        if (val_a.IsNull() && val_b.IsNull()) continue;
        if (val_a.IsNull()) return !ascending;
        if (val_b.IsNull()) return ascending;
        
        if (val_a.Type() != val_b.Type()) {
          return ascending ? (val_a.Type() < val_b.Type()) : (val_a.Type() > val_b.Type());
        }
        
        if (val_a < val_b) return ascending;
        if (val_a > val_b) return !ascending;
      }
      return false;
    });
}

std::unique_ptr<PhysicalOperator> Sort::Clone() const {
  auto clone = std::make_unique<Sort>(sort_items_);
  for (const auto& child : children_) {
    clone->AddChild(std::shared_ptr<PhysicalOperator>(child->Clone()));
  }
  clone->buffered_records_.clear();
  clone->current_index_ = 0;
  clone->sorted_ = false;
  return clone;
}

// ============================================================================
// Limit Implementation
// ============================================================================

Limit::Limit(size_t limit) : limit_(limit) {}

bool Limit::Init(ExecutionContext* ctx) {
  context_ = ctx;
  if (!children_.empty()) {
    return children_[0]->Init(ctx);
  }
  return true;
}

std::shared_ptr<Record> Limit::Next() {
  if (count_ >= limit_) {
    return nullptr;
  }
  auto record = children_[0]->Next();
  if (record) {
    ++count_;
  }
  return record;
}

std::string Limit::GetDetails() const {
  return std::to_string(limit_);
}

std::unique_ptr<PhysicalOperator> Limit::Clone() const {
  auto clone = std::make_unique<Limit>(limit_);
  for (const auto& child : children_) {
    clone->AddChild(std::shared_ptr<PhysicalOperator>(child->Clone()));
  }
  clone->count_ = 0;
  return clone;
}

// ============================================================================
// Skip Implementation
// ============================================================================

Skip::Skip(size_t skip) : skip_(skip) {}

bool Skip::Init(ExecutionContext* ctx) {
  context_ = ctx;
  if (!children_.empty()) {
    return children_[0]->Init(ctx);
  }
  return true;
}

std::shared_ptr<Record> Skip::Next() {
  if (!initialized_) {
    initialized_ = true;
    for (size_t i = 0; i < skip_; ++i) {
      if (!children_[0]->Next()) {
        break;
      }
      ++skipped_;
    }
  }
  return children_[0]->Next();
}

std::string Skip::GetDetails() const {
  return std::to_string(skip_);
}

std::unique_ptr<PhysicalOperator> Skip::Clone() const {
  auto clone = std::make_unique<Skip>(skip_);
  for (const auto& child : children_) {
    clone->AddChild(std::shared_ptr<PhysicalOperator>(child->Clone()));
  }
  clone->skipped_ = 0;
  clone->initialized_ = false;
  return clone;
}

// ============================================================================
// Distinct Implementation
// ============================================================================

Distinct::Distinct(std::vector<std::shared_ptr<Expression>> keys)
    : keys_(std::move(keys)) {}

bool Distinct::Init(ExecutionContext* ctx) {
  context_ = ctx;
  if (!children_.empty()) {
    return children_[0]->Init(ctx);
  }
  return true;
}

std::shared_ptr<Record> Distinct::Next() {
  ExpressionEvaluator evaluator(context_);

  while (auto record = children_[0]->Next()) {
    DistinctKey key;
    key.hash = 0;
    for (const auto& expr : keys_) {
      if (expr) {
        auto val = evaluator.Evaluate(*expr, *record);
        key.hash = key.hash * 31 + val.Hash();
        key.values.push_back(std::move(val));
      }
    }
    if (seen_keys_.find(key) == seen_keys_.end()) {
      seen_keys_.insert(std::move(key));
      return record;
    }
  }
  return nullptr;
}

std::unique_ptr<PhysicalOperator> Distinct::Clone() const {
  auto clone = std::make_unique<Distinct>(keys_);
  for (const auto& child : children_) {
    clone->AddChild(std::shared_ptr<PhysicalOperator>(child->Clone()));
  }
  clone->seen_keys_.clear();
  return clone;
}

// ============================================================================
// Aggregate Implementation
// ============================================================================

Aggregate::Aggregate(std::vector<AggregationItem> items)
    : items_(std::move(items)) {}

bool Aggregate::Init(ExecutionContext* ctx) {
  context_ = ctx;
  if (!children_.empty()) {
    return children_[0]->Init(ctx);
  }
  return true;
}

std::shared_ptr<Record> Aggregate::Next() {
  if (!aggregated_) {
    DoAggregate();
    aggregated_ = true;
  }
  
  if (current_index_ < buffered_records_.size()) {
    return buffered_records_[current_index_++];
  }
  return nullptr;
}

void Aggregate::DoAggregate() {
  ExpressionEvaluator evaluator(context_);
  
  // Group records by group_by_key (if any)
  std::map<std::string, std::vector<Record>> groups;
  
  while (auto record = children_[0]->Next()) {
    std::string group_key;
    for (const auto& item : items_) {
      if (item.group_by_key.has_value()) {
        auto val = record->Get(*item.group_by_key);
        if (val) {
          group_key += val->ToString() + "|";
        }
      }
    }
    groups[group_key].push_back(*record);
  }
  
  // If no grouping and no input records, use a single empty-key group
  if (groups.empty()) {
    bool has_group_by = false;
    for (const auto& item : items_) {
      if (item.group_by_key.has_value()) {
        has_group_by = true;
        break;
      }
    }
    if (!has_group_by) {
      groups[""];  // Empty group only for non-GROUP BY queries
    }
  }
  
  // Compute aggregates per group
  for (auto& [group_key, records] : groups) {
    auto result = std::make_shared<Record>();
    
    for (const auto& item : items_) {
      switch (item.func) {
        case AggregationFunc::kCount: {
          result->Set(item.output_name, Value(static_cast<int64_t>(records.size())));
          break;
        }
        case AggregationFunc::kSum: {
          int64_t int_sum = 0;
          double float_sum = 0.0;
          bool has_float = false;
          for (const auto& r : records) {
            if (item.expression) {
              auto val = evaluator.Evaluate(*item.expression, r);
              if (val.IsInt()) int_sum += val.GetInt();
              else if (val.IsFloat()) { float_sum += val.GetFloat(); has_float = true; }
            }
          }
          if (has_float) result->Set(item.output_name, Value(float_sum));
          else result->Set(item.output_name, Value(int_sum));
          break;
        }
        case AggregationFunc::kAvg: {
          double sum = 0.0;
          size_t count = 0;
          for (const auto& r : records) {
            if (item.expression) {
              auto val = evaluator.Evaluate(*item.expression, r);
              if (val.IsInt()) { sum += static_cast<double>(val.GetInt()); ++count; }
              else if (val.IsFloat()) { sum += val.GetFloat(); ++count; }
            }
          }
          if (count > 0) result->Set(item.output_name, Value(sum / count));
          else result->Set(item.output_name, Value());
          break;
        }
        case AggregationFunc::kMin: {
          Value min_val;
          bool first = true;
          for (const auto& r : records) {
            if (item.expression) {
              auto val = evaluator.Evaluate(*item.expression, r);
              if (first || val < min_val) {
                min_val = val;
                first = false;
              }
            }
          }
          result->Set(item.output_name, min_val);
          break;
        }
        case AggregationFunc::kMax: {
          Value max_val;
          bool first = true;
          for (const auto& r : records) {
            if (item.expression) {
              auto val = evaluator.Evaluate(*item.expression, r);
              if (first || val > max_val) {
                max_val = val;
                first = false;
              }
            }
          }
          result->Set(item.output_name, max_val);
          break;
        }
        case AggregationFunc::kCollect: {
          std::vector<Value> collected;
          for (const auto& r : records) {
            if (item.expression) {
              collected.push_back(evaluator.Evaluate(*item.expression, r));
            }
          }
          result->Set(item.output_name, Value(collected));
          break;
        }
      }
    }
    
    buffered_records_.push_back(result);
  }
}

std::unique_ptr<PhysicalOperator> Aggregate::Clone() const {
  auto clone = std::make_unique<Aggregate>(items_);
  for (const auto& child : children_) {
    clone->AddChild(std::shared_ptr<PhysicalOperator>(child->Clone()));
  }
  clone->buffered_records_.clear();
  clone->current_index_ = 0;
  clone->aggregated_ = false;
  return clone;
}

}  // namespace cypher
}  // namespace cedar
