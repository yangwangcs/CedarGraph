// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Execution Plan with Storage Layer Integration

#include "cedar/cypher/execution_plan.h"
#include "cedar/cypher/expression_evaluator.h"
#include "cedar/graph/cedar_graph.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/core/logging.h"
#include "cedar/core/status.h"
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <sstream>

namespace cedar {
namespace cypher {

// ============================================================================
// Index helpers
// ============================================================================

static uint16_t PropertyNameToColumnId(const std::string& name) {
  return static_cast<uint16_t>(std::hash<std::string>{}(name) & 0x0FFF);
}

static std::string ValueToIndexString(const Value& value) {
  if (value.IsInt()) {
    return std::to_string(value.GetInt());
  }
  if (value.IsFloat()) {
    return std::to_string(value.GetFloat());
  }
  if (value.IsString()) {
    return value.GetString();
  }
  if (value.IsBool()) {
    return value.GetBool() ? "1" : "0";
  }
  return "";
}

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

PhysicalOperator::ProfileData PhysicalOperator::GetProfile() const {
  ProfileData data;
  data.name = GetName();
  data.details = GetDetails();
  data.time_us = profile_time_us_;
  data.rows_processed = profile_rows_;
  data.depth = 0;
  
  for (const auto& child : children_) {
    auto child_profile = child->GetProfile();
    child_profile.depth = data.depth + 1;
    data.children.push_back(std::move(child_profile));
  }
  
  return data;
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

NodeScan::NodeScan(std::string variable, std::optional<std::string> label,
                       std::map<std::string, std::shared_ptr<Expression>> properties)
    : variable_(variable), label_(label), properties_(std::move(properties)), current_index_(0) {}

bool NodeScan::Init(ExecutionContext* ctx) {
  context_ = ctx;
  node_ids_.clear();
  
  // If properties contain 'id' literal, do point lookup instead of scan
  auto id_it = properties_.find("id");
  if (id_it != properties_.end() && id_it->second) {
    if (id_it->second->expr_type == ExprType::LITERAL) {
      auto* literal = static_cast<LiteralExpr*>(id_it->second.get());
      if (literal->value.IsInt()) {
        int64_t id_val = literal->value.GetInt();
        if (id_val > 0) {
          uint64_t node_id = static_cast<uint64_t>(id_val);
          // If graph or storage is available, verify the node actually exists
          bool exists = true;
          if (ctx->graph) {
            exists = ctx->graph->HasVertex(node_id);
          } else if (ctx->storage) {
            auto versions = ctx->storage->Scan(node_id, Timestamp(0), Timestamp::Max());
            exists = !versions.empty();
          }
          if (exists) {
            node_ids_.push_back(node_id);
          }
          current_index_ = 0;
          return true;
        }
      }
    }
  }
  
  // Generic node scan - iterate over a configurable entity range.
  // Range can be customized by setting the CEDAR_SCAN_MAX_ENTITIES env var.
  constexpr uint64_t kDefaultMinEntityId = 1;
  constexpr uint64_t kDefaultMaxEntityId = 1000;
  uint64_t min_entity_id = kDefaultMinEntityId;
  uint64_t max_entity_id = kDefaultMaxEntityId;
  const char* env_max = std::getenv("CEDAR_SCAN_MAX_ENTITIES");
  if (env_max) {
    char* end_ptr = nullptr;
    errno = 0;
    unsigned long long parsed = std::strtoull(env_max, &end_ptr, 10);
    if (end_ptr != env_max && *end_ptr == '\0' && errno == 0) {
      constexpr uint64_t kMaxAllowedEntities = 10000000;  // 10M hard cap
      if (parsed >= min_entity_id && parsed <= kMaxAllowedEntities) {
        max_entity_id = static_cast<uint64_t>(parsed);
      } else if (parsed > kMaxAllowedEntities) {
        max_entity_id = kMaxAllowedEntities;
      }
    }
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
  auto clone = std::make_unique<NodeScan>(variable_, label_, properties_);
  for (const auto& child : children_) {
    clone->AddChild(std::shared_ptr<PhysicalOperator>(child->Clone()));
  }
  clone->current_index_ = 0;
  clone->node_ids_.clear();
  return clone;
}

// ============================================================================
// IndexScan Implementation
// ============================================================================

IndexScan::IndexScan(std::string variable,
                     std::optional<std::string> label,
                     std::string property,
                     ComparisonExpr::Op op,
                     Value literal)
    : variable_(std::move(variable)),
      label_(std::move(label)),
      property_(std::move(property)),
      op_(op),
      literal_(std::move(literal)),
      current_index_(0),
      used_index_(false) {}

bool IndexScan::Init(ExecutionContext* ctx) {
  context_ = ctx;
  node_ids_.clear();
  used_index_ = false;

  // Try to use the real secondary index via storage
  if (ctx->storage) {
    auto* engine = ctx->storage->GetLsmEngine();
    if (engine) {
      if (!property_.empty() && op_ == ComparisonExpr::EQ) {
        uint16_t col_id = PropertyNameToColumnId(property_);
        std::string val = ValueToIndexString(literal_);
        if (!val.empty()) {
          node_ids_ = engine->LookupPropertyIndex(col_id, val);
          used_index_ = !node_ids_.empty();
        }
      }
      if (!used_index_ && label_.has_value()) {
        node_ids_ = engine->LookupLabelIndex(*label_);
        used_index_ = !node_ids_.empty();
      }
    }
  }

  // Fallback to range scan if index returned nothing or storage is unavailable
  if (!used_index_) {
    constexpr uint64_t kDefaultMinEntityId = 1;
    constexpr uint64_t kDefaultMaxEntityId = 1000;
    uint64_t min_entity_id = kDefaultMinEntityId;
    uint64_t max_entity_id = kDefaultMaxEntityId;

    const char* env_max = std::getenv("CEDAR_SCAN_MAX_ENTITIES");
    if (env_max) {
      char* end_ptr = nullptr;
      errno = 0;
      unsigned long long parsed = std::strtoull(env_max, &end_ptr, 10);
      if (end_ptr != env_max && *end_ptr == '\0' && errno == 0) {
        constexpr uint64_t kMaxAllowedEntities = 10000000;
        if (parsed >= min_entity_id && parsed <= kMaxAllowedEntities) {
          max_entity_id = static_cast<uint64_t>(parsed);
        } else if (parsed > kMaxAllowedEntities) {
          max_entity_id = kMaxAllowedEntities;
        }
      }
    }

    if (ctx->get_all_entities_fn) {
      node_ids_ = ctx->get_all_entities_fn(min_entity_id, max_entity_id, 1);
    } else if (ctx->graph) {
      node_ids_ = ctx->graph->ScanVertices(ctx->time_range.first, ctx->time_range.second);
    } else {
      node_ids_.reserve(max_entity_id - min_entity_id + 1);
      for (uint64_t i = min_entity_id; i <= max_entity_id; ++i) {
        node_ids_.push_back(i);
      }
    }
  }

  current_index_ = 0;
  return true;
}

std::shared_ptr<Record> IndexScan::Next() {
  while (current_index_ < node_ids_.size()) {
    uint64_t node_id = node_ids_[current_index_++];

    Node node;
    node.id = node_id;
    if (label_) {
      node.labels.push_back(*label_);
    } else {
      node.labels.push_back("Node");
    }
    node.properties["id"] = Value(static_cast<int64_t>(node_id));

    // If a graph/storage is available, try to fetch the real property value
    // so the predicate is evaluated against actual data.
    if (context_->graph) {
      // CedarGraph doesn't expose property fetch by id directly in the public
      // header used here, so we rely on the mock / test path or fallback.
    }

    if (!used_index_ && !MatchesPredicate(node)) {
      continue;
    }

    auto record = std::make_shared<Record>();
    record->Set(variable_, Value(node));
    return record;
  }
  return nullptr;
}

bool IndexScan::MatchesPredicate(const Node& node) const {
  // For the initial implementation we match against the property bag
  // that the scan constructs. In production this should read from storage.
  auto it = node.properties.find(property_);
  if (it == node.properties.end()) {
    return false;
  }
  const Value& val = it->second;

  switch (op_) {
    case ComparisonExpr::EQ: return val == literal_;
    case ComparisonExpr::NE: return val != literal_;
    case ComparisonExpr::LT: return val < literal_;
    case ComparisonExpr::GT: return val > literal_;
    case ComparisonExpr::LE: return val <= literal_;
    case ComparisonExpr::GE: return val >= literal_;
  }
  return false;
}

std::string IndexScan::GetDetails() const {
  std::string details = variable_;
  if (label_) {
    details += ":" + *label_;
  }
  details += "." + property_;
  std::string op_str;
  switch (op_) {
    case ComparisonExpr::EQ: op_str = "="; break;
    case ComparisonExpr::NE: op_str = "<>"; break;
    case ComparisonExpr::LT: op_str = "<"; break;
    case ComparisonExpr::GT: op_str = ">"; break;
    case ComparisonExpr::LE: op_str = "<="; break;
    case ComparisonExpr::GE: op_str = ">="; break;
  }
  details += " " + op_str + " " + literal_.ToString();
  return details;
}

std::unique_ptr<PhysicalOperator> IndexScan::Clone() const {
  auto clone = std::make_unique<IndexScan>(
      variable_, label_, property_, op_, literal_);
  for (const auto& child : children_) {
    clone->AddChild(std::shared_ptr<PhysicalOperator>(child->Clone()));
  }
  clone->current_index_ = 0;
  clone->node_ids_.clear();
  clone->used_index_ = false;
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
      neighbors_.clear();  // CRITICAL FIX: clear stale neighbors
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
// VariableLengthExpand Implementation
// ============================================================================

VariableLengthExpand::VariableLengthExpand(
    std::string from_variable,
    std::string rel_variable,
    std::string to_variable,
    Direction direction,
    std::optional<std::string> rel_type,
    uint64_t min_hops,
    uint64_t max_hops)
    : from_variable_(std::move(from_variable)),
      rel_variable_(std::move(rel_variable)),
      to_variable_(std::move(to_variable)),
      direction_(direction),
      rel_type_(std::move(rel_type)),
      min_hops_(min_hops),
      max_hops_(max_hops),
      result_index_(0) {}

bool VariableLengthExpand::Init(ExecutionContext* ctx) {
  context_ = ctx;
  if (children_.empty()) {
    return false;
  }
  if (!children_[0]->Init(ctx)) {
    return false;
  }
  current_record_ = children_[0]->Next();
  result_index_ = 0;
  result_buffer_.clear();
  return true;
}

std::shared_ptr<Record> VariableLengthExpand::Next() {
  while (current_record_) {
    if (result_index_ < result_buffer_.size()) {
      return result_buffer_[result_index_++];
    }

    if (result_index_ == 0 && result_buffer_.empty()) {
      // First time processing this record
      ExpandCurrentRecord();
      if (!result_buffer_.empty()) {
        continue;  // Results produced, return them
      }
    }

    // No results (or already consumed) — advance to next input record
    current_record_ = children_[0]->Next();
    result_index_ = 0;
    result_buffer_.clear();
  }
  return nullptr;
}

void VariableLengthExpand::ExpandCurrentRecord() {
  result_buffer_.clear();
  if (!current_record_) return;

  auto from_val = current_record_->Get(from_variable_);
  if (!from_val || !from_val->IsNode()) return;

  uint64_t start_id = from_val->GetNode().id;

  // Bounded BFS
  std::deque<BfsState> queue;
  queue.push_back({start_id, {}, 0});

  std::unordered_set<uint64_t> visited_at_depth;

  while (!queue.empty()) {
    BfsState state = queue.front();
    queue.pop_front();

    if (state.depth >= max_hops_) continue;

    auto neighbors = GetNeighbors(state.node_id);
    for (const auto& [rel_id, target_id] : neighbors) {
      auto new_path = state.path;
      new_path.push_back({rel_id, target_id});

      uint64_t new_depth = state.depth + 1;
      if (new_depth >= min_hops_ && new_depth <= max_hops_) {
        // Produce a result record
        auto record = std::make_shared<Record>(*current_record_);

        // Build relationship from the last hop
        Relationship rel;
        rel.id = rel_id;
        rel.start_id = state.node_id;
        rel.end_id = target_id;
        rel.type = rel_type_.value_or("CONNECTED_TO");
        record->Set(rel_variable_, Value(rel));

        Node to_node;
        to_node.id = target_id;
        to_node.labels.push_back("Node");
        to_node.properties["id"] = Value(static_cast<int64_t>(target_id));
        record->Set(to_variable_, Value(to_node));

        result_buffer_.push_back(record);
      }

      if (new_depth < max_hops_) {
        queue.push_back({target_id, new_path, new_depth});
      }
    }
  }
}

std::vector<std::pair<uint64_t, uint64_t>> VariableLengthExpand::GetNeighbors(
    uint64_t node_id) {
  std::vector<std::pair<uint64_t, uint64_t>> result;

  uint16_t edge_type = 0;
  if (rel_type_ && !rel_type_->empty()) {
    char* end = nullptr;
    long parsed = std::strtol(rel_type_->c_str(), &end, 10);
    if (end != rel_type_->c_str() && *end == '\0') {
      edge_type = static_cast<uint16_t>(parsed);
    }
  }

  if (context_->gcn_traversal_callback) {
    auto neighbor_ids = context_->gcn_traversal_callback(
        node_id, static_cast<uint32_t>(edge_type),
        context_->query_timestamp.value());
    for (uint64_t nid : neighbor_ids) {
      result.emplace_back(nid, nid);  // rel_id = target_id as placeholder
    }
  } else if (direction_ == Direction::INCOMING && context_->get_in_neighbors_fn) {
    auto neighbor_list = context_->get_in_neighbors_fn(
        node_id, edge_type, Timestamp(0), Timestamp::Max());
    for (const auto& n : neighbor_list) {
      result.emplace_back(n.id, n.id);
    }
  } else if (direction_ != Direction::INCOMING && context_->get_out_neighbors_fn) {
    auto neighbor_list = context_->get_out_neighbors_fn(
        node_id, edge_type, Timestamp(0), Timestamp::Max());
    for (const auto& n : neighbor_list) {
      result.emplace_back(n.id, n.id);
    }
  } else if (context_->graph) {
    if (direction_ == Direction::INCOMING) {
      auto neighbor_list = context_->graph->GetInNeighbors(
          node_id, edge_type, Timestamp(0), Timestamp::Max());
      for (const auto& n : neighbor_list) {
        result.emplace_back(n.id, n.id);
      }
    } else if (direction_ == Direction::BOTH) {
      auto out_list = context_->graph->GetOutNeighbors(
          node_id, edge_type, Timestamp(0), Timestamp::Max());
      for (const auto& n : out_list) {
        result.emplace_back(n.id, n.id);
      }
      auto in_list = context_->graph->GetInNeighbors(
          node_id, edge_type, Timestamp(0), Timestamp::Max());
      for (const auto& n : in_list) {
        result.emplace_back(n.id, n.id);
      }
    } else {
      auto neighbor_list = context_->graph->GetOutNeighbors(
          node_id, edge_type, Timestamp(0), Timestamp::Max());
      for (const auto& n : neighbor_list) {
        result.emplace_back(n.id, n.id);
      }
    }
  }

  return result;
}

void VariableLengthExpand::Reset() {
  result_index_ = 0;
  result_buffer_.clear();
  current_record_.reset();
}

std::string VariableLengthExpand::GetDetails() const {
  std::string details = "(" + from_variable_ + ")";
  details += (direction_ == Direction::INCOMING) ? "<-" : "-";
  details += "[" + rel_variable_;
  if (rel_type_) {
    details += ":" + *rel_type_;
  }
  details += "*" + std::to_string(min_hops_) + ".." + std::to_string(max_hops_);
  details += "]";
  details += (direction_ == Direction::INCOMING) ? "-" : "->";
  details += "(" + to_variable_ + ")";
  return details;
}

std::unique_ptr<PhysicalOperator> VariableLengthExpand::Clone() const {
  auto clone = std::make_unique<VariableLengthExpand>(
      from_variable_, rel_variable_, to_variable_,
      direction_, rel_type_, min_hops_, max_hops_);
  for (const auto& child : children_) {
    clone->AddChild(std::shared_ptr<PhysicalOperator>(child->Clone()));
  }
  clone->current_record_.reset();
  clone->result_index_ = 0;
  clone->result_buffer_.clear();
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
  if (children_.empty()) {
    return nullptr;
  }
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
  if (children_.empty()) {
    return nullptr;
  }
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
// UnwindOperator Implementation
// ============================================================================

UnwindOperator::UnwindOperator(std::shared_ptr<Expression> list_expr,
                               std::string alias)
    : list_expr_(std::move(list_expr)), alias_(std::move(alias)) {}

bool UnwindOperator::Init(ExecutionContext* ctx) {
  context_ = ctx;
  if (!children_.empty()) {
    if (!children_[0]->Init(ctx)) {
      return false;
    }
  }

  current_record_.reset();
  current_list_.clear();
  list_index_ = 0;

  // Stand-alone UNWIND has no input records; evaluate the list once against an
  // empty record so that `UNWIND [1,2,3] AS x RETURN x` still produces rows.
  if (children_.empty() && list_expr_) {
    ExpressionEvaluator evaluator(context_);
    Record empty_record;
    Value list_val = evaluator.Evaluate(*list_expr_, empty_record);
    if (list_val.IsList()) {
      current_record_ = std::make_shared<Record>();
      current_list_ = list_val.GetList();
    }
  }

  initialized_ = true;
  return true;
}

std::shared_ptr<Record> UnwindOperator::Next() {
  if (!initialized_) {
    return nullptr;
  }

  while (true) {
    // If we have pending list elements, emit the next one
    if (list_index_ < current_list_.size()) {
      auto record = std::make_shared<Record>(*current_record_);
      record->Set(alias_, current_list_[list_index_]);
      ++list_index_;
      return record;
    }

    // Need next input record
    if (children_.empty()) {
      return nullptr;
    }

    current_record_ = children_[0]->Next();
    if (!current_record_) {
      return nullptr;
    }

    // Evaluate the list expression
    ExpressionEvaluator evaluator(context_);
    Value list_val = evaluator.Evaluate(*list_expr_, *current_record_);

    if (!list_val.IsList()) {
      CEDAR_LOG_WARN() << "UnwindOperator: expression did not evaluate to a list";
      continue;  // Skip non-list records
    }

    current_list_ = list_val.GetList();
    list_index_ = 0;
  }
}

std::string UnwindOperator::GetDetails() const {
  return "AS " + alias_;
}

std::unique_ptr<PhysicalOperator> UnwindOperator::Clone() const {
  auto clone = std::make_unique<UnwindOperator>(list_expr_, alias_);
  for (const auto& child : children_) {
    clone->AddChild(std::shared_ptr<PhysicalOperator>(child->Clone()));
  }
  clone->current_record_.reset();
  clone->current_list_.clear();
  clone->list_index_ = 0;
  clone->initialized_ = false;
  return clone;
}

// ============================================================================
// ExecutionPlanBuilder
// ============================================================================

static std::shared_ptr<PhysicalOperator> ApplyPredicatePushdown(
    std::shared_ptr<PhysicalOperator> root,
    const std::vector<PushablePredicate>& predicates);

static std::unordered_set<std::string> ExtractRequiredColumns(
    const std::vector<std::pair<std::string, std::shared_ptr<Expression>>>& projections);

static std::shared_ptr<PhysicalOperator> ApplyProjectionPushdown(
    std::shared_ptr<PhysicalOperator> root,
    const std::unordered_set<std::string>& required_columns);

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
  std::shared_ptr<CreateClause> create_clause;
  std::shared_ptr<SetClause> set_clause;
  std::shared_ptr<DeleteClause> delete_clause;
  std::shared_ptr<MergeClause> merge_clause;
  std::shared_ptr<WithClause> with_clause;
  std::shared_ptr<UnwindClause> unwind_clause;
  
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
      case ClauseType::CREATE:
        create_clause = std::static_pointer_cast<CreateClause>(clause);
        break;
      case ClauseType::SET:
        set_clause = std::static_pointer_cast<SetClause>(clause);
        break;
      case ClauseType::DELETE:
        delete_clause = std::static_pointer_cast<DeleteClause>(clause);
        break;
      case ClauseType::MERGE:
        merge_clause = std::static_pointer_cast<MergeClause>(clause);
        break;
      case ClauseType::WITH:
        with_clause = std::static_pointer_cast<WithClause>(clause);
        break;
      case ClauseType::UNWIND:
        unwind_clause = std::static_pointer_cast<UnwindClause>(clause);
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
  
  // 1b. CREATE → CreateOperator
  if (create_clause) {
    auto create_op = BuildCreatePlan(create_clause);
    if (create_op) {
      if (root) {
        create_op->AddChild(root);
      }
      root = create_op;
    }
  }
  
  // 1c. SET → SetOperator (must follow MATCH or CREATE)
  if (set_clause) {
    auto set_op = BuildSetPlan(set_clause);
    if (set_op) {
      if (root) {
        set_op->AddChild(root);
      }
      root = set_op;
    }
  }
  
  // 1d. DELETE → DeleteOperator (must follow MATCH)
  if (delete_clause) {
    auto delete_op = BuildDeletePlan(delete_clause);
    if (delete_op) {
      if (root) {
        delete_op->AddChild(root);
      }
      root = delete_op;
    }
  }

  // 1e. MERGE → MergeOperator (like CREATE but with existence check)
  if (merge_clause) {
    auto merge_op = BuildMergePlan(merge_clause);
    if (merge_op) {
      if (root) {
        merge_op->AddChild(root);
      }
      root = merge_op;
    }
  }

  // 1f. WITH → Project (reuses existing Project operator)
  if (with_clause) {
    auto with_op = BuildWithPlan(with_clause);
    if (with_op) {
      if (root) {
        with_op->AddChild(root);
      }
      root = with_op;
    }
  }

  // 1g. UNWIND → UnwindOperator
  if (unwind_clause) {
    auto unwind_op = BuildUnwindPlan(unwind_clause);
    if (unwind_op) {
      if (root) {
        unwind_op->AddChild(root);
      }
      root = unwind_op;
    }
  }
  
  // 2. WHERE → Filter (with predicate pushdown into scan)
  if (where_clause && where_clause->condition && root) {
    auto analysis = AnalyzePredicates(*where_clause->condition);

    // Try to push predicates into the leaf scan operator
    if (!analysis.pushable.empty()) {
      root = ApplyPredicatePushdown(root, analysis.pushable);
    }

    // If there are remaining predicates, keep a Filter on top
    if (analysis.remaining) {
      auto filter = std::make_shared<Filter>(analysis.remaining);
      filter->AddChild(root);
      root = filter;
    }
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
    
    // Apply projection pushdown to reduce I/O
    if (root) {
      auto required_columns = ExtractRequiredColumns(projections);
      root = ApplyProjectionPushdown(root, required_columns);
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
  
  // Apply CBO optimization if we have a valid plan
  if (root) {
    CostOptimizer optimizer;
    auto optimized = optimizer.Optimize(root);
    if (optimized) {
      root = optimized;
    }
  }
  
  return root;
}

// ============================================================================
// Predicate Pushdown
// ============================================================================

std::shared_ptr<PhysicalOperator> ExecutionPlanBuilder::BuildLabelIndex(
    const std::string& variable,
    const std::string& label) {
  // Label-only scan: look up all entities with this label.
  // For the MVP we reuse IndexScan with an empty property and a dummy EQ
  // predicate that forces Init to fall through to the label index lookup.
  return std::make_shared<IndexScan>(
      variable, std::optional(label), "",
      ComparisonExpr::EQ, Value::Null());
}

std::shared_ptr<PhysicalOperator> ExecutionPlanBuilder::BuildPropertyIndex(
    const std::string& variable,
    const std::optional<std::string>& label,
    const std::string& property,
    ComparisonExpr::Op op,
    const Value& literal) {
  return std::make_shared<IndexScan>(
      variable, label, property, op, literal);
}

static std::shared_ptr<PhysicalOperator> ApplyPredicatePushdown(
    std::shared_ptr<PhysicalOperator> root,
    const std::vector<PushablePredicate>& predicates) {
  // Walk down to find the leaf scan operator
  if (!root) return root;
  if (predicates.empty()) return root;

  // If root is a NodeScan, replace or augment it
  if (auto node_scan = std::dynamic_pointer_cast<NodeScan>(root)) {
    // Pick the best predicate for index scan (equality first, then range)
    const PushablePredicate* best_eq = nullptr;
    const PushablePredicate* best_range = nullptr;
    
    for (const auto& pp : predicates) {
      if (pp.op == ComparisonExpr::EQ) {
        best_eq = &pp;
        break;  // Equality is always best
      } else if (!best_range) {
        best_range = &pp;
      }
    }
    
    const PushablePredicate* best = best_eq ? best_eq : best_range;
    if (best) {
      auto index_scan = ExecutionPlanBuilder::BuildPropertyIndex(
          best->variable,
          node_scan->label(),
          best->property,
          best->op,
          best->literal);
      
      // If there are remaining predicates, add a Filter on top
      std::vector<PushablePredicate> remaining;
      for (const auto& pp : predicates) {
        if (&pp != best) {
          remaining.push_back(pp);
        }
      }
      
      if (!remaining.empty()) {
        // Build filter expressions for remaining predicates
        std::shared_ptr<Expression> filter_expr;
        for (const auto& pp : remaining) {
          auto prop = std::make_shared<PropertyExpr>(pp.variable, pp.property);
          auto lit = std::make_shared<LiteralExpr>(pp.literal);
          auto comp = std::make_shared<ComparisonExpr>(pp.op, prop, lit);
          if (filter_expr) {
            filter_expr = std::make_shared<LogicalExpr>(
                LogicalExpr::Op::AND, filter_expr, comp);
          } else {
            filter_expr = comp;
          }
        }
        auto filter = std::make_shared<Filter>(filter_expr);
        filter->AddChild(index_scan);
        return filter;
      }
      
      return index_scan;
    }
  }

  // If root has children, try to push into the first child
  auto& children = root->GetChildren();
  if (!children.empty()) {
    auto new_child = ApplyPredicatePushdown(children[0], predicates);
    const_cast<std::vector<std::shared_ptr<PhysicalOperator>>&>(children)[0] = new_child;
  }

  return root;
}

// =============================================================================
// Projection Pushdown
// =============================================================================

// Extract required columns from expressions
static std::unordered_set<std::string> ExtractRequiredColumns(
    const std::vector<std::pair<std::string, std::shared_ptr<Expression>>>& projections) {
  std::unordered_set<std::string> columns;
  for (const auto& [name, expr] : projections) {
    // Extract column references from expression
    if (expr->expr_type == ExprType::PROPERTY) {
      auto prop = std::static_pointer_cast<PropertyExpr>(expr);
      columns.insert(prop->variable + "." + prop->property);
    }
  }
  return columns;
}

// Apply projection pushdown to reduce I/O
static std::shared_ptr<PhysicalOperator> ApplyProjectionPushdown(
    std::shared_ptr<PhysicalOperator> root,
    const std::unordered_set<std::string>& required_columns) {
  if (!root) return root;
  if (required_columns.empty()) return root;

  // If root is a NodeScan, add column hints
  if (auto node_scan = std::dynamic_pointer_cast<NodeScan>(root)) {
    // NodeScan doesn't have column filtering yet, but we can add it
    // For now, just return the node scan as-is
    // TODO: Add column filtering to NodeScan
    return root;
  }

  // If root is an IndexScan, it already only reads the indexed column
  if (auto index_scan = std::dynamic_pointer_cast<IndexScan>(root)) {
    return root;
  }

  // If root is an Expand, we can push down to both sides
  if (auto expand = std::dynamic_pointer_cast<Expand>(root)) {
    // TODO: Push required columns to expand's source and target scans
    return root;
  }

  // If root has children, try to push into each child
  auto& children = root->GetChildren();
  if (!children.empty()) {
    for (size_t i = 0; i < children.size(); ++i) {
      auto new_child = ApplyProjectionPushdown(children[i], required_columns);
      const_cast<std::vector<std::shared_ptr<PhysicalOperator>>&>(children)[i] = new_child;
    }
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

std::shared_ptr<PhysicalOperator> ExecutionPlanBuilder::BuildCreatePlan(
    std::shared_ptr<CreateClause> create) {
  if (!create || create->patterns.empty()) {
    return nullptr;
  }
  return std::make_shared<CreateOperator>(create);
}

std::shared_ptr<PhysicalOperator> ExecutionPlanBuilder::BuildSetPlan(
    std::shared_ptr<SetClause> set) {
  if (!set || set->items.empty()) {
    return nullptr;
  }
  return std::make_shared<SetOperator>(set);
}

std::shared_ptr<PhysicalOperator> ExecutionPlanBuilder::BuildDeletePlan(
    std::shared_ptr<DeleteClause> del) {
  if (!del || del->expressions.empty()) {
    return nullptr;
  }
  return std::make_shared<DeleteOperator>(del);
}

std::shared_ptr<PhysicalOperator> ExecutionPlanBuilder::BuildMergePlan(
    std::shared_ptr<MergeClause> merge) {
  if (!merge || merge->patterns.empty()) {
    return nullptr;
  }
  return std::make_shared<MergeOperator>(merge);
}

std::shared_ptr<PhysicalOperator> ExecutionPlanBuilder::BuildWithPlan(
    std::shared_ptr<WithClause> with_clause) {
  if (!with_clause || with_clause->items.empty()) {
    return nullptr;
  }

  std::vector<std::pair<std::string, std::shared_ptr<Expression>>> projections;
  for (const auto& item : with_clause->items) {
    std::string col_name = item.alias.value_or("column");
    projections.push_back({col_name, item.expression});
  }

  auto project = std::make_shared<Project>(projections);

  // DISTINCT
  if (with_clause->distinct) {
    std::vector<std::shared_ptr<Expression>> distinct_keys;
    for (const auto& item : with_clause->items) {
      distinct_keys.push_back(item.expression);
    }
    auto distinct_op = std::make_shared<Distinct>(distinct_keys);
    distinct_op->AddChild(project);
    return distinct_op;
  }

  return project;
}

std::shared_ptr<PhysicalOperator> ExecutionPlanBuilder::BuildUnwindPlan(
    std::shared_ptr<UnwindClause> unwind) {
  if (!unwind || !unwind->expression || unwind->alias.empty()) {
    return nullptr;
  }
  return std::make_shared<UnwindOperator>(unwind->expression, unwind->alias);
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
      // Prefer label index scan if a label is present and no properties
      if (!node.labels.empty() && node.properties.empty()) {
        scan = BuildLabelIndex(node.variable, node.labels[0]);
      } else {
        scan = std::make_shared<NodeScan>(
            node.variable,
            node.labels.empty() ? std::nullopt : std::optional(node.labels[0]),
            node.properties);
      }
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
          
          std::shared_ptr<PhysicalOperator> expand;
          bool has_hop_range = rel.min_hops.has_value() || rel.max_hops.has_value();
          if (has_hop_range) {
            uint64_t min_hops = rel.min_hops.value_or(1);
            uint64_t max_hops = rel.max_hops.value_or(min_hops);
            if (max_hops < min_hops) max_hops = min_hops;
            expand = std::make_shared<VariableLengthExpand>(
                std::get<NodePattern>(pattern.elements[i - 1]).variable,
                rel.variable,
                next_node.variable,
                rel.direction,
                rel.types.empty() ? std::nullopt : std::optional(rel.types[0]),
                min_hops,
                max_hops);
          } else {
            expand = std::make_shared<Expand>(
                std::get<NodePattern>(pattern.elements[i - 1]).variable,  // Previous node
                rel.variable,
                next_node.variable,
                rel.direction,
                rel.types.empty() ? std::nullopt : std::optional(rel.types[0]));
          }
          
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
  if (children_.empty()) {
    return;
  }
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
  if (children_.empty()) {
    return nullptr;
  }
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
  if (children_.empty()) {
    return nullptr;
  }
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
  if (children_.empty()) {
    return nullptr;
  }
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
  if (children_.empty()) {
    return nullptr;
  }
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
  if (children_.empty()) {
    return;
  }
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
              if (val.IsNull()) continue;  // SKIP NULL
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
              if (val.IsNull()) continue;  // SKIP NULL
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
        default:
          std::cerr << "[Aggregate] Unknown aggregation function" << std::endl;
          break;
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

// ============================================================================
// Predicate Analysis Implementation
// ============================================================================

static std::optional<PushablePredicate> TryExtractPushable(
    const ComparisonExpr& comp) {
  // Look for: PropertyExpr <op> LiteralExpr  or  LiteralExpr <op> PropertyExpr
  const PropertyExpr* prop = nullptr;
  const LiteralExpr* lit = nullptr;
  ComparisonExpr::Op op = comp.op;

  if (comp.left->expr_type == ExprType::PROPERTY &&
      comp.right->expr_type == ExprType::LITERAL) {
    prop = static_cast<const PropertyExpr*>(comp.left.get());
    lit = static_cast<const LiteralExpr*>(comp.right.get());
  } else if (comp.left->expr_type == ExprType::LITERAL &&
             comp.right->expr_type == ExprType::PROPERTY) {
    prop = static_cast<const PropertyExpr*>(comp.right.get());
    lit = static_cast<const LiteralExpr*>(comp.left.get());
    // Flip the operator
    switch (op) {
      case ComparisonExpr::LT: op = ComparisonExpr::GT; break;
      case ComparisonExpr::GT: op = ComparisonExpr::LT; break;
      case ComparisonExpr::LE: op = ComparisonExpr::GE; break;
      case ComparisonExpr::GE: op = ComparisonExpr::LE; break;
      default: break;
    }
  }

  if (!prop || !lit) {
    return std::nullopt;
  }

  PushablePredicate pp;
  pp.variable = prop->variable;
  pp.property = prop->property;
  pp.op = op;
  pp.literal = lit->value;
  return pp;
}

PredicateAnalysis AnalyzePredicates(const Expression& expr) {
  PredicateAnalysis result;

  if (expr.expr_type == ExprType::AND) {
    const auto& logical = static_cast<const LogicalExpr&>(expr);
    auto left = AnalyzePredicates(*logical.left);
    auto right = AnalyzePredicates(*logical.right);

    result.pushable.insert(result.pushable.end(),
                           left.pushable.begin(), left.pushable.end());
    result.pushable.insert(result.pushable.end(),
                           right.pushable.begin(), right.pushable.end());

    if (left.remaining && right.remaining) {
      result.remaining = std::make_shared<LogicalExpr>(
          LogicalExpr::Op::AND, left.remaining, right.remaining);
    } else if (left.remaining) {
      result.remaining = left.remaining;
    } else if (right.remaining) {
      result.remaining = right.remaining;
    }
    return result;
  }

  if (expr.expr_type == ExprType::COMPARISON) {
    const auto& comp = static_cast<const ComparisonExpr&>(expr);
    auto pushable = TryExtractPushable(comp);
    if (pushable) {
      result.pushable.push_back(*pushable);
      return result;
    }
  }

  // Not pushable — keep the whole expression as remaining
  result.remaining = std::shared_ptr<Expression>(
      const_cast<Expression*>(&expr),
      [](Expression*) {});  // Non-owning, just a view
  return result;
}

}  // namespace cypher
}  // namespace cedar
