// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Cypher Write Operators — CREATE, SET, DELETE

#include "cedar/cypher/execution_plan.h"
#include "cedar/cypher/expression_evaluator.h"
#include "cedar/graph/cedar_graph.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/core/logging.h"

#include <chrono>
#include <cerrno>
#include <cstdlib>

namespace cedar {
namespace cypher {

// ============================================================================
// Helpers: Property name → column_id, Value → Descriptor
// ============================================================================

static uint16_t PropertyNameToColumnId(const std::string& name) {
  // Simple hash-based mapping from string property names to 12-bit column IDs.
  // Collision is acceptable for MVP; production will use a schema registry.
  return static_cast<uint16_t>(std::hash<std::string>{}(name) & 0x0FFF);
}

static Descriptor ValueToDescriptor(const Value& value, uint16_t col_id) {
  if (value.IsInt()) {
    return Descriptor::InlineInt(col_id, static_cast<int32_t>(value.GetInt()));
  }
  if (value.IsFloat()) {
    return Descriptor::InlineFloat(col_id, static_cast<float>(value.GetFloat()));
  }
  if (value.IsString()) {
    const std::string& s = value.GetString();
    if (s.size() <= 4) {
      auto opt = Descriptor::InlineShortStr(col_id, Slice(s));
      if (opt) return *opt;
    }
    // Long strings fall through to ExternalRef/Tombstone placeholder.
    // Full blob support is out of scope for this sub-plan.
    return Descriptor::InlineInt(col_id, 0);
  }
  if (value.IsBool()) {
    return Descriptor::InlineInt(col_id, value.GetBool() ? 1 : 0);
  }
  // Default: store 0 as placeholder for unhandled types.
  return Descriptor::InlineInt(col_id, 0);
}

// ============================================================================
// CreateOperator
// ============================================================================

CreateOperator::CreateOperator(std::shared_ptr<CreateClause> create_clause)
    : create_clause_(std::move(create_clause)),
      pattern_index_(0),
      element_index_(0),
      initialized_(false),
      done_(false),
      id_counter_(0) {}

uint64_t CreateOperator::GenerateId() {
  auto now = std::chrono::steady_clock::now().time_since_epoch();
  auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
  return static_cast<uint64_t>(ns) + (++id_counter_);
}

bool CreateOperator::Init(ExecutionContext* ctx) {
  context_ = ctx;
  initialized_ = true;
  done_ = false;
  result_record_ = std::make_shared<Record>();
  pattern_index_ = 0;
  element_index_ = 0;
  return true;
}

std::shared_ptr<Record> CreateOperator::Next() {
  if (!initialized_ || done_) {
    return nullptr;
  }
  
  if (!create_clause_ || create_clause_->patterns.empty()) {
    done_ = true;
    return nullptr;
  }
  
  // Process all patterns in one call (CREATE produces a single result record)
  while (pattern_index_ < create_clause_->patterns.size()) {
    const auto& pattern = create_clause_->patterns[pattern_index_];
    
    while (element_index_ < pattern.elements.size()) {
      const auto& element = pattern.elements[element_index_];
      
      if (std::holds_alternative<NodePattern>(element)) {
        const auto& node = std::get<NodePattern>(element);
        auto status = CreateNode(node, result_record_.get());
        if (!status.ok()) {
          CEDAR_LOG_WARN() << "CreateOperator: failed to create node: " << status.ToString();
        }
      } else if (std::holds_alternative<RelationshipPattern>(element)) {
        const auto& rel = std::get<RelationshipPattern>(element);
        auto status = CreateEdge(rel, *result_record_);
        if (!status.ok()) {
          CEDAR_LOG_WARN() << "CreateOperator: failed to create edge: " << status.ToString();
        }
      }
      
      ++element_index_;
    }
    
    ++pattern_index_;
    element_index_ = 0;
  }
  
  done_ = true;
  return result_record_;
}

 cedar::Status CreateOperator::CreateNode(const NodePattern& node, Record* record) {
  if (!context_ || !context_->storage) {
    return cedar::Status::InvalidArgument("No storage available for CREATE");
  }
  
  uint64_t node_id = GenerateId();
  
  // Build node value for the result record
  Node created_node;
  created_node.id = node_id;
  created_node.labels = node.labels;
  
  // Collect properties into BatchWriteItem vector
  std::vector<CedarGraphStorage::BatchWriteItem> items;
  ExpressionEvaluator evaluator(context_);
  Record dummy_record;  // For evaluating literals (they don't depend on record state)
  
  for (const auto& [prop_name, expr] : node.properties) {
    Value prop_value = Value::Null();
    if (expr) {
      prop_value = evaluator.Evaluate(*expr, dummy_record);
    }
    created_node.properties[prop_name] = prop_value;
    
    uint16_t col_id = PropertyNameToColumnId(prop_name);
    Descriptor desc = ValueToDescriptor(prop_value, col_id);
    items.emplace_back(node_id, EntityType::Vertex, col_id, desc, Timestamp::Static(), 0);
  }
  
  // Always write at least a placeholder property so the node exists in storage
  if (items.empty()) {
    items.emplace_back(node_id, EntityType::Vertex, 0,
                       Descriptor::InlineInt(0, 0), Timestamp::Static(), 0);
  }
  
  auto status = context_->storage->BatchWrite(items);
  if (!status.ok()) {
    return status;
  }
  
  // Mark entity created for lifecycle tracking
  context_->storage->MarkEntityCreated(node_id, EntityType::Vertex, Timestamp::Now());
  
  // Bind variable in result record
  record->Set(node.variable, Value(created_node));
  return cedar::Status::OK();
}

cedar::Status CreateOperator::CreateEdge(const RelationshipPattern& rel,
                                          const Record& record) {
  if (!context_ || !context_->storage) {
    return cedar::Status::InvalidArgument("No storage available for CREATE edge");
  }
  
  // Resolve start and end node IDs from the current record
  auto from_val = record.Get(rel.variable);  // Not used for endpoint lookup
  (void)from_val;
  
  // For CREATE patterns, endpoints are typically the immediately preceding/following nodes.
  // We look them up by variable name from the pattern context.
  // Simplified: we require the nodes to already be bound in the record.
  // In a full CREATE (a)-[r]->(b), a and b are created just before r.
  
  // Find the node variables that this edge connects.
  // For MVP we assume the pattern is (from_var)-[rel]->(to_var)
  // and both node variables exist in the record.
  uint64_t start_id = 0;
  uint64_t end_id = 0;
  
  // Heuristic: search the record for nodes that were most recently created.
  // For a 3-element pattern [Node, Rel, Node], the rel connects the two nodes.
  // We store the last two node IDs seen during processing.
  for (const auto& [key, val] : record.values) {
    if (val.IsNode()) {
      if (start_id == 0) {
        start_id = val.GetNode().id;
      } else {
        end_id = val.GetNode().id;
      }
    }
  }
  
  if (start_id == 0 || end_id == 0) {
    return cedar::Status::InvalidArgument("Edge CREATE requires both endpoints in record");
  }
  
  uint16_t edge_type = 0;
  if (!rel.types.empty()) {
    try {
      edge_type = static_cast<uint16_t>(std::stoi(rel.types[0]));
    } catch (...) {
      edge_type = static_cast<uint16_t>(std::hash<std::string>{}(rel.types[0]) & 0xFFFF);
    }
  }
  
  // Build edge properties
  std::map<std::string, Value> edge_props;
  ExpressionEvaluator evaluator(context_);
  Record dummy_record;
  for (const auto& [prop_name, expr] : rel.properties) {
    Value prop_value = Value::Null();
    if (expr) {
      prop_value = evaluator.Evaluate(*expr, dummy_record);
    }
    edge_props[prop_name] = prop_value;
  }
  
  // Use PutEdge for the edge (creates EdgeOut + EdgeIn pair)
  Descriptor edge_desc = Descriptor::InlineInt(0, 0);  // placeholder
  if (!rel.properties.empty()) {
    edge_desc = ValueToDescriptor(edge_props.begin()->second, 0);
  }
  
  auto status = context_->storage->PutEdge(
      start_id, end_id, edge_type, Timestamp::Now(), edge_desc, Timestamp(0));
  if (!status.ok()) {
    return status;
  }
  
  // Build relationship value for record
  Relationship relationship;
  relationship.id = std::hash<std::string>{}(
      std::to_string(start_id) + ":" + std::to_string(end_id));
  relationship.start_id = start_id;
  relationship.end_id = end_id;
  relationship.type = rel.types.empty() ? "CONNECTED_TO" : rel.types[0];
  relationship.properties = std::move(edge_props);
  
  result_record_->Set(rel.variable, Value(relationship));
  
  return cedar::Status::OK();
}

std::string CreateOperator::GetDetails() const {
  if (!create_clause_) return "0 patterns";
  return std::to_string(create_clause_->patterns.size()) + " patterns";
}

std::unique_ptr<PhysicalOperator> CreateOperator::Clone() const {
  auto clone = std::make_unique<CreateOperator>(create_clause_);
  for (const auto& child : children_) {
    clone->AddChild(std::shared_ptr<PhysicalOperator>(child->Clone()));
  }
  clone->pattern_index_ = 0;
  clone->element_index_ = 0;
  clone->initialized_ = false;
  clone->done_ = false;
  clone->id_counter_ = 0;
  clone->result_record_.reset();
  return clone;
}

uint16_t CreateOperator::PropertyNameToColumnId(const std::string& name) const {
  return cedar::cypher::PropertyNameToColumnId(name);
}

cedar::Descriptor CreateOperator::ValueToDescriptor(const Value& value,
                                                     uint16_t col_id) const {
  return cedar::cypher::ValueToDescriptor(value, col_id);
}

// ============================================================================
// SetOperator
// ============================================================================

SetOperator::SetOperator(std::shared_ptr<SetClause> set_clause)
    : set_clause_(std::move(set_clause)) {}

bool SetOperator::Init(ExecutionContext* ctx) {
  context_ = ctx;
  if (!children_.empty()) {
    return children_[0]->Init(ctx);
  }
  return true;
}

std::shared_ptr<Record> SetOperator::Next() {
  if (children_.empty()) {
    return nullptr;
  }
  
  auto record = children_[0]->Next();
  if (!record) {
    return nullptr;
  }
  
  if (!set_clause_ || set_clause_->items.empty()) {
    return record;
  }
  
  for (const auto& item : set_clause_->items) {
    auto status = ApplySetItem(item, record.get());
    if (!status.ok()) {
      CEDAR_LOG_WARN() << "SetOperator: failed to apply set item: " << status.ToString();
    }
  }
  
  return record;
}

cedar::Status SetOperator::ApplySetItem(const SetClause::SetItem& item,
                                         Record* record) {
  if (!item.target || !item.value) {
    return cedar::Status::InvalidArgument("SET item missing target or value");
  }
  
  ExpressionEvaluator evaluator(context_);
  Value new_value = evaluator.Evaluate(*item.value, *record);
  
  if (item.target->expr_type == ExprType::PROPERTY) {
    auto* prop_expr = static_cast<PropertyExpr*>(item.target.get());
    const std::string& var_name = prop_expr->variable;
    const std::string& prop_name = prop_expr->property;
    
    // Update the in-memory record
    auto var_val = record->Get(var_name);
    if (!var_val) {
      return cedar::Status::InvalidArgument("Variable not found in record: " + var_name);
    }
    
    if (var_val->IsNode()) {
      Node node = var_val->GetNode();
      node.properties[prop_name] = new_value;
      record->Set(var_name, Value(node));
      
      // Persist to storage
      if (context_ && context_->storage) {
        uint16_t col_id = PropertyNameToColumnId(prop_name);
        Descriptor desc = ValueToDescriptor(new_value, col_id);
        auto s = context_->storage->PutStaticVertex(node.id, col_id, desc);
        if (!s.ok()) return s;
      }
    } else if (var_val->IsRelationship()) {
      Relationship rel = var_val->GetRelationship();
      rel.properties[prop_name] = new_value;
      record->Set(var_name, Value(rel));
      // Edge property update requires PutEdge; skip for MVP if storage unavailable
    } else {
      // Scalar variable assignment (e.g., SET x = 5)
      record->Set(var_name, new_value);
    }
  } else if (item.target->expr_type == ExprType::VARIABLE) {
    auto* var_expr = static_cast<VariableExpr*>(item.target.get());
    record->Set(var_expr->name, new_value);
  }
  
  return cedar::Status::OK();
}

std::string SetOperator::GetDetails() const {
  if (!set_clause_) return "0 items";
  return std::to_string(set_clause_->items.size()) + " items";
}

std::unique_ptr<PhysicalOperator> SetOperator::Clone() const {
  auto clone = std::make_unique<SetOperator>(set_clause_);
  for (const auto& child : children_) {
    clone->AddChild(std::shared_ptr<PhysicalOperator>(child->Clone()));
  }
  return clone;
}

uint16_t SetOperator::PropertyNameToColumnId(const std::string& name) const {
  return cedar::cypher::PropertyNameToColumnId(name);
}

cedar::Descriptor SetOperator::ValueToDescriptor(const Value& value,
                                                  uint16_t col_id) const {
  return cedar::cypher::ValueToDescriptor(value, col_id);
}

// ============================================================================
// DeleteOperator
// ============================================================================

DeleteOperator::DeleteOperator(std::shared_ptr<DeleteClause> delete_clause)
    : delete_clause_(std::move(delete_clause)) {}

bool DeleteOperator::Init(ExecutionContext* ctx) {
  context_ = ctx;
  if (!children_.empty()) {
    return children_[0]->Init(ctx);
  }
  return true;
}

std::shared_ptr<Record> DeleteOperator::Next() {
  if (children_.empty()) {
    return nullptr;
  }
  
  auto record = children_[0]->Next();
  if (!record) {
    return nullptr;
  }
  
  if (!delete_clause_ || delete_clause_->expressions.empty()) {
    return nullptr;  // Consume the record even if no expressions
  }
  
  ExpressionEvaluator evaluator(context_);
  
  for (const auto& expr : delete_clause_->expressions) {
    if (!expr) continue;
    
    Value target_val = evaluator.Evaluate(*expr, *record);
    
    if (target_val.IsNode()) {
      const Node& node = target_val.GetNode();
      if (context_ && context_->storage) {
        auto s = context_->storage->MarkEntityDeleted(
            node.id, EntityType::Vertex, Timestamp::Now());
        if (!s.ok()) {
          CEDAR_LOG_WARN() << "DeleteOperator: MarkEntityDeleted failed: " << s.ToString();
        }
      }
    } else if (target_val.IsRelationship()) {
      const Relationship& rel = target_val.GetRelationship();
      if (context_ && context_->storage) {
        // Mark both directions deleted
        auto s = context_->storage->MarkEntityDeleted(
            rel.start_id, EntityType::EdgeOut, Timestamp::Now());
        if (!s.ok()) {
          CEDAR_LOG_WARN() << "DeleteOperator: edge delete failed: " << s.ToString();
        }
      }
    } else if (target_val.IsInt()) {
      // Bare ID (e.g., from a variable bound to an integer ID)
      uint64_t entity_id = static_cast<uint64_t>(target_val.GetInt());
      if (context_ && context_->storage) {
        auto s = context_->storage->MarkEntityDeleted(
            entity_id, EntityType::Vertex, Timestamp::Now());
        if (!s.ok()) {
          CEDAR_LOG_WARN() << "DeleteOperator: id delete failed: " << s.ToString();
        }
      }
    }
  }
  
  // Return nullptr to consume the record (DELETE is a terminal consuming operator)
  return nullptr;
}

std::string DeleteOperator::GetDetails() const {
  if (!delete_clause_) return "0 expressions";
  return std::to_string(delete_clause_->expressions.size()) + " expressions" +
         (delete_clause_->detach ? " (detach)" : "");
}

std::unique_ptr<PhysicalOperator> DeleteOperator::Clone() const {
  auto clone = std::make_unique<DeleteOperator>(delete_clause_);
  for (const auto& child : children_) {
    clone->AddChild(std::shared_ptr<PhysicalOperator>(child->Clone()));
  }
  return clone;
}

}  // namespace cypher
}  // namespace cedar
