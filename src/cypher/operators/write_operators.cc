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

}  // namespace cypher
}  // namespace cedar
