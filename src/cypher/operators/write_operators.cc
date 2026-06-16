// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Cypher Write Operators — CREATE, SET, DELETE

#include "cedar/cypher/execution_plan.h"
#include "cedar/cypher/expression_evaluator.h"
#include "cedar/graph/cedar_graph.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/lsm_engine.h"
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
    // For strings > 4 bytes, store as ExternalRef with inline hash
    // The full string will be stored via PutString/PutBinary if available
    // For now, store a hash of the string as the payload
    uint32_t hash = static_cast<uint32_t>(std::hash<std::string>{}(s));
    return Descriptor(EntryKind::ExternalRef, col_id, hash, 
                      static_cast<uint8_t>(std::min(s.size(), size_t(255))));
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
  
  // If 'id' property is specified, use it as the entity_id
  uint64_t node_id = 0;
  ExpressionEvaluator evaluator(context_);
  Record dummy_record;
  
  auto id_it = node.properties.find("id");
  if (id_it != node.properties.end() && id_it->second) {
    Value id_val = evaluator.Evaluate(*id_it->second, dummy_record);
    if (id_val.IsInt() && id_val.GetInt() > 0) {
      node_id = static_cast<uint64_t>(id_val.GetInt());
    }
  }
  if (node_id == 0) {
    node_id = GenerateId();
  }
  
  // Build node value for the result record
  Node created_node;
  created_node.id = node_id;
  created_node.labels = node.labels;
  
  // Write each property directly via PutStaticVertex (bypasses OCC validation)
  for (const auto& [prop_name, expr] : node.properties) {
    Value prop_value = Value::Null();
    if (expr) {
      prop_value = evaluator.Evaluate(*expr, dummy_record);
    }
    created_node.properties[prop_name] = prop_value;
    
    uint16_t col_id = PropertyNameToColumnId(prop_name);
    
    // For long strings, use PutString which handles blob storage properly
    if (prop_value.IsString() && prop_value.GetString().size() > 4) {
      auto s = context_->storage->PutString(node_id, col_id, prop_value.GetString());
      if (!s.ok()) return s;
    } else {
      Descriptor desc = ValueToDescriptor(prop_value, col_id);
      auto s = context_->storage->PutStaticVertex(node_id, col_id, desc);
      if (!s.ok()) return s;
    }
    
    context_->storage->RegisterPropertyName(col_id, prop_name);
  }

  // Persist labels
  for (const auto& label : node.labels) {
    auto desc_opt = Descriptor::InlineShortStr(LsmEngine::kLabelColumnId, Slice(label));
    if (desc_opt.has_value()) {
      auto s = context_->storage->PutStaticVertex(node_id, LsmEngine::kLabelColumnId, *desc_opt);
      if (!s.ok()) return s;
    }
  }

  // Update the in-memory label index
  if (auto* engine = context_->storage->GetLsmEngine()) {
    for (const auto& label : node.labels) {
      engine->IndexLabel(node_id, label);
    }
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
      edge_type = static_cast<uint16_t>(std::hash<std::string>{}(rel.types[0]) & 0x0FFF);
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
        
        // For long strings, use PutString which handles blob storage properly
        if (new_value.IsString() && new_value.GetString().size() > 4) {
          auto s = context_->storage->PutString(node.id, col_id, new_value.GetString());
          if (!s.ok()) return s;
        } else {
          Descriptor desc = ValueToDescriptor(new_value, col_id);
          auto s = context_->storage->PutStaticVertex(node.id, col_id, desc);
          if (!s.ok()) return s;
        }
        
        // Register property name to column ID mapping for reverse lookup
        context_->storage->RegisterPropertyName(col_id, prop_name);
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

// ============================================================================
// MergeOperator
// ============================================================================

MergeOperator::MergeOperator(std::shared_ptr<MergeClause> merge_clause)
    : merge_clause_(std::move(merge_clause)),
      initialized_(false),
      done_(false),
      id_counter_(0) {}

uint64_t MergeOperator::GenerateId() {
  auto now = std::chrono::steady_clock::now().time_since_epoch();
  auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
  return static_cast<uint64_t>(ns) + (++id_counter_);
}

bool MergeOperator::Init(ExecutionContext* ctx) {
  context_ = ctx;
  initialized_ = true;
  done_ = false;
  result_record_ = std::make_shared<Record>();
  return true;
}

std::shared_ptr<Record> MergeOperator::Next() {
  if (!initialized_ || done_) {
    return nullptr;
  }

  if (!merge_clause_ || merge_clause_->patterns.empty()) {
    done_ = true;
    return nullptr;
  }

  // Process all patterns in one call (MERGE produces a single result record)
  for (const auto& pattern : merge_clause_->patterns) {
    for (const auto& element : pattern.elements) {
      if (std::holds_alternative<NodePattern>(element)) {
        const auto& node = std::get<NodePattern>(element);
        // Attempt to find existing node by properties (id literal optimisation)
        bool found = false;
        auto id_it = node.properties.find("id");
        if (id_it != node.properties.end() && id_it->second &&
            id_it->second->expr_type == ExprType::LITERAL) {
          auto* literal = static_cast<LiteralExpr*>(id_it->second.get());
          if (literal->value.IsInt() && literal->value.GetInt() > 0) {
            uint64_t node_id = static_cast<uint64_t>(literal->value.GetInt());
            if (context_->graph && context_->graph->HasVertex(node_id)) {
              found = true;
              Node n;
              n.id = node_id;
              n.labels = node.labels.empty() ? std::vector<std::string>{"Node"} : node.labels;
              n.properties["id"] = Value(static_cast<int64_t>(node_id));
              result_record_->Set(node.variable, Value(n));
            } else if (context_->storage) {
              auto versions = context_->storage->Scan(node_id, Timestamp(0), Timestamp::Max());
              if (!versions.empty()) {
                found = true;
                Node n;
                n.id = node_id;
                n.labels = node.labels.empty() ? std::vector<std::string>{"Node"} : node.labels;
                n.properties["id"] = Value(static_cast<int64_t>(node_id));
                result_record_->Set(node.variable, Value(n));
              }
            }
          }
        }
        if (!found) {
          auto status = MergeNode(node, result_record_.get());
          if (!status.ok()) {
            CEDAR_LOG_WARN() << "MergeOperator: failed to merge node: " << status.ToString();
          }
        }
      } else if (std::holds_alternative<RelationshipPattern>(element)) {
        const auto& rel = std::get<RelationshipPattern>(element);
        auto status = MergeEdge(rel, *result_record_);
        if (!status.ok()) {
          CEDAR_LOG_WARN() << "MergeOperator: failed to merge edge: " << status.ToString();
        }
      }
    }
  }

  done_ = true;
  return result_record_;
}

cedar::Status MergeOperator::MergeNode(const NodePattern& node, Record* record) {
  if (!context_ || !context_->storage) {
    return cedar::Status::InvalidArgument("No storage available for MERGE");
  }

  uint64_t node_id = GenerateId();
  Node created_node;
  created_node.id = node_id;
  created_node.labels = node.labels;

  std::vector<CedarGraphStorage::BatchWriteItem> items;
  ExpressionEvaluator evaluator(context_);
  Record dummy_record;

  for (const auto& [prop_name, expr] : node.properties) {
    Value prop_value = Value::Null();
    if (expr) {
      prop_value = evaluator.Evaluate(*expr, dummy_record);
    }
    created_node.properties[prop_name] = prop_value;
    uint16_t col_id = PropertyNameToColumnId(prop_name);
    
    // Register property name to column ID mapping for reverse lookup
    context_->storage->RegisterPropertyName(col_id, prop_name);
    
    // For long strings, use PutString after batch write
    if (prop_value.IsString() && prop_value.GetString().size() > 4) {
      // Store a placeholder in batch, will overwrite with PutString below
      items.emplace_back(node_id, EntityType::Vertex, col_id,
                         Descriptor::InlineInt(col_id, 0), Timestamp::Static(), 0);
    } else {
      Descriptor desc = ValueToDescriptor(prop_value, col_id);
      items.emplace_back(node_id, EntityType::Vertex, col_id, desc, Timestamp::Static(), 0);
    }
  }

  if (items.empty()) {
    items.emplace_back(node_id, EntityType::Vertex, 0,
                       Descriptor::InlineInt(0, 0), Timestamp::Static(), 0);
  }

  auto status = context_->storage->BatchWrite(items);
  if (!status.ok()) {
    return status;
  }
  
  // Write long strings via PutString (after batch write)
  for (const auto& [prop_name, expr] : node.properties) {
    Value prop_value = Value::Null();
    if (expr) {
      prop_value = evaluator.Evaluate(*expr, dummy_record);
    }
    if (prop_value.IsString() && prop_value.GetString().size() > 4) {
      uint16_t col_id = PropertyNameToColumnId(prop_name);
      auto s = context_->storage->PutString(node_id, col_id, prop_value.GetString());
      if (!s.ok()) return s;
    }
  }

  context_->storage->MarkEntityCreated(node_id, EntityType::Vertex, Timestamp::Now());
  record->Set(node.variable, Value(created_node));
  return cedar::Status::OK();
}

cedar::Status MergeOperator::MergeEdge(const RelationshipPattern& rel,
                                       const Record& record) {
  if (!context_ || !context_->storage) {
    return cedar::Status::InvalidArgument("No storage available for MERGE edge");
  }

  uint64_t start_id = 0;
  uint64_t end_id = 0;
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
    return cedar::Status::InvalidArgument("MERGE edge requires both endpoints in record");
  }

  uint16_t edge_type = 0;
  if (!rel.types.empty()) {
    try {
      edge_type = static_cast<uint16_t>(std::stoi(rel.types[0]));
    } catch (...) {
      edge_type = static_cast<uint16_t>(std::hash<std::string>{}(rel.types[0]) & 0x0FFF);
    }
  }

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

  Descriptor edge_desc = Descriptor::InlineInt(0, 0);
  if (!rel.properties.empty()) {
    edge_desc = ValueToDescriptor(edge_props.begin()->second, 0);
  }

  auto status = context_->storage->PutEdge(
      start_id, end_id, edge_type, Timestamp::Now(), edge_desc, Timestamp(0));
  if (!status.ok()) {
    return status;
  }

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

std::string MergeOperator::GetDetails() const {
  if (!merge_clause_) return "0 patterns";
  return std::to_string(merge_clause_->patterns.size()) + " patterns";
}

std::unique_ptr<PhysicalOperator> MergeOperator::Clone() const {
  auto clone = std::make_unique<MergeOperator>(merge_clause_);
  for (const auto& child : children_) {
    clone->AddChild(std::shared_ptr<PhysicalOperator>(child->Clone()));
  }
  clone->initialized_ = false;
  clone->done_ = false;
  clone->id_counter_ = 0;
  clone->result_record_.reset();
  return clone;
}

uint16_t MergeOperator::PropertyNameToColumnId(const std::string& name) const {
  return cedar::cypher::PropertyNameToColumnId(name);
}

cedar::Descriptor MergeOperator::ValueToDescriptor(const Value& value,
                                                   uint16_t col_id) const {
  return cedar::cypher::ValueToDescriptor(value, col_id);
}

}  // namespace cypher
}  // namespace cedar
