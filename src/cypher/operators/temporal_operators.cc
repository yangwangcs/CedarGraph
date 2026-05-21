// Copyright (c) 2024 CedarGraph Project
// Licensed under the MIT License.

#include "cedar/cypher/execution_plan.h"
#include "cedar/cypher/value.h"

namespace cedar {
namespace cypher {

// ============================================================================
// TemporalNodeScan
// ============================================================================

TemporalNodeScan::TemporalNodeScan(std::string variable,
                                   std::optional<std::string> label,
                                   TemporalModifierType modifier,
                                   std::optional<TimestampExpression> start_time,
                                   std::optional<TimestampExpression> end_time,
                                   std::optional<uint64_t> version_number)
    : variable_(variable),
      label_(label),
      modifier_(modifier),
      start_time_(start_time),
      end_time_(end_time),
      version_number_(version_number),
      current_index_(0),
      query_start_(0),
      query_end_(Timestamp::Max()) {}

bool TemporalNodeScan::Init(ExecutionContext* ctx) {
  context_ = ctx;
  node_ids_.clear();
  
  uint64_t min_entity_id = 1;
  uint64_t max_entity_id = 1000;
  
  if (ctx->graph) {
    node_ids_ = ctx->graph->GetAllEntities(min_entity_id, max_entity_id, 1);
  } else {
    node_ids_.reserve(max_entity_id - min_entity_id + 1);
    for (uint64_t i = min_entity_id; i <= max_entity_id; ++i) {
      node_ids_.push_back(i);
    }
  }
  
  current_index_ = 0;
  
  // Resolve time range from modifier
  if (ctx->temporal_clause) {
    if (ctx->temporal_clause->start_time) {
      const auto& ts_expr = ctx->temporal_clause->start_time.value();
      if (ts_expr.type == TimestampExprType::kLiteral) {
        query_start_ = std::get<Timestamp>(ts_expr.value);
      }
    }
    if (ctx->temporal_clause->end_time) {
      const auto& ts_expr = ctx->temporal_clause->end_time.value();
      if (ts_expr.type == TimestampExprType::kLiteral) {
        query_end_ = std::get<Timestamp>(ts_expr.value);
      }
    }
  }
  
  return true;
}

std::shared_ptr<Record> TemporalNodeScan::Next() {
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
    
    // Apply temporal modifier constraints
    if (!MatchesTemporalConstraint(node)) {
      continue;
    }
    
    auto record = std::make_shared<Record>();
    record->Set(variable_, Value(node));
    return record;
  }
  
  return nullptr;
}

std::string TemporalNodeScan::GetDetails() const {
  return variable_;
}

bool TemporalNodeScan::MatchesTemporalConstraint(const Node& node) const {
  switch (modifier_) {
    case TemporalModifierType::AS_OF:
    case TemporalModifierType::AT_TIME:
      return MatchesAsOf(node);
    case TemporalModifierType::BETWEEN:
    case TemporalModifierType::FROM_TO:
      return MatchesBetween(node);
    case TemporalModifierType::CONTAINED_IN:
    case TemporalModifierType::DURING:
      return MatchesContainedIn(node);
    case TemporalModifierType::VERSION_K:
      return MatchesVersion(node);
    default:
      return true;
  }
}

bool TemporalNodeScan::MatchesAsOf(const Node& node) const {
  (void)node;
  // Without node-level temporal metadata, all nodes match
  return true;
}

bool TemporalNodeScan::MatchesBetween(const Node& node) const {
  (void)node;
  return true;
}

bool TemporalNodeScan::MatchesContainedIn(const Node& node) const {
  (void)node;
  return true;
}

bool TemporalNodeScan::MatchesVersion(const Node& node) const {
  (void)node;
  return true;
}

std::unique_ptr<PhysicalOperator> TemporalNodeScan::Clone() const {
  auto clone = std::make_unique<TemporalNodeScan>(
      variable_, label_, modifier_, start_time_, end_time_, version_number_);
  for (const auto& child : children_) {
    clone->AddChild(std::shared_ptr<PhysicalOperator>(child->Clone()));
  }
  clone->current_index_ = 0;
  clone->node_ids_.clear();
  clone->query_start_ = 0;
  clone->query_end_ = Timestamp::Max();
  return clone;
}

// ============================================================================
// TemporalExpand
// ============================================================================

TemporalExpand::TemporalExpand(std::string from_variable,
                               std::string rel_variable,
                               std::string to_variable,
                               Direction direction,
                               TemporalModifierType modifier,
                               std::optional<std::string> rel_type,
                               TemporalPathSemantics path_semantics)
    : from_variable_(from_variable),
      rel_variable_(rel_variable),
      to_variable_(to_variable),
      direction_(direction),
      modifier_(modifier),
      rel_type_(rel_type),
      path_semantics_(path_semantics),
      neighbor_index_(0),
      query_start_(0),
      query_end_(Timestamp::Max()) {}

bool TemporalExpand::Init(ExecutionContext* ctx) {
  context_ = ctx;
  if (ctx->temporal_clause) {
    auto range = ctx->temporal_clause->GetTimeRange(nullptr);
    if (range.has_value()) {
      query_start_ = range->first;
      query_end_ = range->second;
    }
  } else {
    query_start_ = ctx->time_range.first;
    query_end_ = ctx->time_range.second;
  }
  if (!children_.empty()) {
    if (!children_[0]->Init(ctx)) {
      return false;
    }
    current_record_ = children_[0]->Next();
    neighbor_index_ = 0;
  }
  return true;
}

std::shared_ptr<Record> TemporalExpand::Next() {
  while (current_record_) {
    if (!neighbors_.empty() && neighbor_index_ >= neighbors_.size()) {
      current_record_ = children_[0]->Next();
      neighbors_.clear();
      neighbor_index_ = 0;
      continue;
    }

    auto from_val = current_record_->Get(from_variable_);
    if (!from_val || !from_val->IsNode()) {
      current_record_ = children_[0]->Next();
      neighbor_index_ = 0;
      continue;
    }

    uint64_t from_id = from_val->GetNode().id;

    if (neighbors_.empty()) {
      uint32_t edge_type = 0;
      if (rel_type_) {
        try {
          edge_type = static_cast<uint32_t>(std::stoi(*rel_type_));
        } catch (...) {
          edge_type = 0;
        }
      }
      
      if (context_->gcn_traversal_callback) {
        auto neighbor_ids = context_->gcn_traversal_callback(
            from_id, edge_type, context_->query_timestamp.value());
        for (uint64_t nid : neighbor_ids) {
          uint64_t rel_hash = std::hash<std::string>{}(
              std::to_string(from_id) + ":" + std::to_string(nid) + ":" + std::to_string(Timestamp(0).value()));
          neighbors_.emplace_back(rel_hash, nid, from_id);
        }
      } else if (direction_ == Direction::INCOMING && context_->get_in_neighbors_fn) {
        auto neighbor_list = context_->get_in_neighbors_fn(
            from_id, static_cast<uint16_t>(edge_type), query_start_, query_end_);
        for (const auto& n : neighbor_list) {
          uint64_t rel_hash = std::hash<std::string>{}(
              std::to_string(from_id) + ":" + std::to_string(n.id) + ":" + std::to_string(n.timestamp.value()));
          neighbors_.emplace_back(rel_hash, n.id, from_id);
        }
      } else if (direction_ != Direction::INCOMING && context_->get_out_neighbors_fn) {
        auto neighbor_list = context_->get_out_neighbors_fn(
            from_id, static_cast<uint16_t>(edge_type), query_start_, query_end_);
        for (const auto& n : neighbor_list) {
          uint64_t rel_hash = std::hash<std::string>{}(
              std::to_string(from_id) + ":" + std::to_string(n.id) + ":" + std::to_string(n.timestamp.value()));
          neighbors_.emplace_back(rel_hash, n.id, from_id);
        }
      } else if (context_->graph) {
        if (direction_ == Direction::INCOMING) {
          auto neighbor_list = context_->graph->GetInNeighbors(
              from_id, static_cast<uint16_t>(edge_type), query_start_, query_end_);
          for (const auto& n : neighbor_list) {
            uint64_t rel_hash = std::hash<std::string>{}(
                std::to_string(from_id) + ":" + std::to_string(n.id) + ":" + std::to_string(n.timestamp.value()));
            neighbors_.emplace_back(rel_hash, n.id, from_id);
          }
        } else if (direction_ == Direction::BOTH) {
          auto out_list = context_->graph->GetOutNeighbors(
              from_id, static_cast<uint16_t>(edge_type), query_start_, query_end_);
          for (const auto& n : out_list) {
            uint64_t rel_hash = std::hash<std::string>{}(
                std::to_string(from_id) + ":" + std::to_string(n.id) + ":" + std::to_string(n.timestamp.value()));
            neighbors_.emplace_back(rel_hash, n.id, from_id);
          }
          auto in_list = context_->graph->GetInNeighbors(
              from_id, static_cast<uint16_t>(edge_type), query_start_, query_end_);
          for (const auto& n : in_list) {
            uint64_t rel_hash = std::hash<std::string>{}(
                std::to_string(from_id) + ":" + std::to_string(n.id) + ":" + std::to_string(n.timestamp.value()));
            neighbors_.emplace_back(rel_hash, n.id, from_id);
          }
        } else {
          auto neighbor_list = context_->graph->GetOutNeighbors(
              from_id, static_cast<uint16_t>(edge_type), query_start_, query_end_);
          for (const auto& n : neighbor_list) {
            uint64_t rel_hash = std::hash<std::string>{}(
                std::to_string(from_id) + ":" + std::to_string(n.id) + ":" + std::to_string(n.timestamp.value()));
            neighbors_.emplace_back(rel_hash, n.id, from_id);
          }
        }
      }
      
      neighbor_index_ = 0;
      if (neighbors_.empty()) {
        current_record_ = children_[0]->Next();
        continue;
      }
    }

    auto record = std::make_shared<Record>(*current_record_);
    uint64_t target_id = std::get<1>(neighbors_[neighbor_index_]);

    Relationship rel;
    rel.id = std::get<0>(neighbors_[neighbor_index_]);
    rel.start_id = from_id;
    rel.end_id = target_id;
    rel.type = rel_type_.value_or("CONNECTED_TO");
    rel.properties["timestamp"] = Value(context_->query_timestamp);
    record->Set(rel_variable_, Value(rel));

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

void TemporalExpand::Reset() {
  neighbor_index_ = 0;
  neighbors_.clear();
  current_record_.reset();
  if (!children_.empty()) {
    children_[0]->Reset();
  }
}

std::string TemporalExpand::GetDetails() const {
  return from_variable_;
}

bool TemporalExpand::MatchesTemporalConstraint(const Relationship& rel) const {
  return true;
}

bool TemporalExpand::IsPathContinuous(const std::vector<Relationship>& path) const {
  return true;
}

std::unique_ptr<PhysicalOperator> TemporalExpand::Clone() const {
  auto clone = std::make_unique<TemporalExpand>(
      from_variable_, rel_variable_, to_variable_, direction_, modifier_,
      rel_type_, path_semantics_);
  for (const auto& child : children_) {
    clone->AddChild(std::shared_ptr<PhysicalOperator>(child->Clone()));
  }
  clone->current_record_.reset();
  clone->neighbor_index_ = 0;
  clone->neighbors_.clear();
  clone->query_start_ = 0;
  clone->query_end_ = Timestamp::Max();
  return clone;
}

// ============================================================================
// SnapshotScan
// ============================================================================

SnapshotScan::SnapshotScan(std::string variable,
                           std::optional<std::string> label,
                           Timestamp snapshot_time)
    : variable_(variable),
      label_(label),
      snapshot_time_(snapshot_time),
      current_index_(0) {}

bool SnapshotScan::Init(ExecutionContext* ctx) {
  context_ = ctx;
  node_ids_.clear();
  uint64_t min_entity_id = 1;
  uint64_t max_entity_id = 1000;
  if (ctx->graph) {
    node_ids_ = ctx->graph->GetAllEntities(min_entity_id, max_entity_id, 1);
  } else {
    node_ids_.reserve(max_entity_id - min_entity_id + 1);
    for (uint64_t i = min_entity_id; i <= max_entity_id; ++i) {
      node_ids_.push_back(i);
    }
  }
  current_index_ = 0;
  return true;
}

std::shared_ptr<Record> SnapshotScan::Next() {
  while (current_index_ < node_ids_.size()) {
    uint64_t node_id = node_ids_[current_index_++];

    // Check entity existence at snapshot time
    if (context_ && context_->graph) {
      auto versions = context_->graph->GetTimeSeries(node_id, snapshot_time_, snapshot_time_);
      if (versions.empty()) {
        continue;
      }
    }

    Node node;
    node.id = node_id;
    if (label_) {
      node.labels.push_back(*label_);
    } else {
      node.labels.push_back("Node");
    }
    node.properties["id"] = Value(static_cast<int64_t>(node_id));
    node.properties["snapshot_time"] = Value(snapshot_time_);

    auto record = std::make_shared<Record>();
    record->Set(variable_, Value(node));
    return record;
  }

  return nullptr;
}

std::string SnapshotScan::GetDetails() const {
  return variable_;
}

std::unique_ptr<PhysicalOperator> SnapshotScan::Clone() const {
  auto clone = std::make_unique<SnapshotScan>(variable_, label_, snapshot_time_);
  for (const auto& child : children_) {
    clone->AddChild(std::shared_ptr<PhysicalOperator>(child->Clone()));
  }
  clone->current_index_ = 0;
  clone->node_ids_.clear();
  return clone;
}

// ============================================================================
// VersionScan
// ============================================================================

VersionScan::VersionScan(std::string variable,
                         std::optional<std::string> label,
                         std::optional<uint64_t> specific_version)
    : variable_(variable),
      label_(label),
      specific_version_(specific_version),
      current_node_index_(0),
      current_version_index_(0) {}

bool VersionScan::Init(ExecutionContext* ctx) {
  context_ = ctx;
  node_ids_.clear();
  
  uint64_t min_entity_id = 1;
  uint64_t max_entity_id = 1000;
  if (ctx->graph) {
    node_ids_ = ctx->graph->GetAllEntities(min_entity_id, max_entity_id, 1);
  } else {
    node_ids_.reserve(max_entity_id - min_entity_id + 1);
    for (uint64_t i = min_entity_id; i <= max_entity_id; ++i) {
      node_ids_.push_back(i);
    }
  }
  
  current_node_index_ = 0;
  current_version_index_ = 0;
  return true;
}

std::shared_ptr<Record> VersionScan::Next() {
  // Without node-level versioning API, return each node once
  // as a single "version"
  while (current_node_index_ < node_ids_.size()) {
    uint64_t node_id = node_ids_[current_node_index_++];
    
    Node node;
    node.id = node_id;
    if (label_) {
      node.labels.push_back(*label_);
    } else {
      node.labels.push_back("Node");
    }
    node.properties["id"] = Value(static_cast<int64_t>(node_id));
    if (specific_version_) {
      node.properties["version"] = Value(static_cast<int64_t>(*specific_version_));
    }
    
    auto record = std::make_shared<Record>();
    record->Set(variable_, Value(node));
    return record;
  }
  
  return nullptr;
}

std::string VersionScan::GetDetails() const {
  return variable_;
}

std::unique_ptr<PhysicalOperator> VersionScan::Clone() const {
  auto clone = std::make_unique<VersionScan>(variable_, label_, specific_version_);
  for (const auto& child : children_) {
    clone->AddChild(std::shared_ptr<PhysicalOperator>(child->Clone()));
  }
  clone->current_node_index_ = 0;
  clone->current_version_index_ = 0;
  clone->node_ids_.clear();
  clone->current_versions_.clear();
  return clone;
}

}  // namespace cypher
}  // namespace cedar
