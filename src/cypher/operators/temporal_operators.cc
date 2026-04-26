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
  return true;
}

std::shared_ptr<Record> TemporalNodeScan::Next() {
  return nullptr;
}

std::string TemporalNodeScan::GetDetails() const {
  return variable_;
}

bool TemporalNodeScan::MatchesTemporalConstraint(const Node& node) const {
  return true;
}

bool TemporalNodeScan::MatchesAsOf(const Node& node) const {
  return true;
}

bool TemporalNodeScan::MatchesBetween(const Node& node) const {
  return true;
}

bool TemporalNodeScan::MatchesContainedIn(const Node& node) const {
  return true;
}

bool TemporalNodeScan::MatchesVersion(const Node& node) const {
  return true;
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
  if (!context_->gcn_traversal_callback) {
    return nullptr;
  }

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
        edge_type = static_cast<uint32_t>(std::stoi(*rel_type_));
      }
      auto neighbor_ids = context_->gcn_traversal_callback(
          from_id, edge_type, context_->query_timestamp.value());
      for (uint64_t nid : neighbor_ids) {
        neighbors_.emplace_back(nid ^ Timestamp(0).value(), nid, from_id);
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
  if (!context_->tmv_engine) {
    return nullptr;
  }

  while (current_index_ < node_ids_.size()) {
    uint64_t node_id = node_ids_[current_index_++];
    auto edges = context_->tmv_engine->ScanAtTime(
        node_id, cedar::gcn::Direction::kOut, snapshot_time_.value());
    if (edges.empty()) {
      continue;
    }

    Node node;
    node.id = node_id;
    if (label_) {
      node.labels.push_back(*label_);
    } else {
      node.labels.push_back("Node");
    }
    node.properties["id"] = Value(static_cast<int64_t>(node_id));

    auto record = std::make_shared<Record>();
    record->Set(variable_, Value(node));
    return record;
  }

  return nullptr;
}

std::string SnapshotScan::GetDetails() const {
  return variable_;
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
  return true;
}

std::shared_ptr<Record> VersionScan::Next() {
  return nullptr;
}

std::string VersionScan::GetDetails() const {
  return variable_;
}

}  // namespace cypher
}  // namespace cedar
