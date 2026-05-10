// Copyright (c) 2025 The Cedar Authors. All rights reserved.

#include "cedar/storage/storage_interface.h"
#include "cedar/storage/cedar_graph_storage.h"

namespace cedar {
namespace storage {

StorageInterface::StorageInterface(CedarGraphStorage* storage)
    : storage_(storage) {}

StorageInterface::~StorageInterface() = default;

Status StorageInterface::InsertVertex(const Vertex& vertex, cedar::Timestamp txn_version) {
  if (!storage_) return Status::IOError("Storage not initialized");
  cedar::Descriptor desc;
  if (!vertex.properties.empty()) {
    const auto& it = vertex.properties.begin();
    if (it->second.Type() == cypher::ValueType::kInt) {
      desc = cedar::Descriptor::InlineInt(1, static_cast<int32_t>(it->second.GetInt()));
    } else if (it->second.Type() == cypher::ValueType::kString) {
      auto opt = cedar::Descriptor::InlineShortStr(1, cedar::Slice(it->second.GetString()));
      if (opt.has_value()) desc = opt.value();
    }
  }
  return storage_->Put(vertex.id, txn_version.value(), desc, txn_version);
}

Status StorageInterface::GetVertex(uint64_t vertex_id, cedar::Timestamp as_of_time,
                                   cedar::Descriptor* descriptor, bool* found) {
  if (!storage_) return Status::IOError("Storage not initialized");
  auto result = storage_->Get(vertex_id, EntityType::Vertex, 0, as_of_time);
  *found = result.has_value();
  if (*found) *descriptor = result.value();
  return Status::OK();
}

Status StorageInterface::ScanVertices(uint64_t vertex_id,
                                      cedar::Timestamp start_time, cedar::Timestamp end_time,
                                      const std::vector<PropertyPredicateItem>& predicates,
                                      std::vector<std::pair<cedar::Timestamp, cedar::Descriptor>>* results) {
  if (!storage_) return Status::IOError("Storage not initialized");
  auto scan_results = storage_->Scan(vertex_id, EntityType::Vertex, 1, start_time, end_time);
  for (const auto& [ts, desc] : scan_results) {
    std::map<std::string, cypher::Value> props;
    if (desc.GetColumnId() != 0) {
      auto int_val = desc.AsInlineInt();
      if (int_val.has_value()) {
        props["value"] = cypher::Value(static_cast<int64_t>(int_val.value()));
      }
      auto str_val = desc.AsInlineShortStr();
      if (!str_val.empty()) {
        props["value"] = cypher::Value(str_val);
      }
    }
    bool passes = true;
    for (const auto& pred : predicates) {
      if (!EvaluatePredicate(pred, props)) {
        passes = false;
        break;
      }
    }
    if (passes) results->push_back({ts, desc});
  }
  return Status::OK();
}

Status StorageInterface::InsertEdge(const Edge& edge, cedar::Timestamp txn_version) {
  if (!storage_) return Status::IOError("Storage not initialized");
  cedar::Descriptor desc;
  if (!edge.properties.empty()) {
    const auto& it = edge.properties.begin();
    if (it->second.Type() == cypher::ValueType::kInt) {
      desc = cedar::Descriptor::InlineInt(1, static_cast<int32_t>(it->second.GetInt()));
    } else if (it->second.Type() == cypher::ValueType::kString) {
      auto opt = cedar::Descriptor::InlineShortStr(1, cedar::Slice(it->second.GetString()));
      if (opt.has_value()) desc = opt.value();
    }
  }
  uint16_t edge_type = static_cast<uint16_t>(std::hash<std::string>{}(edge.type) & 0xFFFF);
  Status s = storage_->PutEdge(edge.src_id, edge.dst_id, edge_type, txn_version, desc, txn_version);
  return s;
}

Status StorageInterface::GetEdge(uint64_t src_id, uint64_t dst_id,
                                 const std::string& type,
                                 cedar::Timestamp as_of_time,
                                 Descriptor* descriptor, bool* found) {
  (void)type;
  if (!storage_) return Status::IOError("Storage not initialized");
  uint16_t edge_type = static_cast<uint16_t>(std::hash<std::string>{}(type) & 0xFFFF);
  auto result = storage_->GetEdge(src_id, dst_id, edge_type, as_of_time);
  *found = result.has_value();
  if (*found) *descriptor = result.value();
  return Status::OK();
}

Status StorageInterface::ScanOutEdges(uint64_t node_id, uint16_t edge_type,
                                      cedar::Timestamp start_time, cedar::Timestamp end_time,
                                      const std::vector<PropertyPredicateItem>& predicates,
                                      std::vector<std::pair<cedar::Timestamp, cedar::Descriptor>>* results) {
  if (!storage_) return Status::IOError("Storage not initialized");
  auto scan_results = storage_->Scan(node_id, EntityType::EdgeOut, edge_type, start_time, end_time);
  for (const auto& [ts, desc] : scan_results) {
    std::map<std::string, cypher::Value> props;
    if (desc.GetColumnId() != 0) {
      auto int_val = desc.AsInlineInt();
      if (int_val.has_value()) {
        props["value"] = cypher::Value(static_cast<int64_t>(int_val.value()));
      }
      auto str_val = desc.AsInlineShortStr();
      if (!str_val.empty()) {
        props["value"] = cypher::Value(str_val);
      }
    }
    bool passes = true;
    for (const auto& pred : predicates) {
      if (!EvaluatePredicate(pred, props)) {
        passes = false;
        break;
      }
    }
    if (passes) results->push_back({ts, desc});
  }
  return Status::OK();
}

Status StorageInterface::ScanInEdges(uint64_t node_id, uint16_t edge_type,
                                     cedar::Timestamp start_time, cedar::Timestamp end_time,
                                     const std::vector<PropertyPredicateItem>& predicates,
                                     std::vector<std::pair<cedar::Timestamp, Descriptor>>* results) {
  if (!storage_) return Status::IOError("Storage not initialized");
  auto scan_results = storage_->Scan(node_id, EntityType::EdgeIn, edge_type, start_time, end_time);
  for (const auto& [ts, desc] : scan_results) {
    std::map<std::string, cypher::Value> props;
    if (desc.GetColumnId() != 0) {
      auto int_val = desc.AsInlineInt();
      if (int_val.has_value()) {
        props["value"] = cypher::Value(static_cast<int64_t>(int_val.value()));
      }
      auto str_val = desc.AsInlineShortStr();
      if (!str_val.empty()) {
        props["value"] = cypher::Value(str_val);
      }
    }
    bool passes = true;
    for (const auto& pred : predicates) {
      if (!EvaluatePredicate(pred, props)) {
        passes = false;
        break;
      }
    }
    if (passes) results->push_back({ts, desc});
  }
  return Status::OK();
}

std::string StorageInterface::SerializeProperties(
    const std::map<std::string, cypher::Value>& props) {
  (void)props;
  return "";
}

bool StorageInterface::EvaluatePredicate(const PropertyPredicateItem& pred,
                                         const std::map<std::string, cypher::Value>& props) {
  auto it = props.find(pred.property_name);
  if (it == props.end()) return false;

  const auto& actual = it->second;
  const auto& expected = pred.value;

  if (actual.Type() != expected.Type()) {
    bool both_numeric = (actual.Type() == cypher::ValueType::kInt && expected.Type() == cypher::ValueType::kFloat) ||
                        (actual.Type() == cypher::ValueType::kFloat && expected.Type() == cypher::ValueType::kInt);
    if (!both_numeric) return false;
  }

  auto get_double = [](const cypher::Value& v) -> std::optional<double> {
    if (v.Type() == cypher::ValueType::kInt) {
      return static_cast<double>(v.GetInt());
    } else if (v.Type() == cypher::ValueType::kFloat) {
      return v.GetFloat();
    }
    return std::nullopt;
  };

  switch (pred.op) {
    case PropertyPredicateItem::EQ: {
      if (actual.Type() == cypher::ValueType::kBool) {
        return actual.GetBool() == expected.GetBool();
      }
      if (actual.Type() == cypher::ValueType::kString) {
        return actual.GetString() == expected.GetString();
      }
      auto ad = get_double(actual);
      auto ed = get_double(expected);
      return ad.has_value() && ed.has_value() && ad.value() == ed.value();
    }
    case PropertyPredicateItem::NE: {
      bool eq = false;
      if (actual.Type() == cypher::ValueType::kBool) {
        eq = actual.GetBool() == expected.GetBool();
      } else if (actual.Type() == cypher::ValueType::kString) {
        eq = actual.GetString() == expected.GetString();
      } else {
        auto ad = get_double(actual);
        auto ed = get_double(expected);
        eq = ad.has_value() && ed.has_value() && ad.value() == ed.value();
      }
      return !eq;
    }
    case PropertyPredicateItem::LT: {
      auto ad = get_double(actual);
      auto ed = get_double(expected);
      return ad.has_value() && ed.has_value() && ad.value() < ed.value();
    }
    case PropertyPredicateItem::LE: {
      auto ad = get_double(actual);
      auto ed = get_double(expected);
      return ad.has_value() && ed.has_value() && ad.value() <= ed.value();
    }
    case PropertyPredicateItem::GT: {
      auto ad = get_double(actual);
      auto ed = get_double(expected);
      return ad.has_value() && ed.has_value() && ad.value() > ed.value();
    }
    case PropertyPredicateItem::GE: {
      auto ad = get_double(actual);
      auto ed = get_double(expected);
      return ad.has_value() && ed.has_value() && ad.value() >= ed.value();
    }
    case PropertyPredicateItem::IN:
      return false;
  }
  return false;
}

}  // namespace storage
}  // namespace cedar
