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
  // Placeholder: store a minimal descriptor under column 0
  cedar::Descriptor desc = cedar::Descriptor::InlineInt(0, 0);
  (void)vertex;
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
  (void)predicates;
  if (!storage_) return Status::IOError("Storage not initialized");
  auto scan_results = storage_->Scan(vertex_id, EntityType::Vertex, 0, start_time, end_time);
  results->insert(results->end(), scan_results.begin(), scan_results.end());
  return Status::OK();
}

Status StorageInterface::InsertEdge(const Edge& edge, cedar::Timestamp txn_version) {
  if (!storage_) return Status::IOError("Storage not initialized");
  cedar::Descriptor desc = cedar::Descriptor::InlineInt(0, 0);
  uint16_t edge_type = 0;  // TODO: map edge.type to type id
  return storage_->PutEdge(edge.src_id, edge.dst_id, edge_type, txn_version, desc, txn_version);
}

Status StorageInterface::GetEdge(uint64_t src_id, uint64_t dst_id,
                                 const std::string& type,
                                 cedar::Timestamp as_of_time,
                                 Descriptor* descriptor, bool* found) {
  (void)type;
  if (!storage_) return Status::IOError("Storage not initialized");
  uint16_t edge_type = 0;  // TODO: map type to id
  auto result = storage_->GetEdge(src_id, dst_id, edge_type, as_of_time);
  *found = result.has_value();
  if (*found) *descriptor = result.value();
  return Status::OK();
}

Status StorageInterface::ScanOutEdges(uint64_t node_id, uint16_t edge_type,
                                      cedar::Timestamp start_time, cedar::Timestamp end_time,
                                      const std::vector<PropertyPredicateItem>& predicates,
                                      std::vector<std::pair<cedar::Timestamp, cedar::Descriptor>>* results) {
  (void)predicates;
  if (!storage_) return Status::IOError("Storage not initialized");
  auto scan_results = storage_->Scan(node_id, EntityType::EdgeOut, edge_type, start_time, end_time);
  results->insert(results->end(), scan_results.begin(), scan_results.end());
  return Status::OK();
}

Status StorageInterface::ScanInEdges(uint64_t node_id, uint16_t edge_type,
                                     cedar::Timestamp start_time, cedar::Timestamp end_time,
                                     const std::vector<PropertyPredicateItem>& predicates,
                                     std::vector<std::pair<cedar::Timestamp, Descriptor>>* results) {
  (void)predicates;
  if (!storage_) return Status::IOError("Storage not initialized");
  auto scan_results = storage_->Scan(node_id, EntityType::EdgeIn, edge_type, start_time, end_time);
  results->insert(results->end(), scan_results.begin(), scan_results.end());
  return Status::OK();
}

std::string StorageInterface::SerializeProperties(
    const std::map<std::string, cypher::Value>& props) {
  (void)props;
  return "";
}

bool StorageInterface::EvaluatePredicate(const PropertyPredicateItem& pred,
                                         const std::map<std::string, cypher::Value>& props) {
  (void)pred;
  (void)props;
  // Stub: predicate evaluation requires cypher::Value operator linkage
  return false;
}

}  // namespace storage
}  // namespace cedar
