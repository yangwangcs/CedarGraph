// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Storage Interface - Translates graph semantics to KV operations

#ifndef CEDAR_STORAGE_STORAGE_INTERFACE_H_
#define CEDAR_STORAGE_STORAGE_INTERFACE_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <map>

#include "cedar/core/status.h"
#include "cedar/types/cedar_types.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include "cedar/cypher/value.h"

namespace cedar {

class CedarGraphStorage;

namespace storage {

struct PropertyPredicate {
  std::string property_name;
  enum Op { EQ, NE, LT, LE, GT, GE, IN } op;
  cypher::Value value;
};

struct Vertex {
  uint64_t id = 0;
  std::vector<std::string> labels;
  std::map<std::string, cypher::Value> properties;
};

struct Edge {
  uint64_t src_id = 0;
  uint64_t dst_id = 0;
  uint64_t edge_id = 0;
  std::string type;
  std::map<std::string, cypher::Value> properties;
  int64_t rank = 0;
};

struct NeighborResult {
  uint64_t neighbor_id = 0;
  Edge edge;
};

class StorageInterface {
 public:
  explicit StorageInterface(CedarGraphStorage* storage);
  ~StorageInterface();

  Status InsertVertex(const Vertex& vertex, Timestamp txn_version);
  Status GetVertex(uint64_t vertex_id, Timestamp as_of_time,
                   Descriptor* descriptor, bool* found);
  Status ScanVertices(uint64_t vertex_id, Timestamp start_time, Timestamp end_time,
                      const std::vector<PropertyPredicate>& predicates,
                      std::vector<std::pair<Timestamp, Descriptor>>* results);

  Status InsertEdge(const Edge& edge, Timestamp txn_version);
  Status GetEdge(uint64_t src_id, uint64_t dst_id, const std::string& type,
                 Timestamp as_of_time, Descriptor* descriptor, bool* found);
  Status ScanOutEdges(uint64_t node_id, uint16_t edge_type,
                      Timestamp start_time, Timestamp end_time,
                      const std::vector<PropertyPredicate>& predicates,
                      std::vector<std::pair<Timestamp, Descriptor>>* results);
  Status ScanInEdges(uint64_t node_id, uint16_t edge_type,
                     Timestamp start_time, Timestamp end_time,
                     const std::vector<PropertyPredicate>& predicates,
                     std::vector<std::pair<Timestamp, Descriptor>>* results);

 private:
  CedarGraphStorage* storage_;
  std::string SerializeProperties(const std::map<std::string, cypher::Value>& props);
  bool EvaluatePredicate(const PropertyPredicate& pred,
                         const std::map<std::string, cypher::Value>& props);
};

}  // namespace storage
}  // namespace cedar

#endif  // CEDAR_STORAGE_STORAGE_INTERFACE_H_
