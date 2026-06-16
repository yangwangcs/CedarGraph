// Cross-partition edge write coordinator using 2PC
// When src and dst are on different partitions, atomically writes:
//   - EdgeOut to src's partition
//   - EdgeIn to dst's partition

#ifndef CEDAR_CROSS_PARTITION_EDGE_WRITER_H_
#define CEDAR_CROSS_PARTITION_EDGE_WRITER_H_

#include <cstdint>
#include <string>
#include <functional>
#include "cedar/core/status.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

namespace cedar {

// Forward declarations
class CedarGraphStorage;

// Partition router function: entity_id -> partition_id
using PartitionRouterFn = std::function<uint16_t(uint64_t entity_id)>;

// Storage accessor function: partition_id -> CedarGraphStorage*
using StorageAccessorFn = std::function<CedarGraphStorage*(uint16_t partition_id)>;

// Cross-partition edge write coordinator
class CrossPartitionEdgeWriter {
 public:
  CrossPartitionEdgeWriter(PartitionRouterFn router,
                            StorageAccessorFn storage_accessor);
  ~CrossPartitionEdgeWriter() = default;

  // Write an edge, handling cross-partition routing automatically
  // If src and dst are on the same partition, uses atomic PutEdge
  // If on different partitions, uses 2PC protocol
  Status WriteEdge(uint64_t src_id, uint64_t dst_id,
                   uint16_t edge_type,
                   const Descriptor& edge_descriptor,
                   Timestamp timestamp);

  // Write edge with explicit properties (convenience method)
  Status WriteEdgeWithProperties(uint64_t src_id, uint64_t dst_id,
                                  uint16_t edge_type,
                                  const std::string& prop_name,
                                  int32_t prop_value,
                                  Timestamp timestamp);

 private:
  // Same-partition atomic write
  Status WriteEdgeSamePartition(CedarGraphStorage* storage,
                                 uint64_t src_id, uint64_t dst_id,
                                 uint16_t edge_type,
                                 const Descriptor& edge_descriptor,
                                 Timestamp timestamp);

  // Cross-partition 2PC write
  Status WriteEdgeCrossPartition(CedarGraphStorage* src_storage,
                                  CedarGraphStorage* dst_storage,
                                  uint64_t src_id, uint64_t dst_id,
                                  uint16_t edge_type,
                                  const Descriptor& edge_descriptor,
                                  Timestamp timestamp);

  PartitionRouterFn router_;
  StorageAccessorFn storage_accessor_;
};

}  // namespace cedar

#endif  // CEDAR_CROSS_PARTITION_EDGE_WRITER_H_
