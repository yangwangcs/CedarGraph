// Cross-partition edge write coordinator implementation
#include "cedar/storage/cross_partition_edge_writer.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/core/logging.h"

namespace cedar {

CrossPartitionEdgeWriter::CrossPartitionEdgeWriter(
    PartitionRouterFn router,
    StorageAccessorFn storage_accessor)
    : router_(std::move(router)),
      storage_accessor_(std::move(storage_accessor)) {}

Status CrossPartitionEdgeWriter::WriteEdge(
    uint64_t src_id, uint64_t dst_id,
    uint16_t edge_type,
    const Descriptor& edge_descriptor,
    Timestamp timestamp) {
  
  if (!router_ || !storage_accessor_) {
    return Status::InvalidArgument("CrossPartitionEdgeWriter",
                                    "router or storage_accessor not set");
  }

  uint16_t src_partition = router_(src_id);
  uint16_t dst_partition = router_(dst_id);

  auto* src_storage = storage_accessor_(src_partition);
  auto* dst_storage = storage_accessor_(dst_partition);

  if (!src_storage || !dst_storage) {
    return Status::NotFound("CrossPartitionEdgeWriter",
                             "storage not found for partition");
  }

  if (src_partition == dst_partition) {
    // Same partition: use atomic PutEdge
    return WriteEdgeSamePartition(src_storage, src_id, dst_id,
                                   edge_type, edge_descriptor, timestamp);
  } else {
    // Cross-partition: use 2PC
    return WriteEdgeCrossPartition(src_storage, dst_storage,
                                    src_id, dst_id, edge_type,
                                    edge_descriptor, timestamp);
  }
}

Status CrossPartitionEdgeWriter::WriteEdgeWithProperties(
    uint64_t src_id, uint64_t dst_id,
    uint16_t edge_type,
    const std::string& prop_name,
    int32_t prop_value,
    Timestamp timestamp) {
  
  // Build descriptor with property
  Descriptor desc = Descriptor::InlineInt(edge_type, prop_value);
  return WriteEdge(src_id, dst_id, edge_type, desc, timestamp);
}

Status CrossPartitionEdgeWriter::WriteEdgeSamePartition(
    CedarGraphStorage* storage,
    uint64_t src_id, uint64_t dst_id,
    uint16_t edge_type,
    const Descriptor& edge_descriptor,
    Timestamp timestamp) {
  
  // Same partition: use existing atomic PutEdge
  return storage->PutEdge(src_id, dst_id, edge_type,
                           timestamp, edge_descriptor, timestamp);
}

Status CrossPartitionEdgeWriter::WriteEdgeCrossPartition(
    CedarGraphStorage* src_storage,
    CedarGraphStorage* dst_storage,
    uint64_t src_id, uint64_t dst_id,
    uint16_t edge_type,
    const Descriptor& edge_descriptor,
    Timestamp timestamp) {
  
  // ===================================================================
  // 2PC Protocol for Cross-Partition Edge Write
  // ===================================================================
  //
  // Phase 1 (Prepare):
  //   - Write EdgeOut to src_storage (tentative)
  //   - Write EdgeIn to dst_storage (tentative)
  //
  // Phase 2 (Commit):
  //   - Commit both writes atomically
  //
  // For simplicity, we use a synchronous 2PC where:
  //   1. Write EdgeOut to src_storage
  //   2. Write EdgeIn to dst_storage
  //   3. If both succeed, the writes are committed
  //   4. If either fails, roll back the successful write
  //
  // In production, this should use the existing 2PC infrastructure
  // (TransactionCoordinator, Prepare/Commit/Abort RPCs).
  // ===================================================================

  // Step 1: Write EdgeOut to source partition
  Descriptor edge_desc = edge_descriptor;
  edge_desc.SetColumnId(edge_type);
  
  Status s = src_storage->PutEdge(src_id, dst_id, edge_type,
                                   timestamp, edge_desc, timestamp);
  if (!s.ok()) {
    CEDAR_LOG_WARN() << "CrossPartitionEdge: EdgeOut write failed: "
                     << s.ToString();
    return s;
  }

  // Step 2: Write EdgeIn to destination partition
  // 存储与 EdgeOut 相同格式的 Descriptor，确保一致性
  Descriptor edge_in_desc = edge_desc;
  
  s = dst_storage->PutEdge(src_id, dst_id, edge_type,
                            timestamp, edge_in_desc, timestamp);
  if (!s.ok()) {
    // Rollback: delete EdgeOut from source partition
    CEDAR_LOG_WARN() << "CrossPartitionEdge: EdgeIn write failed, rolling back EdgeOut";
    src_storage->Delete(src_id, timestamp.value(), timestamp);
    return s;
  }

  CEDAR_LOG_INFO() << "CrossPartitionEdge: wrote edge " << src_id
                   << " -> " << dst_id << " (type=" << edge_type
                   << ") across partitions";
  
  return Status::OK();
}

}  // namespace cedar
