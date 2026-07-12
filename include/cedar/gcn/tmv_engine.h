#pragma once

#include <cstdint>
#include <mutex>
#include <set>
#include <vector>

#include "cedar/gcn/numa_arena.h"
#include "cedar/gcn/tmv_chunk.h"
#include "cedar/gcn/tmv_edge.h"
#include "cedar/gcn/tmv_index.h"
#include "cedar/gcn/tmv_vertex_entry.h"
#include "cedar/core/status.h"

namespace cedar {
namespace gcn {

enum class Direction : uint8_t { kOut = 0, kIn = 1 };

/// Temporal Multi-Version Engine (TMVEngine) — core graph storage engine
/// for vertex/edge data with MVCC and directional traversal support.
class TMVEngine {
 public:
  struct EdgeAppend {
    uint64_t entity_id = 0;
    Direction dir = Direction::kOut;
    TMVEdge edge;
    bool reverse = false;
  };

  explicit TMVEngine(size_t max_chunks);

  // Non-copyable, non-movable
  TMVEngine(const TMVEngine&) = delete;
  TMVEngine& operator=(const TMVEngine&) = delete;

  cedar::Status BootstrapVertex(uint64_t entity_id,
                                Direction dir,
                                const std::vector<TMVEdge>& edges,
                                bool reverse);

  cedar::Status AppendEdge(uint64_t entity_id,
                           Direction dir,
                           const TMVEdge& edge,
                           bool reverse);

  // Append all requested edges, including requested reverse edges, as one
  // best-effort atomic mutation. If any append fails, already-appended edges
  // from this call are rolled back before returning the failure status.
  cedar::Status AppendEdgesAtomic(const std::vector<EdgeAppend>& appends);
  cedar::Status ReplaceVerticesAtomic(
      const std::set<uint64_t>& entity_ids,
      const std::vector<EdgeAppend>& appends);
  cedar::Status ReplacePartitionEdgesAtomic(
      uint32_t partition_id,
      const std::vector<EdgeAppend>& appends);

  std::vector<TMVEdge> ScanAtTime(uint64_t entity_id,
                                  Direction dir,
                                  uint64_t query_time);

  size_t DropBelowWatermark(uint64_t watermark);

  // Remove a vertex and all its edge chunks from the engine.
  // Returns number of chunks freed.
  size_t InvalidateVertex(uint64_t entity_id);

  size_t VertexCount() const;
  size_t ChunkCount() const;

 private:
  ArenaPool pool_;
  TMVIndex index_;
  mutable std::mutex append_mutex_;

  struct AppendMutation {
    TMVVertexEntry* entry = nullptr;
    Direction dir = Direction::kOut;
    TMVChunk* chunk = nullptr;
    uint32_t appended_index = 0;
    TMVChunk* new_chunk = nullptr;
    TMVChunk* old_tail = nullptr;
  };
  struct VertexBackup {
    uint64_t entity_id = 0;
    std::vector<TMVEdge> out_edges;
    std::vector<TMVEdge> in_edges;
  };

  bool AppendToEntry(TMVVertexEntry* entry,
                     Direction dir,
                     const TMVEdge& edge,
                     AppendMutation* mutation = nullptr);

  TMVVertexEntry* FindOrCreateEntry(uint64_t entity_id, bool* created);
  void RemoveEntryIfEmpty(uint64_t entity_id);
  void RollbackAppend(const AppendMutation& mutation);
  void RecomputeChunkMetadata(TMVChunk* chunk, uint32_t count);
  void RecomputeEarliestTimestamp(TMVVertexEntry* entry);
  VertexBackup BackupVertex(uint64_t entity_id) const;
  bool RestoreVertex(const VertexBackup& backup);

  size_t DropChunksBelowWatermark(std::atomic<TMVChunk*>* head_ptr,
                                  std::atomic<TMVChunk*>* tail_ptr,
                                  uint64_t watermark);
};

}  // namespace gcn
}  // namespace cedar
