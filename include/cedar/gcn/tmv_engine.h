#pragma once

#include <cstdint>
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

class TMVEngine {
 public:
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

  bool AppendToEntry(TMVVertexEntry* entry,
                     Direction dir,
                     const TMVEdge& edge,
                     TMVChunk** out_new_chunk = nullptr,
                     TMVChunk** out_old_tail = nullptr);

  size_t DropChunksBelowWatermark(std::atomic<TMVChunk*>* head_ptr,
                                  std::atomic<TMVChunk*>* tail_ptr,
                                  uint64_t watermark);
};

}  // namespace gcn
}  // namespace cedar
