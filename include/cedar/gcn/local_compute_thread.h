#pragma once

#include <cstdint>
#include <vector>

namespace cedar {
namespace gcn {

class TMVEngine;

class LocalComputeUtils {
 public:
  LocalComputeUtils() = default;
  ~LocalComputeUtils() = default;

  // Non-copyable, non-movable
  LocalComputeUtils(const LocalComputeUtils&) = delete;
  LocalComputeUtils& operator=(const LocalComputeUtils&) = delete;

  // Breadth-first search up to max_hops from root at query_time.
  // Returns the set of reachable vertex IDs (excluding root).
  std::vector<uint64_t> ExecuteBFS(uint64_t root,
                                   uint64_t query_time,
                                   uint32_t max_hops,
                                   TMVEngine* engine);

  // Depth-first search up to max_hops from root at query_time.
  // Returns the set of reachable vertex IDs (excluding root).
  std::vector<uint64_t> ExecuteDFS(uint64_t root,
                                   uint64_t query_time,
                                   uint32_t max_hops,
                                   TMVEngine* engine);
};

}  // namespace gcn
}  // namespace cedar
