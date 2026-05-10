#include "cedar/gcn/local_compute_thread.h"

#include <queue>
#include <stack>
#include <unordered_set>

#include "cedar/gcn/tmv_engine.h"

namespace cedar {
namespace gcn {

std::vector<uint64_t> LocalComputeThread::ExecuteBFS(uint64_t root,
                                                     uint64_t query_time,
                                                     uint32_t max_hops,
                                                     TMVEngine* engine) {
  if (max_hops == 0 || engine == nullptr) {
    return {};
  }

  std::vector<uint64_t> results;
  std::unordered_set<uint64_t> visited;
  std::queue<std::pair<uint64_t, uint32_t>> q;

  visited.insert(root);
  q.emplace(root, 0);

  while (!q.empty()) {
    auto [vertex, depth] = q.front();
    q.pop();

    if (depth >= max_hops) {
      continue;
    }

    // SIMD optimization opportunity: batch-check valid_from <= query_time < valid_to
    // for 2 edges per iteration using AVX2 (future performance work).
    auto edges = engine->ScanAtTime(vertex, Direction::kOut, query_time);

    for (const auto& edge : edges) {
      uint64_t neighbor = edge.target_id;
      if (visited.find(neighbor) == visited.end()) {
        visited.insert(neighbor);
        results.push_back(neighbor);
        q.emplace(neighbor, depth + 1);
      }
    }
  }

  return results;
}

std::vector<uint64_t> LocalComputeThread::ExecuteDFS(uint64_t root,
                                                     uint64_t query_time,
                                                     uint32_t max_hops,
                                                     TMVEngine* engine) {
  if (max_hops == 0 || engine == nullptr) {
    return {};
  }

  std::vector<uint64_t> results;
  std::unordered_set<uint64_t> visited;
  std::stack<std::pair<uint64_t, uint32_t>> s;

  visited.insert(root);
  s.emplace(root, 0);

  while (!s.empty()) {
    auto [vertex, depth] = s.top();
    s.pop();

    if (depth >= max_hops) {
      continue;
    }

    // SIMD optimization opportunity: batch-check valid_from <= query_time < valid_to
    // for 2 edges per iteration using AVX2 (future performance work).
    auto edges = engine->ScanAtTime(vertex, Direction::kOut, query_time);

    for (const auto& edge : edges) {
      uint64_t neighbor = edge.target_id;
      if (visited.find(neighbor) == visited.end()) {
        visited.insert(neighbor);
        results.push_back(neighbor);
        s.emplace(neighbor, depth + 1);
      }
    }
  }

  return results;
}

}  // namespace gcn
}  // namespace cedar
