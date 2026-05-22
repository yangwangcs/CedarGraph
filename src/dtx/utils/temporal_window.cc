// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "cedar/dtx/temporal_window.h"

#include <algorithm>

namespace cedar {
namespace dtx {

// =============================================================================
// WindowMergeOptimizer 实现
// =============================================================================

TemporalWindow WindowMergeOptimizer::MergeAdjacentWindows(
    const std::vector<TemporalWindow>& windows,
    uint64_t gap_threshold_us) {
  
  if (windows.empty()) {
    return TemporalWindow();
  }
  
  if (windows.size() == 1) {
    return windows[0];
  }
  
  // 按start时间排序
  std::vector<TemporalWindow> sorted = windows;
  std::sort(sorted.begin(), sorted.end(), 
            [](const TemporalWindow& a, const TemporalWindow& b) {
              return a.start < b.start;
            });
  
  // 合并相邻窗口
  TemporalWindow result = sorted[0];
  for (size_t i = 1; i < sorted.size(); ++i) {
    const auto& current = sorted[i];
    
    // 检查是否相邻（gap小于阈值）
    if (result.end.value() == 0 || current.start.value() == 0) {
      // 有一个是无上限的，直接合并
      result.Merge(current);
    } else if (current.start.value() <= result.end.value() + gap_threshold_us) {
      // 相邻或重叠，合并
      result.Merge(current);
    }
    // 不相邻，保持分开（只返回第一个合并后的结果）
    // 注意：这里简化处理，实际可能需要返回多个窗口
  }
  
  return result;
}

std::vector<TemporalWindow> WindowMergeOptimizer::SplitLargeWindow(
    const TemporalWindow& window,
    uint64_t max_span_us) {
  
  std::vector<TemporalWindow> result;
  
  if (window.IsEmpty()) {
    return result;
  }
  
  // 检查是否需要分裂
  uint64_t span = window.Span();
  if (span == 0 || span <= max_span_us) {
    result.push_back(window);
    return result;
  }
  
  // 分裂为大小的窗口
  uint64_t current_start = window.start.value();
  uint64_t window_end = window.end.value();
  
  while (current_start < window_end) {
    uint64_t current_end = std::min(current_start + max_span_us, window_end);
    result.emplace_back(Timestamp(current_start), Timestamp(current_end));
    current_start = current_end;
  }
  
  return result;
}

std::vector<TemporalWindow> WindowMergeOptimizer::OptimizeWindows(
    const std::vector<TemporalWindow>& windows,
    uint64_t merge_threshold_us,
    uint64_t split_threshold_us) {
  
  if (windows.empty()) {
    return {};
  }
  
  // 第一步：合并相邻窗口
  TemporalWindow merged = MergeAdjacentWindows(windows, merge_threshold_us);
  
  // 第二步：如果合并后的窗口太大，分裂它
  auto split = SplitLargeWindow(merged, split_threshold_us);
  
  return split;
}

}  // namespace dtx
}  // namespace cedar
