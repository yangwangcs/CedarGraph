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

#include "cedar/gcn/backpressure_controller.h"

#include <mutex>

namespace cedar {
namespace gcn {

bool BackpressureController::AcquireSlot(const std::string& gcn_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto& slot = slots_[gcn_id];
  uint32_t current = slot.in_use.load(std::memory_order_relaxed);
  uint32_t max = slot.max_concurrency.load(std::memory_order_relaxed);
  if (current >= max) {
    return false;
  }
  slot.in_use.store(current + 1, std::memory_order_relaxed);
  return true;
}

void BackpressureController::ReleaseSlot(const std::string& gcn_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = slots_.find(gcn_id);
  if (it == slots_.end()) return;
  uint32_t current = it->second.in_use.load(std::memory_order_relaxed);
  if (current > 0) {
    it->second.in_use.store(current - 1, std::memory_order_relaxed);
  }
}

void BackpressureController::UpdateHealth(const std::string& gcn_id,
                                          double health_score) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto& slot = slots_[gcn_id];
  constexpr double kCriticalHealthThreshold = 30.0;
  constexpr double kDegradedHealthThreshold = 60.0;
  constexpr uint32_t kCriticalConcurrency = 4;
  constexpr uint32_t kDegradedConcurrency = 8;
  constexpr uint32_t kHealthyConcurrency = 16;
  if (health_score < kCriticalHealthThreshold) {
    slot.max_concurrency.store(kCriticalConcurrency, std::memory_order_relaxed);
  } else if (health_score < kDegradedHealthThreshold) {
    slot.max_concurrency.store(kDegradedConcurrency, std::memory_order_relaxed);
  } else {
    slot.max_concurrency.store(kHealthyConcurrency, std::memory_order_relaxed);
  }
}

uint32_t BackpressureController::InFlight(const std::string& gcn_id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = slots_.find(gcn_id);
  if (it == slots_.end()) return 0;
  return it->second.in_use.load(std::memory_order_relaxed);
}

}  // namespace gcn
}  // namespace cedar
