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

#include "cedar/dtx/hybrid_logical_clock.h"

#include <cstring>
#include <iostream>
#include <thread>

namespace cedar {
namespace dtx {

HybridLogicalClock::HybridLogicalClock()
    : last_physical_(0),
      last_logical_(0) {}

uint64_t HybridLogicalClock::GetPhysicalTimeMicros() {
  auto now = std::chrono::system_clock::now();
  auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
      now.time_since_epoch()).count();
  return static_cast<uint64_t>(micros);
}

HlcTimestamp HybridLogicalClock::Now() {
  std::lock_guard<std::mutex> lock(mutex_);
  
  uint64_t physical = GetPhysicalTimeMicros();
  
  // Check for significant clock drift BEFORE updating last_physical_
  if (last_physical_ > 0) {  // skip on first call
    uint64_t drift = (physical > last_physical_) ? (physical - last_physical_) : 0;
    if (drift > kMaxClockDriftMicros) {
      std::cerr << "[HLC] Significant clock drift detected: " << drift << "us" << std::endl;
    }
  }
  
  if (physical > last_physical_) {
    // Physical clock has advanced, reset logical counter
    last_physical_ = physical;
    last_logical_ = 0;
  } else if (physical == last_physical_) {
    // Same physical time, increment logical counter
    if (last_logical_ >= kMaxLogicalCounter) {
      // Logical counter overflow protection
      // Wait for physical clock to advance by 1 microsecond
      // This is extremely rare in practice
      std::this_thread::sleep_for(std::chrono::microseconds(1));
      physical = GetPhysicalTimeMicros();
      last_physical_ = physical;
      last_logical_ = 0;
    } else {
      last_logical_++;
    }
  } else {
    // Physical clock went backwards (clock skew)
    if (last_logical_ >= kMaxLogicalCounter) {
      // If we've used too many logical values at this physical time,
      // we wait for the physical clock to catch up, but with a bounded retry.
      const int kMaxWaitIterations = 100;  // ~100us max wait
      int wait_iter = 0;
      while (physical <= last_physical_ && wait_iter < kMaxWaitIterations) {
        std::this_thread::sleep_for(std::chrono::microseconds(1));
        physical = GetPhysicalTimeMicros();
        ++wait_iter;
      }
      if (physical <= last_physical_) {
        // Clock still hasn't caught up after bounded wait.
        // Force advance by bumping last_physical_ forward by 1us to maintain progress.
        // This sacrifices strict physical-time monotonicity but guarantees liveness.
        last_physical_ += 1;
        last_logical_ = 0;
        // In production, emit a metric or log here:
        // CEDAR_LOG_WARNING() << "HLC forced advance due to persistent clock skew";
      } else {
        last_physical_ = physical;
        last_logical_ = 0;
      }
    } else {
      last_logical_++;
    }
  }
  
  return HlcTimestamp(last_physical_, last_logical_);
}

void HybridLogicalClock::Update(const HlcTimestamp& received) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  uint64_t physical = GetPhysicalTimeMicros();
  
  // HLC update rule:
  // l'.physical = max(l.physical, m.physical, pt)
  // if (l'.physical == l.physical == m.physical)
  //   l'.logical = max(l.logical, m.logical) + 1
  // else if (l'.physical == l.physical)
  //   l'.logical = l.logical + 1
  // else if (l'.physical == m.physical)
  //   l'.logical = m.logical + 1
  // else
  //   l'.logical = 0
  
  uint64_t new_physical = std::max({last_physical_, received.physical, physical});
  uint64_t new_logical;
  
  if (new_physical == last_physical_ && new_physical == received.physical) {
    // All three are equal
    new_logical = std::max(last_logical_, received.logical) + 1;
  } else if (new_physical == last_physical_) {
    // New physical equals local
    new_logical = last_logical_ + 1;
  } else if (new_physical == received.physical) {
    // New physical equals received
    new_logical = received.logical + 1;
  } else {
    // New physical is just wall clock time
    new_logical = 0;
  }
  
  last_physical_ = new_physical;
  last_logical_ = new_logical;
}

HlcTimestamp HybridLogicalClock::Peek() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return HlcTimestamp(last_physical_, last_logical_);
}

void HybridLogicalClock::ResetForTesting() {
  std::lock_guard<std::mutex> lock(mutex_);
  last_physical_ = 0;
  last_logical_ = 0;
}

}  // namespace dtx
}  // namespace cedar
