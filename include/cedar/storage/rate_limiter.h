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

// =============================================================================
// Rate Limiter - Token bucket rate limiter for I/O throttling
// =============================================================================

#ifndef CEDAR_STORAGE_RATE_LIMITER_H_
#define CEDAR_STORAGE_RATE_LIMITER_H_

#include <chrono>
#include <mutex>
#include <cstdint>

namespace cedar {

// Token bucket rate limiter for controlling I/O bandwidth
class RateLimiter {
 public:
  // rate_bytes_per_sec: maximum bytes per second
  // burst_bytes: maximum burst size (default: rate / 10)
  explicit RateLimiter(int64_t rate_bytes_per_sec, int64_t burst_bytes = 0)
      : rate_bytes_per_sec_(rate_bytes_per_sec),
        burst_bytes_(burst_bytes > 0 ? burst_bytes : rate_bytes_per_sec / 10),
        available_bytes_(burst_bytes > 0 ? burst_bytes : rate_bytes_per_sec / 10),
        last_refill_time_(std::chrono::steady_clock::now()) {}
  
  // Request tokens (bytes). Blocks until tokens are available.
  // Returns actual bytes granted (may be less than requested).
  int64_t Request(int64_t bytes) {
    std::lock_guard<std::mutex> lock(mutex_);
    Refill();
    
    if (available_bytes_ >= bytes) {
      available_bytes_ -= bytes;
      return bytes;
    }
    
    // Return what's available
    int64_t granted = available_bytes_;
    available_bytes_ = 0;
    return granted;
  }
  
  // Try to request tokens without blocking.
  // Returns 0 if no tokens available.
  int64_t TryRequest(int64_t bytes) {
    std::lock_guard<std::mutex> lock(mutex_);
    Refill();
    
    if (available_bytes_ >= bytes) {
      available_bytes_ -= bytes;
      return bytes;
    }
    return 0;
  }
  
  // Get current rate
  int64_t GetRate() const { return rate_bytes_per_sec_; }
  
  // Set new rate
  void SetRate(int64_t rate_bytes_per_sec) {
    std::lock_guard<std::mutex> lock(mutex_);
    rate_bytes_per_sec_ = rate_bytes_per_sec;
    burst_bytes_ = rate_bytes_per_sec / 10;
    if (available_bytes_ > burst_bytes_) {
      available_bytes_ = burst_bytes_;
    }
  }

 private:
  void Refill() {
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        now - last_refill_time_).count();
    
    if (elapsed <= 0) return;
    
    // Add tokens based on elapsed time
    int64_t new_tokens = (rate_bytes_per_sec_ * elapsed) / 1000000;
    available_bytes_ = std::min(available_bytes_ + new_tokens, burst_bytes_);
    last_refill_time_ = now;
  }
  
  int64_t rate_bytes_per_sec_;
  int64_t burst_bytes_;
  int64_t available_bytes_;
  std::chrono::steady_clock::time_point last_refill_time_;
  mutable std::mutex mutex_;
};

}  // namespace cedar

#endif  // CEDAR_STORAGE_RATE_LIMITER_H_
