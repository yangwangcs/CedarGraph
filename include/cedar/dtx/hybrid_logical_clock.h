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
// Hybrid Logical Clock (HLC)
// =============================================================================
// Implementation based on the paper:
// "Logical Physical Clocks and Consistent Snapshots in Distributed Systems"
// by Kulkarni et al.
//
// HLC provides:
// 1. Causal consistency without explicit vector clocks
// 2. Bounded clock drift (physical component)
// 3. No single point of failure (unlike TSO)
// =============================================================================

#ifndef CEDAR_DTX_HYBRID_LOGICAL_CLOCK_H_
#define CEDAR_DTX_HYBRID_LOGICAL_CLOCK_H_

#include <cstdint>
#include <chrono>
#include <mutex>
#include <string>

namespace cedar {
namespace dtx {

// HLC Timestamp structure
struct HlcTimestamp {
  uint64_t physical;  // Physical time (microseconds since epoch)
  uint64_t logical;   // Logical counter for events at same physical time
  
  // Default constructor
  HlcTimestamp() : physical(0), logical(0) {}
  
  // Constructor
  HlcTimestamp(uint64_t phys, uint64_t log) : physical(phys), logical(log) {}
  
  // Comparison operators
  bool operator<(const HlcTimestamp& other) const {
    return physical < other.physical || 
           (physical == other.physical && logical < other.logical);
  }
  
  bool operator>(const HlcTimestamp& other) const {
    return other < *this;
  }
  
  bool operator==(const HlcTimestamp& other) const {
    return physical == other.physical && logical == other.logical;
  }
  
  bool operator!=(const HlcTimestamp& other) const {
    return !(*this == other);
  }
  
  bool operator<=(const HlcTimestamp& other) const {
    return !(other < *this);
  }
  
  bool operator>=(const HlcTimestamp& other) const {
    return !(*this < other);
  }
  
  // Check if this timestamp happens before another (causality)
  bool HappensBefore(const HlcTimestamp& other) const {
    return *this < other;
  }
  
  // Check if timestamps are concurrent (neither happens before the other)
  bool IsConcurrentWith(const HlcTimestamp& other) const {
    return !(*this < other) && !(other < *this);
  }
  
  // Convert to string for debugging
  std::string ToString() const {
    return "(" + std::to_string(physical) + ", " + std::to_string(logical) + ")";
  }
  
  // Serialize to bytes (for network transmission)
  void Serialize(uint8_t* buffer) const {
    memcpy(buffer, &physical, sizeof(physical));
    memcpy(buffer + sizeof(physical), &logical, sizeof(logical));
  }
  
  // Deserialize from bytes
  static HlcTimestamp Deserialize(const uint8_t* buffer) {
    uint64_t phys, log;
    memcpy(&phys, buffer, sizeof(phys));
    memcpy(&log, buffer + sizeof(phys), sizeof(log));
    return HlcTimestamp(phys, log);
  }
  
  static constexpr size_t kSerializedSize = sizeof(uint64_t) * 2;
};

// =============================================================================
// Hybrid Logical Clock Implementation
// =============================================================================

class HybridLogicalClock {
 public:
  HybridLogicalClock();
  ~HybridLogicalClock() = default;
  
  // Disable copy, allow move
  HybridLogicalClock(const HybridLogicalClock&) = delete;
  HybridLogicalClock& operator=(const HybridLogicalClock&) = delete;
  HybridLogicalClock(HybridLogicalClock&&) = default;
  HybridLogicalClock& operator=(HybridLogicalClock&&) = default;
  
  // Get current HLC timestamp
  // Thread-safe
  HlcTimestamp Now();
  
  // Update local clock based on received timestamp
  // This is called when receiving a message from another node
  // Thread-safe
  void Update(const HlcTimestamp& received);
  
  // Get current physical time (microseconds since epoch)
  static uint64_t GetPhysicalTimeMicros();
  
  // Get the maximum clock drift allowed (in microseconds)
  // If drift exceeds this, a warning should be logged
  static constexpr uint64_t kMaxClockDriftMicros = 10 * 1000;  // 10ms
  
  // Peek current timestamp without incrementing logical counter
  // For read-only operations that need a timestamp
  HlcTimestamp Peek() const;
  
  // Check if clock has been initialized
  bool IsInitialized() const {
    return last_physical_ > 0;
  }
  
  // Get the last timestamp (for debugging/monitoring)
  HlcTimestamp GetLastTimestamp() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return HlcTimestamp(last_physical_, last_logical_);
  }
  
  // Reset clock (for testing only)
  void ResetForTesting();

 private:
  mutable std::mutex mutex_;
  uint64_t last_physical_;
  uint64_t last_logical_;
  
  // Maximum logical counter value before we wait for physical clock to advance
  // This prevents logical counter overflow in high-throughput scenarios
  static constexpr uint64_t kMaxLogicalCounter = 0xFFFFFFFF;
};

// =============================================================================
// HLC Clock Manager (per-node singleton)
// =============================================================================

class HlcClockManager {
 public:
  static HybridLogicalClock& GetInstance() {
    static HybridLogicalClock instance;
    return instance;
  }
  
 private:
  HlcClockManager() = default;
};

// Convenience function to get global HLC timestamp
inline HlcTimestamp GetHlcNow() {
  return HlcClockManager::GetInstance().Now();
}

// Convenience function to update global HLC
inline void UpdateHlc(const HlcTimestamp& received) {
  HlcClockManager::GetInstance().Update(received);
}

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_HYBRID_LOGICAL_CLOCK_H_
