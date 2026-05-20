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
// Phi Accrual Failure Detector
// =============================================================================
// Implementation of the Phi Accrual failure detection algorithm described in
// "The Phi Accrual Failure Detector" (Hayashibara et al., 2004).
//
// Unlike fixed-timeout heartbeats, Phi Accrual adapts to network jitter by
// maintaining a sliding window of heartbeat arrival intervals and computing
// a suspicion level (phi) based on the distribution.
// =============================================================================

#ifndef CEDAR_DTX_PHI_ACCRUAL_H_
#define CEDAR_DTX_PHI_ACCRUAL_H_

#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>

namespace cedar {
namespace dtx {

class PhiAccrualDetector {
 public:
  explicit PhiAccrualDetector(size_t window_size = 1000);

  // Record a heartbeat interval (time since last heartbeat, in milliseconds).
  void RecordInterval(double interval_ms);

  // Compute the phi value for a given silence duration (ms).
  // phi = -log10(1 - CDF(t)) where CDF is derived from historical intervals.
  double Phi(double silence_ms) const;

  // Convenience: check if the node is suspected failed.
  bool IsSuspected(double silence_ms, double threshold = 8.0) const {
    return Phi(silence_ms) >= threshold;
  }

  // Number of samples collected so far.
  size_t SampleCount() const;

  // Reset all samples.
  void Reset();

 private:
  mutable std::mutex mutex_;
  std::deque<double> intervals_;
  size_t window_size_;

  // Compute mean and variance of the current window.
  struct Distribution {
    double mean{0.0};
    double variance{0.0};
    bool valid{false};
  };
  Distribution ComputeDistribution() const;

  // CDF of normal distribution.
  static double NormalCDF(double x, double mean, double stddev);
};

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_PHI_ACCRUAL_H_
