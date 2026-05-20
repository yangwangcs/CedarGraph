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

#include "cedar/dtx/phi_accrual.h"

#include <algorithm>
#include <limits>

namespace cedar {
namespace dtx {

PhiAccrualDetector::PhiAccrualDetector(size_t window_size)
    : window_size_(window_size) {}

void PhiAccrualDetector::RecordInterval(double interval_ms) {
  if (interval_ms <= 0.0) interval_ms = 0.1;  // Guard against zero/negative
  std::lock_guard<std::mutex> lock(mutex_);
  intervals_.push_back(interval_ms);
  while (intervals_.size() > window_size_) {
    intervals_.pop_front();
  }
}

double PhiAccrualDetector::Phi(double silence_ms) const {
  if (silence_ms <= 0.0) return 0.0;

  std::lock_guard<std::mutex> lock(mutex_);
  if (intervals_.size() < 2) {
    // Not enough samples: fallback to a conservative fixed timeout equivalent.
    // phi grows linearly with silence, reaching 8.0 at 5000ms.
    return silence_ms / 625.0;
  }

  auto dist = ComputeDistribution();
  if (!dist.valid || dist.variance <= 0.0) {
    return silence_ms / 625.0;
  }

  double stddev = std::sqrt(dist.variance);
  if (stddev < 1e-6) {
    // All samples are essentially identical.
    return (silence_ms > dist.mean) ? 8.0 : 0.0;
  }

  // Phi = -log10(1 - CDF(silence))
  double cdf = NormalCDF(silence_ms, dist.mean, stddev);
  // Clamp CDF to [0, 1] to guard against numerical overshoot.
  if (cdf >= 1.0) cdf = 1.0 - 1e-15;
  if (cdf <= 0.0) cdf = 1e-15;
  double phi = -std::log10(1.0 - cdf);
  return phi;
}

size_t PhiAccrualDetector::SampleCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return intervals_.size();
}

void PhiAccrualDetector::Reset() {
  std::lock_guard<std::mutex> lock(mutex_);
  intervals_.clear();
}

PhiAccrualDetector::Distribution PhiAccrualDetector::ComputeDistribution() const {
  Distribution dist;
  if (intervals_.size() < 2) return dist;

  double sum = 0.0;
  for (double v : intervals_) sum += v;
  dist.mean = sum / static_cast<double>(intervals_.size());

  double sq_sum = 0.0;
  for (double v : intervals_) {
    double d = v - dist.mean;
    sq_sum += d * d;
  }
  // Sample variance (Bessel-corrected) for better small-sample accuracy.
  dist.variance = sq_sum / static_cast<double>(intervals_.size() - 1);
  dist.valid = true;
  return dist;
}

double PhiAccrualDetector::NormalCDF(double x, double mean, double stddev) {
  // Standardize: z = (x - mean) / stddev
  double z = (x - mean) / stddev;
  // Abramowitz & Stegun approximation for the standard normal CDF.
  // Error < 7.5e-8.
  static constexpr double a1 =  0.254829592;
  static constexpr double a2 = -0.284496736;
  static constexpr double a3 =  1.421413741;
  static constexpr double a4 = -1.453152027;
  static constexpr double a5 =  1.061405429;
  static constexpr double p  =  0.3275911;

  int sign = (z < 0.0) ? -1 : 1;
  double abs_z = std::abs(z) / std::sqrt(2.0);

  double t = 1.0 / (1.0 + p * abs_z);
  double y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * std::exp(-abs_z * abs_z);

  return 0.5 * (1.0 + sign * y);
}

}  // namespace dtx
}  // namespace cedar
