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

#include <algorithm>
#include <iostream>

#include "cedar/driver/retry_policy.h"

#include <random>
#include <thread>

namespace cedar {
namespace driver {

ErrorClass ErrorClassifier::Classify(const Status& status) {
  // 基于状态码分类
  // 注意：这里使用 ToString() 进行简单判断，实际应该扩展 Status 类
  std::string msg = status.ToString();
  
  // 瞬态错误 - 可以重试
  if (msg.find("Conflict") != std::string::npos ||
      msg.find("Busy") != std::string::npos ||
      msg.find("Lock") != std::string::npos ||
      msg.find("Try again") != std::string::npos) {
    return ErrorClass::kTransientError;
  }
  
  // 系统错误 - 不应重试
  if (msg.find("IO error") != std::string::npos ||
      msg.find("Corruption") != std::string::npos ||
      msg.find("Not supported") != std::string::npos) {
    return ErrorClass::kSystemError;
  }
  
  // 客户端错误 - 不应重试
  if (msg.find("Invalid argument") != std::string::npos ||
      msg.find("Not found") != std::string::npos ||
      msg.find("Already exists") != std::string::npos) {
    return ErrorClass::kClientError;
  }
  
  // 默认可用性错误（如超时）- 可以重试
  if (msg.find("Timeout") != std::string::npos ||
      msg.find("Network") != std::string::npos) {
    return ErrorClass::kAvailabilityError;
  }
  
  return ErrorClass::kClientError;
}

std::chrono::milliseconds RetryPolicy::CalculateDelay(
    std::chrono::milliseconds base_delay) const {
  
  auto delay_count = std::max<int64_t>(0, base_delay.count());
  
  // 添加抖动
  if (config_.jitter) {
    static thread_local std::mt19937 rng(std::random_device{}());
    std::uniform_real_distribution<double> dist(0.0, config_.jitter_factor);
    double jitter_mult = 1.0 + dist(rng);
    auto max_count = std::chrono::milliseconds::max().count();
    double jittered = static_cast<double>(delay_count) * jitter_mult;
    if (jittered >= static_cast<double>(max_count)) {
      delay_count = max_count;
    } else {
      delay_count = static_cast<int64_t>(jittered);
    }
  }
  
  return std::chrono::milliseconds(delay_count);
}

std::chrono::milliseconds RetryPolicy::NextDelay(
    std::chrono::milliseconds current) const {
  
  switch (config_.backoff_strategy) {
    case BackoffStrategy::kFixed:
      return config_.initial_backoff;
    
    case BackoffStrategy::kLinear: {
      auto max_count = config_.max_backoff.count();
      auto current_count = current.count();
      auto increment = config_.initial_backoff.count();
      if (current_count >= max_count ||
          increment > max_count - current_count) {
        return config_.max_backoff;
      }
      return std::min(
          current + config_.initial_backoff,
          config_.max_backoff);
    }
    
    case BackoffStrategy::kExponential: {
      auto max_count = config_.max_backoff.count();
      auto current_count = current.count();
      if (current_count >= max_count || current_count > max_count / 2) {
        return config_.max_backoff;
      }
      return std::min(
          std::chrono::milliseconds(current.count() * 2),
          config_.max_backoff);
    }
    default:
      std::cerr << "[RetryPolicy] Unknown backoff strategy" << std::endl;
      return std::chrono::milliseconds(0);
  }
  
  return config_.initial_backoff;
}

}  // namespace driver
}  // namespace cedar
