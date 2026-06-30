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
// Retry Policy - 重试策略
// =============================================================================
// 提供可配置的重试机制，支持多种退避策略
// =============================================================================

#ifndef CEDAR_DRIVER_RETRY_POLICY_H_
#define CEDAR_DRIVER_RETRY_POLICY_H_

#include <chrono>
#include <functional>
#include <limits>
#include <optional>
#include <random>
#include <thread>
#include <type_traits>

#include "cedar/core/status.h"

namespace cedar {
namespace driver {

// 错误分类
enum class ErrorClass {
  kClientError,        // 客户端错误 - 不应重试
  kTransientError,     // 瞬态错误 - 可以重试（锁冲突、OCC验证失败）
  kSystemError,        // 系统错误 - 不应重试（磁盘满、损坏）
  kAvailabilityError,  // 可用性错误 - 可以重试（网络超时）
};

// 退避策略类型
enum class BackoffStrategy {
  kFixed,        // 固定间隔
  kLinear,       // 线性增长
  kExponential,  // 指数增长（推荐）
};

// 错误分类器
class ErrorClassifier {
 public:
  // 根据状态码分类错误
  static ErrorClass Classify(const Status& status);
  
  // 是否可重试
  static bool IsRetryable(const Status& status) {
    auto cls = Classify(status);
    return cls == ErrorClass::kTransientError ||
           cls == ErrorClass::kAvailabilityError;
  }
};

// 重试配置
struct RetryConfig {
  // 最大尝试次数（包括首次尝试）
  // 默认: 3（首次 + 2次重试）
  size_t max_attempts = 3;
  
  // 初始退避时间
  std::chrono::milliseconds initial_backoff{100};
  
  // 最大退避时间
  std::chrono::milliseconds max_backoff{5000};
  
  // 退避策略
  BackoffStrategy backoff_strategy = BackoffStrategy::kExponential;
  
  // 是否添加抖动（防止惊群）
  // 抖动范围: [0, backoff * jitter_factor]
  bool jitter = true;
  double jitter_factor = 0.1;  // 10% 抖动
  
  // 可重试错误判断函数（自定义扩展点）
  std::function<bool(const Status&)> retry_predicate;
  
  // 每次重试前的回调（用于日志/监控）
  std::function<void(size_t attempt, const Status& error, 
                     std::chrono::milliseconds delay)> on_retry;
};

// 重试策略执行器
class RetryPolicy {
 public:
  explicit RetryPolicy(const RetryConfig& config) : config_(NormalizeConfig(config)) {}
  
  // 执行带重试的操作
  template<typename Func>
  auto Execute(Func&& func) -> decltype(func()) {
    using ReturnType = decltype(func());
    
    std::chrono::milliseconds current_delay = config_.initial_backoff;
    
    for (size_t attempt = 1; attempt <= config_.max_attempts; ++attempt) {
      try {
        ReturnType result = func();
        
        // 获取错误状态（假设结果有 ok() 方法）
        Status status = ExtractStatus(result);
        
        // 成功或最后一次尝试
        if (status.ok() || attempt == config_.max_attempts) {
          return result;
        }
        
        // 判断是否可重试
        if (!ShouldRetry(status)) {
          return result;  // 不可重试，直接返回错误
        }
        
        // 计算下一次延迟
        if (attempt < config_.max_attempts) {
          auto delay = CalculateDelay(current_delay);
          
          if (config_.on_retry) {
            config_.on_retry(attempt, status, delay);
          }
          
          std::this_thread::sleep_for(delay);
          
          // 更新下一次的延迟
          current_delay = NextDelay(current_delay);
        }
      } catch (const std::exception& e) {
        if (attempt == config_.max_attempts) {
          if constexpr (std::is_same_v<ReturnType, Status>) {
            return Status::IOError("Retry", std::string("Unhandled exception: ") + e.what());
          } else {
            // For non-Status return types, we cannot construct an error result.
            // Re-throw to preserve existing behavior while still logging.
            throw;
          }
        }
        auto delay = CalculateDelay(current_delay);
        if (config_.on_retry) {
          config_.on_retry(attempt,
                           Status::IOError("Retry", std::string("Exception: ") + e.what()),
                           delay);
        }
        std::this_thread::sleep_for(delay);
        current_delay = NextDelay(current_delay);
      }
    }
    
    // 理论上不会到达这里
    try {
      return func();
    } catch (const std::exception& e) {
      if constexpr (std::is_same_v<ReturnType, Status>) {
        return Status::IOError("Retry", std::string("Unhandled exception: ") + e.what());
      } else {
        throw;
      }
    }
  }
  
 private:
  RetryConfig config_;

  static RetryConfig NormalizeConfig(RetryConfig config) {
    if (config.max_attempts == 0) {
      config.max_attempts = 1;
    }
    if (config.initial_backoff.count() < 0) {
      config.initial_backoff = std::chrono::milliseconds(0);
    }
    if (config.max_backoff.count() < 0) {
      config.max_backoff = std::chrono::milliseconds(0);
    }
    if (config.max_backoff < config.initial_backoff) {
      config.max_backoff = config.initial_backoff;
    }
    if (config.jitter_factor < 0.0) {
      config.jitter_factor = 0.0;
    }
    return config;
  }
  
  // 计算延迟（包含抖动）
  std::chrono::milliseconds CalculateDelay(
      std::chrono::milliseconds base_delay) const;
  
  // 计算下一次延迟（不含抖动，用于状态更新）
  std::chrono::milliseconds NextDelay(
      std::chrono::milliseconds current) const;
  
  // 判断是否应重试
  bool ShouldRetry(const Status& status) const {
    if (config_.retry_predicate) {
      return config_.retry_predicate(status);
    }
    return ErrorClassifier::IsRetryable(status);
  }
  
  // 从结果提取状态（模板特化）
  template<typename T>
  Status ExtractStatus(const T& result) const {
    if constexpr (std::is_same_v<T, Status>) {
      return result;
    } else if constexpr (std::is_same_v<T, bool>) {
      return result ? Status::OK() : Status::IOError("Retry", "Operation failed");
    } else {
      // 假设有 ok() 和 status() 方法
      return result.ok() ? Status::OK() : result.status();
    }
  }
};

// 预设重试策略工厂
class RetryPolicies {
 public:
  // 默认策略 - 适合大多数场景
  // 3次尝试，指数退避 100ms -> 200ms -> 400ms
  static RetryConfig Default() {
    RetryConfig config;
    config.max_attempts = 3;
    config.initial_backoff = std::chrono::milliseconds(100);
    config.max_backoff = std::chrono::milliseconds(1000);
    config.backoff_strategy = BackoffStrategy::kExponential;
    config.jitter = true;
    return config;
  }
  
  // 激进策略 - 适合高冲突场景
  // 5次尝试，更短的初始延迟
  static RetryConfig Aggressive() {
    RetryConfig config;
    config.max_attempts = 5;
    config.initial_backoff = std::chrono::milliseconds(10);
    config.max_backoff = std::chrono::milliseconds(500);
    config.backoff_strategy = BackoffStrategy::kExponential;
    config.jitter = true;
    return config;
  }
  
  // 保守策略 - 适合低频关键操作
  // 3次尝试，较长延迟，避免系统过载
  static RetryConfig Conservative() {
    RetryConfig config;
    config.max_attempts = 3;
    config.initial_backoff = std::chrono::milliseconds(500);
    config.max_backoff = std::chrono::milliseconds(5000);
    config.backoff_strategy = BackoffStrategy::kLinear;
    config.jitter = true;
    return config;
  }
  
  // 不重试 - 适合一次性操作
  static RetryConfig NoRetry() {
    RetryConfig config;
    config.max_attempts = 1;
    config.initial_backoff = std::chrono::milliseconds(0);
    return config;
  }
  
  // 仅瞬态错误重试
  static RetryConfig TransientOnly() {
    RetryConfig config = Default();
    config.retry_predicate = [](const Status& status) {
      return ErrorClassifier::Classify(status) == ErrorClass::kTransientError;
    };
    return config;
  }
};

}  // namespace driver
}  // namespace cedar

#endif  // CEDAR_DRIVER_RETRY_POLICY_H_
