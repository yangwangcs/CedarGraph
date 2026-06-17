// Copyright 2025 The Cedar Authors
//
// Retry Handler with exponential backoff

#ifndef CEDAR_CLIENT_RETRY_HANDLER_H_
#define CEDAR_CLIENT_RETRY_HANDLER_H_

#include <functional>
#include <chrono>
#include <string>

namespace cedar {
namespace client {

// Retry configuration
struct RetryConfig {
  int max_retries = 3;
  int initial_backoff_ms = 100;
  int max_backoff_ms = 10000;
  double backoff_multiplier = 2.0;
  bool retry_on_timeout = true;
  bool retry_on_unavailable = true;
};

// Retry result
struct RetryResult {
  bool success;
  int attempts;
  int total_time_ms;
  std::string error_message;
};

// Retry Handler
class RetryHandler {
 public:
  RetryHandler(const RetryConfig& config = RetryConfig());
  ~RetryHandler();

  // Execute a function with retry logic
  template<typename Func>
  RetryResult Execute(Func func);

  // Execute with custom retry condition
  template<typename Func, typename ShouldRetry>
  RetryResult Execute(Func func, ShouldRetry should_retry);

  // Get current retry count
  int GetRetryCount() const { return retry_count_; }

  // Reset retry count
  void ResetRetryCount() { retry_count_ = 0; }

 private:
  RetryConfig config_;
  int retry_count_ = 0;

  // Calculate backoff time
  int CalculateBackoff(int attempt) const;

  // Wait for backoff
  void WaitForBackoff(int backoff_ms);
};

// Template implementations
template<typename Func>
RetryResult RetryHandler::Execute(Func func) {
  return Execute(func, [this](const std::string& error) {
    // Default retry condition
    return retry_count_ < config_.max_retries;
  });
}

template<typename Func, typename ShouldRetry>
RetryResult RetryHandler::Execute(Func func, ShouldRetry should_retry) {
  auto start = std::chrono::high_resolution_clock::now();
  
  RetryResult result;
  result.attempts = 0;
  result.success = false;

  for (int attempt = 0; attempt <= config_.max_retries; ++attempt) {
    result.attempts++;
    retry_count_ = attempt;

    try {
      auto func_result = func();
      if (func_result.success) {
        result.success = true;
        break;
      }

      result.error_message = func_result.error_message;

      // Check if we should retry
      if (!should_retry(result.error_message)) {
        break;
      }

      // Wait before retry
      if (attempt < config_.max_retries) {
        int backoff = CalculateBackoff(attempt);
        WaitForBackoff(backoff);
      }
    } catch (const std::exception& e) {
      result.error_message = e.what();
      break;
    }
  }

  auto end = std::chrono::high_resolution_clock::now();
  result.total_time_ms = 
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

  return result;
}

}  // namespace client
}  // namespace cedar

#endif  // CEDAR_CLIENT_RETRY_HANDLER_H_
