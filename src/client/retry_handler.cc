// Copyright 2025 The Cedar Authors
//
// Retry Handler implementation

#include "cedar/client/retry_handler.h"

#include <thread>
#include <algorithm>

namespace cedar {
namespace client {

RetryHandler::RetryHandler(const RetryConfig& config)
    : config_(config) {}

RetryHandler::~RetryHandler() = default;

int RetryHandler::CalculateBackoff(int attempt) const {
  // Exponential backoff: initial_backoff * multiplier^attempt
  int backoff = static_cast<int>(
      config_.initial_backoff_ms * std::pow(config_.backoff_multiplier, attempt));
  
  // Cap at max_backoff_ms
  return std::min(backoff, config_.max_backoff_ms);
}

void RetryHandler::WaitForBackoff(int backoff_ms) {
  std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
}

}  // namespace client
}  // namespace cedar
