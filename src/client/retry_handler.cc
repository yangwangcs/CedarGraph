// Copyright 2025 The Cedar Authors
//
// Retry Handler implementation

#include "cedar/client/retry_handler.h"

#include <thread>
#include <algorithm>
#include <cmath>
#include <limits>

namespace cedar {
namespace client {

namespace {

RetryConfig NormalizeConfig(RetryConfig config) {
  config.max_retries = std::max(0, config.max_retries);
  config.initial_backoff_ms = std::max(0, config.initial_backoff_ms);
  config.max_backoff_ms = std::max(0, config.max_backoff_ms);
  if (config.max_backoff_ms < config.initial_backoff_ms) {
    config.max_backoff_ms = config.initial_backoff_ms;
  }
  if (!std::isfinite(config.backoff_multiplier) ||
      config.backoff_multiplier < 1.0) {
    config.backoff_multiplier = 1.0;
  }
  return config;
}

}  // namespace

RetryHandler::RetryHandler(const RetryConfig& config)
    : config_(NormalizeConfig(config)) {}

RetryHandler::~RetryHandler() = default;

int RetryHandler::CalculateBackoff(int attempt) const {
  if (attempt < 0 || config_.initial_backoff_ms <= 0 ||
      config_.max_backoff_ms <= 0) {
    return 0;
  }

  // Exponential backoff: initial_backoff * multiplier^attempt
  double backoff = static_cast<double>(config_.initial_backoff_ms) *
                   std::pow(config_.backoff_multiplier, attempt);
  
  // Cap at max_backoff_ms
  if (!std::isfinite(backoff) ||
      backoff >= static_cast<double>(config_.max_backoff_ms)) {
    return config_.max_backoff_ms;
  }

  if (backoff <= 0.0) {
    return 0;
  }
  return static_cast<int>(backoff);
}

void RetryHandler::WaitForBackoff(int backoff_ms) {
  if (backoff_ms <= 0) {
    return;
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
}

}  // namespace client
}  // namespace cedar
