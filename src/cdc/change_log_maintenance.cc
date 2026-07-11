// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#include "cedar/cdc/change_log_maintenance.h"

#include <charconv>
#include <limits>
#include <string>

namespace cedar::cdc {

StatusOr<uint64_t> ParseUnsignedConfigValue(const std::string& key,
                                            const std::string& value) {
  if (value.empty()) {
    return Status::InvalidArgument("CDC config " + key + " must not be empty");
  }

  uint64_t parsed = 0;
  const char* begin = value.data();
  const char* end = begin + value.size();
  auto [ptr, ec] = std::from_chars(begin, end, parsed, 10);
  if (ec == std::errc::invalid_argument || ptr != end) {
    return Status::InvalidArgument("CDC config " + key +
                                   " must be an unsigned integer");
  }
  if (ec == std::errc::result_out_of_range) {
    return Status::InvalidArgument("CDC config " + key + " is out of range");
  }
  if (ec != std::errc()) {
    return Status::InvalidArgument("CDC config " + key + " is invalid");
  }
  return parsed;
}

ChangeLogMaintenance::ChangeLogMaintenance(Config config)
    : config_(std::move(config)) {}

ChangeLogMaintenance::~ChangeLogMaintenance() {
  Stop();
}

Status ChangeLogMaintenance::ValidateConfig(const Config& config) {
  if (config.min_retention_hours == 0) {
    return Status::InvalidArgument("CDC min_retention_hours must be positive");
  }
  if (config.max_retained_bytes == 0) {
    return Status::InvalidArgument("CDC max_retained_bytes must be positive");
  }
  if (config.segment_size_bytes == 0) {
    return Status::InvalidArgument("CDC segment_size_bytes must be positive");
  }
  if (config.maintenance_interval <= std::chrono::milliseconds(0)) {
    return Status::InvalidArgument("CDC maintenance_interval must be positive");
  }
  if (config.max_rpc_records == 0 || config.max_rpc_bytes == 0) {
    return Status::InvalidArgument("CDC RPC limits must be positive");
  }
  constexpr uint64_t kMaxReasonableBytes =
      std::numeric_limits<uint64_t>::max() / 2;
  if (config.max_retained_bytes > kMaxReasonableBytes) {
    return Status::InvalidArgument("CDC max_retained_bytes is too large");
  }
  if (config.segment_size_bytes > kMaxReasonableBytes) {
    return Status::InvalidArgument("CDC segment_size_bytes is too large");
  }
  if (config.max_rpc_bytes > kMaxReasonableBytes) {
    return Status::InvalidArgument("CDC max_rpc_bytes is too large");
  }
  if (config.max_rpc_records > std::numeric_limits<uint32_t>::max() / 2) {
    return Status::InvalidArgument("CDC max_rpc_records is too large");
  }
  if (config.maintenance_interval > std::chrono::hours(24 * 365)) {
    return Status::InvalidArgument("CDC maintenance_interval is too large");
  }
  return Status::OK();
}

Status ChangeLogMaintenance::RunOnce(PartitionChangeLog& log) {
  CEDAR_RETURN_IF_ERROR(ValidateConfig(config_));
  const auto state = log.GetState();
  if (state.segment_bytes <= config_.max_retained_bytes) {
    return Status::OK();
  }
  if (state.segment_count <= 1 ||
      state.youngest_closed_segment_age_hours < config_.min_retention_hours) {
    return Status::OK();
  }
  if (state.active_segment_first_offset == 0 ||
      state.active_segment_first_offset > state.high_watermark + 1) {
    return Status::InvalidArgument("CDC active segment offset is invalid");
  }
  return log.Compact(state.active_segment_first_offset);
}

Status ChangeLogMaintenance::Start(LogProvider provider) {
  CEDAR_RETURN_IF_ERROR(ValidateConfig(config_));
  if (!provider) {
    return Status::InvalidArgument("CDC maintenance log provider is empty");
  }
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true)) {
    return Status::OK();
  }
  provider_ = std::move(provider);
  worker_ = std::thread(&ChangeLogMaintenance::Loop, this);
  return Status::OK();
}

void ChangeLogMaintenance::Stop() {
  if (!running_.exchange(false)) {
    return;
  }
  cv_.notify_all();
  if (worker_.joinable()) {
    worker_.join();
  }
}

void ChangeLogMaintenance::Loop() {
  while (running_.load()) {
    if (provider_) {
      for (auto* log : provider_()) {
        if (!running_.load()) {
          break;
        }
        if (log != nullptr) {
          (void)RunOnce(*log);
        }
      }
    }
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait_for(lock, config_.maintenance_interval,
                 [this] { return !running_.load(); });
  }
}

}  // namespace cedar::cdc
