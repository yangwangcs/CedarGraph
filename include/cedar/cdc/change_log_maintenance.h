// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#ifndef CEDAR_CDC_CHANGE_LOG_MAINTENANCE_H_
#define CEDAR_CDC_CHANGE_LOG_MAINTENANCE_H_

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "cedar/cdc/partition_change_log.h"
#include "cedar/core/status.h"

namespace cedar::cdc {

StatusOr<uint64_t> ParseUnsignedConfigValue(const std::string& key,
                                            const std::string& value);

class ChangeLogMaintenance {
 public:
  struct Config {
    uint64_t min_retention_hours = 24;
    uint64_t max_retained_bytes = 1ULL << 30;
    uint64_t segment_size_bytes = 64ULL * 1024ULL * 1024ULL;
    std::chrono::milliseconds maintenance_interval{60000};
    uint32_t max_rpc_records = 4096;
    uint64_t max_rpc_bytes = 4ULL * 1024ULL * 1024ULL;
  };

  using LogProvider = std::function<std::vector<PartitionChangeLog*>()>;

  explicit ChangeLogMaintenance(Config config);
  ~ChangeLogMaintenance();

  static Status ValidateConfig(const Config& config);

  Status RunOnce(PartitionChangeLog& log);
  Status Start(LogProvider provider);
  void Stop();

 private:
  void Loop();

  Config config_;
  LogProvider provider_;
  std::atomic<bool> running_{false};
  std::thread worker_;
  std::mutex mutex_;
  std::condition_variable cv_;
};

}  // namespace cedar::cdc

#endif  // CEDAR_CDC_CHANGE_LOG_MAINTENANCE_H_
