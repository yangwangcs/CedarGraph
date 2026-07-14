#include "cedar/gcn/watermark_gc.h"

#include <chrono>
#include <algorithm>
#include <thread>

#include "cedar/gcn/tmv_engine.h"

namespace cedar {
namespace gcn {

uint64_t ComputeSafeWatermark(const WatermarkInputs& inputs) {
  if (inputs.minimum_applied_version == 0) {
    return 0;
  }
  return std::min({inputs.minimum_applied_version,
                   inputs.minimum_active_query_version,
                   inputs.retention_floor_version});
}

WatermarkGc::WatermarkGc(TMVEngine* engine)
    : engine_(engine),
      stop_flag_(false),
      watermark_(0) {}

WatermarkGc::~WatermarkGc() {
  Stop();
}

void WatermarkGc::Start(uint64_t interval_ms) {
  std::lock_guard<std::mutex> lock(start_stop_mutex_);
  if (thread_.joinable()) {
    return;
  }
  stop_flag_.store(false, std::memory_order_relaxed);
  thread_ = std::thread(&WatermarkGc::RunLoop, this, interval_ms);
}

void WatermarkGc::Stop() {
  std::lock_guard<std::mutex> lock(start_stop_mutex_);
  if (!thread_.joinable()) {
    return;
  }
  stop_flag_.store(true, std::memory_order_relaxed);
  cv_.notify_all();
  thread_.join();
}

void WatermarkGc::UpdateWatermark(uint64_t watermark) {
  watermark_.store(watermark, std::memory_order_relaxed);
}

void WatermarkGc::RunLoop(uint64_t interval_ms) {
  while (!stop_flag_.load(std::memory_order_relaxed)) {
    uint64_t watermark = watermark_.load(std::memory_order_relaxed);
    if (watermark > 0 && engine_ != nullptr) {
      engine_->DropBelowWatermark(watermark);
    }
    std::unique_lock<std::mutex> lock(cv_mutex_);
    cv_.wait_for(lock, std::chrono::milliseconds(interval_ms),
                 [this]() { return stop_flag_.load(std::memory_order_relaxed); });
  }
}

}  // namespace gcn
}  // namespace cedar
