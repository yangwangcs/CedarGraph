#include "cedar/gcn/watermark_gc.h"

#include <chrono>
#include <thread>

#include "cedar/gcn/tmv_engine.h"

namespace cedar {
namespace gcn {

WatermarkGc::WatermarkGc(TMVEngine* engine)
    : engine_(engine),
      stop_flag_(false),
      watermark_(0) {}

WatermarkGc::~WatermarkGc() {
  Stop();
}

void WatermarkGc::Start(uint64_t interval_ms) {
  if (thread_.joinable()) {
    return;
  }
  stop_flag_.store(false, std::memory_order_relaxed);
  thread_ = std::thread(&WatermarkGc::RunLoop, this, interval_ms);
}

void WatermarkGc::Stop() {
  bool expected = false;
  if (stop_flag_.compare_exchange_strong(expected, true,
                                         std::memory_order_relaxed)) {
    if (thread_.joinable()) {
      thread_.join();
    }
  }
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
    std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
  }
}

}  // namespace gcn
}  // namespace cedar
