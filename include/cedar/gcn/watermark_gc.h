#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <thread>

namespace cedar {
namespace gcn {

class TMVEngine;

struct WatermarkInputs {
  uint64_t minimum_applied_version = 0;
  uint64_t minimum_active_query_version = 0;
  uint64_t retention_floor_version = 0;
};

uint64_t ComputeSafeWatermark(const WatermarkInputs& inputs);

// Background thread that periodically calls TMVEngine::DropBelowWatermark()
// to free chunks whose max_valid_to is below the current watermark.
class WatermarkGc {
 public:
  explicit WatermarkGc(TMVEngine* engine);
  ~WatermarkGc();

  // Non-copyable, non-movable
  WatermarkGc(const WatermarkGc&) = delete;
  WatermarkGc& operator=(const WatermarkGc&) = delete;

  // Launch the background GC thread.  It wakes every |interval_ms| and
  // performs a drop pass using the current watermark.
  void Start(uint64_t interval_ms);

  // Signal the background thread to stop and block until it exits.
  void Stop();

  // Thread-safe update of the watermark value.
  void UpdateWatermark(uint64_t watermark);

 private:
  void RunLoop(uint64_t interval_ms);

  TMVEngine* engine_;
  std::atomic<bool> stop_flag_;
  std::atomic<uint64_t> watermark_;
  std::thread thread_;
  mutable std::mutex start_stop_mutex_;
  std::mutex cv_mutex_;
  std::condition_variable cv_;
};

}  // namespace gcn
}  // namespace cedar
