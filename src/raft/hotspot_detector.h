#ifndef CEDAR_RAFT_HOTSPOT_DETECTOR_H_
#define CEDAR_RAFT_HOTSPOT_DETECTOR_H_

#include <cstdint>
#include <vector>
#include <unordered_map>
#include <atomic>
#include <mutex>
#include <thread>
#include <memory>
#include <chrono>
#include "cedar/core/status.h"

namespace cedar {
namespace raft {

struct HotspotDetectorConfig {
  uint32_t check_interval_ms = 1000;
  uint32_t qps_threshold = 10000;
  double cpu_threshold = 0.8;
  uint32_t min_partition_size = 10000;
  uint32_t window_size = 10;
};

struct PartitionLoadStats {
  uint32_t partition_id;
  uint64_t read_qps = 0;
  uint64_t write_qps = 0;
  double cpu_usage = 0.0;
  size_t key_count = 0;
  size_t data_size = 0;
  bool is_hotspot = false;
};

class HotspotDetector {
 public:
  explicit HotspotDetector(const HotspotDetectorConfig& config);
  ~HotspotDetector();
  HotspotDetector(const HotspotDetector&) = delete;
  HotspotDetector& operator=(const HotspotDetector&) = delete;

  Status Start();
  void Stop();
  void RecordAccess(uint32_t partition_id, bool is_write, size_t key_count = 1);
  void RecordCPU(uint32_t partition_id, double cpu_usage);
  std::vector<PartitionLoadStats> DetectHotspots();
  bool IsHotspot(uint32_t partition_id) const;

 private:
  HotspotDetectorConfig config_;
  
  struct PartitionMetrics {
    std::atomic<uint64_t> read_count{0};
    std::atomic<uint64_t> write_count{0};
    std::atomic<double> cpu_usage{0.0};
    std::atomic<size_t> key_count{0};
  };
  
  mutable std::mutex metrics_mutex_;
  std::unordered_map<uint32_t, std::unique_ptr<PartitionMetrics>> metrics_;
  std::atomic<bool> running_{false};
  std::thread detector_thread_;
  
  void DetectorLoop();
  void ResetCounters();
};

}  // namespace raft
}  // namespace cedar

#endif
