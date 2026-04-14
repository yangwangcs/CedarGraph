#include "src/raft/hotspot_detector.h"

#include <algorithm>
#include <chrono>

namespace cedar {
namespace raft {

HotspotDetector::HotspotDetector(const HotspotDetectorConfig& config)
    : config_(config) {}

HotspotDetector::~HotspotDetector() {
  Stop();
}

Status HotspotDetector::Start() {
  if (running_.exchange(true)) {
    return Status::OK();  // Already running
  }
  
  detector_thread_ = std::thread(&HotspotDetector::DetectorLoop, this);
  return Status::OK();
}

void HotspotDetector::Stop() {
  if (!running_.exchange(false)) {
    return;  // Not running
  }
  
  if (detector_thread_.joinable()) {
    detector_thread_.join();
  }
}

void HotspotDetector::RecordAccess(uint32_t partition_id, bool is_write, size_t key_count) {
  std::unique_lock<std::mutex> lock(metrics_mutex_);
  auto& metrics = metrics_[partition_id];
  if (!metrics) {
    metrics = std::make_unique<PartitionMetrics>();
  }
  lock.unlock();
  
  if (is_write) {
    metrics->write_count.fetch_add(key_count, std::memory_order_relaxed);
  } else {
    metrics->read_count.fetch_add(key_count, std::memory_order_relaxed);
  }
}

void HotspotDetector::RecordCPU(uint32_t partition_id, double cpu_usage) {
  std::unique_lock<std::mutex> lock(metrics_mutex_);
  auto& metrics = metrics_[partition_id];
  if (!metrics) {
    metrics = std::make_unique<PartitionMetrics>();
  }
  lock.unlock();
  
  // Simple sliding average: new_value = (old_value * (window-1) + new_sample) / window
  double old_cpu = metrics->cpu_usage.load(std::memory_order_relaxed);
  uint32_t window = config_.window_size > 0 ? config_.window_size : 10;
  double new_cpu = (old_cpu * (window - 1) + cpu_usage) / window;
  metrics->cpu_usage.store(new_cpu, std::memory_order_relaxed);
}

std::vector<PartitionLoadStats> HotspotDetector::DetectHotspots() {
  std::vector<PartitionLoadStats> hotspots;
  
  std::lock_guard<std::mutex> lock(metrics_mutex_);
  
  for (const auto& [partition_id, metrics] : metrics_) {
    uint64_t read_count = metrics->read_count.load(std::memory_order_relaxed);
    uint64_t write_count = metrics->write_count.load(std::memory_order_relaxed);
    double cpu_usage = metrics->cpu_usage.load(std::memory_order_relaxed);
    size_t key_count = metrics->key_count.load(std::memory_order_relaxed);
    
    // Calculate QPS based on the check interval
    double interval_seconds = config_.check_interval_ms / 1000.0;
    uint64_t read_qps = static_cast<uint64_t>(read_count / interval_seconds);
    uint64_t write_qps = static_cast<uint64_t>(write_count / interval_seconds);
    uint64_t total_qps = read_qps + write_qps;
    
    bool is_hotspot = (total_qps > config_.qps_threshold) || 
                      (cpu_usage > config_.cpu_threshold);
    
    PartitionLoadStats stats;
    stats.partition_id = partition_id;
    stats.read_qps = read_qps;
    stats.write_qps = write_qps;
    stats.cpu_usage = cpu_usage;
    stats.key_count = key_count;
    stats.data_size = 0;  // Not tracked yet
    stats.is_hotspot = is_hotspot;
    
    if (is_hotspot) {
      hotspots.push_back(stats);
    }
  }
  
  // Sort by total QPS descending
  std::sort(hotspots.begin(), hotspots.end(), [](const auto& a, const auto& b) {
    return (a.read_qps + a.write_qps) > (b.read_qps + b.write_qps);
  });
  
  return hotspots;
}

bool HotspotDetector::IsHotspot(uint32_t partition_id) const {
  std::lock_guard<std::mutex> lock(metrics_mutex_);
  
  auto it = metrics_.find(partition_id);
  if (it == metrics_.end() || !it->second) {
    return false;
  }
  
  const auto& metrics = it->second;
  uint64_t read_count = metrics->read_count.load(std::memory_order_relaxed);
  uint64_t write_count = metrics->write_count.load(std::memory_order_relaxed);
  double cpu_usage = metrics->cpu_usage.load(std::memory_order_relaxed);
  
  double interval_seconds = config_.check_interval_ms / 1000.0;
  uint64_t total_qps = static_cast<uint64_t>((read_count + write_count) / interval_seconds);
  
  return (total_qps > config_.qps_threshold) || (cpu_usage > config_.cpu_threshold);
}

void HotspotDetector::DetectorLoop() {
  while (running_.load(std::memory_order_relaxed)) {
    // Sleep for the check interval
    std::this_thread::sleep_for(std::chrono::milliseconds(config_.check_interval_ms));
    
    if (!running_.load(std::memory_order_relaxed)) {
      break;
    }
    
    // Detect hotspots (this also resets counters internally after detection)
    auto hotspots = DetectHotspots();
    
    // Reset counters after detection
    ResetCounters();
    
    // Hotspots are detected, can be used for logging or triggering actions
    // The caller can also call DetectHotspots() to get the results
    (void)hotspots;
  }
}

void HotspotDetector::ResetCounters() {
  std::lock_guard<std::mutex> lock(metrics_mutex_);
  
  for (auto& [partition_id, metrics] : metrics_) {
    if (metrics) {
      metrics->read_count.store(0, std::memory_order_relaxed);
      metrics->write_count.store(0, std::memory_order_relaxed);
      // Keep CPU usage as it's a sliding average
    }
  }
}

}  // namespace raft
}  // namespace cedar
