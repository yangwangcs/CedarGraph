// Copyright 2025 The Cedar Authors
//
// Auto Scaler - automatic scaling based on load metrics

#ifndef CEDAR_CLIENT_AUTO_SCALER_H_
#define CEDAR_CLIENT_AUTO_SCALER_H_

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "cedar/client/cluster_types.h"

namespace cedar {
namespace client {

// Scaling policy
enum class ScalingPolicy {
  CPU_BASED,           // Scale based on CPU usage
  MEMORY_BASED,        // Scale based on memory usage
  QPS_BASED,           // Scale based on queries per second
  LATENCY_BASED,       // Scale based on latency
  CUSTOM               // Custom scaling logic
};

// Scaling rule
struct ScalingRule {
  std::string component;          // metad, storaged, graphd, queryd
  ScalingPolicy policy;
  double scale_up_threshold;      // Threshold to scale up
  double scale_down_threshold;    // Threshold to scale down
  int min_replicas;               // Minimum replicas
  int max_replicas;               // Maximum replicas
  int cooldown_seconds;           // Cooldown period between scaling
  int evaluation_periods;         // Number of periods before scaling
};

// Scaling event
struct ScalingEvent {
  std::string component;
  int old_replicas;
  int new_replicas;
  std::string reason;
  int64_t timestamp;
};

// Scaling callback
using ScalingCallback = std::function<void(const ScalingEvent&)>;

// Auto Scaler
class AutoScaler {
 public:
  AutoScaler();
  ~AutoScaler();

  // Initialize auto scaler
  bool Initialize(class ClusterManager* cluster_manager,
                  class ClusterMonitor* cluster_monitor);

  // Start auto scaling
  bool Start(int interval_seconds = 60);

  // Stop auto scaling
  void Stop();

  // Add scaling rule
  void AddRule(const ScalingRule& rule);

  // Remove scaling rule
  void RemoveRule(const std::string& component);

  // Get scaling rules
  std::vector<ScalingRule> GetRules();

  // Get scaling events
  std::vector<ScalingEvent> GetEvents(int limit = 100);

  // Set callback
  void SetCallback(ScalingCallback callback);

  // Manual scaling
  bool ScaleUp(const std::string& component);
  bool ScaleDown(const std::string& component);

  // Get current replicas
  int GetCurrentReplicas(const std::string& component);

 private:
  class ClusterManager* cluster_manager_;
  class ClusterMonitor* cluster_monitor_;
  std::atomic<bool> running_{false};
  std::thread scaler_thread_;
  mutable std::mutex mutex_;
  
  std::vector<ScalingRule> rules_;
  std::vector<ScalingEvent> events_;
  ScalingCallback callback_;
  
  // Track last scaling time for cooldown
  std::unordered_map<std::string, int64_t> last_scaling_time_;

  // Scaler loop
  void ScalerLoop();

  // Evaluate scaling rules
  void EvaluateRules();

  // Check if scaling is needed
  bool ShouldScaleUp(const ScalingRule& rule, double current_value);
  bool ShouldScaleDown(const ScalingRule& rule, double current_value);

  // Get current metric value
  double GetCurrentMetricValue(const ScalingRule& rule);

  // Check cooldown
  bool IsInCooldown(const std::string& component);

  // Perform scaling
  bool PerformScale(const std::string& component, int new_replicas, const std::string& reason);
};

}  // namespace client
}  // namespace cedar

#endif  // CEDAR_CLIENT_AUTO_SCALER_H_
