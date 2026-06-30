// Copyright 2025 The Cedar Authors
//
// Auto Scaler implementation

#include "cedar/client/auto_scaler.h"
#include "cedar/client/cluster_manager.h"
#include "cedar/client/cluster_monitor.h"

#include <chrono>
#include <iostream>

namespace cedar {
namespace client {

AutoScaler::AutoScaler() = default;

AutoScaler::~AutoScaler() {
  Stop();
}

bool AutoScaler::Initialize(ClusterManager* cluster_manager,
                             ClusterMonitor* cluster_monitor) {
  cluster_manager_ = cluster_manager;
  cluster_monitor_ = cluster_monitor;
  return true;
}

bool AutoScaler::Start(int interval_seconds) {
  if (interval_seconds <= 0) {
    return false;
  }
  if (running_) {
    return true;
  }

  running_ = true;
  scaler_thread_ = std::thread([this, interval_seconds]() {
    while (running_) {
      EvaluateRules();
      std::unique_lock<std::mutex> lock(scaler_cv_mutex_);
      scaler_cv_.wait_for(lock, std::chrono::seconds(interval_seconds),
                          [this]() { return !running_.load(); });
    }
  });

  return true;
}

void AutoScaler::Stop() {
  running_ = false;
  scaler_cv_.notify_all();
  if (scaler_thread_.joinable()) {
    scaler_thread_.join();
  }
}

void AutoScaler::AddRule(const ScalingRule& rule) {
  std::lock_guard<std::mutex> lock(mutex_);
  rules_.push_back(rule);
}

void AutoScaler::RemoveRule(const std::string& component) {
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto it = rules_.begin(); it != rules_.end(); ++it) {
    if (it->component == component) {
      rules_.erase(it);
      break;
    }
  }
}

std::vector<ScalingRule> AutoScaler::GetRules() {
  std::lock_guard<std::mutex> lock(mutex_);
  return rules_;
}

std::vector<ScalingEvent> AutoScaler::GetEvents(int limit) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (limit <= 0) {
    return {};
  }
  if (events_.size() > limit) {
    return std::vector<ScalingEvent>(events_.end() - limit, events_.end());
  }
  return events_;
}

void AutoScaler::SetCallback(ScalingCallback callback) {
  std::lock_guard<std::mutex> lock(mutex_);
  callback_ = callback;
}

bool AutoScaler::ScaleUp(const std::string& component) {
  if (!cluster_manager_) {
    return false;
  }
  return cluster_manager_->ScaleUp(component);
}

bool AutoScaler::ScaleDown(const std::string& component) {
  if (!cluster_manager_) {
    return false;
  }
  return cluster_manager_->ScaleDown(component);
}

int AutoScaler::GetCurrentReplicas(const std::string& component) {
  if (!cluster_manager_) {
    return 0;
  }

  auto status = cluster_manager_->GetClusterStatus();

  if (component == "metad") {
    return status.metad_nodes;
  } else if (component == "storaged") {
    return status.storaged_nodes;
  } else if (component == "graphd") {
    return status.graphd_nodes;
  } else if (component == "queryd") {
    return status.queryd_nodes;
  }

  return 0;
}

// ============================================================================
// Private methods
// ============================================================================

void AutoScaler::ScalerLoop() {
  while (running_) {
    EvaluateRules();
    std::unique_lock<std::mutex> lock(scaler_cv_mutex_);
    scaler_cv_.wait_for(lock, std::chrono::seconds(60),
                        [this]() { return !running_.load(); });
  }
}

void AutoScaler::EvaluateRules() {
  std::vector<ScalingRule> rules;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    rules = rules_;
  }

  for (const auto& rule : rules) {
    // Check cooldown
    if (IsInCooldown(rule.component)) {
      continue;
    }

    // Get current metric value
    double current_value = GetCurrentMetricValue(rule);

    // Check if scaling is needed
    if (ShouldScaleUp(rule, current_value)) {
      int current_replicas = GetCurrentReplicas(rule.component);
      int new_replicas = std::min(current_replicas + 1, rule.max_replicas);

      if (new_replicas > current_replicas) {
        std::string reason = "Scale up due to " + 
                            std::to_string(current_value) + " > " + 
                            std::to_string(rule.scale_up_threshold);
        PerformScale(rule.component, new_replicas, reason);
      }
    } else if (ShouldScaleDown(rule, current_value)) {
      int current_replicas = GetCurrentReplicas(rule.component);
      int new_replicas = std::max(current_replicas - 1, rule.min_replicas);

      if (new_replicas < current_replicas) {
        std::string reason = "Scale down due to " + 
                            std::to_string(current_value) + " < " + 
                            std::to_string(rule.scale_down_threshold);
        PerformScale(rule.component, new_replicas, reason);
      }
    }
  }
}

bool AutoScaler::ShouldScaleUp(const ScalingRule& rule, double current_value) {
  return current_value > rule.scale_up_threshold;
}

bool AutoScaler::ShouldScaleDown(const ScalingRule& rule, double current_value) {
  return current_value < rule.scale_down_threshold;
}

double AutoScaler::GetCurrentMetricValue(const ScalingRule& rule) {
  if (!cluster_monitor_) {
    return 0.0;
  }

  auto metrics = cluster_monitor_->GetClusterMetrics();

  switch (rule.policy) {
    case ScalingPolicy::CPU_BASED:
      return metrics.cpu_usage_avg;
    case ScalingPolicy::MEMORY_BASED:
      return metrics.memory_usage_avg;
    case ScalingPolicy::QPS_BASED:
      return metrics.qps;
    case ScalingPolicy::LATENCY_BASED:
      return metrics.latency_p95;
    default:
      return 0.0;
  }
}

bool AutoScaler::IsInCooldown(const std::string& component) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = last_scaling_time_.find(component);
  if (it == last_scaling_time_.end()) {
    return false;
  }

  auto now = std::chrono::system_clock::now().time_since_epoch().count();
  auto elapsed = (now - it->second) / 1000000000;  // Convert to seconds

  int cooldown_seconds = 300;
  for (const auto& rule : rules_) {
    if (rule.component == component) {
      cooldown_seconds = rule.cooldown_seconds;
      break;
    }
  }

  return elapsed < cooldown_seconds;
}

bool AutoScaler::PerformScale(const std::string& component, int new_replicas,
                               const std::string& reason) {
  if (!cluster_manager_) {
    return false;
  }

  int old_replicas = GetCurrentReplicas(component);

  bool success = cluster_manager_->ScaleComponent(component, new_replicas);

  if (success) {
    ScalingEvent event;
    event.component = component;
    event.old_replicas = old_replicas;
    event.new_replicas = new_replicas;
    event.reason = reason;
    event.timestamp = std::chrono::system_clock::now().time_since_epoch().count();

    ScalingCallback callback;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      last_scaling_time_[component] = event.timestamp;
      events_.push_back(event);
      callback = callback_;
    }

    if (callback) {
      try {
        callback(event);
      } catch (const std::exception& e) {
        std::cerr << "Scaling callback exception: " << e.what() << std::endl;
      } catch (...) {
        std::cerr << "Scaling callback unknown exception" << std::endl;
      }
    }
  }

  return success;
}

}  // namespace client
}  // namespace cedar
