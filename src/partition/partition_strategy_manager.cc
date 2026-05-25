// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#include "cedar/partition/partition_strategy_manager.h"

#include <sstream>

namespace cedar {
namespace partition {

PartitionStrategyManager::PartitionStrategyManager() = default;

PartitionStrategyManager::~PartitionStrategyManager() = default;

Status PartitionStrategyManager::Initialize(const StrategySelectionConfig& config) {
  std::lock_guard<std::mutex> lock(mutex_);
  config_ = config;
  return Status::OK();
}

Status PartitionStrategyManager::RegisterStrategy(
    std::unique_ptr<IPartitionStrategy> strategy) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (!strategy) {
    return Status::InvalidArgument("Strategy cannot be null");
  }
  
  std::string name = strategy->Name();
  if (strategies_.find(name) != strategies_.end()) {
    return Status::Conflict("Strategy already registered: " + name);
  }
  
  strategies_[name] = std::move(strategy);
  
  // 如果这是第一个策略，设为活跃
  if (!active_strategy_) {
    active_strategy_ = strategies_[name].get();
  }
  
  return Status::OK();
}

Status PartitionStrategyManager::SetActiveStrategy(StrategyType type) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::string target_name;
  switch (type) {
    case StrategyType::STATIC_HASH:
      target_name = "StaticHash";
      break;
    case StrategyType::MTH_STREAM:
      target_name = "MTHStream";
      break;
    case StrategyType::AUTO:
      auto_mode_ = true;
      // Start with the default strategy (STATIC_HASH) and let
      // MaybeAutoSwitchStrategy evolve based on workload.
      target_name = "StaticHash";
      break;
    default:
      return Status::InvalidArgument("Unknown strategy type");
  }
  
  auto it = strategies_.find(target_name);
  if (it == strategies_.end()) {
    return Status::NotFound("Strategy not registered: " + target_name);
  }
  
  active_strategy_ = it->second.get();
  return Status::OK();
}

Status PartitionStrategyManager::SetActiveStrategy(const std::string& strategy_name) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto it = strategies_.find(strategy_name);
  if (it == strategies_.end()) {
    return Status::NotFound("Strategy not registered: " + strategy_name);
  }
  
  active_strategy_ = it->second.get();
  return Status::OK();
}

IPartitionStrategy* PartitionStrategyManager::GetActiveStrategy() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return active_strategy_;
}

PartitionAssignment PartitionStrategyManager::RouteVertex(uint64_t vertex_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (!active_strategy_) {
    // Fallback: use modulo
    return PartitionAssignment(static_cast<uint32_t>(vertex_id % 65536), 1.0, "Fallback");
  }
  
  return active_strategy_->RouteVertex(vertex_id);
}

PartitionAssignment PartitionStrategyManager::RouteVertexTemporal(
    uint64_t vertex_id, uint64_t timestamp) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (!active_strategy_) {
    return PartitionAssignment(static_cast<uint32_t>(vertex_id % 65536), 1.0, "Fallback");
  }
  
  return active_strategy_->RouteVertexTemporal(vertex_id, timestamp);
}

std::pair<PartitionAssignment, PartitionAssignment> 
PartitionStrategyManager::RouteEdge(uint64_t src_id, uint64_t dst_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (!active_strategy_) {
    auto src_part = static_cast<uint32_t>(src_id % 65536);
    auto dst_part = static_cast<uint32_t>(dst_id % 65536);
    return {
      PartitionAssignment(src_part, 1.0, "Fallback"),
      PartitionAssignment(dst_part, 1.0, "Fallback")
    };
  }
  
  return active_strategy_->RouteEdge(src_id, dst_id);
}

Status PartitionStrategyManager::ProcessEventStream(const std::vector<GraphEvent>& events) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (!active_strategy_) {
    return Status::InvalidArgument("No active strategy");
  }
  
  return active_strategy_->ProcessEventStream(events);
}

void PartitionStrategyManager::UpdateQueryStats(bool is_temporal_query, bool has_locality) {
  std::lock_guard<std::mutex> lock(mutex_);

  stats_.total_queries++;
  if (is_temporal_query) {
    stats_.temporal_queries++;
  }
  if (has_locality) {
    stats_.locality_queries++;
  }

  if (auto_mode_) {
    MaybeAutoSwitchStrategyUnlocked();
  }
}

void PartitionStrategyManager::MaybeAutoSwitchStrategy() {
  std::lock_guard<std::mutex> lock(mutex_);
  MaybeAutoSwitchStrategyUnlocked();
}

void PartitionStrategyManager::MaybeAutoSwitchStrategyUnlocked() {
  if (!config_.enable_auto_selection) {
    return;
  }
  
  if (stats_.total_queries < config_.temporal_query_threshold) {
    return;
  }
  
  double temporal_ratio = static_cast<double>(stats_.temporal_queries) / stats_.total_queries;
  double locality_ratio = static_cast<double>(stats_.locality_queries) / stats_.total_queries;
  
  // 如果时态查询比例高且局部性好，切换到 MTH
  if (temporal_ratio > 0.5 && locality_ratio > config_.locality_ratio_threshold) {
    auto it = strategies_.find("MTHStream");
    if (it != strategies_.end()) {
      active_strategy_ = it->second.get();
    }
  }
}

std::string PartitionStrategyManager::GetAllStats() const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::ostringstream oss;
  oss << "=== Partition Strategy Manager Stats ===\n";
  oss << "Active Strategy: " << (active_strategy_ ? active_strategy_->Name() : "None") << "\n";
  oss << "Registered Strategies: " << strategies_.size() << "\n";
  
  for (const auto& [name, strategy] : strategies_) {
    oss << "\n--- " << name << " ---\n";
    auto stats_result = strategy->GetStats();
    if (stats_result.ok()) {
      oss << stats_result.ValueOrDie() << "\n";
    } else {
      oss << "  Stats not available\n";
    }
  }
  
  oss << "\n--- Query Stats ---\n";
  oss << "  Total Queries: " << stats_.total_queries << "\n";
  oss << "  Temporal Queries: " << stats_.temporal_queries << "\n";
  oss << "  Locality Queries: " << stats_.locality_queries << "\n";
  
  return oss.str();
}

} // namespace partition
} // namespace cedar
