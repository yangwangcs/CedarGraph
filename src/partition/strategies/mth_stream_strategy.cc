// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#include "partition/strategies/mth_stream_strategy.h"
#include "partition/mth/op_type.h"
#include <sstream>

namespace cedar {
namespace partition {

MTHStreamStrategy::MTHStreamStrategy(const Config& config)
    : config_(config),
      partitioner_(std::make_unique<MTHPartitioner>(
          static_cast<uint16_t>(config.num_partitions),
          config.capacity,
          config.alpha,
          config.beta,
          config.gamma,
          config.eta,
          config.temporal_alpha,
          config.sketch_depth,
          config.sketch_width,
          config.fast_path_threshold,
          config.load_relaxation,
          config.decay_interval,
          config.decay_factor)) {}

PartitionAssignment MTHStreamStrategy::RouteVertex(uint64_t vertex_id) {
  uint64_t now = 0;  // Use 0 timestamp for non-temporal routing
  auto [pid, score] = partitioner_->sketch().SuggestPartition(vertex_id, now);
  return PartitionAssignment(pid, std::min(1.0, score / config_.fast_path_threshold), "MTHStream");
}

PartitionAssignment MTHStreamStrategy::RouteVertexTemporal(
    uint64_t vertex_id, uint64_t timestamp) {
  // Convert microseconds to big-endian for internal use
  uint64_t ts_be = CedarKey::EncodeTimestamp(timestamp);
  auto [pid, score] = partitioner_->sketch().SuggestPartition(vertex_id, ts_be);
  return PartitionAssignment(pid, std::min(1.0, score / config_.fast_path_threshold), "MTHStream");
}

std::pair<PartitionAssignment, PartitionAssignment> 
MTHStreamStrategy::RouteEdge(uint64_t src_id, uint64_t dst_id) {
  // For edges, route by both endpoints
  return {
    RouteVertex(src_id),
    RouteVertex(dst_id)
  };
}

Status MTHStreamStrategy::ProcessEventStream(const std::vector<GraphEvent>& events) {
  for (const auto& event : events) {
    CedarKey key = ConvertToCedarKey(event);
    partitioner_->AssignEvent(key);
  }
  return Status::OK();
}

Status MTHStreamStrategy::Configure(const std::string& key, const std::string& value) {
  try {
    if (key == "fast_path_threshold") {
      config_.fast_path_threshold = std::stod(value);
    } else if (key == "temporal_alpha") {
      config_.temporal_alpha = std::stod(value);
    } else if (key == "load_relaxation") {
      config_.load_relaxation = std::stod(value);
    } else if (key == "decay_interval") {
      config_.decay_interval = std::stoi(value);
    } else if (key == "decay_factor") {
      config_.decay_factor = std::stod(value);
    } else {
      return Status::InvalidArgument("Unknown configuration key: " + key);
    }
    return Status::OK();
  } catch (...) {
    return Status::InvalidArgument("Invalid value for key: " + key);
  }
}

StatusOr<std::string> MTHStreamStrategy::GetStats() const {
  std::ostringstream oss;
  oss << "MTHStreamStrategy Stats:\n"
      << "  Num Partitions: " << config_.num_partitions << "\n"
      << "  Fast Path Threshold: " << config_.fast_path_threshold << "\n"
      << "  Fast Path Ratio: " << GetFastPathRatio() << "\n"
      << "  Sketch Depth: " << config_.sketch_depth << "\n"
      << "  Sketch Width: " << config_.sketch_width << "\n"
      << "  Temporal Alpha: " << config_.temporal_alpha << "\n"
      << "  Load Relaxation: " << config_.load_relaxation;
  return oss.str();
}

double MTHStreamStrategy::GetFastPathRatio() const {
  return partitioner_->FastPathRatio();
}

void MTHStreamStrategy::WarmStartFrom(const MTHStreamStrategy& other) {
  partitioner_->WarmStart(other.partitioner_->sketch());
}

CedarKey MTHStreamStrategy::ConvertToCedarKey(const GraphEvent& event) {
  uint64_t ts_be = CedarKey::EncodeTimestamp(event.timestamp);
  uint8_t flags = event.op_type & KeyFlags::kOpTypeMask;
  
  if (event.entity_type == 0) {  // Vertex
    return CedarKey::Vertex(event.entity_id, event.type_id, ts_be, 0, 0, 0, flags);
  } else if (event.entity_type == 1) {  // EdgeOut
    return CedarKey::EdgeOut(event.entity_id, event.target_id, event.type_id, ts_be, 0, 0, flags);
  } else {  // EdgeIn
    return CedarKey::EdgeIn(event.entity_id, event.target_id, event.type_id, ts_be, 0, 0, flags);
  }
}

} // namespace partition
} // namespace cedar
