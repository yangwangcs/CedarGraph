// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#include "cedar/dtx/partition.h"
#include "cedar/partition/strategies/static_hash_strategy.h"
#include "cedar/partition/strategies/mth_stream_strategy.h"
#include "cedar/partition/partition_strategy.h"
#include <sstream>

namespace cedar {
namespace dtx {

// PIMPL struct definition
struct DualModePartitionStrategy::SubStrategies {
  std::unique_ptr<cedar::partition::StaticHashStrategy> static_hash;
  std::unique_ptr<cedar::partition::MTHStreamStrategy> mth_stream;
};

DualModePartitionStrategy::DualModePartitionStrategy(const Config& config)
    : config_(config),
      mode_(config.mode),
      sub_(std::make_unique<SubStrategies>()) {
  InitializeSubStrategies();
}

DualModePartitionStrategy::~DualModePartitionStrategy() = default;

void DualModePartitionStrategy::InitializeSubStrategies() {
  // Initialize StaticHash strategy
  sub_->static_hash = std::make_unique<cedar::partition::StaticHashStrategy>(
      static_cast<uint32_t>(config_.num_partitions));
  
  // Initialize MTHStream strategy
  cedar::partition::MTHStreamStrategy::Config mth_config;
  mth_config.num_partitions = static_cast<uint32_t>(config_.num_partitions);
  mth_config.capacity = config_.sketch_capacity;
  mth_config.alpha = config_.mth_alpha;
  mth_config.beta = config_.mth_beta;
  mth_config.gamma = config_.mth_gamma;
  mth_config.eta = config_.mth_eta;
  mth_config.temporal_alpha = config_.temporal_alpha;
  mth_config.sketch_depth = config_.sketch_depth;
  mth_config.sketch_width = config_.sketch_width;
  mth_config.fast_path_threshold = config_.fast_path_threshold;
  mth_config.load_relaxation = config_.load_relaxation;
  mth_config.decay_interval = config_.decay_interval;
  mth_config.decay_factor = config_.decay_factor;
  
  sub_->mth_stream = std::make_unique<cedar::partition::MTHStreamStrategy>(mth_config);
}

PartitionID DualModePartitionStrategy::ComputePartition(const CedarKey& key, 
                                                         PartitionID num_partitions) {
  // If key already has distributed flag and part_id set, use it
  if (key.IsDistributed() && key.part_id() != 0) {
    return key.part_id();
  }
  
  Mode current_mode = mode_.load();
  
  switch (current_mode) {
    case Mode::STATIC_HASH: {
      // Simple hash-based routing
      auto assign = sub_->static_hash->RouteVertex(key.entity_id());
      return static_cast<PartitionID>(assign.partition_id);
    }
    
    case Mode::MTH_STREAM: {
      // Convert to GraphEvent
      cedar::partition::GraphEvent event;
      event.entity_id = key.entity_id();
      event.target_id = key.target_id();
      event.timestamp = key.timestamp().value();
      event.type_id = key.column_id();
      
      // Convert EntityType to uint8
      switch (key.entity_type()) {
        case EntityType::Vertex:
          event.entity_type = 0;
          break;
        case EntityType::EdgeOut:
          event.entity_type = 1;
          break;
        case EntityType::EdgeIn:
          event.entity_type = 2;
          break;
      }
      
      event.op_type = key.GetOpType();
      
      // Batch sketch updates to amortize lock contention in MTHPartitioner::AssignEvent
      thread_local std::vector<cedar::partition::GraphEvent> event_batch;
      constexpr size_t kMthBatchSize = 32;
      event_batch.push_back(event);
      if (event_batch.size() >= kMthBatchSize) {
        sub_->mth_stream->ProcessEventStream(event_batch);
        event_batch.clear();
      }
      
      // Route using temporal-aware algorithm (read-only on sketch, fully concurrent)
      auto assign = sub_->mth_stream->RouteVertexTemporal(
          key.entity_id(), 
          key.timestamp().value());
      return static_cast<PartitionID>(assign.partition_id);
    }
    
    case Mode::AUTO: {
      // Auto-select based on statistics
      if (stats_.total > config_.temporal_query_threshold) {
        double temporal_ratio = static_cast<double>(stats_.temporal.load()) 
                                / stats_.total.load();
        if (temporal_ratio > 0.5) {
          // Use MTH mode for temporal-heavy workloads
          cedar::partition::GraphEvent event;
          event.entity_id = key.entity_id();
          event.target_id = key.target_id();
          event.timestamp = key.timestamp().value();
          event.type_id = key.column_id();
          
          switch (key.entity_type()) {
            case EntityType::Vertex:
              event.entity_type = 0;
              break;
            case EntityType::EdgeOut:
              event.entity_type = 1;
              break;
            case EntityType::EdgeIn:
              event.entity_type = 2;
              break;
          }
          
          event.op_type = key.GetOpType();
          
          // Batch sketch updates to amortize lock contention
          thread_local std::vector<cedar::partition::GraphEvent> event_batch;
          constexpr size_t kMthBatchSize = 32;
          event_batch.push_back(event);
          if (event_batch.size() >= kMthBatchSize) {
            sub_->mth_stream->ProcessEventStream(event_batch);
            event_batch.clear();
          }
          
          auto assign = sub_->mth_stream->RouteVertexTemporal(
              key.entity_id(), 
              key.timestamp().value());
          return static_cast<PartitionID>(assign.partition_id);
        }
      }
      
      auto assign = sub_->static_hash->RouteVertex(key.entity_id());
      return static_cast<PartitionID>(assign.partition_id);
    }
  }
  
  // Fallback to simple hash
  return static_cast<PartitionID>(key.entity_id() % num_partitions);
}

std::string DualModePartitionStrategy::Name() const {
  switch (mode_.load()) {
    case Mode::STATIC_HASH: return "DualMode(StaticHash)";
    case Mode::MTH_STREAM: return "DualMode(MTHStream)";
    case Mode::AUTO: return "DualMode(Auto)";
  }
  return "DualMode(Unknown)";
}

void DualModePartitionStrategy::SetMode(Mode mode) {
  mode_.store(mode);
}

void DualModePartitionStrategy::UpdateQueryStats(bool is_temporal_query, 
                                                  bool has_locality) {
  stats_.total++;
  if (is_temporal_query) stats_.temporal++;
  if (has_locality) stats_.locality++;
}

std::string DualModePartitionStrategy::GetStats() const {
  std::ostringstream oss;
  oss << "DualModePartitionStrategy Stats:\n"
      << "  Mode: " << Name() << "\n"
      << "  Total Queries: " << stats_.total.load() << "\n"
      << "  Temporal Queries: " << stats_.temporal.load() << "\n"
      << "  Locality Queries: " << stats_.locality.load() << "\n";
  
  auto mth_stats = sub_->mth_stream->GetStats();
  if (mth_stats.ok()) {
    oss << "\n" << mth_stats.ValueOrDie() << "\n";
  }
  
  return oss.str();
}

}  // namespace dtx
}  // namespace cedar
