// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "cedar/dtx/txn_context.h"

#include <sstream>
#include <iomanip>

namespace cedar {
namespace dtx {

// =============================================================================
// DistributedTxnContext 实现
// =============================================================================

std::string DistributedTxnContext::Serialize() const {
  std::ostringstream oss;
  
  // 基础信息
  oss << txn_id_ << ","
      << start_ts_ << ","
      << commit_ts_ << ","
      << static_cast<int>(type_) << ","
      << static_cast<int>(state_.load()) << ","
      << coordinator_node_;
  
  // 时序窗口
  oss << "," << temporal_window_.start.value()
      << "," << temporal_window_.end.value();
  
  // 参与者
  oss << "," << participant_partitions_.size();
  for (const auto& pid : participant_partitions_) {
    oss << "," << pid;
  }
  
  // 读写集大小（完整序列化数据量太大，这里只序列化元数据）
  oss << "," << read_set_.size();
  oss << "," << write_set_.size();
  
  // 统计信息
  oss << "," << execution_time_ms_
      << "," << coord_time_ms_
      << "," << retry_count_;
  
  return oss.str();
}

std::unique_ptr<DistributedTxnContext> DistributedTxnContext::Deserialize(
    const std::string& data) {
  
  auto ctx = std::make_unique<DistributedTxnContext>();
  std::istringstream iss(data);
  std::string token;
  
  auto safe_getline = [&iss, &token]() -> bool {
    if (!std::getline(iss, token, ',')) return false;
    return !token.empty();
  };
  
  try {
    // 解析基础信息
    if (!safe_getline()) return nullptr;
    ctx->txn_id_ = std::stoull(token);
    
    if (!safe_getline()) return nullptr;
    ctx->start_ts_ = std::stoull(token);
    
    if (!safe_getline()) return nullptr;
    ctx->commit_ts_ = std::stoull(token);
    
    if (!safe_getline()) return nullptr;
    ctx->type_ = static_cast<TxnType>(std::stoi(token));
    
    if (!safe_getline()) return nullptr;
    ctx->state_.store(static_cast<DistributedTxnState>(std::stoi(token)));
    
    if (!safe_getline()) return nullptr;
    ctx->coordinator_node_ = static_cast<NodeID>(std::stoul(token));
    
    // 解析时序窗口
    if (!safe_getline()) return nullptr;
    uint64_t start_ts = std::stoull(token);
    if (!safe_getline()) return nullptr;
    uint64_t end_ts = std::stoull(token);
    ctx->temporal_window_ = TemporalWindow(Timestamp(start_ts), Timestamp(end_ts));
    
    // 解析参与者
    if (!safe_getline()) return nullptr;
    size_t participant_count = std::stoul(token);
    for (size_t i = 0; i < participant_count; ++i) {
      if (!safe_getline()) return nullptr;
      ctx->participant_partitions_.insert(static_cast<PartitionID>(std::stoul(token)));
    }
    
    // 解析读写集大小（实际数据未序列化）
    if (!safe_getline()) return nullptr;
    // size_t read_set_size = std::stoul(token);
    
    if (!safe_getline()) return nullptr;
    // size_t write_set_size = std::stoul(token);
    
    // 解析统计信息
    if (!safe_getline()) return nullptr;
    ctx->execution_time_ms_ = std::stoull(token);
    
    if (!safe_getline()) return nullptr;
    ctx->coord_time_ms_ = std::stoull(token);
    
    if (!safe_getline()) return nullptr;
    ctx->retry_count_ = static_cast<uint32_t>(std::stoul(token));
  } catch (const std::exception& e) {
    // Corrupted or tampered data
    return nullptr;
  }
  
  return ctx;
}

}  // namespace dtx
}  // namespace cedar
