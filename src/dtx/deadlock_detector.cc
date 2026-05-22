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

#include "cedar/dtx/deadlock_detector.h"

#include <algorithm>
#include <iostream>
#include <sstream>

namespace cedar {
namespace dtx {

// =============================================================================
// DeadlockDetectionResult Implementation
// =============================================================================

std::string DeadlockDetectionResult::ToString() const {
  if (!has_deadlock) {
    return "No deadlock detected";
  }
  
  std::ostringstream oss;
  oss << "Deadlock detected! Cycle: ";
  for (size_t i = 0; i < cycle.size(); ++i) {
    if (i > 0) oss << " -> ";
    oss << cycle[i];
  }
  oss << " -> " << cycle[0];
  oss << ", Victim: " << victim;
  return oss.str();
}

// =============================================================================
// WaitForGraph Implementation
// =============================================================================

void WaitForGraph::AddEdge(const WaitForEdge& edge) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  
  // 添加出边
  outgoing_edges_[edge.waiter].push_back(edge);
  
  // 添加入边
  incoming_edges_[edge.holder].push_back(edge);
  
  // 添加节点
  nodes_.insert(edge.waiter);
  nodes_.insert(edge.holder);
}

void WaitForGraph::RemoveEdge(TxnID waiter, TxnID holder) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  
  // 移除出边
  auto out_it = outgoing_edges_.find(waiter);
  if (out_it != outgoing_edges_.end()) {
    auto& edges = out_it->second;
    edges.erase(
        std::remove_if(edges.begin(), edges.end(),
            [holder](const WaitForEdge& e) { return e.holder == holder; }),
        edges.end());
    if (edges.empty()) {
      outgoing_edges_.erase(out_it);
    }
  }
  
  // 移除入边
  auto in_it = incoming_edges_.find(holder);
  if (in_it != incoming_edges_.end()) {
    auto& edges = in_it->second;
    edges.erase(
        std::remove_if(edges.begin(), edges.end(),
            [waiter](const WaitForEdge& e) { return e.waiter == waiter; }),
        edges.end());
    if (edges.empty()) {
      incoming_edges_.erase(in_it);
    }
  }
}

void WaitForGraph::RemoveTxn(TxnID txn_id) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  
  // 移除所有出边
  outgoing_edges_.erase(txn_id);
  
  // 从其他节点的出边中移除
  for (auto& [id, edges] : outgoing_edges_) {
    edges.erase(
        std::remove_if(edges.begin(), edges.end(),
            [txn_id](const WaitForEdge& e) { return e.holder == txn_id; }),
        edges.end());
  }
  
  // 移除所有入边
  incoming_edges_.erase(txn_id);
  
  // 从其他节点的入边中移除
  for (auto& [id, edges] : incoming_edges_) {
    edges.erase(
        std::remove_if(edges.begin(), edges.end(),
            [txn_id](const WaitForEdge& e) { return e.waiter == txn_id; }),
        edges.end());
  }
  
  // 移除节点
  nodes_.erase(txn_id);
}

DeadlockDetectionResult WaitForGraph::DetectDeadlock(size_t max_cycle_size) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  std::unordered_set<TxnID> visited;
  std::unordered_set<TxnID> in_stack;
  std::vector<TxnID> path;
  std::vector<TxnID> cycle;
  
  for (TxnID node : nodes_) {
    if (visited.find(node) == visited.end()) {
      if (FindCycle(node, visited, in_stack, path, cycle, 0, max_cycle_size)) {
        DeadlockDetectionResult result;
        result.has_deadlock = true;
        result.cycle = cycle;
        result.victim = SelectVictim(cycle);
        return result;
      }
    }
  }
  
  return DeadlockDetectionResult{};
}

DeadlockDetectionResult WaitForGraph::DetectDeadlockForTxn(TxnID txn_id, size_t max_cycle_size) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  if (nodes_.find(txn_id) == nodes_.end()) {
    return DeadlockDetectionResult{};
  }
  
  std::unordered_set<TxnID> visited;
  std::unordered_set<TxnID> in_stack;
  std::vector<TxnID> path;
  std::vector<TxnID> cycle;
  
  if (FindCycle(txn_id, visited, in_stack, path, cycle, 0, max_cycle_size)) {
    DeadlockDetectionResult result;
    result.has_deadlock = true;
    result.cycle = cycle;
    result.victim = SelectVictim(cycle);
    return result;
  }
  
  return DeadlockDetectionResult{};
}

bool WaitForGraph::FindCycle(TxnID start,
                              std::unordered_set<TxnID>& visited,
                              std::unordered_set<TxnID>& in_stack,
                              std::vector<TxnID>& path,
                              std::vector<TxnID>& cycle,
                              size_t depth,
                              size_t max_depth) {
  if (max_depth > 0 && depth >= max_depth) {
    return false;  // 超过最大环深度限制，防止栈溢出
  }
  
  visited.insert(start);
  in_stack.insert(start);
  path.push_back(start);
  
  auto it = outgoing_edges_.find(start);
  if (it != outgoing_edges_.end()) {
    for (const auto& edge : it->second) {
      TxnID neighbor = edge.holder;
      
      if (in_stack.find(neighbor) != in_stack.end()) {
        // 找到环
        auto cycle_start = std::find(path.begin(), path.end(), neighbor);
        cycle.assign(cycle_start, path.end());
        return true;
      }
      
      if (visited.find(neighbor) == visited.end()) {
        if (FindCycle(neighbor, visited, in_stack, path, cycle, depth + 1, max_depth)) {
          return true;
        }
      }
    }
  }
  
  path.pop_back();
  in_stack.erase(start);
  return false;
}

TxnID WaitForGraph::SelectVictim(const std::vector<TxnID>& cycle) const {
  // 选择 youngest 事务（事务ID最大的）作为牺牲者
  // 也可以考虑：事务持续时间、已做的工作量等
  if (cycle.empty()) return 0;
  return *std::max_element(cycle.begin(), cycle.end());
}

std::vector<WaitForEdge> WaitForGraph::GetWaitsFor(TxnID txn_id) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  auto it = outgoing_edges_.find(txn_id);
  if (it != outgoing_edges_.end()) {
    return it->second;
  }
  return {};
}

std::vector<WaitForEdge> WaitForGraph::GetWaitsBy(TxnID txn_id) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  auto it = incoming_edges_.find(txn_id);
  if (it != incoming_edges_.end()) {
    return it->second;
  }
  return {};
}

size_t WaitForGraph::GetEdgeCount() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  size_t count = 0;
  for (const auto& [id, edges] : outgoing_edges_) {
    count += edges.size();
  }
  return count;
}

size_t WaitForGraph::GetNodeCount() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return nodes_.size();
}

size_t WaitForGraph::CleanupExpiredEdges(uint64_t timeout_ms) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  
  size_t removed = 0;
  std::vector<std::pair<TxnID, TxnID>> edges_to_remove;
  
  for (const auto& [waiter, edges] : outgoing_edges_) {
    for (const auto& edge : edges) {
      if (edge.IsExpired(timeout_ms)) {
        edges_to_remove.emplace_back(edge.waiter, edge.holder);
        ++removed;
      }
    }
  }
  
  // 在锁内直接移除，避免解锁期间新边被误删
  for (const auto& [waiter, holder] : edges_to_remove) {
    auto out_it = outgoing_edges_.find(waiter);
    if (out_it != outgoing_edges_.end()) {
      auto& edges = out_it->second;
      edges.erase(
          std::remove_if(edges.begin(), edges.end(),
              [holder](const WaitForEdge& e) { return e.holder == holder; }),
          edges.end());
      if (edges.empty()) {
        outgoing_edges_.erase(out_it);
      }
    }
    
    auto in_it = incoming_edges_.find(holder);
    if (in_it != incoming_edges_.end()) {
      auto& edges = in_it->second;
      edges.erase(
          std::remove_if(edges.begin(), edges.end(),
              [waiter](const WaitForEdge& e) { return e.waiter == waiter; }),
          edges.end());
      if (edges.empty()) {
        incoming_edges_.erase(in_it);
      }
    }
  }
  
  return removed;
}

std::string WaitForGraph::ToString() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  size_t edge_count = 0;
  for (const auto& [id, edges] : outgoing_edges_) {
    edge_count += edges.size();
  }
  
  std::ostringstream oss;
  oss << "Wait-For Graph (" << nodes_.size() << " nodes, " << edge_count << " edges):\n";
  
  for (const auto& [waiter, edges] : outgoing_edges_) {
    oss << "  T" << waiter << " waits for: ";
    for (size_t i = 0; i < edges.size(); ++i) {
      if (i > 0) oss << ", ";
      oss << "T" << edges[i].holder;
    }
    oss << "\n";
  }
  
  return oss.str();
}

// =============================================================================
// DistributedDeadlockDetector Implementation
// =============================================================================

DistributedDeadlockDetector::DistributedDeadlockDetector()
    : graph_(std::make_unique<WaitForGraph>()) {}

DistributedDeadlockDetector::~DistributedDeadlockDetector() {
  Shutdown();
}

Status DistributedDeadlockDetector::Initialize(const Config& config) {
  if (running_.exchange(true)) {
    return Status::InvalidArgument("Already initialized");
  }
  
  config_ = config;
  
  try {
    // 启动检测线程
    detection_thread_ = std::thread([this]() {
      DetectionLoop();
    });
    
    // 启动清理线程
    cleanup_thread_ = std::thread([this]() {
      CleanupLoop();
    });
  } catch (...) {
    std::cerr << "[DeadlockDetector] Initialization exception" << std::endl;
    running_ = false;
    if (detection_thread_.joinable()) {
      detection_thread_.join();
    }
    throw;
  }
  
  return Status::OK();
}

void DistributedDeadlockDetector::Shutdown() noexcept {
  if (!running_.exchange(false)) {
    return;
  }
  
  try {
    if (detection_thread_.joinable()) {
      detection_thread_.join();
    }
  } catch (...) {
    std::cerr << "[DeadlockDetector] Detection thread join exception" << std::endl;
  }

  try {
    if (cleanup_thread_.joinable()) {
      cleanup_thread_.join();
    }
  } catch (...) {
    std::cerr << "[DeadlockDetector] Cleanup thread join exception" << std::endl;
  }
}

void DistributedDeadlockDetector::RegisterWait(TxnID txn_id, TxnID holder,
                                                PartitionID partition,
                                                const std::string& resource_id) {
  if (!running_) return;
  
  WaitForEdge edge;
  edge.waiter = txn_id;
  edge.holder = holder;
  edge.partition = partition;
  edge.created_at = std::chrono::steady_clock::now();
  edge.resource_id = resource_id;
  
  graph_->AddEdge(edge);
  
  bool has_deadlock = false;
  DeadlockDetectionResult result;
  {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    ++stats_.edges_added;
    
    // 立即检查此事务是否参与死锁
    result = graph_->DetectDeadlockForTxn(txn_id, config_.max_cycle_size);
    has_deadlock = result.has_deadlock;
  }
  if (has_deadlock) {
    HandleDeadlock(result);
  }
}

void DistributedDeadlockDetector::UnregisterWait(TxnID txn_id, TxnID holder) {
  if (!running_) return;
  
  graph_->RemoveEdge(txn_id, holder);
  
  std::lock_guard<std::mutex> lock(stats_mutex_);
  ++stats_.edges_removed;
}

void DistributedDeadlockDetector::UnregisterTxn(TxnID txn_id) {
  if (!running_) return;
  
  graph_->RemoveTxn(txn_id);
  
  std::lock_guard<std::mutex> lock(stats_mutex_);
  ++stats_.edges_removed;
}

DeadlockDetectionResult DistributedDeadlockDetector::DetectNow() {
  if (!running_) {
    return DeadlockDetectionResult{};
  }
  
  auto result = graph_->DetectDeadlock();
  
  {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    ++stats_.detections_run;
    if (result.has_deadlock) {
      ++stats_.total_deadlocks;
    }
  }
  
  if (result.has_deadlock) {
    HandleDeadlock(result);
  }
  
  return result;
}

DeadlockDetectionResult DistributedDeadlockDetector::CheckTxn(TxnID txn_id) {
  if (!running_) {
    return DeadlockDetectionResult{};
  }
  
  return graph_->DetectDeadlockForTxn(txn_id);
}

void DistributedDeadlockDetector::SetVictimHandler(VictimHandler handler) {
  victim_handler_ = handler;
}

void DistributedDeadlockDetector::DetectionLoop() {
  while (running_) {
    // 等待检测间隔
    for (uint64_t i = 0; i < config_.detection_interval_ms && running_; ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    
    if (!running_) break;
    
    // 执行死锁检测
    auto result = graph_->DetectDeadlock(config_.max_cycle_size);
    
    {
      std::lock_guard<std::mutex> lock(stats_mutex_);
      ++stats_.detections_run;
      if (result.has_deadlock) {
        ++stats_.total_deadlocks;
      }
    }
    
    if (result.has_deadlock) {
      HandleDeadlock(result);
    }
  }
}

void DistributedDeadlockDetector::CleanupLoop() {
  while (running_) {
    // 等待清理间隔
    for (uint64_t i = 0; i < config_.cleanup_interval_ms && running_; ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    
    if (!running_) break;
    
    // 清理过期边
    auto removed = graph_->CleanupExpiredEdges(config_.edge_timeout_ms);
    
    if (removed > 0) {
      std::lock_guard<std::mutex> lock(stats_mutex_);
      stats_.expired_edges_cleaned += removed;
    }
  }
}

void DistributedDeadlockDetector::HandleDeadlock(const DeadlockDetectionResult& result) {
  if (!result.has_deadlock) return;
  
  std::lock_guard<std::mutex> lock(handle_mutex_);
  
  TxnID victim = result.victim;
  
  // 调用牺牲者处理回调
  if (victim_handler_ && victim != 0) {
    try {
      victim_handler_(victim);
      
      std::lock_guard<std::mutex> stats_lock(stats_mutex_);
      ++stats_.victims_aborted;
    } catch (...) {
      std::cerr << "[DeadlockDetector] Victim handler exception" << std::endl;
    }
  } else if (victim == 0 && !result.cycle.empty()) {
    // No victim selected - log and attempt to break cycle by removing youngest
    victim = *std::max_element(result.cycle.begin(), result.cycle.end());
  }
  
  // 完全注销牺牲者事务，移除其所有等待边
  // 这确保了等待图中不再包含该事务的任何边
  if (victim != 0) {
    graph_->RemoveTxn(victim);
  }
  
  // 移除死锁环中的剩余边（如果还有）
  for (size_t i = 0; i < result.cycle.size(); ++i) {
    TxnID current = result.cycle[i];
    TxnID next = result.cycle[(i + 1) % result.cycle.size()];
    graph_->RemoveEdge(current, next);
  }
}

DistributedDeadlockDetector::Stats DistributedDeadlockDetector::GetStats() const {
  std::lock_guard<std::mutex> lock(stats_mutex_);
  return stats_;
}

void DistributedDeadlockDetector::ResetStats() {
  std::lock_guard<std::mutex> lock(stats_mutex_);
  stats_ = Stats{};
}

std::string DistributedDeadlockDetector::GetGraphInfo() const {
  return graph_->ToString();
}

// =============================================================================
// Global Instance
// =============================================================================

DistributedDeadlockDetector* GetGlobalDeadlockDetector() {
  static DistributedDeadlockDetector instance;
  return &instance;
}

}  // namespace dtx
}  // namespace cedar
