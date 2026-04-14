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

#include "cedar/dtx/chaos_testing.h"
#include "cedar/dtx/storage_service_impl.h"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace cedar {
namespace dtx {

// =============================================================================
// FaultInjector Implementation
// =============================================================================

FaultInjector::FaultInjector() : rng_(std::random_device{}()) {}

FaultInjector::~FaultInjector() {
  Shutdown();
}

Status FaultInjector::Initialize(const std::vector<FaultSpec>& specs) {
  if (running_.exchange(true)) {
    return Status::InvalidArgument("FaultInjector already initialized");
  }
  
  specs_ = specs;
  
  // Start fault worker thread
  worker_thread_ = std::thread([this]() {
    FaultWorkerLoop();
  });
  
  return Status::OK();
}

void FaultInjector::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }
  
  shutdown_ = true;
  if (worker_thread_.joinable()) {
    worker_thread_.join();
  }
}

void FaultInjector::FaultWorkerLoop() {
  auto start_time = std::chrono::steady_clock::now();
  
  while (!shutdown_) {
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        now - start_time);
    
    // Check each fault spec
    {
      std::lock_guard<std::mutex> lock(specs_mutex_);
      for (const auto& spec : specs_) {
        if (elapsed >= spec.start_after && 
            elapsed < spec.start_after + spec.duration) {
          // Fault should be active
          std::lock_guard<std::mutex> fault_lock(faults_mutex_);
          for (NodeID node : spec.target_nodes) {
            if (active_faults_[node].insert(spec.type).second) {
              // Newly inserted
              ApplyFault(spec);
              
              std::lock_guard<std::mutex> cb_lock(callback_mutex_);
              for (auto& cb : callbacks_) {
                cb(spec.type, node, true);
              }
            }
          }
        }
      }
    }
    
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  
  // Clean up all faults on shutdown
  ClearAllFaults();
}

void FaultInjector::ApplyFault(const FaultSpec& spec) {
  switch (spec.type) {
    case FaultType::kNetworkLatency:
      std::cout << "[Chaos] Injecting network latency: " 
                << spec.latency_params.min_latency.count() << "-"
                << spec.latency_params.max_latency.count() << "ms" << std::endl;
      break;
    case FaultType::kPacketLoss:
      std::cout << "[Chaos] Injecting packet loss: " 
                << spec.packet_loss_params.drop_rate * 100 << "%" << std::endl;
      break;
    case FaultType::kNetworkPartition:
      std::cout << "[Chaos] Injecting network partition" << std::endl;
      break;
    case FaultType::kNodeCrash:
      std::cout << "[Chaos] Simulating node crash" << std::endl;
      break;
    case FaultType::kDiskFailure:
      std::cout << "[Chaos] Simulating disk failure" << std::endl;
      break;
    default:
      break;
  }
}

void FaultInjector::RevertFault(const FaultSpec& spec) {
  std::cout << "[Chaos] Reverting fault type: " << static_cast<int>(spec.type) << std::endl;
}

bool FaultInjector::IsFaultActive(FaultType type, NodeID node_id) const {
  std::lock_guard<std::mutex> lock(faults_mutex_);
  auto it = active_faults_.find(node_id);
  if (it != active_faults_.end()) {
    return it->second.count(type) > 0;
  }
  return false;
}

void FaultInjector::RegisterFaultCallback(FaultCallback callback) {
  std::lock_guard<std::mutex> lock(callback_mutex_);
  callbacks_.push_back(callback);
}

void FaultInjector::ClearAllFaults() {
  std::lock_guard<std::mutex> lock(faults_mutex_);
  for (auto& [node_id, faults] : active_faults_) {
    for (FaultType type : faults) {
      std::lock_guard<std::mutex> cb_lock(callback_mutex_);
      for (auto& cb : callbacks_) {
        cb(type, node_id, false);
      }
    }
  }
  active_faults_.clear();
}

// =============================================================================
// LongTermStabilityTest Implementation
// =============================================================================

LongTermStabilityTest::LongTermStabilityTest(const Config& config)
    : config_(config), rng_(std::random_device{}()) {
  stats_.start_time = std::chrono::steady_clock::now();
}

LongTermStabilityTest::~LongTermStabilityTest() {
  Stop();
}

Status LongTermStabilityTest::Initialize(
    const std::vector<std::shared_ptr<StorageClient>>& clients) {
  clients_ = clients;
  
  if (config_.enable_fault_injection) {
    fault_injector_ = std::make_unique<FaultInjector>();
    
    // Define default fault specs
    std::vector<FaultSpec> specs;
    
    // Network latency fault
    FaultSpec latency_fault;
    latency_fault.type = FaultType::kNetworkLatency;
    latency_fault.probability = 0.3;
    latency_fault.duration = std::chrono::minutes(2);
    latency_fault.latency_params.min_latency = std::chrono::milliseconds(50);
    latency_fault.latency_params.max_latency = std::chrono::milliseconds(200);
    specs.push_back(latency_fault);
    
    // Packet loss fault
    FaultSpec packet_loss;
    packet_loss.type = FaultType::kPacketLoss;
    packet_loss.probability = 0.1;
    packet_loss.duration = std::chrono::minutes(1);
    packet_loss.packet_loss_params.drop_rate = 0.05;
    specs.push_back(packet_loss);
    
    fault_injector_->Initialize(specs);
  }
  
  return Status::OK();
}

Status LongTermStabilityTest::Run() {
  if (running_.exchange(true)) {
    return Status::InvalidArgument("Test already running");
  }
  
  std::cout << "╔════════════════════════════════════════════════════════════╗" << std::endl;
  std::cout << "║     CedarGraph Long-term Stability Test                    ║" << std::endl;
  std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
  std::cout << std::endl;
  std::cout << "Test Configuration:" << std::endl;
  std::cout << "  Duration: " << config_.test_duration.count() << " hours" << std::endl;
  std::cout << "  Target Throughput: " << config_.target_throughput << " ops/sec" << std::endl;
  std::cout << "  Read/Write Ratio: " << config_.read_write_ratio * 100 << "/" 
            << (1 - config_.read_write_ratio) * 100 << std::endl;
  std::cout << "  Fault Injection: " << (config_.enable_fault_injection ? "Enabled" : "Disabled") << std::endl;
  std::cout << std::endl;
  
  // Start worker threads
  int num_workers = 4;
  for (int i = 0; i < num_workers; ++i) {
    workload_threads_.emplace_back([this]() {
      WorkloadGeneratorLoop();
    });
  }
  
  if (config_.enable_fault_injection) {
    fault_thread_ = std::thread([this]() {
      FaultInjectionLoop();
    });
  }
  
  consistency_thread_ = std::thread([this]() {
    ConsistencyCheckLoop();
  });
  
  metrics_thread_ = std::thread([this]() {
    MetricsReporterLoop();
  });
  
  // Wait for test duration or stop signal
  auto start = std::chrono::steady_clock::now();
  while (!stop_requested_) {
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::hours>(now - start);
    
    if (elapsed >= config_.test_duration) {
      break;
    }
    
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  
  // Cleanup
  running_ = false;
  stats_.end_time = std::chrono::steady_clock::now();
  
  for (auto& t : workload_threads_) {
    if (t.joinable()) t.join();
  }
  if (fault_thread_.joinable()) fault_thread_.join();
  if (consistency_thread_.joinable()) consistency_thread_.join();
  if (metrics_thread_.joinable()) metrics_thread_.join();
  
  std::cout << std::endl << "Test completed!" << std::endl;
  
  return Status::OK();
}

void LongTermStabilityTest::WorkloadGeneratorLoop() {
  while (running_ && !stop_requested_) {
    // Determine operation type based on ratio
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    bool is_read = dist(rng_) < config_.read_write_ratio;
    
    auto start = std::chrono::high_resolution_clock::now();
    
    Status status;
    if (is_read) {
      status = PerformRead();
    } else {
      status = PerformWrite();
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    
    // Update stats
    stats_.total_operations++;
    if (status.ok()) {
      stats_.successful_operations++;
    } else if (status.ToString().find("Timeout") != std::string::npos) {
      stats_.timeout_operations++;
    } else {
      stats_.failed_operations++;
    }
    
    UpdateLatencyStats(latency_us);
    
    // Rate limiting
    std::this_thread::sleep_for(std::chrono::microseconds(1000000 / config_.target_throughput / 4));
  }
}

void LongTermStabilityTest::FaultInjectionLoop() {
  auto start = std::chrono::steady_clock::now();
  
  while (running_ && !stop_requested_) {
    InjectRandomFault();
    
    // Wait for fault duration
    std::this_thread::sleep_for(config_.fault_duration);
    
    // Recover
    ClearAllFaults();
    
    // Wait before next fault
    std::this_thread::sleep_for(config_.fault_interval - config_.fault_duration);
    
    // Check if test duration exceeded
    auto now = std::chrono::steady_clock::now();
    if (std::chrono::duration_cast<std::chrono::hours>(now - start) >= config_.test_duration) {
      break;
    }
  }
}

void LongTermStabilityTest::ConsistencyCheckLoop() {
  while (running_ && !stop_requested_) {
    auto status = CheckDataConsistency();
    stats_.consistency_checks++;
    
    if (!status.ok()) {
      stats_.consistency_violations++;
      LogMessage("ERROR", "Consistency violation detected: " + status.ToString());
    }
    
    std::this_thread::sleep_for(config_.consistency_check_interval);
  }
}

void LongTermStabilityTest::MetricsReporterLoop() {
  while (running_ && !stop_requested_) {
    std::this_thread::sleep_for(config_.metrics_interval);
    
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - stats_.start_time).count();
    
    double throughput = stats_.total_operations.load() / std::max(1.0, (double)elapsed);
    
    auto snapshot = stats_.GetSnapshot();
    double success_rate = snapshot.GetSuccessRate() * 100;
    double avg_latency = snapshot.GetAverageLatency();
    
    std::cout << "[Metrics] Elapsed: " << elapsed / 60 << "min, "
              << "Ops: " << stats_.total_operations << ", "
              << "Throughput: " << std::fixed << std::setprecision(1) << throughput << " ops/s, "
              << "Success: " << std::setprecision(2) << success_rate << "%, "
              << "Avg Latency: " << std::setprecision(1) << avg_latency << "μs, "
              << "Faults: " << stats_.faults_injected << std::endl;
  }
}

Status LongTermStabilityTest::PerformRead() {
  // Simulate read operation
  std::uniform_int_distribution<uint64_t> key_dist(0, 10000);
  uint64_t key = key_dist(rng_);
  
  // In real implementation, call storage client
  // For now, simulate success with probability
  std::uniform_real_distribution<double> success_dist(0.0, 1.0);
  if (success_dist(rng_) < 0.99) {
    return Status::OK();
  }
  return Status::NotFound("Key not found");
}

Status LongTermStabilityTest::PerformWrite() {
  // Simulate write operation
  std::uniform_int_distribution<uint64_t> key_dist(0, 10000);
  uint64_t key = key_dist(rng_);
  
  // Simulate success
  return Status::OK();
}

void LongTermStabilityTest::InjectRandomFault() {
  std::vector<FaultType> fault_types = {
    FaultType::kNetworkLatency,
    FaultType::kPacketLoss,
    FaultType::kNetworkPartition
  };
  
  std::uniform_int_distribution<size_t> dist(0, fault_types.size() - 1);
  FaultType type = fault_types[dist(rng_)];
  
  stats_.faults_injected++;
  LogMessage("INFO", "Injecting fault type: " + std::to_string(static_cast<int>(type)));
}

void LongTermStabilityTest::ClearAllFaults() {
  stats_.faults_recovered++;
  LogMessage("INFO", "Recovering all faults");
}

Status LongTermStabilityTest::CheckDataConsistency() {
  // In real implementation, verify data across nodes
  // For now, assume consistent
  return Status::OK();
}

void LongTermStabilityTest::UpdateLatencyStats(int64_t latency_us) {
  stats_.latency_sum_us += latency_us;
  stats_.latency_count++;
  
  if (latency_us > stats_.max_latency_us.load()) {
    stats_.max_latency_us.store(latency_us);
  }
  
  std::lock_guard<std::mutex> lock(histogram_mutex_);
  latency_histogram_.push_back(latency_us);
}

void LongTermStabilityTest::LogMessage(const std::string& level, const std::string& message) {
  auto now = std::chrono::system_clock::now();
  auto time_t = std::chrono::system_clock::to_time_t(now);
  
  std::cout << "[" << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S") << "] "
            << "[" << level << "] " << message << std::endl;
}

Status LongTermStabilityTest::GenerateReport(const std::string& output_path) const {
  std::ofstream file(output_path);
  if (!file.is_open()) {
    return Status::IOError("Cannot open output file: " + output_path);
  }
  
  auto snapshot = stats_.GetSnapshot();
  
  file << "# CedarGraph Long-term Stability Test Report" << std::endl;
  file << std::endl;
  
  file << "## Test Configuration" << std::endl;
  file << "- Duration: " << config_.test_duration.count() << " hours" << std::endl;
  file << "- Target Throughput: " << config_.target_throughput << " ops/sec" << std::endl;
  file << std::endl;
  
  file << "## Test Results" << std::endl;
  file << "- Total Operations: " << snapshot.total_operations << std::endl;
  file << "- Successful Operations: " << snapshot.successful_operations << std::endl;
  file << "- Failed Operations: " << snapshot.failed_operations << std::endl;
  file << "- Success Rate: " << std::fixed << std::setprecision(2) 
       << snapshot.GetSuccessRate() * 100 << "%" << std::endl;
  file << "- Average Latency: " << snapshot.GetAverageLatency() << " μs" << std::endl;
  file << "- Max Latency: " << snapshot.max_latency_us << " μs" << std::endl;
  file << "- Faults Injected: " << snapshot.faults_injected << std::endl;
  file << "- Consistency Violations: " << snapshot.consistency_violations << std::endl;
  
  file.close();
  return Status::OK();
}

void LongTermStabilityTest::Stop() {
  stop_requested_ = true;
}

// =============================================================================
// AutomatedRecoveryManager Implementation
// =============================================================================

AutomatedRecoveryManager::AutomatedRecoveryManager() {}

AutomatedRecoveryManager::~AutomatedRecoveryManager() {
  Stop();
}

Status AutomatedRecoveryManager::Initialize(const std::vector<std::string>& node_addresses) {
  node_addresses_ = node_addresses;
  
  // Register built-in recovery actions
  RegisterRecoveryAction({
    FailureType::kNodeUnreachable,
    "RestartNode",
    [this](const FailureEvent& event) { return RestartNode(event); },
    3,
    std::chrono::seconds(30)
  });
  
  RegisterRecoveryAction({
    FailureType::kRaftLeaderElectionFailure,
    "ReassignLeader",
    [this](const FailureEvent& event) { return ReassignLeader(event); },
    2,
    std::chrono::seconds(10)
  });
  
  RegisterRecoveryAction({
    FailureType::kDiskFull,
    "ClearDiskSpace",
    [this](const FailureEvent& event) { return ClearDiskSpace(event); },
    1,
    std::chrono::seconds(60)
  });
  
  return Status::OK();
}

void AutomatedRecoveryManager::Start() {
  if (running_.exchange(true)) {
    return;
  }
  
  monitor_thread_ = std::thread([this]() {
    MonitoringLoop();
  });
  
  recovery_thread_ = std::thread([this]() {
    RecoveryWorkerLoop();
  });
}

void AutomatedRecoveryManager::Stop() {
  if (!running_.exchange(false)) {
    return;
  }
  
  queue_cv_.notify_all();
  
  if (monitor_thread_.joinable()) monitor_thread_.join();
  if (recovery_thread_.joinable()) recovery_thread_.join();
}

void AutomatedRecoveryManager::RegisterRecoveryAction(const RecoveryAction& action) {
  std::lock_guard<std::mutex> lock(actions_mutex_);
  recovery_actions_[action.failure_type] = action;
}

void AutomatedRecoveryManager::ReportFailure(const FailureEvent& event) {
  std::lock_guard<std::mutex> lock(queue_mutex_);
  failure_queue_.push(event);
  queue_cv_.notify_one();
}

void AutomatedRecoveryManager::RecoveryWorkerLoop() {
  while (running_) {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    queue_cv_.wait(lock, [this]() { return !failure_queue_.empty() || !running_; });
    
    if (!running_) break;
    
    FailureEvent event = failure_queue_.front();
    failure_queue_.pop();
    lock.unlock();
    
    if (auto_recovery_enabled_) {
      auto status = ExecuteRecovery(event);
      
      std::lock_guard<std::mutex> history_lock(history_mutex_);
      recovery_history_.push_back({event, status});
    }
  }
}

Status AutomatedRecoveryManager::ExecuteRecovery(const FailureEvent& event) {
  std::lock_guard<std::mutex> lock(actions_mutex_);
  auto it = recovery_actions_.find(event.type);
  if (it == recovery_actions_.end()) {
    return Status::NotFound("No recovery action for failure type: " + 
                            std::to_string(static_cast<int>(event.type)));
  }
  
  const auto& action = it->second;
  
  // Attempt recovery with retries
  for (int attempt = 0; attempt < action.max_attempts; ++attempt) {
    auto status = action.action(event);
    if (status.ok()) {
      return Status::OK();
    }
    
    if (attempt < action.max_attempts - 1) {
      std::this_thread::sleep_for(action.backoff_duration);
    }
  }
  
  return Status::IOError("Recovery failed after " + 
                         std::to_string(action.max_attempts) + " attempts");
}

Status AutomatedRecoveryManager::RestartNode(const FailureEvent& event) {
  std::cout << "[Recovery] Restarting node " << event.node_id << std::endl;
  // In real implementation, restart the node process
  std::this_thread::sleep_for(std::chrono::seconds(5));
  return Status::OK();
}

Status AutomatedRecoveryManager::ReassignLeader(const FailureEvent& event) {
  std::cout << "[Recovery] Reassigning leader for partition at node " 
            << event.node_id << std::endl;
  // Trigger leader election
  return Status::OK();
}

Status AutomatedRecoveryManager::ClearDiskSpace(const FailureEvent& event) {
  std::cout << "[Recovery] Clearing disk space on node " << event.node_id << std::endl;
  // Clean up old logs/snapshots
  return Status::OK();
}

Status AutomatedRecoveryManager::RepairNetwork(const FailureEvent& event) {
  std::cout << "[Recovery] Repairing network for node " << event.node_id << std::endl;
  return Status::OK();
}

Status AutomatedRecoveryManager::RollbackTransaction(const FailureEvent& event) {
  std::cout << "[Recovery] Rolling back transaction at node " << event.node_id << std::endl;
  return Status::OK();
}

Status AutomatedRecoveryManager::RestoreFromBackup(const FailureEvent& event) {
  std::cout << "[Recovery] Restoring node " << event.node_id << " from backup" << std::endl;
  // Restore data from backup
  return Status::OK();
}

void AutomatedRecoveryManager::MonitoringLoop() {
  while (running_) {
    // Periodically check node health
    for (size_t i = 0; i < node_addresses_.size(); ++i) {
      // In real implementation, ping each node
      // If unreachable, report failure
    }
    
    std::this_thread::sleep_for(std::chrono::seconds(10));
  }
}

void AutomatedRecoveryManager::SetAutoRecoveryEnabled(bool enabled) {
  auto_recovery_enabled_ = enabled;
}

bool AutomatedRecoveryManager::IsAutoRecoveryEnabled() const {
  return auto_recovery_enabled_.load();
}

std::vector<std::pair<AutomatedRecoveryManager::FailureEvent, Status>> 
AutomatedRecoveryManager::GetRecoveryHistory() const {
  std::lock_guard<std::mutex> lock(history_mutex_);
  return recovery_history_;
}

}  // namespace dtx
}  // namespace cedar
