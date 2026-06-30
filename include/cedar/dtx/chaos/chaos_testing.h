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

#ifndef CEDAR_DTX_CHAOS_CHAOS_TESTING_H_
#define CEDAR_DTX_CHAOS_CHAOS_TESTING_H_

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>
#include <thread>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"

namespace cedar {
namespace dtx {
namespace chaos {

// =============================================================================
// Fault Types
// =============================================================================

enum class FaultType : uint8_t {
  kNodeCrash = 0,           // Kill a node
  kNetworkPartition = 1,    // Network isolation
  kNetworkDelay = 2,        // Network latency
  kNetworkDrop = 3,         // Packet loss
  kDiskFailure = 4,         // Disk errors
  kCpuOverload = 5,         // CPU stress
  kMemoryPressure = 6,      // Memory exhaustion
  kClockSkew = 7,           // Time drift
  kLeaderStepdown = 8,      // Force leader resignation
  kSplitBrain = 9,          // Create network partition between leaders
};

inline std::string FaultTypeToString(FaultType type) {
  switch (type) {
    case FaultType::kNodeCrash: return "NodeCrash";
    case FaultType::kNetworkPartition: return "NetworkPartition";
    case FaultType::kNetworkDelay: return "NetworkDelay";
    case FaultType::kNetworkDrop: return "NetworkDrop";
    case FaultType::kDiskFailure: return "DiskFailure";
    case FaultType::kCpuOverload: return "CpuOverload";
    case FaultType::kMemoryPressure: return "MemoryPressure";
    case FaultType::kClockSkew: return "ClockSkew";
    case FaultType::kLeaderStepdown: return "LeaderStepdown";
    case FaultType::kSplitBrain: return "SplitBrain";
    default: return "Unknown";
  }
}

// =============================================================================
// Fault Specification
// =============================================================================

struct FaultSpec {
  FaultType type;
  std::vector<NodeID> target_nodes;
  double probability = 0.1;  // 0.0 - 1.0
  std::chrono::milliseconds duration{1000};
  std::chrono::milliseconds interval{5000};
  
  // Type-specific parameters
  std::unordered_map<std::string, std::string> parameters;
};

// =============================================================================
// Fault Injection Result
// =============================================================================

struct FaultResult {
  bool success = false;
  std::string error_message;
  std::chrono::system_clock::time_point injected_at;
  std::chrono::system_clock::time_point recovered_at;
  std::vector<std::string> affected_services;
};

// =============================================================================
// Chaos Experiment
// =============================================================================

struct ChaosExperiment {
  std::string name;
  std::string description;
  std::vector<FaultSpec> fault_sequence;
  std::chrono::milliseconds duration{60000};
  
  // Success criteria
  uint32_t max_acceptable_errors = 0;
  std::chrono::milliseconds max_recovery_time{30000};
};

// =============================================================================
// Chaos Testing Framework
// =============================================================================

class ChaosFramework {
 public:
  using FaultInjector = std::function<Status(FaultType, 
                                               const std::vector<NodeID>&,
                                               const std::unordered_map<std::string, std::string>&)>;
  using HealthChecker = std::function<bool()>;
  
  ChaosFramework();
  ~ChaosFramework();
  
  // Initialize with fault injector
  Status Initialize(FaultInjector injector, HealthChecker health_checker);
  void Shutdown();
  
  // Register predefined experiments
  void RegisterExperiment(const ChaosExperiment& experiment);
  
  // Run experiment
  Status RunExperiment(const std::string& experiment_name);
  
  // Run continuous chaos
  Status StartContinuousChaos(const std::vector<FaultSpec>& faults);
  void StopContinuousChaos();
  
  // Manual fault injection
  Status InjectFault(const FaultSpec& spec);
  Status RecoverFault(FaultType type);
  
  // Get results
  std::vector<FaultResult> GetResults() const;
  void ClearResults();

 private:
  void ContinuousChaosLoop();
  FaultResult ExecuteFault(const FaultSpec& spec);
  bool WaitForShutdown(std::chrono::milliseconds timeout);
  bool WaitForContinuousStop(std::chrono::milliseconds timeout);
  
  std::atomic<bool> running_{false};
  std::atomic<bool> shutdown_requested_{false};
  mutable std::mutex stop_mutex_;
  std::condition_variable stop_cv_;
  FaultInjector fault_injector_;
  HealthChecker health_checker_;
  
  std::vector<ChaosExperiment> experiments_;
  std::vector<FaultSpec> continuous_faults_;
  mutable std::mutex results_mutex_;
  std::vector<FaultResult> results_;
  
  std::unique_ptr<std::thread> chaos_thread_;
  std::mt19937 random_gen_;
};

// =============================================================================
// Predefined Chaos Experiments
// =============================================================================

namespace experiments {

// Random node failures
ChaosExperiment RandomNodeFailures();

// Network partition scenarios
ChaosExperiment NetworkPartition();

// Leader election stress test
ChaosExperiment LeaderElectionStress();

// Split brain scenario
ChaosExperiment SplitBrain();

// Resource exhaustion
ChaosExperiment ResourceExhaustion();

// Full cluster restart
ChaosExperiment FullClusterRestart();

}  // namespace experiments

}  // namespace chaos
}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_CHAOS_CHAOS_TESTING_H_
