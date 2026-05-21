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

// =============================================================================
// Chaos Testing Implementation
// =============================================================================

#include "cedar/dtx/chaos/chaos_testing.h"

#include <algorithm>
#include <random>

namespace cedar {
namespace dtx {
namespace chaos {

ChaosFramework::ChaosFramework() 
    : random_gen_(std::random_device{}()) {}

ChaosFramework::~ChaosFramework() {
  Shutdown();
}

Status ChaosFramework::Initialize(FaultInjector injector, 
                                   HealthChecker health_checker) {
  fault_injector_ = injector;
  health_checker_ = health_checker;
  
  // Register default experiments
  RegisterExperiment(experiments::RandomNodeFailures());
  RegisterExperiment(experiments::NetworkPartition());
  RegisterExperiment(experiments::LeaderElectionStress());
  
  return Status::OK();
}

void ChaosFramework::Shutdown() {
  StopContinuousChaos();
}

void ChaosFramework::RegisterExperiment(const ChaosExperiment& experiment) {
  experiments_.push_back(experiment);
}

Status ChaosFramework::RunExperiment(const std::string& experiment_name) {
  auto it = std::find_if(experiments_.begin(), experiments_.end(),
                         [&experiment_name](const ChaosExperiment& e) {
                           return e.name == experiment_name;
                         });
  
  if (it == experiments_.end()) {
    return Status::NotFound("Experiment not found: " + experiment_name);
  }
  
  const auto& experiment = *it;
  auto start_time = std::chrono::steady_clock::now();
  uint32_t error_count = 0;
  
  for (const auto& fault : experiment.fault_sequence) {
    if (std::chrono::steady_clock::now() - start_time >= experiment.duration) {
      break;
    }
    
    auto result = ExecuteFault(fault);
    {
      std::lock_guard<std::mutex> lock(results_mutex_);
      results_.push_back(result);
    }
    
    if (!result.success) {
      error_count++;
    }
    
    // Wait before next fault
    std::this_thread::sleep_for(fault.interval);
  }
  
  // Check success criteria
  if (error_count > experiment.max_acceptable_errors) {
    return Status::IOError("Experiment failed: too many errors");
  }
  
  return Status::OK();
}

Status ChaosFramework::StartContinuousChaos(const std::vector<FaultSpec>& faults) {
  if (running_.exchange(true)) {
    return Status::InvalidArgument("Continuous chaos already running");
  }
  
  continuous_faults_ = faults;
  chaos_thread_ = std::make_unique<std::thread>(
      &ChaosFramework::ContinuousChaosLoop, this);
  
  return Status::OK();
}

void ChaosFramework::StopContinuousChaos() {
  if (!running_.exchange(false)) {
    return;
  }
  
  if (chaos_thread_ && chaos_thread_->joinable()) {
    chaos_thread_->join();
  }
}

void ChaosFramework::ContinuousChaosLoop() {
  std::uniform_real_distribution<double> prob_dist(0.0, 1.0);
  
  while (running_.load()) {
    for (const auto& fault : continuous_faults_) {
      if (!running_.load()) break;
      
      // Check probability
      if (prob_dist(random_gen_) <= fault.probability) {
        auto result = ExecuteFault(fault);
        {
          std::lock_guard<std::mutex> lock(results_mutex_);
          results_.push_back(result);
        }
        
        // Wait for recovery
        std::this_thread::sleep_for(fault.duration);
      }
    }
    
    // Small delay between cycles
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

FaultResult ChaosFramework::ExecuteFault(const FaultSpec& spec) {
  FaultResult result;
  result.injected_at = std::chrono::system_clock::now();
  
  if (!fault_injector_) {
    result.error_message = "Fault injector not set";
    return result;
  }
  
  auto status = fault_injector_(spec.type, spec.target_nodes, spec.parameters);
  
  result.success = status.ok();
  if (!status.ok()) {
    result.error_message = status.ToString();
  }
  
  result.recovered_at = std::chrono::system_clock::now();
  return result;
}

Status ChaosFramework::InjectFault(const FaultSpec& spec) {
  auto result = ExecuteFault(spec);
  return result.success ? Status::OK() 
                        : Status::IOError(result.error_message);
}

std::vector<FaultResult> ChaosFramework::GetResults() const {
  std::lock_guard<std::mutex> lock(results_mutex_);
  return results_;
}

void ChaosFramework::ClearResults() {
  std::lock_guard<std::mutex> lock(results_mutex_);
  results_.clear();
}

// =============================================================================
// Predefined Experiments
// =============================================================================

namespace experiments {

ChaosExperiment RandomNodeFailures() {
  ChaosExperiment exp;
  exp.name = "random_node_failures";
  exp.description = "Randomly kill and restart nodes";
  exp.duration = std::chrono::minutes(5);
  exp.max_acceptable_errors = 3;
  
  FaultSpec fault;
  fault.type = FaultType::kNodeCrash;
  fault.probability = 0.2;
  fault.duration = std::chrono::seconds(30);
  fault.interval = std::chrono::seconds(60);
  
  exp.fault_sequence.push_back(fault);
  
  return exp;
}

ChaosExperiment NetworkPartition() {
  ChaosExperiment exp;
  exp.name = "network_partition";
  exp.description = "Create network partitions between nodes";
  exp.duration = std::chrono::minutes(3);
  
  FaultSpec fault;
  fault.type = FaultType::kNetworkPartition;
  fault.probability = 1.0;
  fault.duration = std::chrono::seconds(20);
  fault.interval = std::chrono::seconds(40);
  fault.parameters["partition_size"] = "1";  // Isolate one node
  
  exp.fault_sequence.push_back(fault);
  
  return exp;
}

ChaosExperiment LeaderElectionStress() {
  ChaosExperiment exp;
  exp.name = "leader_election_stress";
  exp.description = "Force frequent leader elections";
  exp.duration = std::chrono::minutes(10);
  
  for (int i = 0; i < 20; ++i) {
    FaultSpec fault;
    fault.type = FaultType::kLeaderStepdown;
    fault.probability = 1.0;
    fault.duration = std::chrono::seconds(5);
    fault.interval = std::chrono::seconds(30);
    exp.fault_sequence.push_back(fault);
  }
  
  return exp;
}

ChaosExperiment SplitBrain() {
  ChaosExperiment exp;
  exp.name = "split_brain";
  exp.description = "Create split brain scenario";
  exp.duration = std::chrono::minutes(2);
  
  FaultSpec fault;
  fault.type = FaultType::kSplitBrain;
  fault.probability = 1.0;
  fault.duration = std::chrono::seconds(45);
  fault.interval = std::chrono::seconds(60);
  
  exp.fault_sequence.push_back(fault);
  
  return exp;
}

ChaosExperiment ResourceExhaustion() {
  ChaosExperiment exp;
  exp.name = "resource_exhaustion";
  exp.description = "Test under resource pressure";
  exp.duration = std::chrono::minutes(5);
  
  // CPU pressure
  FaultSpec cpu_fault;
  cpu_fault.type = FaultType::kCpuOverload;
  cpu_fault.probability = 0.5;
  cpu_fault.duration = std::chrono::seconds(60);
  cpu_fault.interval = std::chrono::seconds(120);
  cpu_fault.parameters["load_percent"] = "80";
  exp.fault_sequence.push_back(cpu_fault);
  
  // Memory pressure
  FaultSpec mem_fault;
  mem_fault.type = FaultType::kMemoryPressure;
  mem_fault.probability = 0.3;
  mem_fault.duration = std::chrono::seconds(30);
  mem_fault.interval = std::chrono::seconds(180);
  exp.fault_sequence.push_back(mem_fault);
  
  return exp;
}

ChaosExperiment FullClusterRestart() {
  ChaosExperiment exp;
  exp.name = "full_cluster_restart";
  exp.description = "Restart entire cluster";
  exp.duration = std::chrono::minutes(10);
  exp.max_recovery_time = std::chrono::minutes(5);
  
  FaultSpec fault;
  fault.type = FaultType::kNodeCrash;
  fault.probability = 1.0;
  fault.duration = std::chrono::seconds(10);
  fault.interval = std::chrono::seconds(0);
  
  exp.fault_sequence.push_back(fault);
  
  return exp;
}

}  // namespace experiments

}  // namespace chaos
}  // namespace dtx
}  // namespace cedar
