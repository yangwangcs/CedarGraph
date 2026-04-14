// Copyright 2025 The Cedar Authors
// Long-term Stability and Recovery Test for CedarGraph

#include <iostream>
#include <chrono>
#include <thread>

#include "cedar/dtx/chaos_testing.h"
#include "cedar/dtx/production_config.h"

using namespace cedar::dtx;

void DemoChaosTesting() {
  std::cout << "╔════════════════════════════════════════════════════════════╗" << std::endl;
  std::cout << "║     Chaos Testing - Fault Injection Demo                   ║" << std::endl;
  std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
  std::cout << std::endl;
  
  // Create fault specs
  std::vector<FaultSpec> specs;
  
  FaultSpec latency;
  latency.type = FaultType::kNetworkLatency;
  latency.probability = 0.3;
  latency.duration = std::chrono::seconds(30);
  latency.start_after = std::chrono::seconds(5);
  latency.latency_params.min_latency = std::chrono::milliseconds(50);
  latency.latency_params.max_latency = std::chrono::milliseconds(200);
  specs.push_back(latency);
  
  FaultSpec packet_loss;
  packet_loss.type = FaultType::kPacketLoss;
  packet_loss.probability = 0.1;
  packet_loss.duration = std::chrono::seconds(20);
  packet_loss.start_after = std::chrono::seconds(10);
  packet_loss.packet_loss_params.drop_rate = 0.05;
  specs.push_back(packet_loss);
  
  // Create fault injector
  FaultInjector injector;
  injector.Initialize(specs);
  
  injector.RegisterFaultCallback([](FaultType type, NodeID node, bool injected) {
    std::cout << "[Chaos] Fault " << (injected ? "injected" : "recovered") 
              << " type=" << static_cast<int>(type) << " node=" << node << std::endl;
  });
  
  std::cout << "Fault injector initialized with " << specs.size() << " fault types" << std::endl;
  std::cout << "Running for 15 seconds..." << std::endl;
  
  std::this_thread::sleep_for(std::chrono::seconds(15));
  
  injector.Shutdown();
  std::cout << "Chaos testing demo completed" << std::endl;
}

void DemoAutomatedRecovery() {
  std::cout << std::endl;
  std::cout << "╔════════════════════════════════════════════════════════════╗" << std::endl;
  std::cout << "║     Automated Recovery Manager Demo                        ║" << std::endl;
  std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
  std::cout << std::endl;
  
  AutomatedRecoveryManager recovery_mgr;
  recovery_mgr.Initialize({"127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"});
  
  std::cout << "Automated recovery manager initialized" << std::endl;
  std::cout << "Auto-recovery enabled: " << (recovery_mgr.IsAutoRecoveryEnabled() ? "Yes" : "No") << std::endl;
  
  recovery_mgr.Start();
  std::cout << "Recovery manager started" << std::endl;
  
  // Simulate some failures
  AutomatedRecoveryManager::FailureEvent event1;
  event1.type = AutomatedRecoveryManager::FailureType::kNodeUnreachable;
  event1.node_id = 1;
  event1.timestamp = std::chrono::system_clock::now();
  event1.details = "Node not responding to heartbeat";
  event1.severity = 4;
  
  std::cout << std::endl << "Reporting failure: NodeUnreachable" << std::endl;
  recovery_mgr.ReportFailure(event1);
  
  std::this_thread::sleep_for(std::chrono::seconds(2));
  
  AutomatedRecoveryManager::FailureEvent event2;
  event2.type = AutomatedRecoveryManager::FailureType::kRaftLeaderElectionFailure;
  event2.node_id = 2;
  event2.timestamp = std::chrono::system_clock::now();
  event2.details = "Leader election timeout";
  event2.severity = 3;
  
  std::cout << "Reporting failure: RaftLeaderElectionFailure" << std::endl;
  recovery_mgr.ReportFailure(event2);
  
  std::this_thread::sleep_for(std::chrono::seconds(3));
  
  auto history = recovery_mgr.GetRecoveryHistory();
  std::cout << std::endl << "Recovery history: " << history.size() << " events" << std::endl;
  
  recovery_mgr.Stop();
  std::cout << "Automated recovery demo completed" << std::endl;
}

void DemoLongTermStability() {
  std::cout << std::endl;
  std::cout << "╔════════════════════════════════════════════════════════════╗" << std::endl;
  std::cout << "║     Long-term Stability Test Framework                     ║" << std::endl;
  std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
  std::cout << std::endl;
  
  LongTermStabilityTest::Config config;
  config.test_duration = std::chrono::hours(1);  // Short demo
  config.target_throughput = 100;
  config.read_write_ratio = 0.8;
  config.enable_fault_injection = true;
  config.fault_interval = std::chrono::minutes(1);
  config.fault_duration = std::chrono::minutes(1);
  config.metrics_interval = std::chrono::seconds(10);
  
  std::cout << "Stability Test Configuration:" << std::endl;
  std::cout << "  Duration: " << config.test_duration.count() << " minutes" << std::endl;
  std::cout << "  Target Throughput: " << config.target_throughput << " ops/sec" << std::endl;
  std::cout << "  Read/Write Ratio: " << config.read_write_ratio * 100 << "/" 
            << (1 - config.read_write_ratio) * 100 << std::endl;
  std::cout << "  Fault Injection: " << (config.enable_fault_injection ? "Enabled" : "Disabled") << std::endl;
  std::cout << std::endl;
  
  std::cout << "Note: This is a framework demonstration." << std::endl;
  std::cout << "In production, this would run for hours or days with real storage clients." << std::endl;
  std::cout << std::endl;
  
  // Demo the API usage without actual long-running test
  std::cout << "API Usage Example:" << std::endl;
  std::cout << "  // Create test configuration" << std::endl;
  std::cout << "  LongTermStabilityTest::Config config;" << std::endl;
  std::cout << "  config.test_duration = std::chrono::hours(24);" << std::endl;
  std::cout << "  config.target_throughput = 10000;" << std::endl;
  std::cout << "  config.enable_fault_injection = true;" << std::endl;
  std::cout << std::endl;
  std::cout << "  // Initialize and run test" << std::endl;
  std::cout << "  LongTermStabilityTest test(config);" << std::endl;
  std::cout << "  test.Initialize(clients);" << std::endl;
  std::cout << "  test.Run();  // Blocks for 24 hours" << std::endl;
  std::cout << std::endl;
  std::cout << "  // Generate report" << std::endl;
  std::cout << "  test.GenerateReport(\"/path/to/report.md\");" << std::endl;
}

void PrintProductionReadinessChecklist() {
  std::cout << std::endl;
  std::cout << "╔════════════════════════════════════════════════════════════╗" << std::endl;
  std::cout << "║     Production Readiness Checklist - Stability & Recovery  ║" << std::endl;
  std::cout << "╠════════════════════════════════════════════════════════════╣" << std::endl;
  std::cout << "║  Chaos Testing                                             ║" << std::endl;
  std::cout << "║  [✓] Network latency injection                             ║" << std::endl;
  std::cout << "║  [✓] Packet loss simulation                                ║" << std::endl;
  std::cout << "║  [✓] Network partition testing                             ║" << std::endl;
  std::cout << "║  [ ] Node crash testing (requires real cluster)            ║" << std::endl;
  std::cout << "║  [ ] Disk failure simulation                               ║" << std::endl;
  std::cout << "║                                                            ║" << std::endl;
  std::cout << "║  Automated Recovery                                        ║" << std::endl;
  std::cout << "║  [✓] Node restart on failure                               ║" << std::endl;
  std::cout << "║  [✓] Leader reassignment                                   ║" << std::endl;
  std::cout << "║  [✓] Disk space cleanup                                    ║" << std::endl;
  std::cout << "║  [ ] Automatic backup restoration                          ║" << std::endl;
  std::cout << "║  [ ] Multi-step recovery workflows                         ║" << std::endl;
  std::cout << "║                                                            ║" << std::endl;
  std::cout << "║  Long-term Stability                                       ║" << std::endl;
  std::cout << "║  [✓] 24-hour continuous workload framework                 ║" << std::endl;
  std::cout << "║  [✓] Metrics collection and reporting                      ║" << std::endl;
  std::cout << "║  [✓] Consistency checking                                  ║" << std::endl;
  std::cout << "║  [ ] 7-day burn-in test                                    ║" << std::endl;
  std::cout << "║  [ ] Memory leak detection                                 ║" << std::endl;
  std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
}

int main(int argc, char* argv[]) {
  std::cout << "╔════════════════════════════════════════════════════════════╗" << std::endl;
  std::cout << "║     CedarGraph Stability & Recovery Test Suite             ║" << std::endl;
  std::cout << "╚════════════════════════════════════════════════════════════╝" << std::endl;
  std::cout << std::endl;
  
  // Run demos
  DemoChaosTesting();
  DemoAutomatedRecovery();
  DemoLongTermStability();
  PrintProductionReadinessChecklist();
  
  std::cout << std::endl;
  std::cout << "All demos completed!" << std::endl;
  return 0;
}
