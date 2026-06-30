// Copyright 2026 CedarGraph Authors
//
// Licensed under the Apache License, Version 2.0

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>

#include "cedar/core/status.h"
#include "cedar/dtx/monitoring.h"

namespace cedar {
namespace dtx {
namespace monitoring {
namespace {

AlertRule MakeRule(const std::string& name) {
  AlertRule rule;
  rule.name = name;
  rule.description = "test alert";
  rule.severity = AlertSeverity::kWarning;
  rule.condition_metric = "metric";
  rule.threshold = 1.0;
  rule.comparison = ">";
  return rule;
}

class ReentrantNotifier : public AlertNotifier {
 public:
  explicit ReentrantNotifier(std::atomic<int>* calls) : calls_(calls) {}

  void Notify(const Alert& alert) override {
    (void)alert;
    AlertManager::GetInstance()->GetActiveAlerts();
    calls_->fetch_add(1);
  }

  std::string GetName() const override { return "reentrant"; }

 private:
  std::atomic<int>* calls_;
};

TEST(NetworkSinkTest, DestructorWakesSendThreadPromptly) {
  NetworkSink::Config config;
  config.flush_interval = std::chrono::seconds(60);
  config.endpoint.clear();

  auto start = std::chrono::steady_clock::now();
  {
    NetworkSink sink(config);
  }
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
            1000);
}

TEST(AlertManagerTest, ShutdownWakesEvaluationThreadPromptly) {
  auto* manager = AlertManager::GetInstance();
  manager->Shutdown();

  AlertManager::Config config;
  config.evaluation_interval = std::chrono::seconds(60);

  ASSERT_TRUE(manager->Initialize(config).ok());
  auto start = std::chrono::steady_clock::now();
  manager->Shutdown();
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
            1000);
}

TEST(AlertManagerTest, NotifierCanQueryAlertsWithoutDeadlock) {
  auto* manager = AlertManager::GetInstance();
  manager->Shutdown();

  AlertManager::Config config;
  config.evaluation_interval = std::chrono::seconds(60);
  ASSERT_TRUE(manager->Initialize(config).ok());

  std::atomic<int> calls{0};
  manager->AddNotifier(std::make_unique<ReentrantNotifier>(&calls));
  manager->AddRule(MakeRule("reentrant_rule"));

  manager->FireAlert("reentrant_rule");

  EXPECT_EQ(calls.load(), 1);
  EXPECT_EQ(manager->GetActiveAlerts().size(), 1u);

  manager->Shutdown();
}

TEST(AlertManagerTest, ShutdownClearsRuntimeStateBeforeRestart) {
  auto* manager = AlertManager::GetInstance();
  manager->Shutdown();

  AlertManager::Config config;
  config.evaluation_interval = std::chrono::seconds(60);
  ASSERT_TRUE(manager->Initialize(config).ok());
  manager->AddRule(MakeRule("restart_rule"));
  manager->FireAlert("restart_rule");
  ASSERT_EQ(manager->GetActiveAlerts().size(), 1u);

  manager->Shutdown();
  ASSERT_TRUE(manager->GetActiveAlerts().empty());

  ASSERT_TRUE(manager->Initialize(config).ok());
  manager->FireAlert("restart_rule");
  EXPECT_TRUE(manager->GetActiveAlerts().empty());

  manager->Shutdown();
}

TEST(LoggerTest, ReinitializeRestartsAsyncWorker) {
  auto* logger = Logger::GetInstance();
  logger->Shutdown();

  Logger::Config config;
  config.enable_console = false;
  config.enable_file = false;
  config.enable_network = false;
  config.async_mode = true;

  ASSERT_TRUE(logger->Initialize(config).ok());
  logger->Info("test", "first");
  logger->Shutdown();

  ASSERT_TRUE(logger->Initialize(config).ok());
  logger->Info("test", "second");
  logger->Shutdown();
}

}  // namespace
}  // namespace monitoring
}  // namespace dtx
}  // namespace cedar
