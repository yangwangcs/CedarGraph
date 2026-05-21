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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <limits>
#include <thread>
#include <vector>

#include "cedar/core/threading.h"
#include "cedar/gcn/event_applier.h"
#include "cedar/gcn/gcn_service.h"
#include "cedar/gcn/tmv_engine.h"
#include "cedar/governance/config_manager.h"
#include "cedar/governance/service_registry.h"

#include <grpcpp/grpcpp.h>
#include "gcn_service.grpc.pb.h"

using namespace cedar;

// =============================================================================
// EventApplier Thread Safety
// =============================================================================

TEST(EventApplierCriticalTest, ConcurrentApplyUnordered) {
  // Use nullptr engine to avoid TMV capacity limits; this test targets
  // EventApplier internal synchronization only.
  gcn::EventApplier applier(nullptr);

  const int kNumThreads = 4;
  const int kEventsPerThread = 25;
  std::atomic<uint64_t> version_counter{0};

  std::vector<std::thread> threads;
  for (int t = 0; t < kNumThreads; ++t) {
    threads.emplace_back([&]() {
      for (int i = 0; i < kEventsPerThread; ++i) {
        uint64_t version = ++version_counter;
        gcn::GraphCDCEvent event{
            version,
            100,
            200 + static_cast<uint64_t>(version),
            1000,
            std::numeric_limits<uint32_t>::max(),
            1,
            gcn::CDCEventOp::kCreate};
        auto s = applier.ApplyUnordered(event);
        EXPECT_TRUE(s.ok());
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }

  EXPECT_EQ(applier.applied_version(),
            static_cast<uint64_t>(kNumThreads * kEventsPerThread));
}

// =============================================================================
// ThreadPool Exception Handling
// =============================================================================

TEST(ThreadPoolCriticalTest, TaskExceptionDoesNotCrashPool) {
  ThreadPool pool(2);
  std::atomic<int> normal_count{0};

  pool.Schedule([]() { throw std::runtime_error("expected exception"); });
  pool.Schedule([&]() { ++normal_count; });

  pool.WaitForAll();
  EXPECT_EQ(normal_count.load(), 1);

  pool.Schedule([&]() { ++normal_count; });
  pool.WaitForAll();
  EXPECT_EQ(normal_count.load(), 2);
}

// =============================================================================
// ServiceRegistry Deadlock
// =============================================================================

TEST(ServiceRegistryCriticalTest, WatchCallbackReentrancyNoDeadlock) {
  governance::ServiceRegistry registry;

  std::atomic<int> callback_count{0};
  auto watch_result = registry.Watch(
      "storaged", [&](const governance::ServiceEvent& event) {
        (void)event;
        ++callback_count;
        (void)registry.GetServiceCount();
        (void)registry.GetHealthyServiceCount();
      });
  ASSERT_TRUE(watch_result.ok());

  governance::ServiceInfo info;
  info.id = "storaged-1";
  info.name = "storaged";
  info.host = "10.0.0.1";
  info.port = 50051;
  info.status = governance::ServiceStatus::kStarting;

  EXPECT_TRUE(registry.Register(info).ok());
  EXPECT_TRUE(registry.Heartbeat("storaged-1").ok());
  EXPECT_TRUE(
      registry.UpdateStatus("storaged-1", governance::ServiceStatus::kUnhealthy)
          .ok());
  EXPECT_TRUE(registry.Deregister("storaged-1").ok());

  EXPECT_EQ(callback_count.load(), 4);
}

// =============================================================================
// ConfigManager Deadlock
// =============================================================================

TEST(ConfigManagerCriticalTest, WatchCallbackReentrancyNoDeadlock) {
  governance::ConfigManager config;

  std::atomic<int> callback_count{0};
  auto watch_result = config.Watch(
      "test.key", [&](const governance::ConfigChangeEvent& event) {
        (void)event;
        ++callback_count;
        (void)config.GetString("test.key");
        (void)config.HasKey("test.key");
      });
  ASSERT_TRUE(watch_result.ok());

  config.SetString("test.key", "value1");
  config.SetString("test.key", "value2");
  config.RemoveKey("test.key");

  EXPECT_EQ(callback_count.load(), 3);
}

// =============================================================================
// GcnServiceImpl Stream Timeout
// =============================================================================

class GcnStreamTimeoutTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto callback = [](const gcn::CDCEvent&) {};
    service_ = std::make_unique<gcn::GcnServiceImpl>(std::move(callback));
    server_address_ = "127.0.0.1:0";

    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials(),
                             &port_);
    builder.RegisterService(service_.get());
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);

    std::ostringstream oss;
    oss << "127.0.0.1:" << port_;
    server_address_ = oss.str();

    server_thread_ = std::thread([this]() { server_->Wait(); });

    auto channel =
        grpc::CreateChannel(server_address_, grpc::InsecureChannelCredentials());
    stub_ = gcn::GcnService::NewStub(channel);
  }

  void TearDown() override {
    if (service_) {
      service_->Shutdown();
    }
    if (server_) {
      server_->Shutdown();
    }
    if (server_thread_.joinable()) {
      server_thread_.join();
    }
  }

  std::unique_ptr<gcn::GcnServiceImpl> service_;
  std::unique_ptr<grpc::Server> server_;
  std::thread server_thread_;
  std::string server_address_;
  int port_ = 0;
  std::unique_ptr<gcn::GcnService::Stub> stub_;
};

TEST_F(GcnStreamTimeoutTest, StreamSurvivesIdlePeriod) {
  grpc::ClientContext context;
  auto stream = stub_->OnEventStream(&context);

  // Sleep longer than the old 100ms timeout to verify stream doesn't break
  std::this_thread::sleep_for(std::chrono::milliseconds(250));

  // Enqueue an event after the idle period
  gcn::CDCEvent event;
  event.set_event_id("post-idle-event");
  event.set_entity_id(100);
  event.set_version(1);
  event.set_timestamp(1000);
  event.set_event_type("CREATE");
  service_->EnqueueEvent(event);

  // The event should still be readable
  gcn::CDCEvent received;
  EXPECT_TRUE(stream->Read(&received));
  EXPECT_EQ(received.event_id(), "post-idle-event");

  gcn::Ack ack;
  ack.set_event_id(received.event_id());
  ack.set_accepted(true);
  EXPECT_TRUE(stream->Write(ack));

  // Signal server-side stream closure so the RPC handler can return
  // and the client can finish cleanly.
  service_->Shutdown();

  stream->WritesDone();
  grpc::Status status = stream->Finish();
  EXPECT_TRUE(status.ok());
}

// =============================================================================
// Main
// =============================================================================

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
