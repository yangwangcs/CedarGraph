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

#include <chrono>
#include <string>
#include <thread>

#include "cedar/integration/event_bus.h"

using namespace cedar::integration;

// =============================================================================
// Test Fixture
// =============================================================================

class EventBusTest : public ::testing::Test {
 protected:
  void SetUp() override { event_bus_ = std::make_unique<EventBus>(); }

  void TearDown() override {
    event_bus_->Stop();
    event_bus_.reset();
  }

  std::unique_ptr<EventBus> event_bus_;
};

// =============================================================================
// Event Class Tests
// =============================================================================

TEST(EventTest, ConstructorWithType) {
  Event event("test.event");
  EXPECT_EQ(event.GetType(), "test.event");
  EXPECT_GT(event.GetTimestamp(), 0);
}

TEST(EventTest, DefaultConstructor) {
  Event event;
  EXPECT_EQ(event.GetType(), "");
  EXPECT_EQ(event.GetTimestamp(), 0);
}

TEST(EventTest, SetAndGetString) {
  Event event("storage.flush");
  event.Set("table", std::string("users"));

  auto table = event.Get<std::string>("table");
  EXPECT_EQ(table, "users");
}

TEST(EventTest, SetAndGetInt) {
  Event event("storage.flush");
  event.Set("size", 1024);

  auto size = event.Get<int>("size");
  EXPECT_EQ(size, 1024);
}

TEST(EventTest, SetAndGetMultipleTypes) {
  Event event("storage.operation");
  event.Set("table", std::string("users"));
  event.Set("size", 1024);
  event.Set("success", true);
  event.Set("duration_ms", 42.5);

  EXPECT_EQ(event.Get<std::string>("table"), "users");
  EXPECT_EQ(event.Get<int>("size"), 1024);
  EXPECT_EQ(event.Get<bool>("success"), true);
  EXPECT_DOUBLE_EQ(event.Get<double>("duration_ms"), 42.5);
}

TEST(EventTest, GetMissingKeyReturnsDefault) {
  Event event("test.event");

  EXPECT_EQ(event.Get<std::string>("missing"), "");
  EXPECT_EQ(event.Get<int>("missing"), 0);
  EXPECT_EQ(event.Get<bool>("missing"), false);
}

TEST(EventTest, GetOrDefault) {
  Event event("test.event");

  EXPECT_EQ(event.GetOrDefault<std::string>("key", "default"), "default");

  event.Set("key", std::string("value"));
  EXPECT_EQ(event.GetOrDefault<std::string>("key", "default"), "value");
}

TEST(EventTest, HasKey) {
  Event event("test.event");
  EXPECT_FALSE(event.HasKey("key"));

  event.Set("key", 123);
  EXPECT_TRUE(event.HasKey("key"));
}

TEST(EventTest, GetKeys) {
  Event event("test.event");
  event.Set("a", 1);
  event.Set("b", 2);
  event.Set("c", 3);

  auto keys = event.GetKeys();
  EXPECT_EQ(keys.size(), 3);

  // Keys may be in any order (unordered_map)
  std::unordered_set<std::string> key_set(keys.begin(), keys.end());
  EXPECT_EQ(key_set.count("a"), 1);
  EXPECT_EQ(key_set.count("b"), 1);
  EXPECT_EQ(key_set.count("c"), 1);
}

TEST(EventTest, Clear) {
  Event event("test.event");
  event.Set("key", 123);
  EXPECT_TRUE(event.HasKey("key"));

  event.Clear();
  EXPECT_FALSE(event.HasKey("key"));
}

TEST(EventTest, CopyConstructor) {
  Event original("test.event");
  original.Set("key", std::string("value"));

  Event copy(original);
  EXPECT_EQ(copy.GetType(), original.GetType());
  EXPECT_EQ(copy.Get<std::string>("key"), "value");
}

TEST(EventTest, MoveConstructor) {
  Event original("test.event");
  original.Set("key", std::string("value"));

  Event moved(std::move(original));
  EXPECT_EQ(moved.GetType(), "test.event");
  EXPECT_EQ(moved.Get<std::string>("key"), "value");
}

// =============================================================================
// EventBus Tests
// =============================================================================

TEST_F(EventBusTest, PublishAndSubscribe) {
  bool received = false;
  std::string received_table;
  int received_size = 0;

  // Subscribe to event
  auto sub = event_bus_->Subscribe("storage.flush", [&](const Event& event) {
    received = true;
    received_table = event.Get<std::string>("table");
    received_size = event.Get<int>("size");
  });

  // Publish event
  Event event("storage.flush");
  event.Set("table", std::string("users"));
  event.Set("size", 1024);
  event_bus_->Publish(event);

  // Verify callback received the event
  EXPECT_TRUE(received);
  EXPECT_EQ(received_table, "users");
  EXPECT_EQ(received_size, 1024);

  // Cleanup
  EXPECT_TRUE(event_bus_->Unsubscribe(sub));
}

TEST_F(EventBusTest, MultipleSubscribers) {
  int counter = 0;

  // Subscribe multiple times to same event type
  auto sub1 = event_bus_->Subscribe("test.event", [&counter](const Event&) { counter++; });

  auto sub2 = event_bus_->Subscribe("test.event", [&counter](const Event&) { counter++; });

  auto sub3 = event_bus_->Subscribe("test.event", [&counter](const Event&) { counter++; });

  // Publish event
  Event event("test.event");
  event_bus_->Publish(event);

  // All subscribers should have been called
  EXPECT_EQ(counter, 3);

  // Cleanup
  EXPECT_TRUE(event_bus_->Unsubscribe(sub1));
  EXPECT_TRUE(event_bus_->Unsubscribe(sub2));
  EXPECT_TRUE(event_bus_->Unsubscribe(sub3));
}

TEST_F(EventBusTest, Unsubscribe) {
  int counter = 0;

  // Subscribe
  auto sub = event_bus_->Subscribe("test.event", [&counter](const Event&) { counter++; });

  // Publish - should trigger callback
  Event event("test.event");
  event_bus_->Publish(event);
  EXPECT_EQ(counter, 1);

  // Unsubscribe
  EXPECT_TRUE(event_bus_->Unsubscribe(sub));

  // Publish again - should NOT trigger callback
  event_bus_->Publish(event);
  EXPECT_EQ(counter, 1);  // Still 1, not incremented
}

TEST_F(EventBusTest, UnsubscribeInvalidId) {
  // Unsubscribe with non-existent ID should return false
  EXPECT_FALSE(event_bus_->Unsubscribe(999));
}

TEST_F(EventBusTest, EventTypeFiltering) {
  int flush_count = 0;
  int compact_count = 0;

  // Subscribe to different event types
  auto sub1 = event_bus_->Subscribe("storage.flush", [&flush_count](const Event&) { flush_count++; });

  auto sub2 = event_bus_->Subscribe("storage.compact", [&compact_count](const Event&) { compact_count++; });

  // Publish flush event
  Event flush_event("storage.flush");
  event_bus_->Publish(flush_event);

  EXPECT_EQ(flush_count, 1);
  EXPECT_EQ(compact_count, 0);

  // Publish compact event
  Event compact_event("storage.compact");
  event_bus_->Publish(compact_event);

  EXPECT_EQ(flush_count, 1);
  EXPECT_EQ(compact_count, 1);

  // Cleanup
  event_bus_->Unsubscribe(sub1);
  event_bus_->Unsubscribe(sub2);
}

TEST_F(EventBusTest, WildcardSubscription) {
  int wildcard_count = 0;
  int specific_count = 0;

  // Subscribe to all events
  auto sub_all = event_bus_->SubscribeAll([&wildcard_count](const Event&) { wildcard_count++; });

  // Subscribe to specific event
  auto sub_specific =
      event_bus_->Subscribe("storage.flush", [&specific_count](const Event&) { specific_count++; });

  // Publish event
  Event event("storage.flush");
  event_bus_->Publish(event);

  // Both should receive the event
  EXPECT_EQ(wildcard_count, 1);
  EXPECT_EQ(specific_count, 1);

  // Publish different event type
  Event other_event("other.event");
  event_bus_->Publish(other_event);

  // Only wildcard should receive
  EXPECT_EQ(wildcard_count, 2);
  EXPECT_EQ(specific_count, 1);

  // Cleanup
  event_bus_->Unsubscribe(sub_all);
  event_bus_->Unsubscribe(sub_specific);
}

TEST_F(EventBusTest, AsyncDelivery) {
  std::atomic<int> counter{0};

  // Start async processing
  event_bus_->Start();
  EXPECT_TRUE(event_bus_->IsRunning());

  // Subscribe to event
  auto sub = event_bus_->Subscribe("test.event", [&counter](const Event&) { counter++; });

  // Publish event asynchronously
  Event event("test.event");
  event_bus_->PublishAsync(event);

  // Wait for async processing (with timeout)
  int attempts = 0;
  while (counter.load() == 0 && attempts < 100) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    attempts++;
  }

  EXPECT_EQ(counter.load(), 1);

  // Cleanup
  event_bus_->Stop();
  EXPECT_FALSE(event_bus_->IsRunning());
  event_bus_->Unsubscribe(sub);
}

TEST_F(EventBusTest, AsyncMultipleEvents) {
  std::atomic<int> counter{0};
  const int num_events = 10;

  // Start async processing
  event_bus_->Start();

  // Subscribe
  auto sub = event_bus_->Subscribe("test.event", [&counter](const Event&) { counter++; });

  // Publish multiple events asynchronously
  for (int i = 0; i < num_events; i++) {
    Event event("test.event");
    event.Set("index", i);
    event_bus_->PublishAsync(event);
  }

  // Wait for all events to be processed
  int attempts = 0;
  while (counter.load() < num_events && attempts < 200) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    attempts++;
  }

  EXPECT_EQ(counter.load(), num_events);

  // Cleanup
  event_bus_->Stop();
  event_bus_->Unsubscribe(sub);
}

TEST_F(EventBusTest, GetSubscriptionCount) {
  EXPECT_EQ(event_bus_->GetSubscriptionCount(), 0);

  auto sub1 = event_bus_->Subscribe("event.a", [](const Event&) {});
  EXPECT_EQ(event_bus_->GetSubscriptionCount(), 1);

  auto sub2 = event_bus_->Subscribe("event.b", [](const Event&) {});
  EXPECT_EQ(event_bus_->GetSubscriptionCount(), 2);

  auto sub3 = event_bus_->SubscribeAll([](const Event&) {});
  EXPECT_EQ(event_bus_->GetSubscriptionCount(), 3);

  event_bus_->Unsubscribe(sub1);
  EXPECT_EQ(event_bus_->GetSubscriptionCount(), 2);

  event_bus_->Unsubscribe(sub2);
  event_bus_->Unsubscribe(sub3);
  EXPECT_EQ(event_bus_->GetSubscriptionCount(), 0);
}

TEST_F(EventBusTest, GetSubscriptionCountForType) {
  EXPECT_EQ(event_bus_->GetSubscriptionCountForType("event.a"), 0);

  auto sub1 = event_bus_->Subscribe("event.a", [](const Event&) {});
  auto sub2 = event_bus_->Subscribe("event.a", [](const Event&) {});
  auto sub3 = event_bus_->Subscribe("event.b", [](const Event&) {});

  EXPECT_EQ(event_bus_->GetSubscriptionCountForType("event.a"), 2);
  EXPECT_EQ(event_bus_->GetSubscriptionCountForType("event.b"), 1);
  EXPECT_EQ(event_bus_->GetSubscriptionCountForType("nonexistent"), 0);

  event_bus_->Unsubscribe(sub1);
  event_bus_->Unsubscribe(sub2);
  event_bus_->Unsubscribe(sub3);
}

TEST_F(EventBusTest, GetTotalPublishedCount) {
  EXPECT_EQ(event_bus_->GetTotalPublishedCount(), 0);

  Event event("test.event");
  event_bus_->Publish(event);
  EXPECT_EQ(event_bus_->GetTotalPublishedCount(), 1);

  event_bus_->Publish(event);
  event_bus_->Publish(event);
  EXPECT_EQ(event_bus_->GetTotalPublishedCount(), 3);

  // Async publish eventually counts when processed
  event_bus_->Start();
  event_bus_->PublishAsync(event);

  // Wait for async processing (event will be counted when processed)
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // Async events are counted when they are processed (via Publish)
  EXPECT_EQ(event_bus_->GetTotalPublishedCount(), 4);

  event_bus_->Stop();
}

TEST_F(EventBusTest, ExampleUsage) {
  // This test demonstrates the example usage from the requirements

  std::string received_table;
  int received_size = 0;

  // Subscribe
  auto sub = event_bus_->Subscribe("storage.flush", [&](const Event& event) {
    auto table = event.Get<std::string>("table");
    auto size = event.Get<int>("size");
    received_table = table;
    received_size = size;
  });

  // Publish
  Event event("storage.flush");
  event.Set("table", std::string("users"));
  event.Set("size", 1024);
  event_bus_->Publish(event);

  // Verify
  EXPECT_EQ(received_table, "users");
  EXPECT_EQ(received_size, 1024);

  // Unsubscribe
  EXPECT_TRUE(event_bus_->Unsubscribe(sub));
}

// =============================================================================
// Thread Safety Tests
// =============================================================================

TEST_F(EventBusTest, ConcurrentSubscribeAndPublish) {
  std::atomic<int> counter{0};
  const int num_threads = 4;
  const int events_per_thread = 100;

  // Start async processing
  event_bus_->Start();

  // Subscribe to events
  std::vector<EventBus::SubscriptionId> subs;
  for (int i = 0; i < num_threads; i++) {
    auto sub = event_bus_->Subscribe("test.event", [&counter](const Event&) { counter++; });
    subs.push_back(sub);
  }

  // Publish events from multiple threads
  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back([&, i]() {
      for (int j = 0; j < events_per_thread; j++) {
        Event event("test.event");
        event.Set("thread", i);
        event.Set("index", j);
        event_bus_->Publish(event);
      }
    });
  }

  // Wait for all threads to finish
  for (auto& t : threads) {
    t.join();
  }

  // Each event should be received by all subscribers
  // num_threads (subscribers) * num_threads (publishing threads) * events_per_thread
  EXPECT_EQ(counter.load(), num_threads * num_threads * events_per_thread);

  // Cleanup
  for (auto sub : subs) {
    event_bus_->Unsubscribe(sub);
  }
}

TEST_F(EventBusTest, ConcurrentAsyncPublish) {
  std::atomic<int> counter{0};
  const int num_threads = 4;
  const int events_per_thread = 100;

  // Start async processing
  event_bus_->Start();

  // Subscribe
  auto sub = event_bus_->Subscribe("test.event", [&counter](const Event&) { counter++; });

  // Publish events asynchronously from multiple threads
  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back([&, i]() {
      for (int j = 0; j < events_per_thread; j++) {
        Event event("test.event");
        event.Set("thread", i);
        event.Set("index", j);
        event_bus_->PublishAsync(event);
      }
    });
  }

  // Wait for all publishing threads to finish
  for (auto& t : threads) {
    t.join();
  }

  // Wait for all events to be processed
  int attempts = 0;
  while (counter.load() < num_threads * events_per_thread && attempts < 500) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    attempts++;
  }

  EXPECT_EQ(counter.load(), num_threads * events_per_thread);

  // Cleanup
  event_bus_->Stop();
  event_bus_->Unsubscribe(sub);
}
