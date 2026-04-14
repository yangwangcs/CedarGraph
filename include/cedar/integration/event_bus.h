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

#ifndef CEDAR_INTEGRATION_EVENT_BUS_H_
#define CEDAR_INTEGRATION_EVENT_BUS_H_

#include <any>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace cedar {
namespace integration {

// =============================================================================
// Event Class - Type-safe event payload container
// =============================================================================

class Event {
 public:
  // Constructor with event type
  explicit Event(const std::string& type);

  // Copy constructor and assignment
  Event(const Event& other);
  Event& operator=(const Event& other);

  // Move constructor and assignment
  Event(Event&& other) noexcept;
  Event& operator=(Event&& other) noexcept;

  // Default constructor (creates empty event)
  Event() = default;

  // Set a value for a key (type-safe)
  template <typename T>
  void Set(const std::string& key, const T& value) {
    data_[key] = value;
  }

  // Set a value for a key (move version for efficiency)
  template <typename T>
  void Set(const std::string& key, T&& value) {
    data_[key] = std::forward<T>(value);
  }

  // Get a value by key (type-safe, throws std::bad_any_cast on type mismatch)
  template <typename T>
  T Get(const std::string& key) const {
    auto it = data_.find(key);
    if (it == data_.end()) {
      return T{};  // Return default value if key not found
    }
    return std::any_cast<T>(it->second);
  }

  // Get a value by key with default value
  template <typename T>
  T GetOrDefault(const std::string& key, const T& default_value) const {
    auto it = data_.find(key);
    if (it == data_.end()) {
      return default_value;
    }
    try {
      return std::any_cast<T>(it->second);
    } catch (const std::bad_any_cast&) {
      return default_value;
    }
  }

  // Check if a key exists
  bool HasKey(const std::string& key) const;

  // Get the event type
  const std::string& GetType() const;

  // Get the event timestamp (milliseconds since epoch)
  int64_t GetTimestamp() const;

  // Get all keys
  std::vector<std::string> GetKeys() const;

  // Clear all data
  void Clear();

 private:
  std::string type_;
  int64_t timestamp_ms_ = 0;
  std::unordered_map<std::string, std::any> data_;
};

// =============================================================================
// EventBus Class - Publish-subscribe messaging system
// =============================================================================

class EventBus {
 public:
  using EventCallback = std::function<void(const Event&)>;
  using SubscriptionId = uint64_t;

  // Constructor
  EventBus();

  // Destructor
  ~EventBus();

  // Disable copy and move
  EventBus(const EventBus&) = delete;
  EventBus& operator=(const EventBus&) = delete;
  EventBus(EventBus&&) = delete;
  EventBus& operator=(EventBus&&) = delete;

  // ---------------------------------------------------------------------------
  // Subscription Management
  // ---------------------------------------------------------------------------

  // Subscribe to an event type, returns subscription ID
  // The callback will be invoked when events of the specified type are published
  SubscriptionId Subscribe(const std::string& event_type, EventCallback callback);

  // Subscribe to all events (wildcard subscription)
  SubscriptionId SubscribeAll(EventCallback callback);

  // Unsubscribe by ID
  // Returns true if subscription was found and removed, false otherwise
  bool Unsubscribe(SubscriptionId id);

  // ---------------------------------------------------------------------------
  // Event Publishing
  // ---------------------------------------------------------------------------

  // Publish event synchronously - calls all matching callbacks immediately
  void Publish(const Event& event);

  // Publish event asynchronously - queues for background processing
  void PublishAsync(const Event& event);

  // ---------------------------------------------------------------------------
  // Async Processing Control
  // ---------------------------------------------------------------------------

  // Start the background processing thread for async events
  void Start();

  // Stop the background processing thread
  void Stop();

  // Check if async processing is running
  bool IsRunning() const;

  // ---------------------------------------------------------------------------
  // Statistics
  // ---------------------------------------------------------------------------

  // Get the number of active subscriptions
  size_t GetSubscriptionCount() const;

  // Get the number of subscriptions for a specific event type
  size_t GetSubscriptionCountForType(const std::string& event_type) const;

  // Get the current size of the async event queue
  size_t GetAsyncQueueSize() const;

  // Get total events published (synchronous + asynchronous)
  uint64_t GetTotalPublishedCount() const;

  // Get total events processed asynchronously
  uint64_t GetAsyncProcessedCount() const;

 private:
  // Forward declaration of implementation
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace integration
}  // namespace cedar

#endif  // CEDAR_INTEGRATION_EVENT_BUS_H_
