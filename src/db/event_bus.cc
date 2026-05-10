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

#include "cedar/integration/event_bus.h"

#include <cassert>
#include <condition_variable>

namespace cedar {
namespace integration {

// =============================================================================
// Event Implementation
// =============================================================================

// Get current timestamp in milliseconds
static inline int64_t CurrentTimeMillis() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

Event::Event(const std::string& type) : type_(type), timestamp_ms_(CurrentTimeMillis()) {}

Event::Event(const Event& other)
    : type_(other.type_), timestamp_ms_(other.timestamp_ms_), data_(other.data_) {}

Event& Event::operator=(const Event& other) {
  if (this != &other) {
    type_ = other.type_;
    timestamp_ms_ = other.timestamp_ms_;
    data_ = other.data_;
  }
  return *this;
}

Event::Event(Event&& other) noexcept
    : type_(std::move(other.type_)),
      timestamp_ms_(other.timestamp_ms_),
      data_(std::move(other.data_)) {
  other.timestamp_ms_ = 0;
}

Event& Event::operator=(Event&& other) noexcept {
  if (this != &other) {
    type_ = std::move(other.type_);
    timestamp_ms_ = other.timestamp_ms_;
    data_ = std::move(other.data_);
    other.timestamp_ms_ = 0;
  }
  return *this;
}

bool Event::HasKey(const std::string& key) const {
  return data_.find(key) != data_.end();
}

const std::string& Event::GetType() const {
  return type_;
}

int64_t Event::GetTimestamp() const {
  return timestamp_ms_;
}

std::vector<std::string> Event::GetKeys() const {
  std::vector<std::string> keys;
  keys.reserve(data_.size());
  for (const auto& pair : data_) {
    keys.push_back(pair.first);
  }
  return keys;
}

void Event::Clear() {
  data_.clear();
}

// =============================================================================
// EventBus Implementation
// =============================================================================

// Subscription entry structure
struct SubscriptionEntry {
  EventBus::SubscriptionId id;
  std::string event_type;  // Empty string means wildcard (all events)
  EventBus::EventCallback callback;
};

class EventBus::Impl {
 public:
  Impl() : next_subscription_id_(1), running_(false), total_published_(0), async_processed_(0) {}

  ~Impl() { Stop(); }

  // Subscribe to a specific event type
  SubscriptionId Subscribe(const std::string& event_type, EventCallback callback) {
    std::lock_guard<std::mutex> lock(subscriptions_mutex_);

    SubscriptionId id = next_subscription_id_++;
    SubscriptionEntry entry{id, event_type, std::move(callback)};

    subscriptions_[id] = std::move(entry);
    subscriptions_by_type_[event_type].insert(id);

    return id;
  }

  // Subscribe to all events
  SubscriptionId SubscribeAll(EventCallback callback) {
    std::lock_guard<std::mutex> lock(subscriptions_mutex_);

    SubscriptionId id = next_subscription_id_++;
    SubscriptionEntry entry{id, "", std::move(callback)};

    subscriptions_[id] = std::move(entry);
    wildcard_subscriptions_.insert(id);

    return id;
  }

  // Unsubscribe by ID
  bool Unsubscribe(SubscriptionId id) {
    std::lock_guard<std::mutex> lock(subscriptions_mutex_);

    auto it = subscriptions_.find(id);
    if (it == subscriptions_.end()) {
      return false;
    }

    // Remove from type index
    const std::string& event_type = it->second.event_type;
    if (event_type.empty()) {
      // Wildcard subscription
      wildcard_subscriptions_.erase(id);
    } else {
      // Type-specific subscription
      auto type_it = subscriptions_by_type_.find(event_type);
      if (type_it != subscriptions_by_type_.end()) {
        type_it->second.erase(id);
        if (type_it->second.empty()) {
          subscriptions_by_type_.erase(type_it);
        }
      }
    }

    subscriptions_.erase(it);
    return true;
  }

  // Publish event synchronously
  void Publish(const Event& event) {
    total_published_++;

    // Collect callbacks to invoke (outside lock to avoid deadlock)
    std::vector<EventCallback> callbacks_to_invoke;
    {
      std::lock_guard<std::mutex> lock(subscriptions_mutex_);

      // Get type-specific subscriptions
      auto type_it = subscriptions_by_type_.find(event.GetType());
      if (type_it != subscriptions_by_type_.end()) {
        for (SubscriptionId id : type_it->second) {
          auto sub_it = subscriptions_.find(id);
          if (sub_it != subscriptions_.end()) {
            callbacks_to_invoke.push_back(sub_it->second.callback);
          }
        }
      }

      // Get wildcard subscriptions
      for (SubscriptionId id : wildcard_subscriptions_) {
        auto sub_it = subscriptions_.find(id);
        if (sub_it != subscriptions_.end()) {
          callbacks_to_invoke.push_back(sub_it->second.callback);
        }
      }
    }

    // Invoke callbacks outside the lock
    for (const auto& callback : callbacks_to_invoke) {
      callback(event);
    }
  }

  // Publish event asynchronously
  void PublishAsync(const Event& event) {
    // Note: Don't increment total_published_ here, it will be incremented
    // when the event is processed synchronously in ProcessAsyncEvents

    {
      std::lock_guard<std::mutex> lock(queue_mutex_);
      async_event_queue_.push(event);
    }
    queue_cv_.notify_one();
  }

  // Start async processing thread
  void Start() {
    std::lock_guard<std::mutex> lock(control_mutex_);

    if (running_) {
      return;  // Already running
    }

    running_ = true;
    processing_thread_ = std::thread(&Impl::ProcessAsyncEvents, this);
  }

  // Stop async processing thread
  void Stop() {
    {
      std::lock_guard<std::mutex> lock(control_mutex_);
      running_ = false;
    }
    queue_cv_.notify_all();

    if (processing_thread_.joinable()) {
      processing_thread_.join();
    }
  }

  // Check if running
  bool IsRunning() const {
    std::lock_guard<std::mutex> lock(control_mutex_);
    return running_;
  }

  // Get subscription count
  size_t GetSubscriptionCount() const {
    std::lock_guard<std::mutex> lock(subscriptions_mutex_);
    return subscriptions_.size();
  }

  // Get subscription count for type
  size_t GetSubscriptionCountForType(const std::string& event_type) const {
    std::lock_guard<std::mutex> lock(subscriptions_mutex_);

    auto it = subscriptions_by_type_.find(event_type);
    if (it != subscriptions_by_type_.end()) {
      return it->second.size();
    }
    return 0;
  }

  // Get async queue size
  size_t GetAsyncQueueSize() const {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    return async_event_queue_.size();
  }

  // Get total published count
  uint64_t GetTotalPublishedCount() const {
    return total_published_.load();
  }

  // Get async processed count
  uint64_t GetAsyncProcessedCount() const {
    return async_processed_.load();
  }

 private:
  // Background thread function for processing async events
  void ProcessAsyncEvents() {
    while (true) {
      Event event;

      {
        std::unique_lock<std::mutex> lock(queue_mutex_);

        // Wait until there's an event or we're stopping
        queue_cv_.wait(lock, [this] { return !async_event_queue_.empty() || !IsRunningInternal(); });

        if (!IsRunningInternal() && async_event_queue_.empty()) {
          break;  // Stopping and queue is empty
        }

        if (!async_event_queue_.empty()) {
          event = std::move(async_event_queue_.front());
          async_event_queue_.pop();
        }
      }

      // Process the event (outside the lock)
      if (!event.GetType().empty() || !event.GetKeys().empty()) {
        Publish(event);  // Reuse synchronous publish for callback invocation
        async_processed_++;
      }
    }
  }

  // Internal check for running state (must be called with queue_mutex_ held or
  // from a context where control_mutex_ is held)
  bool IsRunningInternal() const {
    // This is called from within the condition variable wait, which holds queue_mutex_
    // We need to check running_ safely, but control_mutex_ might be held elsewhere
    // So we use an atomic for running_ which allows lock-free reads
    return running_.load();
  }

  // Subscription management
  mutable std::mutex subscriptions_mutex_;
  std::unordered_map<SubscriptionId, SubscriptionEntry> subscriptions_;
  std::unordered_map<std::string, std::unordered_set<SubscriptionId>> subscriptions_by_type_;
  std::unordered_set<SubscriptionId> wildcard_subscriptions_;
  std::atomic<SubscriptionId> next_subscription_id_;

  // Async event queue
  mutable std::mutex queue_mutex_;
  std::queue<Event> async_event_queue_;
  std::condition_variable queue_cv_;

  // Thread control
  mutable std::mutex control_mutex_;
  std::atomic<bool> running_;
  std::thread processing_thread_;

  // Statistics
  std::atomic<uint64_t> total_published_;
  std::atomic<uint64_t> async_processed_;
};

// =============================================================================
// EventBus Public API
// =============================================================================

EventBus::EventBus() : impl_(std::make_unique<Impl>()) {}

EventBus::~EventBus() = default;

EventBus::SubscriptionId EventBus::Subscribe(const std::string& event_type, EventCallback callback) {
  return impl_->Subscribe(event_type, std::move(callback));
}

EventBus::SubscriptionId EventBus::SubscribeAll(EventCallback callback) {
  return impl_->SubscribeAll(std::move(callback));
}

bool EventBus::Unsubscribe(SubscriptionId id) {
  return impl_->Unsubscribe(id);
}

void EventBus::Publish(const Event& event) {
  impl_->Publish(event);
}

void EventBus::PublishAsync(const Event& event) {
  impl_->PublishAsync(event);
}

void EventBus::Start() {
  impl_->Start();
}

void EventBus::Stop() {
  impl_->Stop();
}

bool EventBus::IsRunning() const {
  return impl_->IsRunning();
}

size_t EventBus::GetSubscriptionCount() const {
  return impl_->GetSubscriptionCount();
}

size_t EventBus::GetSubscriptionCountForType(const std::string& event_type) const {
  return impl_->GetSubscriptionCountForType(event_type);
}

size_t EventBus::GetAsyncQueueSize() const {
  return impl_->GetAsyncQueueSize();
}

uint64_t EventBus::GetTotalPublishedCount() const {
  return impl_->GetTotalPublishedCount();
}

uint64_t EventBus::GetAsyncProcessedCount() const {
  return impl_->GetAsyncProcessedCount();
}

}  // namespace integration
}  // namespace cedar
