// Copyright 2025 The Cedar Authors
//
// Configuration file watcher for hot-reload

#ifndef CEDAR_CLIENT_CONFIG_WATCHER_H_
#define CEDAR_CLIENT_CONFIG_WATCHER_H_

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "cedar/client/config_loader.h"

namespace cedar {
namespace client {

// Configuration change callback
using ConfigChangeCallback = std::function<void(const ConfigLoader&)>;

// Configuration watcher configuration
struct ConfigWatcherConfig {
  std::string file_path;
  int poll_interval_ms = 1000;  // Check every 1 second
  bool enable_auto_reload = true;
};

// Configuration file watcher
class ConfigWatcher {
 public:
  ConfigWatcher(const ConfigWatcherConfig& config = ConfigWatcherConfig());
  ~ConfigWatcher();

  // Start watching
  bool Start();

  // Stop watching
  void Stop();

  // Check if watching
  bool IsWatching() const;

  // Get current config
  const ConfigLoader& GetConfig() const;

  // Reload config manually
  bool Reload();

  // Set change callback
  void SetChangeCallback(ConfigChangeCallback callback);

  // Get file path
  const std::string& GetFilePath() const;

 private:
  ConfigWatcherConfig config_;
  ConfigLoader loader_;
  mutable std::mutex mutex_;
  std::thread watch_thread_;
  std::atomic<bool> running_{false};
  std::chrono::system_clock::time_point last_modified_;
  ConfigChangeCallback callback_;

  // Get file modification time
  std::chrono::system_clock::time_point GetFileModificationTime() const;

  // Watch loop
  void WatchLoop();
};

}  // namespace client
}  // namespace cedar

#endif  // CEDAR_CLIENT_CONFIG_WATCHER_H_
