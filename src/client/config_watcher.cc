// Copyright 2025 The Cedar Authors
//
// Configuration file watcher implementation

#include "cedar/client/config_watcher.h"

#include <chrono>
#include <iostream>

#ifdef _WIN32
#include <windows.h>
#else
#include <sys/stat.h>
#endif

namespace cedar {
namespace client {

ConfigWatcher::ConfigWatcher(const ConfigWatcherConfig& config)
    : config_(config) {}

ConfigWatcher::~ConfigWatcher() {
  Stop();
}

bool ConfigWatcher::Start() {
  if (running_) {
    return true;
  }

  // Load initial config
  if (!Reload()) {
    std::cerr << "Failed to load initial config from: " << config_.file_path << std::endl;
    return false;
  }

  // Get initial modification time
  last_modified_ = GetFileModificationTime();

  // Start watch thread
  running_ = true;
  watch_thread_ = std::thread(&ConfigWatcher::WatchLoop, this);

  return true;
}

void ConfigWatcher::Stop() {
  if (!running_) {
    return;
  }

  running_ = false;
  watch_cv_.notify_all();
  if (watch_thread_.joinable()) {
    watch_thread_.join();
  }
}

bool ConfigWatcher::IsWatching() const {
  return running_;
}

const ConfigLoader& ConfigWatcher::GetConfig() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return loader_;
}

bool ConfigWatcher::Reload() {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (!loader_.LoadFromFile(config_.file_path)) {
    return false;
  }

  return true;
}

void ConfigWatcher::SetChangeCallback(ConfigChangeCallback callback) {
  std::lock_guard<std::mutex> lock(mutex_);
  callback_ = callback;
}

const std::string& ConfigWatcher::GetFilePath() const {
  return config_.file_path;
}

std::chrono::system_clock::time_point ConfigWatcher::GetFileModificationTime() const {
#ifdef _WIN32
  HANDLE handle = CreateFileA(config_.file_path.c_str(), GENERIC_READ, FILE_SHARE_READ,
                               nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
  if (handle == INVALID_HANDLE_VALUE) {
    return std::chrono::system_clock::time_point{};
  }

  FILETIME ft;
  if (!GetFileTime(handle, nullptr, nullptr, &ft)) {
    CloseHandle(handle);
    return std::chrono::system_clock::time_point{};
  }
  CloseHandle(handle);

  // Convert FILETIME to time_point
  ULARGE_INTEGER uli;
  uli.LowPart = ft.dwLowDateTime;
  uli.HighPart = ft.dwHighDateTime;
  auto duration = std::chrono::nanoseconds((uli.QuadPart - 116444736000000000LL) * 100);
  return std::chrono::system_clock::time_point(duration);
#else
  struct stat file_stat;
  if (stat(config_.file_path.c_str(), &file_stat) != 0) {
    return std::chrono::system_clock::time_point{};
  }

  return std::chrono::system_clock::from_time_t(file_stat.st_mtime);
#endif
}

void ConfigWatcher::WatchLoop() {
  while (running_) {
    std::unique_lock<std::mutex> wait_lock(watch_cv_mutex_);
    watch_cv_.wait_for(wait_lock, std::chrono::milliseconds(config_.poll_interval_ms),
                       [this]() { return !running_.load(); });
    wait_lock.unlock();

    if (!running_) {
      break;
    }

    // Check if file has been modified
    auto current_modified = GetFileModificationTime();
    if (current_modified > last_modified_) {
      // File has been modified, reload config
      ConfigLoader loaded_config;
      if (loaded_config.LoadFromFile(config_.file_path)) {
        ConfigChangeCallback callback;
        {
          std::lock_guard<std::mutex> lock(mutex_);
          loader_ = loaded_config;
          last_modified_ = current_modified;
          callback = callback_;
        }

        if (callback) {
          try {
            callback(loaded_config);
          } catch (const std::exception& e) {
            std::cerr << "Config change callback exception: " << e.what()
                      << std::endl;
          } catch (...) {
            std::cerr << "Config change callback unknown exception"
                      << std::endl;
          }
        }

        std::cout << "Config reloaded from: " << config_.file_path << std::endl;
      } else {
        std::cerr << "Failed to reload config from: " << config_.file_path << std::endl;
      }
    }
  }
}

}  // namespace client
}  // namespace cedar
