// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");

#ifndef CEDAR_COMMON_JSON_LOGGER_H_
#define CEDAR_COMMON_JSON_LOGGER_H_

#ifdef CEDAR_NO_GLOG
#include <iostream>
#else
#include <glog/logging.h>
#endif
#include <chrono>
#include <iomanip>
#include <sstream>
#include <string>
#include <thread>

namespace cedar {
namespace common {

// Thread-local request ID
inline thread_local std::string kCurrentRequestId;

inline void SetRequestId(const std::string& id) { kCurrentRequestId = id; }
inline std::string GetRequestId() { return kCurrentRequestId; }
inline void ClearRequestId() { kCurrentRequestId.clear(); }

// Generate a simple request ID
inline std::string GenerateRequestId() {
  auto now = std::chrono::high_resolution_clock::now().time_since_epoch().count();
  std::ostringstream oss;
  oss << std::hex << now << "-" << std::this_thread::get_id();
  return oss.str();
}

// JSON-formatted log output via glog
// Usage: JSON_LOG(INFO) << "message" << KV("key", value);
class JsonLogStream {
 public:
  explicit JsonLogStream(int severity) : severity_(severity), first_field_(true) {
    ss_ << "{";
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();
    auto t = std::chrono::system_clock::to_time_t(now);
    std::tm tm;
    localtime_r(&t, &tm);
    ss_ << "\"timestamp\":\"" << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S");
    ss_ << "." << std::setfill('0') << std::setw(3) << (ms % 1000) << "Z\"";
    first_field_ = false;
    if (!kCurrentRequestId.empty()) {
      AddField("request_id", kCurrentRequestId);
    }
  }

  template <typename T>
  JsonLogStream& operator<<(const T& value) {
    ss_ << value;
    return *this;
  }

  template <typename T>
  JsonLogStream& KV(const std::string& key, const T& value) {
    AddField(key, value);
    return *this;
  }

  ~JsonLogStream() {
    ss_ << "}";
#ifdef CEDAR_NO_GLOG
    std::cerr << ss_.str() << std::endl;
    if (severity_ == 3) {
      std::abort();
    }
#else
    switch (severity_) {
      case 0:
        LOG(INFO) << ss_.str();
        break;
      case 1:
        LOG(WARNING) << ss_.str();
        break;
      case 2:
        LOG(ERROR) << ss_.str();
        break;
      case 3:
        LOG(FATAL) << ss_.str();
        break;
      default:
        LOG(INFO) << ss_.str();
        break;
    }
#endif
  }

 private:
  template <typename T>
  void AddField(const std::string& key, const T& value) {
    if (!first_field_) ss_ << ",";
    first_field_ = false;
    ss_ << "\"" << key << "\":\"" << value << "\"";
  }

  void AddField(const std::string& key, const char* value) {
    if (!first_field_) ss_ << ",";
    first_field_ = false;
    ss_ << "\"" << key << "\":\"" << value << "\"";
  }

  void AddField(const std::string& key, int value) {
    if (!first_field_) ss_ << ",";
    first_field_ = false;
    ss_ << "\"" << key << "\":" << value;
  }

  void AddField(const std::string& key, long value) {
    if (!first_field_) ss_ << ",";
    first_field_ = false;
    ss_ << "\"" << key << "\":" << value;
  }

  void AddField(const std::string& key, long long value) {
    if (!first_field_) ss_ << ",";
    first_field_ = false;
    ss_ << "\"" << key << "\":" << value;
  }

  void AddField(const std::string& key, double value) {
    if (!first_field_) ss_ << ",";
    first_field_ = false;
    ss_ << "\"" << key << "\":" << value;
  }

  void AddField(const std::string& key, bool value) {
    if (!first_field_) ss_ << ",";
    first_field_ = false;
    ss_ << "\"" << key << "\":" << (value ? "true" : "false");
  }

  int severity_;
  bool first_field_;
  std::ostringstream ss_;
};

#define JSON_LOG_SEVERITY_INFO 0
#define JSON_LOG_SEVERITY_WARNING 1
#define JSON_LOG_SEVERITY_ERROR 2
#define JSON_LOG_SEVERITY_FATAL 3

#define JSON_LOG(severity) \
  cedar::common::JsonLogStream(JSON_LOG_SEVERITY_##severity)

}  // namespace common
}  // namespace cedar

#endif  // CEDAR_COMMON_JSON_LOGGER_H_
