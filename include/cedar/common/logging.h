// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");

#ifndef CEDAR_COMMON_LOGGING_H_
#define CEDAR_COMMON_LOGGING_H_

#ifdef CEDAR_NO_GLOG

#include <iostream>
#include <sstream>

namespace cedar {
namespace common {

class NoGlogStream {
 public:
  explicit NoGlogStream(int severity) : severity_(severity) {}
  ~NoGlogStream() {
    if (severity_ >= 2) {
      std::cerr << ss_.str() << std::endl;
    } else {
      std::cout << ss_.str() << std::endl;
    }
    if (severity_ == 3) {
      std::abort();
    }
  }
  template <typename T>
  NoGlogStream& operator<<(const T& value) {
    ss_ << value;
    return *this;
  }
 private:
  int severity_;
  std::ostringstream ss_;
};

}  // namespace common
}  // namespace cedar

#define LOG_INFO_STREAM cedar::common::NoGlogStream(0)
#define LOG_WARNING_STREAM cedar::common::NoGlogStream(1)
#define LOG_ERROR_STREAM cedar::common::NoGlogStream(2)
#define LOG_FATAL_STREAM cedar::common::NoGlogStream(3)
#define LOG(severity) LOG_##severity##_STREAM
#define VLOG(level) cedar::common::NoGlogStream(0)
#define DLOG(severity) cedar::common::NoGlogStream(0)
#define CHECK(condition) \
  if (!(condition)) cedar::common::NoGlogStream(3) << "Check failed: " #condition " "

namespace google {
inline void InitGoogleLogging(const char* argv0) { (void)argv0; }
inline void ShutdownGoogleLogging() {}
}  // namespace google

static int FLAGS_logbufsecs = 0;
static int FLAGS_max_log_size = 1800;
static bool FLAGS_stop_logging_if_full_disk = false;

#else

#include <glog/logging.h>

#endif  // CEDAR_NO_GLOG

#endif  // CEDAR_COMMON_LOGGING_H_
