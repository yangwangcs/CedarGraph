// Copyright 2025 The Cedar Authors
//
// Simple logging framework

#ifndef CEDAR_CLIENT_LOGGER_H_
#define CEDAR_CLIENT_LOGGER_H_

#include <fstream>
#include <iostream>
#include <mutex>
#include <string>
#include <sstream>

namespace cedar {
namespace client {

// Log levels
enum class LogLevel {
  DEBUG = 0,
  INFO = 1,
  WARN = 2,
  ERROR = 3,
  FATAL = 4
};

// Log configuration
struct LogConfig {
  LogLevel console_level = LogLevel::INFO;
  LogLevel file_level = LogLevel::DEBUG;
  std::string log_file_path;
  bool enable_console = true;
  bool enable_file = false;
  bool enable_timestamp = true;
  bool enable_thread_id = false;
  int max_file_size_mb = 100;
  int max_file_count = 10;
};

// Logger class
class Logger {
 public:
  Logger(const LogConfig& config = LogConfig());
  ~Logger();

  // Static instance (singleton)
  static Logger& GetInstance();

  // Initialize logger
  void Initialize(const LogConfig& config);

  // Log methods
  void Debug(const std::string& message);
  void Info(const std::string& message);
  void Warn(const std::string& message);
  void Error(const std::string& message);
  void Fatal(const std::string& message);

  // Log with format
  template<typename... Args>
  void Debug(const std::string& format, Args... args);

  template<typename... Args>
  void Info(const std::string& format, Args... args);

  template<typename... Args>
  void Warn(const std::string& format, Args... args);

  template<typename... Args>
  void Error(const std::string& format, Args... args);

  template<typename... Args>
  void Fatal(const std::string& format, Args... args);

  // Set log level
  void SetConsoleLevel(LogLevel level);
  void SetFileLevel(LogLevel level);

  // Get current log level
  LogLevel GetConsoleLevel() const;
  LogLevel GetFileLevel() const;

  // Flush logs
  void Flush();

  // Check if level is enabled
  bool IsDebugEnabled() const;
  bool IsInfoEnabled() const;
  bool IsWarnEnabled() const;
  bool IsErrorEnabled() const;
  bool IsFatalEnabled() const;

 private:
  LogConfig config_;
  std::ofstream log_file_;
  mutable std::mutex mutex_;
  int current_file_size_ = 0;
  int current_file_index_ = 0;

  // Internal log method
  void Log(LogLevel level, const std::string& message);

  // Format log message
  std::string FormatMessage(LogLevel level, const std::string& message) const;

  // Get level string
  std::string GetLevelString(LogLevel level) const;

  // Get timestamp
  std::string GetTimestamp() const;

  // Get thread ID
  std::string GetThreadId() const;

  // Rotate log file
  void RotateLogFile();

  // Open log file
  void OpenLogFile();
};

// Convenience macros
#define LOG_DEBUG(message) \
  if (cedar::client::Logger::GetInstance().IsDebugEnabled()) \
    cedar::client::Logger::GetInstance().Debug(message)

#define LOG_INFO(message) \
  if (cedar::client::Logger::GetInstance().IsInfoEnabled()) \
    cedar::client::Logger::GetInstance().Info(message)

#define LOG_WARN(message) \
  if (cedar::client::Logger::GetInstance().IsWarnEnabled()) \
    cedar::client::Logger::GetInstance().Warn(message)

#define LOG_ERROR(message) \
  if (cedar::client::Logger::GetInstance().IsErrorEnabled()) \
    cedar::client::Logger::GetInstance().Error(message)

#define LOG_FATAL(message) \
  if (cedar::client::Logger::GetInstance().IsFatalEnabled()) \
    cedar::client::Logger::GetInstance().Fatal(message)

}  // namespace client
}  // namespace cedar

#endif  // CEDAR_CLIENT_LOGGER_H_
