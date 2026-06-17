// Copyright 2025 The Cedar Authors
//
// Simple logging framework implementation

#include "cedar/client/logger.h"

#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <thread>

namespace cedar {
namespace client {

Logger::Logger(const LogConfig& config)
    : config_(config) {
  if (config_.enable_file && !config_.log_file_path.empty()) {
    OpenLogFile();
  }
}

Logger::~Logger() {
  if (log_file_.is_open()) {
    log_file_.close();
  }
}

Logger& Logger::GetInstance() {
  static Logger instance;
  return instance;
}

void Logger::Initialize(const LogConfig& config) {
  std::lock_guard<std::mutex> lock(mutex_);
  config_ = config;

  if (log_file_.is_open()) {
    log_file_.close();
  }

  if (config_.enable_file && !config_.log_file_path.empty()) {
    OpenLogFile();
  }
}

void Logger::Debug(const std::string& message) {
  Log(LogLevel::DEBUG, message);
}

void Logger::Info(const std::string& message) {
  Log(LogLevel::INFO, message);
}

void Logger::Warn(const std::string& message) {
  Log(LogLevel::WARN, message);
}

void Logger::Error(const std::string& message) {
  Log(LogLevel::ERROR, message);
}

void Logger::Fatal(const std::string& message) {
  Log(LogLevel::FATAL, message);
}

void Logger::SetConsoleLevel(LogLevel level) {
  std::lock_guard<std::mutex> lock(mutex_);
  config_.console_level = level;
}

void Logger::SetFileLevel(LogLevel level) {
  std::lock_guard<std::mutex> lock(mutex_);
  config_.file_level = level;
}

LogLevel Logger::GetConsoleLevel() const {
  return config_.console_level;
}

LogLevel Logger::GetFileLevel() const {
  return config_.file_level;
}

void Logger::Flush() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (config_.enable_console) {
    std::cout << std::flush;
  }
  if (config_.enable_file && log_file_.is_open()) {
    log_file_ << std::flush;
  }
}

bool Logger::IsDebugEnabled() const {
  return config_.console_level <= LogLevel::DEBUG || 
         (config_.enable_file && config_.file_level <= LogLevel::DEBUG);
}

bool Logger::IsInfoEnabled() const {
  return config_.console_level <= LogLevel::INFO || 
         (config_.enable_file && config_.file_level <= LogLevel::INFO);
}

bool Logger::IsWarnEnabled() const {
  return config_.console_level <= LogLevel::WARN || 
         (config_.enable_file && config_.file_level <= LogLevel::WARN);
}

bool Logger::IsErrorEnabled() const {
  return config_.console_level <= LogLevel::ERROR || 
         (config_.enable_file && config_.file_level <= LogLevel::ERROR);
}

bool Logger::IsFatalEnabled() const {
  return config_.console_level <= LogLevel::FATAL || 
         (config_.enable_file && config_.file_level <= LogLevel::FATAL);
}

void Logger::Log(LogLevel level, const std::string& message) {
  std::lock_guard<std::mutex> lock(mutex_);

  std::string formatted_message = FormatMessage(level, message);

  // Console output
  if (config_.enable_console && level >= config_.console_level) {
    if (level >= LogLevel::ERROR) {
      std::cerr << formatted_message << std::endl;
    } else {
      std::cout << formatted_message << std::endl;
    }
  }

  // File output
  if (config_.enable_file && log_file_.is_open() && level >= config_.file_level) {
    log_file_ << formatted_message << std::endl;
    current_file_size_ += formatted_message.size() + 1;

    // Check if we need to rotate
    if (current_file_size_ >= config_.max_file_size_mb * 1024 * 1024) {
      RotateLogFile();
    }
  }
}

std::string Logger::FormatMessage(LogLevel level, const std::string& message) const {
  std::stringstream ss;

  if (config_.enable_timestamp) {
    ss << GetTimestamp() << " ";
  }

  ss << "[" << GetLevelString(level) << "] ";

  if (config_.enable_thread_id) {
    ss << GetThreadId() << " ";
  }

  ss << message;

  return ss.str();
}

std::string Logger::GetLevelString(LogLevel level) const {
  switch (level) {
    case LogLevel::DEBUG: return "DEBUG";
    case LogLevel::INFO:  return "INFO ";
    case LogLevel::WARN:  return "WARN ";
    case LogLevel::ERROR: return "ERROR";
    case LogLevel::FATAL: return "FATAL";
    default:              return "UNKNOWN";
  }
}

std::string Logger::GetTimestamp() const {
  auto now = std::chrono::system_clock::now();
  auto time = std::chrono::system_clock::to_time_t(now);
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      now.time_since_epoch()) % 1000;

  std::stringstream ss;
  ss << std::put_time(std::localtime(&time), "%Y-%m-%d %H:%M:%S");
  ss << "." << std::setfill('0') << std::setw(3) << ms.count();

  return ss.str();
}

std::string Logger::GetThreadId() const {
  std::stringstream ss;
  ss << std::this_thread::get_id();
  return ss.str();
}

void Logger::RotateLogFile() {
  if (log_file_.is_open()) {
    log_file_.close();
  }

  current_file_index_++;
  if (current_file_index_ >= config_.max_file_count) {
    current_file_index_ = 0;
  }

  OpenLogFile();
}

void Logger::OpenLogFile() {
  std::string filename = config_.log_file_path;
  if (current_file_index_ > 0) {
    filename += "." + std::to_string(current_file_index_);
  }

  log_file_.open(filename, std::ios::app);
  current_file_size_ = 0;
}

}  // namespace client
}  // namespace cedar
