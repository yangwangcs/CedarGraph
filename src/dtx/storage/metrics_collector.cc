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

#include "cedar/dtx/monitoring.h"

#include <thread>
#include <fstream>
#include <iomanip>

#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <sstream>

#include <curl/curl.h>

#include "cedar/core/status.h"

namespace cedar {
namespace dtx {
namespace monitoring {

// =============================================================================
// LogEntry Implementation
// =============================================================================

std::string LogEntry::ToJson() const {
  std::ostringstream oss;
  
  auto time_t = std::chrono::system_clock::to_time_t(timestamp);
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      timestamp.time_since_epoch()).count() % 1000;
  
  struct tm timeinfo;
  localtime_r(&time_t, &timeinfo);
  oss << "{";
  oss << "\"timestamp\":\"" << std::put_time(&timeinfo,
                                               "%Y-%m-%dT%H:%M:%S");
  oss << "." << std::setfill('0') << std::setw(3) << ms << "Z\",";
  oss << "\"level\":\"" << LogLevelToString(level) << "\",";
  oss << "\"component\":\"" << component << "\",";
  oss << "\"message\":\"" << message << "\",";
  
  if (!fields.empty()) {
    oss << "\"fields\":{";
    bool first = true;
    for (const auto& [key, value] : fields) {
      if (!first) oss << ",";
      first = false;
      oss << "\"" << key << "\":\"" << value << "\"";
    }
    oss << "},";
  }
  
  if (!file.empty()) {
    oss << "\"file\":\"" << file << "\",";
    oss << "\"line\":" << line << ",";
  }
  
  oss << "\"thread\":\"" << thread_id << "\"";
  oss << "}";
  
  return oss.str();
}

std::string LogEntry::ToText() const {
  std::ostringstream oss;
  
  auto time_t = std::chrono::system_clock::to_time_t(timestamp);
  
  struct tm timeinfo;
  localtime_r(&time_t, &timeinfo);
  oss << "[" << std::put_time(&timeinfo, "%Y-%m-%d %H:%M:%S") << "]";
  oss << " [" << LogLevelToString(level) << "]";
  oss << " [" << component << "]";
  oss << " " << message;
  
  if (!fields.empty()) {
    oss << " {";
    bool first = true;
    for (const auto& [key, value] : fields) {
      if (!first) oss << ", ";
      first = false;
      oss << key << "=" << value;
    }
    oss << "}";
  }
  
  return oss.str();
}

// =============================================================================
// ConsoleSink Implementation
// =============================================================================

void ConsoleSink::Write(const LogEntry& entry) {
  // 根据级别选择输出流
  if (entry.level >= LogLevel::kError) {
    std::cerr << entry.ToText() << std::endl;
  } else {
    std::cerr << entry.ToText() << std::endl;
  }
}

void ConsoleSink::Flush() {
  std::cerr.flush();
  std::cerr.flush();
}

// =============================================================================
// FileSink Implementation
// =============================================================================

FileSink::FileSink(const Config& config) : config_(config) {
  std::filesystem::create_directories(config_.log_dir);
  OpenNewFile();
}

FileSink::~FileSink() {
  if (current_file_.is_open()) {
    current_file_.close();
  }
}

void FileSink::Write(const LogEntry& entry) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  RotateIfNeeded();
  
  if (!current_file_.is_open()) {
    return;
  }
  
  std::string line = entry.ToJson();
  current_file_ << line << std::endl;
  current_size_ += line.size() + 1;
}

void FileSink::Flush() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (current_file_.is_open()) {
    current_file_.flush();
  }
}

void FileSink::RotateIfNeeded() {
  if (current_size_ >= config_.max_file_size) {
    if (current_file_.is_open()) {
      current_file_.close();
    }
    OpenNewFile();
  }
}

void FileSink::OpenNewFile() {
  auto now = std::chrono::system_clock::now();
  auto time_t = std::chrono::system_clock::to_time_t(now);
  
  std::ostringstream oss;
  struct tm timeinfo;
  localtime_r(&time_t, &timeinfo);
  oss << config_.filename_prefix << "_";
  oss << std::put_time(&timeinfo, "%Y%m%d_%H%M%S");
  oss << "_" << file_index_++ << ".log";
  
  current_filename_ = config_.log_dir + "/" + oss.str();
  current_file_.open(current_filename_, std::ios::out | std::ios::app);
  current_size_ = 0;
}

// =============================================================================
// NetworkSink Implementation
// =============================================================================

NetworkSink::NetworkSink(const Config& config) : config_(config) {
  running_.store(true);
  send_thread_ = std::thread(&NetworkSink::SendLoop, this);
}

NetworkSink::~NetworkSink() {
  running_.store(false);
  if (send_thread_.joinable()) {
    send_thread_.join();
  }
}

void NetworkSink::Write(const LogEntry& entry) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (buffer_.size() < config_.buffer_size) {
    buffer_.push(entry);
  }
}

void NetworkSink::Flush() {
  // 网络 sink 会定期发送，不需要手动 flush
}

void NetworkSink::SendLoop() {
  while (running_.load()) {
    std::this_thread::sleep_for(config_.flush_interval);
    
    if (!running_.load()) break;
    
    std::queue<LogEntry> to_send;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      std::swap(to_send, buffer_);
    }
    
    // Send logs via HTTP POST to the configured endpoint
    if (!config_.endpoint.empty() && !to_send.empty()) {
      CURL* curl = curl_easy_init();
      if (curl) {
        std::string url = "http://" + config_.endpoint;
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 5000L);
        
        struct curl_slist* headers = nullptr;
        headers = curl_slist_append(headers, "Content-Type: application/json");
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        
        while (!to_send.empty()) {
          const auto& entry = to_send.front();
          std::string payload = entry.ToJson();
          curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload.c_str());
          curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, payload.length());
          
          CURLcode res = curl_easy_perform(curl);
          if (res != CURLE_OK) {
            // Silently drop on failure to avoid blocking
          }
          to_send.pop();
        }
        
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);
      }
    } else {
      while (!to_send.empty()) {
        to_send.pop();
      }
    }
  }
}

// =============================================================================
// Logger Implementation
// =============================================================================

Logger::~Logger() {
  Shutdown();
}

Logger* Logger::GetInstance() {
  static Logger instance;
  return &instance;
}

Status Logger::Initialize(const Config& config) {
  config_ = config;
  current_level_.store(config.min_level);
  
  // 添加控制台 sink
  if (config.enable_console) {
    AddSink(std::make_unique<ConsoleSink>());
  }
  
  // 添加文件 sink
  if (config.enable_file) {
    AddSink(std::make_unique<FileSink>(config.file_config));
  }
  
  // 添加网络 sink
  if (config.enable_network) {
    AddSink(std::make_unique<NetworkSink>(config.network_config));
  }
  
  // 启动异步线程
  if (config.async_mode) {
    async_thread_ = std::thread(&Logger::AsyncWriteLoop, this);
  }
  
  initialized_.store(true);
  return Status::OK();
}

void Logger::Shutdown() {
  if (!initialized_.exchange(false)) {
    return;
  }
  
  shutdown_.store(true);
  queue_cv_.notify_all();
  
  if (async_thread_.joinable()) {
    async_thread_.join();
  }
  
  // 刷新所有 sink
  std::lock_guard<std::mutex> lock(sinks_mutex_);
  for (auto& [name, sink] : sinks_) {
    sink->Flush();
  }
}

void Logger::SetLogLevel(LogLevel level) {
  current_level_.store(level);
}

LogLevel Logger::GetLogLevel() const {
  return current_level_.load();
}

void Logger::AddSink(std::unique_ptr<LogSink> sink) {
  std::lock_guard<std::mutex> lock(sinks_mutex_);
  sinks_.emplace_back("sink_" + std::to_string(sinks_.size()), std::move(sink));
}

void Logger::RemoveSink(const std::string& name) {
  std::lock_guard<std::mutex> lock(sinks_mutex_);
  sinks_.erase(
      std::remove_if(sinks_.begin(), sinks_.end(),
          [&name](const auto& p) { return p.first == name; }),
      sinks_.end());
}

void Logger::Log(LogLevel level, const std::string& component,
                 const std::string& message,
                 const std::map<std::string, std::string>& fields,
                 const char* file, int line) {
  if (!initialized_.load() || level < current_level_.load()) {
    return;
  }
  
  LogEntry entry;
  entry.timestamp = std::chrono::system_clock::now();
  entry.level = level;
  entry.component = component;
  entry.message = message;
  entry.fields = fields;
  entry.file = file ? file : "";
  entry.line = line;
  entry.thread_id = std::this_thread::get_id();
  
  if (config_.async_mode) {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    if (log_queue_.size() < config_.async_queue_size) {
      log_queue_.push(entry);
      queue_cv_.notify_one();
    }
  } else {
    std::lock_guard<std::mutex> lock(sinks_mutex_);
    for (auto& [name, sink] : sinks_) {
      sink->Write(entry);
    }
  }
}

void Logger::AsyncWriteLoop() {
  while (!shutdown_.load()) {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    queue_cv_.wait_for(lock, std::chrono::milliseconds(100),
        [this] { return !log_queue_.empty() || shutdown_.load(); });
    
    std::queue<LogEntry> to_process;
    std::swap(to_process, log_queue_);
    lock.unlock();
    
    while (!to_process.empty()) {
      auto& entry = to_process.front();
      
      std::lock_guard<std::mutex> sink_lock(sinks_mutex_);
      for (auto& [name, sink] : sinks_) {
        sink->Write(entry);
      }
      
      to_process.pop();
    }
  }
}

void Logger::Trace(const std::string& component, const std::string& msg,
                   const char* file, int line) {
  Log(LogLevel::kTrace, component, msg, {}, file, line);
}

void Logger::Debug(const std::string& component, const std::string& msg,
                   const char* file, int line) {
  Log(LogLevel::kDebug, component, msg, {}, file, line);
}

void Logger::Info(const std::string& component, const std::string& msg,
                  const char* file, int line) {
  Log(LogLevel::kInfo, component, msg, {}, file, line);
}

void Logger::Warn(const std::string& component, const std::string& msg,
                  const char* file, int line) {
  Log(LogLevel::kWarn, component, msg, {}, file, line);
}

void Logger::Error(const std::string& component, const std::string& msg,
                   const char* file, int line) {
  Log(LogLevel::kError, component, msg, {}, file, line);
}

void Logger::Fatal(const std::string& component, const std::string& msg,
                   const char* file, int line) {
  Log(LogLevel::kFatal, component, msg, {}, file, line);
}

// =============================================================================
// LogNotifier Implementation
// =============================================================================

void LogNotifier::Notify(const Alert& alert) {
  std::string severity_str;
  switch (alert.severity) {
    case AlertSeverity::kInfo: severity_str = "INFO"; break;
    case AlertSeverity::kWarning: severity_str = "WARNING"; break;
    case AlertSeverity::kCritical: severity_str = "CRITICAL"; break;
    case AlertSeverity::kEmergency: severity_str = "EMERGENCY"; break;
  }
  
  std::ostringstream oss;
  oss << "[ALERT] [" << severity_str << "] ";
  oss << "[" << alert.rule_name << "] ";
  oss << alert.message;
  
  if (!alert.labels.empty()) {
    oss << " {";
    bool first = true;
    for (const auto& [key, value] : alert.labels) {
      if (!first) oss << ", ";
      first = false;
      oss << key << "=" << value;
    }
    oss << "}";
  }
  
  if (alert.is_resolved) {
    std::cerr << oss.str() << " [RESOLVED]" << std::endl;
  } else {
    std::cerr << oss.str() << " [FIRED]" << std::endl;
  }
}

// =============================================================================
// WebhookNotifier Implementation
// =============================================================================

WebhookNotifier::WebhookNotifier(const Config& config) : config_(config) {}

void WebhookNotifier::Notify(const Alert& alert) {
  if (config_.url.empty()) return;
  
  CURL* curl = curl_easy_init();
  if (!curl) return;
  
  // Build JSON payload
  std::ostringstream oss;
  oss << "{";
  oss << "\"alert_id\":" << alert.alert_id << ",";
  oss << "\"rule_name\":\"" << alert.rule_name << "\",";
  oss << "\"severity\":\"" << static_cast<int>(alert.severity) << "\",";
  oss << "\"message\":\"" << alert.message << "\",";
  oss << "\"resolved\":" << (alert.is_resolved ? "true" : "false");
  if (!alert.labels.empty()) {
    oss << ",\"labels\":{";
    bool first = true;
    for (const auto& [k, v] : alert.labels) {
      if (!first) oss << ",";
      first = false;
      oss << "\"" << k << "\":\"" << v << "\"";
    }
    oss << "}";
  }
  oss << "}";
  std::string payload = oss.str();
  
  curl_easy_setopt(curl, CURLOPT_URL, config_.url.c_str());
  curl_easy_setopt(curl, CURLOPT_POST, 1L);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload.c_str());
  curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, payload.length());
  curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, config_.timeout.count());
  
  struct curl_slist* headers = nullptr;
  headers = curl_slist_append(headers, "Content-Type: application/json");
  for (const auto& [key, value] : config_.headers) {
    std::string header = key + ": " + value;
    headers = curl_slist_append(headers, header.c_str());
  }
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  
  curl_easy_perform(curl);
  
  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);
}

// =============================================================================
// AlertManager Implementation
// =============================================================================

AlertManager::~AlertManager() {
  Shutdown();
}

AlertManager* AlertManager::GetInstance() {
  static AlertManager instance;
  return &instance;
}

Status AlertManager::Initialize(const Config& config) {
  config_ = config;
  running_.store(true);
  
  // 添加默认的日志通知器
  AddNotifier(std::make_unique<LogNotifier>());
  
  eval_thread_ = std::thread(&AlertManager::EvaluationLoop, this);
  
  return Status::OK();
}

void AlertManager::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }
  
  if (eval_thread_.joinable()) {
    eval_thread_.join();
  }
}

void AlertManager::AddRule(const AlertRule& rule) {
  std::lock_guard<std::mutex> lock(rules_mutex_);
  rules_[rule.name] = rule;
}

void AlertManager::RemoveRule(const std::string& name) {
  std::lock_guard<std::mutex> lock(rules_mutex_);
  rules_.erase(name);
}

void AlertManager::AddNotifier(std::unique_ptr<AlertNotifier> notifier) {
  std::lock_guard<std::mutex> lock(notifiers_mutex_);
  notifiers_.push_back(std::move(notifier));
}

void AlertManager::FireAlert(const std::string& rule_name,
                              const std::map<std::string, std::string>& labels) {
  std::lock_guard<std::mutex> lock(rules_mutex_);
  
  auto it = rules_.find(rule_name);
  if (it == rules_.end()) {
    return;
  }
  
  const auto& rule = it->second;
  
  Alert alert;
  alert.alert_id = next_alert_id_.fetch_add(1);
  alert.rule_name = rule_name;
  alert.severity = rule.severity;
  alert.message = rule.description;
  alert.labels = labels;
  alert.fired_at = std::chrono::system_clock::now();
  alert.is_resolved = false;
  
  {
    std::lock_guard<std::mutex> alert_lock(alerts_mutex_);
    alerts_[alert.alert_id] = alert;
    active_alerts_.push_back(alert.alert_id);
  }
  
  SendNotification(alert);
}

void AlertManager::ResolveAlert(uint64_t alert_id) {
  std::lock_guard<std::mutex> lock(alerts_mutex_);
  
  auto it = alerts_.find(alert_id);
  if (it != alerts_.end() && !it->second.is_resolved) {
    it->second.is_resolved = true;
    it->second.resolved_at = std::chrono::system_clock::now();
    
    active_alerts_.erase(
        std::remove(active_alerts_.begin(), active_alerts_.end(), alert_id),
        active_alerts_.end());
    
    SendNotification(it->second);
  }
}

std::vector<Alert> AlertManager::GetActiveAlerts() const {
  std::lock_guard<std::mutex> lock(alerts_mutex_);
  
  std::vector<Alert> result;
  for (uint64_t id : active_alerts_) {
    auto it = alerts_.find(id);
    if (it != alerts_.end()) {
      result.push_back(it->second);
    }
  }
  
  return result;
}

std::vector<Alert> AlertManager::GetAlertHistory(const std::string& rule_name) const {
  std::lock_guard<std::mutex> lock(alerts_mutex_);
  
  std::vector<Alert> result;
  
  for (const auto& [id, alert] : alerts_) {
    if (rule_name.empty() || alert.rule_name == rule_name) {
      result.push_back(alert);
    }
  }
  
  // 按时间排序
  std::sort(result.begin(), result.end(),
            [](const Alert& a, const Alert& b) {
              return a.fired_at > b.fired_at;
            });
  
  return result;
}

void AlertManager::EvaluationLoop() {
  while (running_.load()) {
    std::this_thread::sleep_for(config_.evaluation_interval);
    
    if (!running_.load()) break;
    
    if (!metric_provider_) continue;
    
    std::lock_guard<std::mutex> lock(rules_mutex_);
    for (const auto& [name, rule] : rules_) {
      double value = metric_provider_(rule.condition_metric);
      bool triggered = false;
      
      if (rule.comparison == ">") {
        triggered = value > rule.threshold;
      } else if (rule.comparison == ">=") {
        triggered = value >= rule.threshold;
      } else if (rule.comparison == "<") {
        triggered = value < rule.threshold;
      } else if (rule.comparison == "<=") {
        triggered = value <= rule.threshold;
      } else if (rule.comparison == "==") {
        triggered = value == rule.threshold;
      }
      
      // Check if already active
      bool already_active = false;
      {
        std::lock_guard<std::mutex> alert_lock(alerts_mutex_);
        for (uint64_t id : active_alerts_) {
          auto it = alerts_.find(id);
          if (it != alerts_.end() && it->second.rule_name == name) {
            already_active = true;
            if (!triggered) {
              // Auto-resolve if condition is no longer met
              ResolveAlert(id);
            }
            break;
          }
        }
      }
      
      if (triggered && !already_active) {
        FireAlert(name);
      }
    }
  }
}

void AlertManager::SendNotification(const Alert& alert) {
  std::lock_guard<std::mutex> lock(notifiers_mutex_);
  
  for (auto& notifier : notifiers_) {
    notifier->Notify(alert);
  }
}

}  // namespace monitoring
}  // namespace dtx
}  // namespace cedar
