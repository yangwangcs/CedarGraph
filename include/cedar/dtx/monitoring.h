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

// =============================================================================
// Monitoring System - 监控与告警系统
// =============================================================================
// Features:
// - 结构化日志 (JSON)
// - 多级日志级别
// - 实时告警机制
// - 日志聚合与旋转
// - 性能指标收集
// - 分布式追踪支持
// =============================================================================

#ifndef CEDAR_DTX_MONITORING_H_
#define CEDAR_DTX_MONITORING_H_

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

namespace cedar {

class Status;

namespace dtx {
namespace monitoring {

// =============================================================================
// 日志级别
// =============================================================================

enum class LogLevel : uint8_t {
  kTrace = 0,    // 最详细
  kDebug = 1,
  kInfo = 2,
  kWarn = 3,
  kError = 4,
  kFatal = 5,    // 最严重
};

inline std::string LogLevelToString(LogLevel level) {
  switch (level) {
    case LogLevel::kTrace: return "TRACE";
    case LogLevel::kDebug: return "DEBUG";
    case LogLevel::kInfo: return "INFO";
    case LogLevel::kWarn: return "WARN";
    case LogLevel::kError: return "ERROR";
    case LogLevel::kFatal: return "FATAL";
    default: return "UNKNOWN";
  }
}

// =============================================================================
// 结构化日志条目
// =============================================================================

struct LogEntry {
  std::chrono::system_clock::time_point timestamp;
  LogLevel level;
  std::string component;
  std::string message;
  std::map<std::string, std::string> fields;
  std::string file;
  int line{0};
  std::thread::id thread_id;
  
  // 序列化为 JSON
  std::string ToJson() const;
  
  // 格式化为文本
  std::string ToText() const;
};

// =============================================================================
// 日志输出接口
// =============================================================================

class LogSink {
 public:
  virtual ~LogSink() = default;
  virtual void Write(const LogEntry& entry) = 0;
  virtual void Flush() = 0;
};

// 控制台输出
class ConsoleSink : public LogSink {
 public:
  void Write(const LogEntry& entry) override;
  void Flush() override;
};

// 文件输出（支持旋转）
class FileSink : public LogSink {
 public:
  struct Config {
    std::string log_dir;
    std::string filename_prefix{"cedar"};
    size_t max_file_size{100 * 1024 * 1024};  // 100MB
    uint32_t max_files{10};
    bool compress_old{false};
  };
  
  explicit FileSink(const Config& config);
  ~FileSink() override;
  
  void Write(const LogEntry& entry) override;
  void Flush() override;

 private:
  void RotateIfNeeded();
  void OpenNewFile();
  
  Config config_;
  std::mutex mutex_;
  std::ofstream current_file_;
  std::string current_filename_;
  size_t current_size_{0};
  uint32_t file_index_{0};
};

// 网络输出（发送到日志收集器）
class NetworkSink : public LogSink {
 public:
  struct Config {
    std::string endpoint;  // 如 "logstash:5044"
    std::chrono::milliseconds flush_interval{1000};
    size_t buffer_size{1000};
  };
  
  explicit NetworkSink(const Config& config);
  ~NetworkSink() override;
  
  void Write(const LogEntry& entry) override;
  void Flush() override;

 private:
  void SendLoop();
  
  Config config_;
  std::mutex mutex_;
  std::queue<LogEntry> buffer_;
  std::atomic<bool> running_{false};
  std::thread send_thread_;
};

// =============================================================================
// 日志管理器
// =============================================================================

class Logger {
 public:
  struct Config {
    LogLevel min_level{LogLevel::kInfo};
    bool enable_console{true};
    bool enable_file{true};
    FileSink::Config file_config;
    bool enable_network{false};
    NetworkSink::Config network_config;
    bool async_mode{true};  // 异步日志
    size_t async_queue_size{10000};
  };
  
  static Logger* GetInstance();
  
  Status Initialize(const Config& config);
  void Shutdown();
  
  // 设置全局日志级别
  void SetLogLevel(LogLevel level);
  LogLevel GetLogLevel() const;
  
  // 添加/移除 Sink
  void AddSink(std::unique_ptr<LogSink> sink);
  void RemoveSink(const std::string& name);
  
  // 日志记录接口
  void Log(LogLevel level, const std::string& component,
           const std::string& message,
           const std::map<std::string, std::string>& fields = {},
           const char* file = nullptr, int line = 0);
  
  // 便捷方法
  void Trace(const std::string& component, const std::string& msg,
             const char* file = nullptr, int line = 0);
  void Debug(const std::string& component, const std::string& msg,
             const char* file = nullptr, int line = 0);
  void Info(const std::string& component, const std::string& msg,
            const char* file = nullptr, int line = 0);
  void Warn(const std::string& component, const std::string& msg,
            const char* file = nullptr, int line = 0);
  void Error(const std::string& component, const std::string& msg,
             const char* file = nullptr, int line = 0);
  void Fatal(const std::string& component, const std::string& msg,
             const char* file = nullptr, int line = 0);

 private:
  Logger() = default;
  ~Logger();
  
  void AsyncWriteLoop();
  
  Config config_;
  std::atomic<bool> initialized_{false};
  std::atomic<LogLevel> current_level_{LogLevel::kInfo};
  
  std::mutex sinks_mutex_;
  std::vector<std::pair<std::string, std::unique_ptr<LogSink>>> sinks_;
  
  // 异步队列
  std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::queue<LogEntry> log_queue_;
  std::atomic<bool> shutdown_{false};
  std::thread async_thread_;
};

// =============================================================================
// 告警级别
// =============================================================================

enum class AlertSeverity : uint8_t {
  kInfo = 0,      // 信息
  kWarning = 1,   // 警告
  kCritical = 2,  // 严重
  kEmergency = 3, // 紧急
};

// =============================================================================
// 告警规则
// =============================================================================

struct AlertRule {
  std::string name;
  std::string description;
  AlertSeverity severity;
  
  // 条件（可以是指标阈值、错误率等）
  std::string condition_metric;
  double threshold{0.0};
  std::string comparison;  // ">", "<", "==", etc.
  std::chrono::milliseconds duration{0};  // 持续多久触发
  
  // 抑制配置
  std::chrono::milliseconds cooldown{300000};  // 5分钟冷却
  uint32_t max_notifications{10};
};

// =============================================================================
// 告警实例
// =============================================================================

struct Alert {
  uint64_t alert_id{0};
  std::string rule_name;
  AlertSeverity severity;
  std::string message;
  std::map<std::string, std::string> labels;
  std::chrono::system_clock::time_point fired_at;
  std::chrono::system_clock::time_point resolved_at;
  bool is_resolved{false};
  uint32_t notification_count{0};
};

// =============================================================================
// 告警通知接口
// =============================================================================

class AlertNotifier {
 public:
  virtual ~AlertNotifier() = default;
  virtual void Notify(const Alert& alert) = 0;
  virtual std::string GetName() const = 0;
};

// 日志通知
class LogNotifier : public AlertNotifier {
 public:
  void Notify(const Alert& alert) override;
  std::string GetName() const override { return "log"; }
};

// Webhook 通知
class WebhookNotifier : public AlertNotifier {
 public:
  struct Config {
    std::string url;
    std::map<std::string, std::string> headers;
    std::chrono::milliseconds timeout{10000};
  };
  
  explicit WebhookNotifier(const Config& config);
  void Notify(const Alert& alert) override;
  std::string GetName() const override { return "webhook"; }

 private:
  Config config_;
};

// =============================================================================
// 告警管理器
// =============================================================================

class AlertManager {
 public:
  struct Config {
    std::chrono::milliseconds evaluation_interval{60000};  // 1分钟
    std::chrono::milliseconds resolve_timeout{3600000};    // 1小时
  };
  
  static AlertManager* GetInstance();
  
  Status Initialize(const Config& config);
  void Shutdown();
  
  // 添加告警规则
  void AddRule(const AlertRule& rule);
  void RemoveRule(const std::string& name);
  
  // 添加通知器
  void AddNotifier(std::unique_ptr<AlertNotifier> notifier);
  
  // 手动触发告警
  void FireAlert(const std::string& rule_name, 
                 const std::map<std::string, std::string>& labels = {});
  
  // 手动解除告警
  void ResolveAlert(uint64_t alert_id);
  
  // 获取活跃告警
  std::vector<Alert> GetActiveAlerts() const;
  
  // 获取告警历史
  std::vector<Alert> GetAlertHistory(const std::string& rule_name = "") const;
  
  // 设置指标提供者（用于规则评估）
  using MetricProvider = std::function<double(const std::string& metric_name)>;
  void SetMetricProvider(MetricProvider provider) { metric_provider_ = std::move(provider); }

 private:
  AlertManager() = default;
  ~AlertManager();
  
  void EvaluationLoop();
  void SendNotification(const Alert& alert);
  
  Config config_;
  std::atomic<bool> running_{false};
  
  std::mutex rules_mutex_;
  std::map<std::string, AlertRule> rules_;
  
  std::mutex notifiers_mutex_;
  std::vector<std::unique_ptr<AlertNotifier>> notifiers_;
  
  mutable std::mutex alerts_mutex_;
  std::map<uint64_t, Alert> alerts_;
  std::vector<uint64_t> active_alerts_;
  std::atomic<uint64_t> next_alert_id_{1};
  
  std::thread eval_thread_;
  
  MetricProvider metric_provider_;
};

// =============================================================================
// 便捷宏定义
// =============================================================================

#define LOG_TRACE(component, msg) \
  ::cedar::dtx::monitoring::Logger::GetInstance()->Trace(component, msg, __FILE__, __LINE__)

#define LOG_DEBUG(component, msg) \
  ::cedar::dtx::monitoring::Logger::GetInstance()->Debug(component, msg, __FILE__, __LINE__)

#define LOG_INFO(component, msg) \
  ::cedar::dtx::monitoring::Logger::GetInstance()->Info(component, msg, __FILE__, __LINE__)

#define LOG_WARN(component, msg) \
  ::cedar::dtx::monitoring::Logger::GetInstance()->Warn(component, msg, __FILE__, __LINE__)

#define LOG_ERROR(component, msg) \
  ::cedar::dtx::monitoring::Logger::GetInstance()->Error(component, msg, __FILE__, __LINE__)

#define LOG_FATAL(component, msg) \
  ::cedar::dtx::monitoring::Logger::GetInstance()->Fatal(component, msg, __FILE__, __LINE__)

}  // namespace monitoring
}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_MONITORING_H_
