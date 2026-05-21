// Copyright 2026 CedarGraph Authors
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

#ifndef CEDAR_DTX_INDEX_ALERT_CHANNELS_H_
#define CEDAR_DTX_INDEX_ALERT_CHANNELS_H_

#include "cedar/core/status.h"
#include "cedar/dtx/storage/metrics_collector.h"

#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

namespace cedar {
namespace dtx {
namespace index {

// =============================================================================
// Alert Channel Configurations
// =============================================================================

struct DingTalkConfig {
  std::string webhook_url;
  std::string secret;
  std::string title_prefix{"CedarGraph"};
  bool at_all{false};
  std::vector<std::string> at_mobiles;
};

struct PagerDutyConfig {
  std::string api_url{"https://events.pagerduty.com/v2/enqueue"};
  std::string integration_key;
  std::string source{"CedarGraph"};
  std::vector<std::string> severity_mapping{"info", "warning", "critical", "critical"};
};

struct EmailConfig {
  std::string subject_prefix{"[CedarGraph]"};
  std::string smtp_server;
  uint16_t smtp_port{587};
  std::string from;
  std::vector<std::string> to;
  std::string username;
  std::string password;
  bool use_tls{true};
};

struct WebhookConfig {
  std::string url;
  std::string method{"POST"};
  std::string content_type{"application/json"};
  std::map<std::string, std::string> headers;
  std::string template_json;
  uint32_t timeout_ms{10000};
  int retry_count{3};
};

// =============================================================================
// Alert Channel Interface
// =============================================================================

class AlertChannel {
 public:
  virtual ~AlertChannel() = default;
  virtual Status SendAlert(const storage::Alert& alert) = 0;
  virtual bool IsHealthy() const = 0;
  virtual Status TestConnection() = 0;
  virtual std::string GetName() const = 0;
};

// =============================================================================
// DingTalk Channel
// =============================================================================

class DingTalkChannel : public AlertChannel {
 public:
  explicit DingTalkChannel(const DingTalkConfig& config);
  Status SendAlert(const storage::Alert& alert) override;
  bool IsHealthy() const override;
  Status TestConnection() override;
  std::string GetName() const override { return "dingtalk"; }

 private:
  std::string BuildMessage(const storage::Alert& alert);
  std::string SignRequest(uint64_t timestamp);

  DingTalkConfig config_;
  std::atomic<bool> healthy_{true};
  std::atomic<int64_t> last_error_time_{0};
};

// =============================================================================
// PagerDuty Channel
// =============================================================================

class PagerDutyChannel : public AlertChannel {
 public:
  explicit PagerDutyChannel(const PagerDutyConfig& config);
  Status SendAlert(const storage::Alert& alert) override;
  bool IsHealthy() const override;
  Status TestConnection() override;
  std::string GetName() const override { return "pagerduty"; }

 private:
  std::string BuildPayload(const storage::Alert& alert);
  std::string SeverityToString(storage::AlertSeverity severity);

  PagerDutyConfig config_;
  std::atomic<bool> healthy_{true};
  std::atomic<int64_t> last_error_time_{0};
};

// =============================================================================
// Email Channel
// =============================================================================

class EmailChannel : public AlertChannel {
 public:
  explicit EmailChannel(const EmailConfig& config);
  Status SendAlert(const storage::Alert& alert) override;
  bool IsHealthy() const override;
  Status TestConnection() override;
  std::string GetName() const override { return "email"; }

 private:
  std::string BuildSubject(const storage::Alert& alert);
  std::string BuildBody(const storage::Alert& alert);
  Status SendEmail(const std::string& subject, const std::string& body);

  EmailConfig config_;
  std::atomic<bool> healthy_{true};
};

// =============================================================================
// Webhook Channel
// =============================================================================

class WebhookChannel : public AlertChannel {
 public:
  explicit WebhookChannel(const WebhookConfig& config);
  Status SendAlert(const storage::Alert& alert) override;
  bool IsHealthy() const override;
  Status TestConnection() override;
  std::string GetName() const override { return "webhook"; }

 private:
  std::string BuildPayload(const storage::Alert& alert);
  Status SendHttpRequest(const std::string& payload);

  WebhookConfig config_;
  std::atomic<bool> healthy_{true};
};

// =============================================================================
// Alert Channel Manager
// =============================================================================

class AlertChannelManager {
 public:
  AlertChannelManager();
  ~AlertChannelManager();

  void AddChannel(std::shared_ptr<AlertChannel> channel);
  void RemoveChannel(const std::string& name);
  void RouteAlert(const storage::Alert& alert);
  void BroadcastAlert(const storage::Alert& alert);
  std::vector<std::pair<std::string, Status>> TestAllChannels();
  std::vector<std::pair<std::string, bool>> GetChannelHealth() const;
  void SetRoutingRule(storage::AlertSeverity severity,
                      const std::vector<std::string>& channel_names);

 private:
  mutable std::shared_mutex channels_mutex_;
  std::map<std::string, std::shared_ptr<AlertChannel>> channels_;

  std::shared_mutex rules_mutex_;
  std::map<storage::AlertSeverity, std::vector<std::string>> routing_rules_;
};

}  // namespace index
}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_INDEX_ALERT_CHANNELS_H_
