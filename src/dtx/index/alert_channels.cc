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

#include "cedar/dtx/index/alert_channels.h"

#include <curl/curl.h>
#include <openssl/hmac.h>
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/evp.h>
#include <glog/logging.h>

#include <sstream>
#include <iomanip>
#include <chrono>
#include <cstring>
#include <shared_mutex>

namespace cedar {
namespace dtx {
namespace index {

// 简单的 JSON 字符串转义
static std::string EscapeJsonString(const std::string& input) {
  std::ostringstream oss;
  for (char c : input) {
    switch (c) {
      case '"': oss << "\\\""; break;
      case '\\': oss << "\\\\"; break;
      case '\b': oss << "\\b"; break;
      case '\f': oss << "\\f"; break;
      case '\n': oss << "\\n"; break;
      case '\r': oss << "\\r"; break;
      case '\t': oss << "\\t"; break;
      default:
        if (static_cast<unsigned char>(c) < 0x20) {
          oss << std::hex << std::setfill('0') << std::setw(4) << "\\u" << (int)c;
        } else {
          oss << c;
        }
    }
  }
  return oss.str();
}

// Curl global initialization
static std::atomic<bool> curl_initialized_{false};
static std::mutex curl_init_mutex_;

static Status EnsureCurlInitialized() {
  if (!curl_initialized_.load()) {
    std::lock_guard<std::mutex> lock(curl_init_mutex_);
    if (!curl_initialized_.load()) {
      CURLcode res = curl_global_init(CURL_GLOBAL_DEFAULT);
      if (res != CURLE_OK) {
        return Status::IOError("Failed to initialize libcurl");
      }
      curl_initialized_.store(true);
    }
  }
  return Status::OK();
}

// Curl write callback for response
static size_t WriteCallback(void* contents, size_t size, size_t nmemb, std::string* userp) {
  userp->append(static_cast<char*>(contents), size * nmemb);
  return size * nmemb;
}

// Curl read callback for SMTP upload
struct SmtpReadData {
  const std::string* data;
  size_t pos{0};
};

static size_t ReadCallback(void* ptr, size_t size, size_t nmemb, void* userdata) {
  SmtpReadData* rd = static_cast<SmtpReadData*>(userdata);
  if (rd->pos >= rd->data->size()) return 0;
  size_t len = std::min(size * nmemb, rd->data->size() - rd->pos);
  std::memcpy(ptr, rd->data->data() + rd->pos, len);
  rd->pos += len;
  return len;
}

// ============ DingTalkChannel ============

DingTalkChannel::DingTalkChannel(const DingTalkConfig& config) : config_(config) {
  EnsureCurlInitialized();
}

Status DingTalkChannel::SendAlert(const storage::Alert& alert) {
  std::string message = BuildMessage(alert);
  
  CURL* curl = curl_easy_init();
  if (!curl) {
    return Status::IOError("Failed to initialize curl");
  }
  
  struct curl_slist* headers = nullptr;
  headers = curl_slist_append(headers, "Content-Type: application/json");
  
  std::string response;
  std::string url = config_.webhook_url;
  
  // Add timestamp and sign if secret is provided
  if (!config_.secret.empty()) {
    uint64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    std::string sign = SignRequest(timestamp);
    url += "&timestamp=" + std::to_string(timestamp) + "&sign=" + sign;
  }
  
  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, message.c_str());
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 10000L);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
  
  CURLcode res = curl_easy_perform(curl);
  
  long http_code = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
  
  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);
  
  if (res != CURLE_OK) {
    healthy_.store(false);
    last_error_time_.store(std::chrono::system_clock::now().time_since_epoch().count());
    return Status::IOError("HTTP request failed");
  }
  
  if (http_code != 200) {
    healthy_.store(false);
    return Status::IOError(std::string("HTTP error ") + std::to_string(http_code));
  }
  
  healthy_.store(true);
  return Status::OK();
}

bool DingTalkChannel::IsHealthy() const {
  return healthy_.load();
}

Status DingTalkChannel::TestConnection() {
  storage::Alert test_alert;
  test_alert.name = "Connection Test";
  test_alert.description = "This is a test message from CedarGraph";
  test_alert.severity = storage::AlertSeverity::kInfo;
  test_alert.summary = "Test connection from CedarGraph time index system";
  return SendAlert(test_alert);
}

std::string DingTalkChannel::BuildMessage(const storage::Alert& alert) {
  std::ostringstream oss;
  
  std::string severity_emoji;
  switch (alert.severity) {
    case storage::AlertSeverity::kCritical: severity_emoji = "🔴 "; break;
    case storage::AlertSeverity::kWarning: severity_emoji = "🟡 "; break;
    case storage::AlertSeverity::kInfo: severity_emoji = "🟢 "; break;
    case storage::AlertSeverity::kEmergency: severity_emoji = "🔴 "; break;
    default: severity_emoji = "⚪ ";
  }
  
  oss << "{\n";
  oss << "  \"msgtype\": \"markdown\",\n";
  oss << "  \"markdown\": {\n";
  oss << "    \"title\": \"" << EscapeJsonString(config_.title_prefix) << " " << EscapeJsonString(alert.name) << "\",\n";
  oss << "    \"text\": \"### " << severity_emoji << EscapeJsonString(config_.title_prefix) << " " << EscapeJsonString(alert.name) << "\\n\\n";
  oss << "**Severity:** " << static_cast<int>(alert.severity) << "\\n\\n";
  oss << "**Summary:** " << EscapeJsonString(alert.summary) << "\\n\\n";
  oss << "**Description:** " << EscapeJsonString(alert.description) << "\\n\\n";
  
  if (!alert.labels.empty()) {
    oss << "**Labels:**\\n";
    for (const auto& label : alert.labels) {
      oss << "- " << EscapeJsonString(label.first) << ": " << EscapeJsonString(label.second) << "\\n";
    }
    oss << "\\n";
  }
  
  auto time_t = std::chrono::system_clock::to_time_t(alert.fired_at);
  struct tm tm_buf;
  if (localtime_r(&time_t, &tm_buf)) {
    oss << "**Time:** " << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S");
  } else {
    oss << "**Time:** invalid";
  }
  oss << "\"\n";
  oss << "  }";
  
  if (config_.at_all || !config_.at_mobiles.empty()) {
    oss << ",\n  \"at\": {\n";
    if (config_.at_all) {
      oss << "    \"isAtAll\": true\n";
    } else {
      oss << "    \"atMobiles\": [";
      for (size_t i = 0; i < config_.at_mobiles.size(); ++i) {
        if (i > 0) oss << ", ";
        oss << "\"" << config_.at_mobiles[i] << "\"";
      }
      oss << "]\n";
    }
    oss << "  }";
  }
  
  oss << "\n}";
  
  return oss.str();
}

std::string DingTalkChannel::SignRequest(uint64_t timestamp) {
  std::string string_to_sign = std::to_string(timestamp) + "\n" + config_.secret;
  
  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int digest_len = 0;
  
  HMAC(EVP_sha256(),
       reinterpret_cast<const unsigned char*>(config_.secret.data()),
       static_cast<int>(config_.secret.length()),
       reinterpret_cast<const unsigned char*>(string_to_sign.data()),
       static_cast<int>(string_to_sign.length()),
       digest, &digest_len);
  
  // Base64 encode
  BIO* b64 = BIO_new(BIO_f_base64());
  BIO* bio = BIO_new(BIO_s_mem());
  if (!b64 || !bio) {
    if (b64) BIO_free_all(b64);
    if (bio) BIO_free_all(bio);
    return "";
  }
  b64 = BIO_push(b64, bio);
  BIO_write(b64, digest, digest_len);
  BIO_flush(b64);
  
  BUF_MEM* buffer_ptr;
  BIO_get_mem_ptr(b64, &buffer_ptr);
  
  std::string signature;
  if (buffer_ptr && buffer_ptr->length > 0) {
    signature.assign(buffer_ptr->data, buffer_ptr->length - 1);
  }
  BIO_free_all(b64);
  
  // URL encode
  std::ostringstream encoded;
  for (char c : signature) {
    if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
      encoded << c;
    } else if (c == '+') {
      encoded << "%2B";
    } else if (c == '/') {
      encoded << "%2F";
    } else if (c == '=') {
      encoded << "%3D";
    } else {
      encoded << '%' << std::uppercase << std::hex << std::setw(2) << std::setfill('0') 
              << (static_cast<int>(c) & 0xFF);
    }
  }
  
  return encoded.str();
}

// ============ PagerDutyChannel ============

PagerDutyChannel::PagerDutyChannel(const PagerDutyConfig& config) : config_(config) {
  EnsureCurlInitialized();
}

Status PagerDutyChannel::SendAlert(const storage::Alert& alert) {
  std::string payload = BuildPayload(alert);
  
  CURL* curl = curl_easy_init();
  if (!curl) {
    return Status::IOError("Failed to initialize curl");
  }
  
  struct curl_slist* headers = nullptr;
  headers = curl_slist_append(headers, "Content-Type: application/json");
  
  std::string response;
  
  curl_easy_setopt(curl, CURLOPT_URL, config_.api_url.c_str());
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload.c_str());
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 10000L);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
  
  CURLcode res = curl_easy_perform(curl);
  
  long http_code = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
  
  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);
  
  if (res != CURLE_OK) {
    healthy_.store(false);
    last_error_time_.store(std::chrono::system_clock::now().time_since_epoch().count());
    return Status::IOError("HTTP request failed");
  }
  
  if (http_code != 202) {
    healthy_.store(false);
    return Status::IOError(std::string("HTTP error ") + std::to_string(http_code));
  }
  
  healthy_.store(true);
  return Status::OK();
}

bool PagerDutyChannel::IsHealthy() const {
  return healthy_.load();
}

Status PagerDutyChannel::TestConnection() {
  storage::Alert test_alert;
  test_alert.name = "Connection Test";
  test_alert.description = "This is a test message from CedarGraph";
  test_alert.severity = storage::AlertSeverity::kInfo;
  test_alert.summary = "Test connection from CedarGraph";
  return SendAlert(test_alert);
}

std::string PagerDutyChannel::BuildPayload(const storage::Alert& alert) {
  std::ostringstream oss;
  
  oss << "{\n";
  oss << "  \"routing_key\": \"" << EscapeJsonString(config_.integration_key) << "\",\n";
  oss << "  \"event_action\": \"trigger\",\n";
  oss << "  \"payload\": {\n";
  oss << "    \"summary\": \"" << EscapeJsonString(alert.name) << "\",\n";
  oss << "    \"severity\": \"" << EscapeJsonString(SeverityToString(alert.severity)) << "\",\n";
  oss << "    \"source\": \"" << EscapeJsonString(config_.source) << "\",\n";
  oss << "    \"custom_details\": {\n";
  oss << "      \"summary\": \"" << EscapeJsonString(alert.summary) << "\",\n";
  oss << "      \"description\": \"" << EscapeJsonString(alert.description) << "\"";
  
  if (!alert.labels.empty()) {
    oss << ",\n";
    bool first = true;
    for (const auto& label : alert.labels) {
      if (!first) oss << ",\n";
      oss << "      \"" << EscapeJsonString(label.first) << "\": \"" << EscapeJsonString(label.second) << "\"";
      first = false;
    }
  }
  
  oss << "\n    }\n";
  oss << "  }\n";
  oss << "}";
  
  return oss.str();
}

std::string PagerDutyChannel::SeverityToString(storage::AlertSeverity severity) {
  switch (severity) {
    case storage::AlertSeverity::kInfo: return config_.severity_mapping[0];
    case storage::AlertSeverity::kWarning: return config_.severity_mapping[1];
    case storage::AlertSeverity::kCritical: return config_.severity_mapping[2];
    case storage::AlertSeverity::kEmergency: return config_.severity_mapping[3];
    default: return "warning";
  }
}

// ============ EmailChannel ============

EmailChannel::EmailChannel(const EmailConfig& config) : config_(config) {
  EnsureCurlInitialized();
}

Status EmailChannel::SendAlert(const storage::Alert& alert) {
  std::string subject = BuildSubject(alert);
  std::string body = BuildBody(alert);
  return SendEmail(subject, body);
}

bool EmailChannel::IsHealthy() const {
  return healthy_.load();
}

Status EmailChannel::TestConnection() {
  storage::Alert test_alert;
  test_alert.name = "Connection Test";
  test_alert.description = "This is a test message from CedarGraph";
  test_alert.severity = storage::AlertSeverity::kInfo;
  test_alert.summary = "Test connection from CedarGraph";
  return SendAlert(test_alert);
}

std::string EmailChannel::BuildSubject(const storage::Alert& alert) {
  std::string severity_str;
  switch (alert.severity) {
    case storage::AlertSeverity::kEmergency: severity_str = "[EMERGENCY]"; break;
    case storage::AlertSeverity::kCritical: severity_str = "[CRITICAL]"; break;
    case storage::AlertSeverity::kWarning: severity_str = "[WARNING]"; break;
    case storage::AlertSeverity::kInfo: severity_str = "[INFO]"; break;
    default: severity_str = "[ALERT]";
  }
  return config_.subject_prefix + " " + severity_str + " " + alert.name;
}

std::string EmailChannel::BuildBody(const storage::Alert& alert) {
  std::ostringstream oss;
  
  oss << "Alert Details:\n";
  oss << "================\n\n";
  oss << "Name: " << alert.name << "\n";
  oss << "Severity: " << static_cast<int>(alert.severity) << "\n";
  oss << "Summary: " << alert.summary << "\n";
  oss << "Description: " << alert.description << "\n\n";
  
  if (!alert.labels.empty()) {
    oss << "Labels:\n";
    for (const auto& label : alert.labels) {
      oss << "  " << label.first << ": " << label.second << "\n";
    }
    oss << "\n";
  }
  
  auto time_t = std::chrono::system_clock::to_time_t(alert.fired_at);
  struct tm tm_buf;
  if (localtime_r(&time_t, &tm_buf)) {
    oss << "Time: " << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S") << "\n";
  } else {
    oss << "Time: invalid\n";
  }
  
  return oss.str();
}

Status EmailChannel::SendEmail(const std::string& subject, const std::string& body) {
  if (config_.smtp_server.empty() || config_.to.empty()) {
    LOG(WARNING) << "Email alert: SMTP not configured. Subject: " << subject;
    healthy_.store(false);
    return Status::OK();
  }

  CURL* curl = curl_easy_init();
  if (!curl) {
    healthy_.store(false);
    return Status::IOError("Failed to initialize curl");
  }

  struct curl_slist* recipients = nullptr;
  for (const auto& addr : config_.to) {
    recipients = curl_slist_append(recipients, addr.c_str());
  }

  std::string url = config_.use_tls ? "smtps://" : "smtp://";
  url += config_.smtp_server + ":" + std::to_string(config_.smtp_port);

  std::ostringstream payload;
  payload << "To: ";
  for (size_t i = 0; i < config_.to.size(); ++i) {
    if (i > 0) payload << ", ";
    payload << config_.to[i];
  }
  payload << "\r\n";
  payload << "From: " << config_.from << "\r\n";
  payload << "Subject: " << subject << "\r\n";
  payload << "\r\n";
  payload << body << "\r\n";

  std::string payload_str = payload.str();
  SmtpReadData read_data{&payload_str, 0};

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_MAIL_FROM, config_.from.c_str());
  curl_easy_setopt(curl, CURLOPT_MAIL_RCPT, recipients);
  if (!config_.username.empty()) {
    curl_easy_setopt(curl, CURLOPT_USERNAME, config_.username.c_str());
  }
  if (!config_.password.empty()) {
    curl_easy_setopt(curl, CURLOPT_PASSWORD, config_.password.c_str());
  }
  curl_easy_setopt(curl, CURLOPT_USE_SSL,
                   config_.use_tls ? CURLUSESSL_ALL : CURLUSESSL_NONE);
  curl_easy_setopt(curl, CURLOPT_READDATA, &read_data);
  curl_easy_setopt(curl, CURLOPT_READFUNCTION, ReadCallback);
  curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 10000L);

  CURLcode res = curl_easy_perform(curl);
  curl_slist_free_all(recipients);
  curl_easy_cleanup(curl);

  if (res != CURLE_OK) {
    healthy_.store(false);
    LOG(WARNING) << "Email alert failed: " << curl_easy_strerror(res);
    return Status::OK();
  }

  healthy_.store(true);
  return Status::OK();
}

// ============ WebhookChannel ============

WebhookChannel::WebhookChannel(const WebhookConfig& config) : config_(config) {
  EnsureCurlInitialized();
}

Status WebhookChannel::SendAlert(const storage::Alert& alert) {
  std::string payload = BuildPayload(alert);
  return SendHttpRequest(payload);
}

bool WebhookChannel::IsHealthy() const {
  return healthy_.load();
}

Status WebhookChannel::TestConnection() {
  std::string test_payload = "{\"event\":\"test\",\"message\":\"Connection test from CedarGraph\"}";
  return SendHttpRequest(test_payload);
}

std::string WebhookChannel::BuildPayload(const storage::Alert& alert) {
  if (!config_.template_json.empty()) {
    std::string result = config_.template_json;
    size_t pos;
    while ((pos = result.find("{{name}}")) != std::string::npos) {
      result.replace(pos, 8, EscapeJsonString(alert.name));
    }
    while ((pos = result.find("{{description}}")) != std::string::npos) {
      result.replace(pos, 15, EscapeJsonString(alert.description));
    }
    while ((pos = result.find("{{severity}}")) != std::string::npos) {
      result.replace(pos, 12, std::to_string(static_cast<int>(alert.severity)));
    }
    while ((pos = result.find("{{summary}}")) != std::string::npos) {
      result.replace(pos, 11, EscapeJsonString(alert.summary));
    }
    return result;
  }
  
  std::ostringstream oss;
  oss << "{\n";
  oss << "  \"name\": \"" << EscapeJsonString(alert.name) << "\",\n";
  oss << "  \"description\": \"" << EscapeJsonString(alert.description) << "\",\n";
  oss << "  \"severity\": " << static_cast<int>(alert.severity) << ",\n";
  oss << "  \"summary\": \"" << EscapeJsonString(alert.summary) << "\",\n";
  oss << "  \"timestamp\": " << std::chrono::duration_cast<std::chrono::milliseconds>(
      alert.fired_at.time_since_epoch()).count() << ",\n";
  oss << "  \"labels\": {\n";
  
  bool first = true;
  for (const auto& label : alert.labels) {
    if (!first) oss << ",\n";
    oss << "    \"" << EscapeJsonString(label.first) << "\": \"" << EscapeJsonString(label.second) << "\"";
    first = false;
  }
  
  oss << "\n  }\n}";
  
  return oss.str();
}

Status WebhookChannel::SendHttpRequest(const std::string& payload) {
  CURL* curl = curl_easy_init();
  if (!curl) {
    return Status::IOError("Failed to initialize curl");
  }
  
  struct curl_slist* headers = nullptr;
  headers = curl_slist_append(headers, 
                              (std::string("Content-Type: ") + config_.content_type).c_str());
  
  for (const auto& header : config_.headers) {
    headers = curl_slist_append(headers, 
                                (header.first + ": " + header.second).c_str());
  }
  
  std::string response;
  CURLcode res = CURLE_OK;
  int retries = config_.retry_count;
  
  int attempt = 0;
  do {
    curl_easy_setopt(curl, CURLOPT_URL, config_.url.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, static_cast<long>(config_.timeout_ms));
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);

    if (config_.method == "POST") {
      curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload.c_str());
    } else if (config_.method == "PUT") {
      curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
      curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload.c_str());
    } else if (config_.method == "PATCH") {
      curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PATCH");
      curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload.c_str());
    }

    res = curl_easy_perform(curl);
    if (res == CURLE_OK) break;

    // Don't retry on timeout: webhook may have already processed the request
    if (res == CURLE_OPERATION_TIMEDOUT) {
      break;
    }

    if (retries > 0) {
      // Exponential backoff: 100ms, 200ms, 400ms, ... capped at 5s
      int delay_ms = 100 * (1 << std::min(attempt, 5));
      std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
    }
    attempt++;
  } while (--retries >= 0);
  
  long http_code = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
  
  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);
  
  if (res != CURLE_OK) {
    healthy_.store(false);
    return Status::IOError("HTTP request failed after retries");
  }
  
  if (http_code < 200 || http_code >= 300) {
    healthy_.store(false);
    return Status::IOError(std::string("HTTP error ") + std::to_string(http_code));
  }
  
  healthy_.store(true);
  return Status::OK();
}

// ============ AlertChannelManager ============

AlertChannelManager::AlertChannelManager() = default;
AlertChannelManager::~AlertChannelManager() = default;

void AlertChannelManager::AddChannel(std::shared_ptr<AlertChannel> channel) {
  std::unique_lock<std::shared_mutex> lock(channels_mutex_);
  channels_[channel->GetName()] = channel;
}

void AlertChannelManager::RemoveChannel(const std::string& name) {
  std::unique_lock<std::shared_mutex> lock(channels_mutex_);
  channels_.erase(name);
}

void AlertChannelManager::RouteAlert(const storage::Alert& alert) {
  std::vector<std::string> channel_names;
  {
    std::shared_lock<std::shared_mutex> rules_lock(rules_mutex_);
    auto it = routing_rules_.find(alert.severity);
    if (it != routing_rules_.end()) {
      channel_names = it->second;
    }
  }

  if (channel_names.empty()) {
    BroadcastAlert(alert);
    return;
  }

  std::vector<std::pair<std::string, std::shared_ptr<AlertChannel>>> channels;
  {
    std::shared_lock<std::shared_mutex> channels_lock(channels_mutex_);
    channels.reserve(channel_names.size());
    for (const auto& channel_name : channel_names) {
      auto ch_it = channels_.find(channel_name);
      if (ch_it != channels_.end()) {
        channels.emplace_back(channel_name, ch_it->second);
      }
    }
  }

  for (const auto& [channel_name, channel] : channels) {
    auto status = channel->SendAlert(alert);
    if (!status.ok()) {
      LOG(WARNING) << "Failed to send alert via channel " << channel_name
                   << ": " << status.ToString();
    }
  }
}

void AlertChannelManager::BroadcastAlert(const storage::Alert& alert) {
  std::vector<std::pair<std::string, std::shared_ptr<AlertChannel>>> channels;
  {
    std::shared_lock<std::shared_mutex> lock(channels_mutex_);
    channels.reserve(channels_.size());
    for (const auto& [name, channel] : channels_) {
      channels.emplace_back(name, channel);
    }
  }

  for (const auto& [name, channel] : channels) {
    auto status = channel->SendAlert(alert);
    if (!status.ok()) {
      LOG(WARNING) << "Failed to send alert via channel " << name 
                   << ": " << status.ToString();
    }
  }
}

std::vector<std::pair<std::string, Status>> AlertChannelManager::TestAllChannels() {
  std::vector<std::pair<std::string, std::shared_ptr<AlertChannel>>> channels;
  {
    std::shared_lock<std::shared_mutex> lock(channels_mutex_);
    channels.reserve(channels_.size());
    for (const auto& [name, channel] : channels_) {
      channels.emplace_back(name, channel);
    }
  }

  std::vector<std::pair<std::string, Status>> results;
  results.reserve(channels.size());

  for (const auto& [name, channel] : channels) {
    results.emplace_back(name, channel->TestConnection());
  }
  
  return results;
}

std::vector<std::pair<std::string, bool>> AlertChannelManager::GetChannelHealth() const {
  std::vector<std::pair<std::string, std::shared_ptr<AlertChannel>>> channels;
  {
    std::shared_lock<std::shared_mutex> lock(channels_mutex_);
    channels.reserve(channels_.size());
    for (const auto& [name, channel] : channels_) {
      channels.emplace_back(name, channel);
    }
  }

  std::vector<std::pair<std::string, bool>> results;
  results.reserve(channels.size());

  for (const auto& [name, channel] : channels) {
    results.emplace_back(name, channel->IsHealthy());
  }
  
  return results;
}

void AlertChannelManager::SetRoutingRule(storage::AlertSeverity severity,
                                          const std::vector<std::string>& channel_names) {
  std::unique_lock<std::shared_mutex> lock(rules_mutex_);
  routing_rules_[severity] = channel_names;
}

}  // namespace index
}  // namespace dtx
}  // namespace cedar
