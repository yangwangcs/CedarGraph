// Copyright 2025 The Cedar Authors
//
// JWT Manager implementation

#include "cedar/client/jwt_manager.h"

#include <algorithm>
#include <chrono>
#include <random>
#include <sstream>
#include <iomanip>

namespace cedar {
namespace client {

JWTManager::JWTManager(const JWTConfig& config)
    : config_(config) {}

JWTManager::~JWTManager() = default;

JWTToken JWTManager::GenerateToken(const std::string& user_id,
                                    const std::string& user_name) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto now = std::chrono::system_clock::now();
  auto expiration = now + std::chrono::seconds(config_.expiration_seconds);

  JWTToken token;
  token.user_id = user_id;
  token.user_name = user_name;
  token.issued_at = now;
  token.expires_at = expiration;
  token.is_valid = true;

  // Create JWT payload (simplified - in production use a proper JWT library)
  std::stringstream payload_ss;
  payload_ss << "{"
             << "\"sub\":\"" << user_id << "\","
             << "\"name\":\"" << user_name << "\","
             << "\"iss\":\"" << config_.issuer << "\","
             << "\"aud\":\"" << config_.audience << "\","
             << "\"iat\":" << std::chrono::system_clock::to_time_t(now) << ","
             << "\"exp\":" << std::chrono::system_clock::to_time_t(expiration)
             << "}";

  // Create JWT header
  std::string header = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";
  std::string payload = payload_ss.str();

  // Encode header and payload
  std::string encoded_header = Base64Encode(header);
  std::string encoded_payload = Base64Encode(payload);

  // Create signature
  std::string signature = CreateSignature(encoded_header, encoded_payload);

  // Combine into JWT token
  token.token = encoded_header + "." + encoded_payload + "." + signature;

  current_token_ = token.token;

  return token;
}

bool JWTManager::ValidateToken(const std::string& token) const {
  // Parse token parts
  size_t first_dot = token.find('.');
  size_t second_dot = token.find('.', first_dot + 1);

  if (first_dot == std::string::npos || second_dot == std::string::npos) {
    return false;
  }

  std::string header = token.substr(0, first_dot);
  std::string payload = token.substr(first_dot + 1, second_dot - first_dot - 1);
  std::string signature = token.substr(second_dot + 1);

  // Verify signature
  if (!VerifySignature(header, payload, signature)) {
    return false;
  }

  // Check expiration (simplified - in production parse the payload)
  // For now, just return true if signature is valid
  return true;
}

JWTToken JWTManager::GetTokenInfo(const std::string& token) const {
  JWTToken info;
  info.token = token;
  info.is_valid = ValidateToken(token);

  // Parse token to get user info (simplified)
  size_t first_dot = token.find('.');
  size_t second_dot = token.find('.', first_dot + 1);

  if (first_dot != std::string::npos && second_dot != std::string::npos) {
    std::string payload = token.substr(first_dot + 1, second_dot - first_dot - 1);
    std::string decoded_payload = Base64Decode(payload);

    // Parse JSON payload (simplified - in production use a JSON library)
    // For now, just set placeholder values
    info.user_id = "user_id";
    info.user_name = "user_name";
    info.issued_at = std::chrono::system_clock::now();
    info.expires_at = std::chrono::system_clock::now() + std::chrono::seconds(3600);
  }

  return info;
}

bool JWTManager::NeedsRefresh(const std::string& token) const {
  auto info = GetTokenInfo(token);
  if (!info.is_valid) {
    return false;
  }

  auto now = std::chrono::system_clock::now();
  auto time_until_expiration = std::chrono::duration_cast<std::chrono::seconds>(
      info.expires_at - now).count();

  return time_until_expiration < config_.refresh_threshold_seconds;
}

JWTToken JWTManager::RefreshToken(const std::string& token) {
  if (refresh_callback_) {
    return refresh_callback_(token);
  }

  // Default: generate new token with same user info
  auto info = GetTokenInfo(token);
  return GenerateToken(info.user_id, info.user_name);
}

void JWTManager::SetRefreshCallback(std::function<JWTToken(const std::string&)> callback) {
  std::lock_guard<std::mutex> lock(mutex_);
  refresh_callback_ = callback;
}

std::string JWTManager::GetCurrentToken() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return current_token_;
}

void JWTManager::SetCurrentToken(const std::string& token) {
  std::lock_guard<std::mutex> lock(mutex_);
  current_token_ = token;
}

void JWTManager::ClearCurrentToken() {
  std::lock_guard<std::mutex> lock(mutex_);
  current_token_.clear();
}

std::string JWTManager::Base64Encode(const std::string& input) const {
  // Simplified base64 encoding
  // In production, use a proper base64 library
  static const char base64_chars[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

  std::string result;
  int i = 0;
  int j = 0;
  unsigned char char_array_3[3];
  unsigned char char_array_4[4];

  for (char c : input) {
    char_array_3[i++] = c;
    if (i == 3) {
      char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
      char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
      char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
      char_array_4[3] = char_array_3[2] & 0x3f;

      for (i = 0; i < 4; i++) {
        result += base64_chars[char_array_4[i]];
      }
      i = 0;
    }
  }

  if (i) {
    for (j = i; j < 3; j++) {
      char_array_3[j] = '\0';
    }

    char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
    char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
    char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);

    for (j = 0; j < i + 1; j++) {
      result += base64_chars[char_array_4[j]];
    }

    while (i++ < 3) {
      result += '=';
    }
  }

  return result;
}

std::string JWTManager::Base64Decode(const std::string& input) const {
  // Simplified base64 decoding
  // In production, use a proper base64 library
  static const std::string base64_chars =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

  std::string result;
  int i = 0;
  int j = 0;
  unsigned char char_array_4[4], char_array_3[3];

  for (char c : input) {
    if (c == '=') {
      break;
    }

    char_array_4[i++] = c;
    if (i == 4) {
      for (i = 0; i < 4; i++) {
        char_array_4[i] = base64_chars.find(char_array_4[i]);
      }

      char_array_3[0] = (char_array_4[0] << 2) + ((char_array_4[1] & 0x30) >> 4);
      char_array_3[1] = ((char_array_4[1] & 0xf) << 4) + ((char_array_4[2] & 0x3c) >> 2);
      char_array_3[2] = ((char_array_4[2] & 0x3) << 6) + char_array_4[3];

      for (i = 0; i < 3; i++) {
        result += char_array_3[i];
      }
      i = 0;
    }
  }

  if (i) {
    for (j = 0; j < i; j++) {
      char_array_4[j] = base64_chars.find(char_array_4[j]);
    }

    char_array_3[0] = (char_array_4[0] << 2) + ((char_array_4[1] & 0x30) >> 4);
    char_array_3[1] = ((char_array_4[1] & 0xf) << 4) + ((char_array_4[2] & 0x3c) >> 2);

    for (j = 0; j < i - 1; j++) {
      result += char_array_3[j];
    }
  }

  return result;
}

std::string JWTManager::CreateSignature(const std::string& header,
                                          const std::string& payload) const {
  // Simplified HMAC-SHA256 signature
  // In production, use a proper HMAC library
  std::string data = header + "." + payload;
  std::string signature;

  // Simple hash function (not cryptographically secure - for demo only)
  unsigned long hash = 5381;
  for (char c : config_.secret_key + data) {
    hash = ((hash << 5) + hash) + c;
  }

  // Convert to hex string
  std::stringstream ss;
  ss << std::hex << std::setfill('0') << std::setw(16) << hash;
  signature = ss.str();

  return Base64Encode(signature);
}

bool JWTManager::VerifySignature(const std::string& header,
                                   const std::string& payload,
                                   const std::string& signature) const {
  std::string expected_signature = CreateSignature(header, payload);
  return signature == expected_signature;
}

std::string JWTManager::GenerateRandomId() const {
  static std::random_device rd;
  static std::mt19937 gen(rd());
  static std::uniform_int_distribution<> dis(0, 15);

  std::stringstream ss;
  ss << std::hex;
  for (int i = 0; i < 16; ++i) {
    ss << dis(gen);
  }

  return ss.str();
}

}  // namespace client
}  // namespace cedar
