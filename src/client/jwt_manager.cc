// Copyright 2025 The Cedar Authors
//
// JWT Manager implementation

#include "cedar/client/jwt_manager.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <random>
#include <sstream>
#include <iomanip>
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>

namespace cedar {
namespace client {

JWTManager::JWTManager(const JWTConfig& config)
    : config_(config) {}

JWTManager::~JWTManager() = default;

JWTToken JWTManager::GenerateToken(const std::string& user_id,
                                    const std::string& user_name) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (config_.secret_key.empty()) {
    return JWTToken{};
  }

  auto now = std::chrono::system_clock::now();
  auto expiration = now + std::chrono::seconds(config_.expiration_seconds);

  JWTToken token;
  token.user_id = user_id;
  token.user_name = user_name;
  token.issuer = config_.issuer;
  token.audience = config_.audience;
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
  if (config_.secret_key.empty()) {
    return false;
  }

  // Parse token parts
  size_t first_dot = token.find('.');
  size_t second_dot = token.find('.', first_dot + 1);

  if (first_dot == std::string::npos || second_dot == std::string::npos ||
      token.find('.', second_dot + 1) != std::string::npos) {
    return false;
  }

  std::string header = token.substr(0, first_dot);
  std::string payload = token.substr(first_dot + 1, second_dot - first_dot - 1);
  std::string signature = token.substr(second_dot + 1);

  if (!ValidateHeader(header)) {
    return false;
  }

  // Verify signature
  if (!VerifySignature(header, payload, signature)) {
    return false;
  }

  JWTToken info;
  if (!ParsePayload(payload, &info)) {
    return false;
  }

  return info.issuer == config_.issuer &&
         info.audience == config_.audience &&
         std::chrono::system_clock::now() < info.expires_at;
}

JWTToken JWTManager::GetTokenInfo(const std::string& token) const {
  JWTToken info;
  info.token = token;
  info.is_valid = ValidateToken(token);

  size_t first_dot = token.find('.');
  size_t second_dot = token.find('.', first_dot + 1);

  if (info.is_valid && first_dot != std::string::npos && second_dot != std::string::npos) {
    std::string payload = token.substr(first_dot + 1, second_dot - first_dot - 1);
    ParsePayload(payload, &info);
    info.token = token;
    info.is_valid = std::chrono::system_clock::now() < info.expires_at;
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
  std::string data = header + "." + payload;
  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int digest_len = 0;

  HMAC(EVP_sha256(),
       config_.secret_key.data(),
       static_cast<int>(config_.secret_key.size()),
       reinterpret_cast<const unsigned char*>(data.data()),
       data.size(),
       digest,
       &digest_len);

  if (digest_len == 0) {
    return "";
  }

  return Base64Encode(std::string(reinterpret_cast<char*>(digest), digest_len));
}

bool JWTManager::VerifySignature(const std::string& header,
                                   const std::string& payload,
                                   const std::string& signature) const {
  std::string expected_signature = CreateSignature(header, payload);
  return signature.size() == expected_signature.size() &&
         CRYPTO_memcmp(signature.data(), expected_signature.data(), signature.size()) == 0;
}

bool JWTManager::ValidateHeader(const std::string& header) const {
  std::string decoded_header = Base64Decode(header);
  return ExtractJsonString(decoded_header, "alg") == "HS256" &&
         ExtractJsonString(decoded_header, "typ") == "JWT";
}

bool JWTManager::ParsePayload(const std::string& payload, JWTToken* info) const {
  if (info == nullptr) {
    return false;
  }

  std::string decoded_payload = Base64Decode(payload);
  std::string user_id = ExtractJsonString(decoded_payload, "sub");
  std::string user_name = ExtractJsonString(decoded_payload, "name");
  std::string issuer = ExtractJsonString(decoded_payload, "iss");
  std::string audience = ExtractJsonString(decoded_payload, "aud");
  int64_t issued_at = 0;
  int64_t expires_at = 0;
  if (user_id.empty() || user_name.empty() || issuer.empty() || audience.empty() ||
      !ExtractJsonInt64(decoded_payload, "iat", &issued_at) ||
      !ExtractJsonInt64(decoded_payload, "exp", &expires_at) ||
      expires_at <= issued_at) {
    return false;
  }

  info->user_id = user_id;
  info->user_name = user_name;
  info->issuer = issuer;
  info->audience = audience;
  info->issued_at = std::chrono::system_clock::from_time_t(
      static_cast<time_t>(issued_at));
  info->expires_at = std::chrono::system_clock::from_time_t(
      static_cast<time_t>(expires_at));
  return true;
}

std::string JWTManager::ExtractJsonString(const std::string& json,
                                          const std::string& key) const {
  std::string pattern = "\"" + key + "\":\"";
  size_t start = json.find(pattern);
  if (start == std::string::npos) {
    return "";
  }
  start += pattern.size();
  size_t end = json.find('"', start);
  if (end == std::string::npos) {
    return "";
  }
  return json.substr(start, end - start);
}

bool JWTManager::ExtractJsonInt64(const std::string& json,
                                  const std::string& key,
                                  int64_t* value) const {
  if (value == nullptr) {
    return false;
  }

  std::string pattern = "\"" + key + "\":";
  size_t start = json.find(pattern);
  if (start == std::string::npos) {
    return false;
  }
  start += pattern.size();
  size_t end = start;
  while (end < json.size() && std::isdigit(static_cast<unsigned char>(json[end]))) {
    ++end;
  }
  if (end == start) {
    return false;
  }

  try {
    *value = std::stoll(json.substr(start, end - start));
    return true;
  } catch (const std::exception&) {
    return false;
  }
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
