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

#include "cedar/dtx/security.h"

#include <iostream>
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <thread>
#include <fstream>

#include <chrono>
#include <iomanip>
#include <iostream>
#include <optional>
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <random>
#include <sstream>

namespace cedar {
namespace dtx {
namespace security {

// =============================================================================
// TLSContext Implementation
// =============================================================================

TLSContext::TLSContext() = default;

TLSContext::~TLSContext() {
  Shutdown();
}

Status TLSContext::Initialize(const TLSConfig& config) {
  config_ = config;
  
  if (!config_.enable_tls) {
    enabled_ = false;
    return Status::OK();
  }
  
  // Initialize OpenSSL SSL_CTX
  const SSL_METHOD* method = TLS_server_method();
  SSL_CTX* ctx = SSL_CTX_new(method);
  if (!ctx) {
    return Status::IOError("Failed to create SSL_CTX");
  }
  
  // Load certificate and private key
  if (!config_.cert_file.empty()) {
    if (SSL_CTX_use_certificate_file(ctx, config_.cert_file.c_str(), SSL_FILETYPE_PEM) <= 0) {
      SSL_CTX_free(ctx);
      return Status::IOError("Failed to load certificate: " + config_.cert_file);
    }
  }
  if (!config_.key_file.empty()) {
    if (SSL_CTX_use_PrivateKey_file(ctx, config_.key_file.c_str(), SSL_FILETYPE_PEM) <= 0) {
      SSL_CTX_free(ctx);
      return Status::IOError("Failed to load private key: " + config_.key_file);
    }
  }
  if (!config_.ca_file.empty()) {
    if (SSL_CTX_load_verify_locations(ctx, config_.ca_file.c_str(), nullptr) <= 0) {
      SSL_CTX_free(ctx);
      return Status::IOError("Failed to load CA file: " + config_.ca_file);
    }
    if (config_.verify_client) {
      SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, nullptr);
    }
  }
  
  ssl_context_ = ctx;
  enabled_ = true;
  return Status::OK();
}

void TLSContext::Shutdown() {
  enabled_ = false;
  if (ssl_context_) {
    SSL_CTX_free(static_cast<SSL_CTX*>(ssl_context_));
    ssl_context_ = nullptr;
  }
}

// =============================================================================
// Helper Functions
// =============================================================================

static std::string GenerateRandomString(size_t length) {
  static const char charset[] = 
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, sizeof(charset) - 2);
  
  std::string result;
  result.reserve(length);
  
  for (size_t i = 0; i < length; ++i) {
    result += charset[dis(gen)];
  }
  
  return result;
}

// =============================================================================
// Authenticator Implementation
// =============================================================================

Authenticator::Authenticator() = default;

Authenticator::~Authenticator() {
  Shutdown();
}

Status Authenticator::Initialize(const Config& config) {
  config_ = config;
  
  if (config_.accounts.empty()) {
    return Status::InvalidArgument("No accounts configured");
  }
  
  for (const auto& account : config_.accounts) {
    auto status = AddUser(account.username, account.password, account.roles);
    if (!status.ok()) {
      return status;
    }
  }
  
  return Status::OK();
}

void Authenticator::Shutdown() {
  std::lock_guard<std::mutex> lock(tokens_mutex_);
  active_tokens_.clear();
  revoked_tokens_.clear();
  refresh_tokens_.clear();
}

StatusOr<AuthToken> Authenticator::Authenticate(const std::string& username,
                                                 const std::string& password) {
  std::lock_guard<std::mutex> lock(users_mutex_);
  
  auto it = users_.find(username);
  if (it == users_.end()) {
    return Status::InvalidArgument("Invalid username or password");
  }
  
  auto& user = it->second;
  
  // Check if account is locked
  if (user.locked) {
    if (std::chrono::system_clock::now() < user.locked_until) {
      return Status::InvalidArgument("Account is locked");
    } else {
      user.locked = false;
      user.failed_attempts = 0;
    }
  }
  
  // Verify password
  if (!VerifyPassword(password, user.password_hash)) {
    user.failed_attempts++;
    
    if (user.failed_attempts >= config_.max_login_attempts) {
      user.locked = true;
      user.locked_until = std::chrono::system_clock::now() + 
                          config_.lockout_duration;
    }
    
    return Status::InvalidArgument("Invalid username or password");
  }
  
  // Reset failed attempts
  user.failed_attempts = 0;
  
  // Generate Token
  AuthToken token;
  token.token_id = GenerateRandomString(32);
  token.user_id = user.username;
  token.user_name = user.username;
  token.roles = user.roles;
  token.issued_at = std::chrono::system_clock::now();
  token.expires_at = token.issued_at + config_.token_ttl;

  if (config_.enable_refresh_token) {
    token.refresh_token = GenerateRandomString(32);
    token.refresh_expires_at = token.issued_at + refresh_token_ttl_;
  }

  {
    std::lock_guard<std::mutex> token_lock(tokens_mutex_);
    active_tokens_[token.token_id] = token;
    if (!token.refresh_token.empty()) {
      refresh_tokens_[token.refresh_token] = token.token_id;
    }
  }

  return token;
}

StatusOr<AuthToken> Authenticator::ValidateToken(const std::string& token_str) {
  auto token_result = ParseJWT(token_str);
  if (!token_result.ok()) {
    return token_result.status();
  }
  
  auto token = token_result.value();
  
  // Check if expired
  if (token.IsExpired()) {
    return Status::InvalidArgument("Token expired");
  }
  
  // Check if active and not revoked
  {
    std::lock_guard<std::mutex> lock(tokens_mutex_);
    auto it = active_tokens_.find(token.token_id);
    if (it == active_tokens_.end()) {
      return Status::InvalidArgument("Token not active");
    }
    if (revoked_tokens_.find(token.token_id) != revoked_tokens_.end()) {
      return Status::InvalidArgument("Token revoked");
    }
    token = it->second;  // 使用存储的完整 token 信息
  }
  
  return token;
}

StatusOr<AuthToken> Authenticator::RefreshToken(const std::string& refresh_token) {
  if (!config_.enable_refresh_token) {
    return Status::NotSupported("Refresh token is disabled");
  }

  std::string token_id;
  {
    std::lock_guard<std::mutex> lock(tokens_mutex_);
    auto it = refresh_tokens_.find(refresh_token);
    if (it == refresh_tokens_.end()) {
      return Status::InvalidArgument("Invalid refresh token");
    }
    token_id = it->second;
    refresh_tokens_.erase(it);  // Single-use: remove old refresh token
  }

  AuthToken old_token;
  {
    std::lock_guard<std::mutex> lock(tokens_mutex_);
    auto it = active_tokens_.find(token_id);
    if (it == active_tokens_.end()) {
      return Status::InvalidArgument("Token no longer active");
    }
    old_token = it->second;
    if (old_token.IsRefreshExpired()) {
      active_tokens_.erase(it);
      return Status::InvalidArgument("Refresh token expired");
    }
  }

  // Generate new token pair (rotation)
  AuthToken new_token;
  new_token.token_id = GenerateRandomString(32);
  new_token.user_id = old_token.user_id;
  new_token.user_name = old_token.user_name;
  new_token.roles = old_token.roles;
  new_token.issued_at = std::chrono::system_clock::now();
  new_token.expires_at = new_token.issued_at + config_.token_ttl;
  new_token.refresh_token = GenerateRandomString(32);
  new_token.refresh_expires_at = new_token.issued_at + refresh_token_ttl_;

  {
    std::lock_guard<std::mutex> lock(tokens_mutex_);
    active_tokens_.erase(token_id);  // Invalidate old access token
    active_tokens_[new_token.token_id] = new_token;
    refresh_tokens_[new_token.refresh_token] = new_token.token_id;
  }

  return new_token;
}

Status Authenticator::RevokeToken(const std::string& token) {
  std::lock_guard<std::mutex> lock(tokens_mutex_);

  auto result = ParseJWT(token);
  if (!result.ok()) {
    return result.status();
  }

  const std::string& token_id = result.value().token_id;
  revoked_tokens_.insert(token_id);
  active_tokens_.erase(token_id);

  // Also remove any associated refresh token
  for (auto it = refresh_tokens_.begin(); it != refresh_tokens_.end(); ) {
    if (it->second == token_id) {
      it = refresh_tokens_.erase(it);
    } else {
      ++it;
    }
  }

  return Status::OK();
}

Status Authenticator::AddUser(const std::string& username,
                               const std::string& password,
                               const std::vector<std::string>& roles) {
  std::lock_guard<std::mutex> lock(users_mutex_);
  
  if (users_.find(username) != users_.end()) {
    return Status::InvalidArgument("User already exists");
  }
  
  UserInfo user;
  user.username = username;
  user.password_hash = HashPassword(password);
  user.roles = roles;
  
  users_[username] = user;
  
  return Status::OK();
}

Status Authenticator::RemoveUser(const std::string& username) {
  std::lock_guard<std::mutex> lock(users_mutex_);
  
  if (users_.erase(username) == 0) {
    return Status::NotFound("User not found");
  }
  
  return Status::OK();
}

Status Authenticator::UpdatePassword(const std::string& username,
                                      const std::string& new_password) {
  std::lock_guard<std::mutex> lock(users_mutex_);
  
  auto it = users_.find(username);
  if (it == users_.end()) {
    return Status::NotFound("User not found");
  }
  
  it->second.password_hash = HashPassword(new_password);
  
  return Status::OK();
}

std::string Authenticator::HashPassword(const std::string& password) {
  std::string salt = GenerateRandomString(16);
  constexpr int kIterations = 100000;
  constexpr int kKeyLen = 32;

  unsigned char derived[kKeyLen];
  if (!PKCS5_PBKDF2_HMAC(password.c_str(), static_cast<int>(password.size()),
                         reinterpret_cast<const unsigned char*>(salt.data()),
                         static_cast<int>(salt.size()),
                         kIterations, EVP_sha256(), kKeyLen, derived)) {
    return "";
  }

  std::ostringstream oss;
  oss << salt;
  for (int i = 0; i < kKeyLen; i++) {
    oss << std::hex << std::setw(2) << std::setfill('0') << (int)derived[i];
  }

  return oss.str();
}

bool Authenticator::VerifyPassword(const std::string& password,
                                    const std::string& hash) {
  constexpr size_t kSaltLen = 16;
  constexpr int kKeyLen = 32;
  constexpr int kIterations = 100000;

  if (hash.size() < kSaltLen + static_cast<size_t>(kKeyLen) * 2) return false;

  std::string salt = hash.substr(0, kSaltLen);

  unsigned char expected_derived[kKeyLen];
  if (!PKCS5_PBKDF2_HMAC(password.c_str(), static_cast<int>(password.size()),
                         reinterpret_cast<const unsigned char*>(salt.data()),
                         static_cast<int>(salt.size()),
                         kIterations, EVP_sha256(), kKeyLen, expected_derived)) {
    return false;
  }

  std::string expected_hash = salt;
  for (int i = 0; i < kKeyLen; i++) {
    std::ostringstream oss;
    oss << std::hex << std::setw(2) << std::setfill('0') << (int)expected_derived[i];
    expected_hash += oss.str();
  }

  // Constant-time comparison to prevent timing attacks
  if (hash.size() != expected_hash.size()) return false;
  volatile unsigned char diff = 0;
  for (size_t i = 0; i < hash.size(); ++i) {
    diff |= static_cast<unsigned char>(hash[i] ^ expected_hash[i]);
  }
  return diff == 0;
}

static std::string Base64UrlEncode(const std::string& input) {
  BIO* b64 = BIO_new(BIO_f_base64());
  BIO* bio = BIO_new(BIO_s_mem());
  b64 = BIO_push(b64, bio);
  BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
  BIO_write(b64, input.data(), static_cast<int>(input.size()));
  BIO_flush(b64);

  BUF_MEM* buffer_ptr;
  BIO_get_mem_ptr(b64, &buffer_ptr);

  std::string encoded(buffer_ptr->data, buffer_ptr->length);
  BIO_free_all(b64);

  for (size_t i = 0; i < encoded.size(); ++i) {
    if (encoded[i] == '+') encoded[i] = '-';
    else if (encoded[i] == '/') encoded[i] = '_';
  }
  while (!encoded.empty() && encoded.back() == '=') {
    encoded.pop_back();
  }
  return encoded;
}

static std::string Base64UrlDecode(const std::string& input) {
  std::string padded = input;
  for (size_t i = 0; i < padded.size(); ++i) {
    if (padded[i] == '-') padded[i] = '+';
    else if (padded[i] == '_') padded[i] = '/';
  }
  while (padded.size() % 4 != 0) {
    padded.push_back('=');
  }

  BIO* b64 = BIO_new(BIO_f_base64());
  BIO* bio = BIO_new_mem_buf(padded.data(), static_cast<int>(padded.size()));
  b64 = BIO_push(b64, bio);
  BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);

  std::vector<char> buffer(padded.size());
  int decoded_len = BIO_read(b64, buffer.data(), static_cast<int>(buffer.size()));
  BIO_free_all(b64);

  if (decoded_len < 0) return "";
  return std::string(buffer.data(), decoded_len);
}

namespace {
std::string JsonEscape(const std::string& input) {
  std::string output;
  output.reserve(input.size());
  for (char c : input) {
    switch (c) {
      case '"': output += "\\\""; break;
      case '\\': output += "\\\\"; break;
      case '\b': output += "\\b"; break;
      case '\f': output += "\\f"; break;
      case '\n': output += "\\n"; break;
      case '\r': output += "\\r"; break;
      case '\t': output += "\\t"; break;
      default:
        if (static_cast<unsigned char>(c) < 0x20) {
          char buf[8];
          snprintf(buf, sizeof(buf), "\\u%04x", c);
          output += buf;
        } else {
          output += c;
        }
    }
  }
  return output;
}
}  // namespace

std::string Authenticator::GenerateJWT(const AuthToken& token) {
  std::string header = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";

  auto time_t = std::chrono::system_clock::to_time_t(token.expires_at);
  std::ostringstream payload;
  payload << "{\"sub\":\"" << JsonEscape(token.user_id) << "\",";
  payload << "\"name\":\"" << JsonEscape(token.user_name) << "\",";
  payload << "\"exp\":" << time_t << ",";
  payload << "\"jti\":\"" << JsonEscape(token.token_id) << "\"}";

  std::string encoded_header = Base64UrlEncode(header);
  std::string encoded_payload = Base64UrlEncode(payload.str());

  std::string signature_input = encoded_header + "." + encoded_payload;

  unsigned char signature[EVP_MAX_MD_SIZE];
  unsigned int signature_len;
  HMAC(EVP_sha256(), config_.jwt_secret.data(),
       static_cast<int>(config_.jwt_secret.size()),
       reinterpret_cast<const unsigned char*>(signature_input.data()),
       static_cast<int>(signature_input.size()), signature, &signature_len);

  std::string encoded_signature = Base64UrlEncode(
      std::string(reinterpret_cast<const char*>(signature), signature_len));

  return encoded_header + "." + encoded_payload + "." + encoded_signature;
}

StatusOr<AuthToken> Authenticator::ParseJWT(const std::string& jwt) {
  // Validate JWT structure: header.payload.signature
  size_t first_dot = jwt.find('.');
  if (first_dot == std::string::npos || first_dot == 0) {
    return Status::InvalidArgument("Invalid JWT format: missing first dot");
  }
  size_t second_dot = jwt.find('.', first_dot + 1);
  if (second_dot == std::string::npos || second_dot == first_dot + 1) {
    return Status::InvalidArgument("Invalid JWT format: missing second dot");
  }
  if (jwt.find('.', second_dot + 1) != std::string::npos) {
    return Status::InvalidArgument("Invalid JWT format: too many dots");
  }
  if (second_dot + 1 >= jwt.size()) {
    return Status::InvalidArgument("Invalid JWT format: empty signature");
  }

  std::string encoded_header = jwt.substr(0, first_dot);
  std::string encoded_payload = jwt.substr(first_dot + 1, second_dot - first_dot - 1);
  std::string encoded_signature = jwt.substr(second_dot + 1);

  // Decode header to validate structure
  std::string header = Base64UrlDecode(encoded_header);
  if (header.empty() || header.find("\"alg\"") == std::string::npos) {
    return Status::InvalidArgument("Invalid JWT header");
  }

  // Verify HMAC-SHA256 signature
  std::string signature_input = encoded_header + "." + encoded_payload;
  unsigned char expected_sig[EVP_MAX_MD_SIZE];
  unsigned int expected_sig_len;
  HMAC(EVP_sha256(), config_.jwt_secret.data(),
       static_cast<int>(config_.jwt_secret.size()),
       reinterpret_cast<const unsigned char*>(signature_input.data()),
       static_cast<int>(signature_input.size()), expected_sig, &expected_sig_len);

  std::string expected_encoded_sig = Base64UrlEncode(
      std::string(reinterpret_cast<const char*>(expected_sig), expected_sig_len));

  if (encoded_signature != expected_encoded_sig) {
    return Status::InvalidArgument("Invalid JWT signature");
  }

  // Decode payload
  std::string payload = Base64UrlDecode(encoded_payload);
  if (payload.empty()) {
    return Status::InvalidArgument("Invalid JWT payload");
  }

  // Helper: extract a JSON string field value robustly
  auto extract_string_field = [](const std::string& json,
                                  const std::string& field_name) -> std::string {
    std::string key = "\"" + field_name + "\"";
    size_t pos = json.find(key);
    if (pos == std::string::npos) return "";
    pos += key.size();
    while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t' ||
                                  json[pos] == '\n' || json[pos] == '\r')) {
      pos++;
    }
    if (pos >= json.size() || json[pos] != ':') return "";
    pos++;
    while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t' ||
                                  json[pos] == '\n' || json[pos] == '\r')) {
      pos++;
    }
    if (pos >= json.size() || json[pos] != '"') return "";
    pos++;
    std::string value;
    while (pos < json.size()) {
      char c = json[pos];
      if (c == '"') {
        break;
      }
      if (c == '\\' && pos + 1 < json.size()) {
        char next = json[pos + 1];
        switch (next) {
          case '"': value += '"'; break;
          case '\\': value += '\\'; break;
          case '/': value += '/'; break;
          case 'b': value += '\b'; break;
          case 'f': value += '\f'; break;
          case 'n': value += '\n'; break;
          case 'r': value += '\r'; break;
          case 't': value += '\t'; break;
          case 'u':
            if (pos + 5 < json.size()) {
              std::string hex = json.substr(pos + 2, 4);
              try {
                int codepoint = std::stoi(hex, nullptr, 16);
                if (codepoint < 0x80) {
                  value += static_cast<char>(codepoint);
                } else {
                  value += '?';
                }
              } catch (...) {
                value += '?';
              }
              pos += 4;
            }
            break;
          default: value += next; break;
        }
        pos += 2;
      } else {
        value += c;
        pos++;
      }
    }
    return value;
  };

  // Helper: extract a JSON numeric field value robustly
  auto extract_number_field = [](const std::string& json,
                                  const std::string& field_name) -> std::optional<int64_t> {
    std::string key = "\"" + field_name + "\"";
    size_t pos = json.find(key);
    if (pos == std::string::npos) return std::nullopt;
    pos += key.size();
    while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t' ||
                                  json[pos] == '\n' || json[pos] == '\r')) {
      pos++;
    }
    if (pos >= json.size() || json[pos] != ':') return std::nullopt;
    pos++;
    while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t' ||
                                  json[pos] == '\n' || json[pos] == '\r')) {
      pos++;
    }
    size_t start = pos;
    if (start < json.size() && json[start] == '-') pos++;
    while (pos < json.size() && json[pos] >= '0' && json[pos] <= '9') {
      pos++;
    }
    if (start == pos) return std::nullopt;
    // Strict digit validation: manual parse to avoid std::stoll edge cases
    int64_t result = 0;
    bool negative = (json[start] == '-');
    size_t digit_start = negative ? start + 1 : start;
    for (size_t i = digit_start; i < pos; ++i) {
      int digit = json[i] - '0';
      if (negative) {
        if (result < (std::numeric_limits<int64_t>::min() + digit) / 10) {
          return std::nullopt;
        }
        result = result * 10 - digit;
      } else {
        if (result > (std::numeric_limits<int64_t>::max() - digit) / 10) {
          return std::nullopt;
        }
        result = result * 10 + digit;
      }
    }
    return result;
  };

  AuthToken token;
  token.token_id = extract_string_field(payload, "jti");
  token.user_id = extract_string_field(payload, "sub");
  token.user_name = extract_string_field(payload, "name");

  auto exp_opt = extract_number_field(payload, "exp");
  if (exp_opt.has_value()) {
    token.expires_at = std::chrono::system_clock::from_time_t(
        static_cast<time_t>(exp_opt.value()));
  }

  return token;
}

// =============================================================================
// Authorizer Implementation
// =============================================================================

Authorizer::Authorizer() = default;

Authorizer::~Authorizer() = default;

Status Authorizer::InitializeDefaultRoles() {
  std::lock_guard<std::mutex> lock(roles_mutex_);
  
  Role admin;
  admin.name = "admin";
  admin.description = "System administrator";
  admin.permissions = Permission::kAll;
  roles_[admin.name] = admin;
  
  Role readwrite;
  readwrite.name = "readwrite";
  readwrite.description = "Read and write access";
  readwrite.permissions = Permission::kRead | Permission::kWrite;
  roles_[readwrite.name] = readwrite;
  
  Role readonly;
  readonly.name = "readonly";
  readonly.description = "Read-only access";
  readonly.permissions = Permission::kRead;
  roles_[readonly.name] = readonly;
  
  Role monitor;
  monitor.name = "monitor";
  monitor.description = "Monitoring access";
  monitor.permissions = Permission::kMonitor;
  roles_[monitor.name] = monitor;
  
  return Status::OK();
}

Status Authorizer::AddRole(const Role& role) {
  std::lock_guard<std::mutex> lock(roles_mutex_);
  roles_[role.name] = role;
  return Status::OK();
}

Status Authorizer::RemoveRole(const std::string& role_name) {
  std::lock_guard<std::mutex> lock(roles_mutex_);
  
  if (roles_.erase(role_name) == 0) {
    return Status::NotFound("Role not found");
  }
  
  return Status::OK();
}

StatusOr<Role> Authorizer::GetRole(const std::string& role_name) const {
  std::lock_guard<std::mutex> lock(roles_mutex_);
  
  auto it = roles_.find(role_name);
  if (it == roles_.end()) {
    return Status::NotFound("Role not found");
  }
  
  return it->second;
}

namespace {
bool GlobMatch(const std::string& pattern, const std::string& text) {
  size_t p = 0, t = 0;
  size_t star = std::string::npos, match = 0;
  while (t < text.size()) {
    if (p < pattern.size() && (pattern[p] == text[t] || pattern[p] == '?')) {
      ++p; ++t;
    } else if (p < pattern.size() && pattern[p] == '*') {
      star = p++;
      match = t;
    } else if (star != std::string::npos) {
      p = star + 1;
      t = ++match;
    } else {
      return false;
    }
  }
  while (p < pattern.size() && pattern[p] == '*') ++p;
  return p == pattern.size();
}
}  // namespace

Status Authorizer::CheckPermission(const AuthToken& token,
                                    Permission permission,
                                    const std::string& resource) {
  Permission granted = Permission::kNone;
  
  for (const auto& role_name : token.roles) {
    auto role_result = GetRole(role_name);
    if (role_result.ok()) {
      granted = granted | role_result.value().permissions;
      
      if (!resource.empty()) {
        const auto& role = role_result.value();
        if (!role.allowed_resources.empty() || !role.denied_resources.empty()) {
          bool allowed = false;
          for (const auto& pattern : role.allowed_resources) {
            if (GlobMatch(pattern, resource) || pattern == resource) {
              allowed = true;
              break;
            }
          }
          for (const auto& pattern : role.denied_resources) {
            if (GlobMatch(pattern, resource) || pattern == resource) {
              return Status::IOError("Access denied for resource");
            }
          }
          if (!role.allowed_resources.empty() && !allowed) {
            return Status::IOError("Access denied for resource");
          }
        }
      }
    }
  }
  
  if (!HasPermission(granted, permission)) {
    return Status::IOError("Permission denied");
  }
  
  return Status::OK();
}

bool Authorizer::CanRead(const AuthToken& token, const std::string& resource) {
  return CheckPermission(token, Permission::kRead, resource).ok();
}

bool Authorizer::CanWrite(const AuthToken& token, const std::string& resource) {
  return CheckPermission(token, Permission::kWrite, resource).ok();
}

bool Authorizer::CanDelete(const AuthToken& token, const std::string& resource) {
  return CheckPermission(token, Permission::kDelete, resource).ok();
}

bool Authorizer::CanAdmin(const AuthToken& token) {
  return CheckPermission(token, Permission::kAdmin, "").ok();
}

// =============================================================================
// AuditLogger Implementation
// =============================================================================

AuditLogger::AuditLogger() = default;

AuditLogger::~AuditLogger() {
  Shutdown();
}

Status AuditLogger::Initialize(const Config& config) {
  config_ = config;
  
  if (!config_.log_file.empty()) {
    if (config_.log_file.find("..") != std::string::npos) {
      return Status::InvalidArgument(
          "Audit log file path contains '..' directory traversal: " + config_.log_file);
    }
    if (!config_.allowed_log_prefix.empty() &&
        (config_.log_file.size() < config_.allowed_log_prefix.size() ||
         config_.log_file.substr(0, config_.allowed_log_prefix.size()) != config_.allowed_log_prefix)) {
      return Status::InvalidArgument(
          "Audit log file path must start with: " + config_.allowed_log_prefix);
    }
    log_file_.open(config_.log_file, std::ios::app);
    if (!log_file_.is_open()) {
      return Status::IOError("Failed to open audit log file: " + config_.log_file);
    }
  }
  
  running_.store(true);
  write_thread_ = std::thread(&AuditLogger::WriteLoop, this);
  
  return Status::OK();
}

void AuditLogger::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }
  
  queue_cv_.notify_all();
  
  if (write_thread_.joinable()) {
    write_thread_.join();
  }
  
  if (log_file_.is_open()) {
    log_file_.close();
  }
}

void AuditLogger::Log(const AuditEntry& entry) {
  bool should_filter = false;
  for (auto filtered : config_.filtered_actions) {
    if (filtered == entry.action) {
      should_filter = true;
      break;
    }
  }
  
  if (should_filter) return;
  
  AuditEntry new_entry = entry;
  new_entry.entry_id = next_entry_id_.fetch_add(1);
  new_entry.timestamp = std::chrono::system_clock::now();
  
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    // 限制队列大小，防止无界增长导致内存耗尽
    constexpr size_t kMaxQueueSize = 10000;
    if (write_queue_.size() >= kMaxQueueSize) {
      write_queue_.pop();  // 丢弃最旧的条目
    }
    write_queue_.push(new_entry);
  }
  
  queue_cv_.notify_one();
}

std::vector<AuditEntry> AuditLogger::Query(const std::string& user_id,
                                            AuditAction action,
                                            std::chrono::system_clock::time_point from,
                                            std::chrono::system_clock::time_point to,
                                            uint32_t limit) {
  std::lock_guard<std::mutex> lock(entries_mutex_);
  
  std::vector<AuditEntry> result;
  
  for (const auto& entry : entries_) {
    if (!user_id.empty() && entry.user_id != user_id) continue;
    if (static_cast<uint8_t>(action) != 255 && entry.action != action) continue;
    if (from.time_since_epoch().count() > 0 && entry.timestamp < from) continue;
    if (to.time_since_epoch().count() > 0 && entry.timestamp > to) continue;
    
    result.push_back(entry);
    
    if (result.size() >= limit) break;
  }
  
  return result;
}

Status AuditLogger::ExportToFile(const std::string& filename,
                                  std::chrono::system_clock::time_point from,
                                  std::chrono::system_clock::time_point to) {
  std::ofstream file(filename);
  if (!file.is_open()) {
    return Status::IOError("Failed to create export file");
  }
  
  auto entries = Query("", static_cast<AuditAction>(255), from, to, 
                       config_.max_entries);
  
  for (const auto& entry : entries) {
    std::ostringstream oss;
    auto time_t = std::chrono::system_clock::to_time_t(entry.timestamp);
    
    oss << "{";
    oss << "\"entry_id\":" << entry.entry_id << ",";
    struct tm tm_buf;
    if (localtime_r(&time_t, &tm_buf)) {
      oss << "\"timestamp\":\"" << std::put_time(&tm_buf, 
                                                   "%Y-%m-%dT%H:%M:%S") << "Z\",";
    } else {
      oss << "\"timestamp\":\"invalid\",";
    }
    oss << "\"user_id\":\"" << JsonEscape(entry.user_id) << "\",";
    oss << "\"action\":" << static_cast<int>(entry.action) << ",";
    oss << "\"resource\":\"" << JsonEscape(entry.resource) << "\",";
    oss << "\"success\":" << (entry.success ? "true" : "false");
    oss << "}";
    
    file << oss.str() << std::endl;
  }
  
  file.close();
  return Status::OK();
}

void AuditLogger::WriteLoop() {
  while (running_.load()) {
    try {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      queue_cv_.wait_for(lock, std::chrono::seconds(1),
          [this] { return !write_queue_.empty() || !running_.load(); });
      
      std::queue<AuditEntry> to_write;
      std::swap(to_write, write_queue_);
      lock.unlock();
      
      while (!to_write.empty()) {
        auto& entry = to_write.front();
        
        std::ostringstream oss;
        auto time_t = std::chrono::system_clock::to_time_t(entry.timestamp);
        
        oss << "{";
        oss << "\"entry_id\":" << entry.entry_id << ",";
        struct tm tm_buf;
        if (localtime_r(&time_t, &tm_buf)) {
          oss << "\"timestamp\":\"" << std::put_time(&tm_buf, 
                                                       "%Y-%m-%dT%H:%M:%S") << "Z\",";
        } else {
          oss << "\"timestamp\":\"invalid\",";
        }
        oss << "\"user_id\":\"" << JsonEscape(entry.user_id) << "\",";
        oss << "\"action\":" << static_cast<int>(entry.action) << ",";
        oss << "\"resource\":\"" << JsonEscape(entry.resource) << "\",";
        oss << "\"success\":" << (entry.success ? "true" : "false");
        oss << "}";
        
        if (config_.log_to_console) {
          std::cerr << oss.str() << std::endl;
        }
        
        if (log_file_.is_open()) {
          log_file_ << oss.str() << std::endl;
        }
        
        {
          std::lock_guard<std::mutex> entries_lock(entries_mutex_);
          entries_.push_back(entry);
          
          if (entries_.size() > config_.max_entries) {
            entries_.erase(entries_.begin());
          }
        }
        
        to_write.pop();
      }
      
      if (log_file_.is_open()) {
        log_file_.flush();
      }
    } catch (...) {
      std::cerr << "[SecurityManager] Log write exception" << std::endl;
    }
  }
}

// =============================================================================
// SecurityManager Implementation
// =============================================================================

SecurityManager::~SecurityManager() {
  Shutdown();
}

SecurityManager* SecurityManager::GetInstance() {
  static SecurityManager instance;
  return &instance;
}

Status SecurityManager::Initialize(const Config& config) {
  config_ = config;
  
  if (config_.enable_encryption) {
    tls_context_ = std::make_unique<TLSContext>();
    auto status = tls_context_->Initialize(config_.tls);
    CEDAR_RETURN_IF_ERROR(status);
  }
  
  if (config_.enable_auth) {
    authenticator_ = std::make_unique<Authenticator>();
    auto status = authenticator_->Initialize(config_.auth);
    CEDAR_RETURN_IF_ERROR(status);
    
    authorizer_ = std::make_unique<Authorizer>();
    authorizer_->InitializeDefaultRoles();
  }
  
  if (config_.enable_audit) {
    audit_logger_ = std::make_unique<AuditLogger>();
    auto status = audit_logger_->Initialize(config_.audit);
    CEDAR_RETURN_IF_ERROR(status);
  }
  
  initialized_.store(true);
  return Status::OK();
}

void SecurityManager::Shutdown() {
  if (!initialized_.exchange(false)) {
    return;
  }
  
  if (audit_logger_) {
    audit_logger_->Shutdown();
  }
  if (authenticator_) {
    authenticator_->Shutdown();
  }
  if (tls_context_) {
    tls_context_->Shutdown();
  }
}

Status SecurityManager::AuthenticateAndAuthorize(const std::string& token,
                                                  Permission permission,
                                                  const std::string& resource) {
  if (!config_.enable_auth) {
    return Status::OK();
  }
  
  auto token_result = authenticator_->ValidateToken(token);
  if (!token_result.ok()) {
    return Status::IOError("Invalid token");
  }
  
  return authorizer_->CheckPermission(token_result.value(), permission, resource);
}

}  // namespace security
}  // namespace dtx
}  // namespace cedar
