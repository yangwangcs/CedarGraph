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

#include <thread>
#include <fstream>

#include <chrono>
#include <iomanip>
#include <iostream>
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
  
  // TODO: Initialize OpenSSL SSL_CTX
  
  enabled_ = true;
  return Status::OK();
}

void TLSContext::Shutdown() {
  enabled_ = false;
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
  
  // Create default users
  AddUser("admin", "admin123", {"admin"});
  AddUser("readonly", "readonly123", {"readonly"});
  
  return Status::OK();
}

void Authenticator::Shutdown() {
  std::lock_guard<std::mutex> lock(tokens_mutex_);
  active_tokens_.clear();
  revoked_tokens_.clear();
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
  
  {
    std::lock_guard<std::mutex> token_lock(tokens_mutex_);
    active_tokens_[token.token_id] = token;
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
  
  // Check if revoked
  {
    std::lock_guard<std::mutex> lock(tokens_mutex_);
    if (revoked_tokens_.find(token.token_id) != revoked_tokens_.end()) {
      return Status::InvalidArgument("Token revoked");
    }
  }
  
  return token;
}

StatusOr<AuthToken> Authenticator::RefreshToken(const std::string& refresh_token) {
  (void)refresh_token;
  return Status::NotSupported("Refresh token not implemented");
}

Status Authenticator::RevokeToken(const std::string& token) {
  std::lock_guard<std::mutex> lock(tokens_mutex_);
  
  auto result = ParseJWT(token);
  if (result.ok()) {
    revoked_tokens_.insert(result.value().token_id);
    active_tokens_.erase(result.value().token_id);
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
  std::string salted = salt + password;
  
  unsigned char hash[SHA256_DIGEST_LENGTH];
  SHA256(reinterpret_cast<const unsigned char*>(salted.data()), 
         salted.size(), hash);
  
  std::ostringstream oss;
  oss << salt;
  for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
    oss << std::hex << std::setw(2) << std::setfill('0') << (int)hash[i];
  }
  
  return oss.str();
}

bool Authenticator::VerifyPassword(const std::string& password,
                                    const std::string& hash) {
  if (hash.size() < 16) return false;
  
  std::string salt = hash.substr(0, 16);
  std::string salted = salt + password;
  
  unsigned char expected_hash[SHA256_DIGEST_LENGTH];
  SHA256(reinterpret_cast<const unsigned char*>(salted.data()), 
         salted.size(), expected_hash);
  
  std::ostringstream oss;
  for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
    oss << std::hex << std::setw(2) << std::setfill('0') << (int)expected_hash[i];
  }
  
  return hash == (salt + oss.str());
}

std::string Authenticator::GenerateJWT(const AuthToken& token) {
  std::string header = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";
  
  auto time_t = std::chrono::system_clock::to_time_t(token.expires_at);
  std::ostringstream payload;
  payload << "{\"sub\":\"" << token.user_id << "\",";
  payload << "\"name\":\"" << token.user_name << "\",";
  payload << "\"exp\":" << time_t << ",";
  payload << "\"jti\":\"" << token.token_id << "\"}";
  
  std::string encoded_header = header;
  std::string encoded_payload = payload.str();
  
  std::string signature_input = encoded_header + "." + encoded_payload;
  
  unsigned char signature[EVP_MAX_MD_SIZE];
  unsigned int signature_len;
  HMAC(EVP_sha256(), config_.jwt_secret.data(), config_.jwt_secret.size(),
       reinterpret_cast<const unsigned char*>(signature_input.data()),
       signature_input.size(), signature, &signature_len);
  
  std::ostringstream sig_oss;
  for (unsigned int i = 0; i < signature_len; i++) {
    sig_oss << std::hex << std::setw(2) << std::setfill('0') << (int)signature[i];
  }
  
  return encoded_header + "." + encoded_payload + "." + sig_oss.str();
}

StatusOr<AuthToken> Authenticator::ParseJWT(const std::string& jwt) {
  size_t first_dot = jwt.find('.');
  size_t second_dot = jwt.find('.', first_dot + 1);
  
  if (first_dot == std::string::npos || second_dot == std::string::npos) {
    return Status::InvalidArgument("Invalid JWT format");
  }
  
  AuthToken token;
  token.token_id = jwt;
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
            if (resource.find(pattern) != std::string::npos) {
              allowed = true;
              break;
            }
          }
          for (const auto& pattern : role.denied_resources) {
            if (resource.find(pattern) != std::string::npos) {
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
    log_file_.open(config_.log_file, std::ios::app);
    if (!log_file_.is_open()) {
      return Status::IOError("Failed to open audit log file");
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
    oss << "\"timestamp\":\"" << std::put_time(std::localtime(&time_t), 
                                                 "%Y-%m-%dT%H:%M:%S") << "Z\",";
    oss << "\"user_id\":\"" << entry.user_id << "\",";
    oss << "\"action\":" << static_cast<int>(entry.action) << ",";
    oss << "\"resource\":\"" << entry.resource << "\",";
    oss << "\"success\":" << (entry.success ? "true" : "false");
    oss << "}";
    
    file << oss.str() << std::endl;
  }
  
  file.close();
  return Status::OK();
}

void AuditLogger::WriteLoop() {
  while (running_.load()) {
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
      oss << "\"timestamp\":\"" << std::put_time(std::localtime(&time_t), 
                                                   "%Y-%m-%dT%H:%M:%S") << "Z\",";
      oss << "\"user_id\":\"" << entry.user_id << "\",";
      oss << "\"action\":" << static_cast<int>(entry.action) << ",";
      oss << "\"resource\":\"" << entry.resource << "\",";
      oss << "\"success\":" << (entry.success ? "true" : "false");
      oss << "}";
      
      if (config_.log_to_console) {
        std::cout << oss.str() << std::endl;
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
