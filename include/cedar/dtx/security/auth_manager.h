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

#ifndef CEDAR_DTX_SECURITY_AUTH_MANAGER_H_
#define CEDAR_DTX_SECURITY_AUTH_MANAGER_H_

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "cedar/core/status.h"

namespace cedar {
namespace dtx {
namespace security {

// =============================================================================
// Permission Types
// =============================================================================

enum class Permission : uint32_t {
  kNone = 0,
  kRead = 1,
  kWrite = 2,
  kDelete = 4,
  kAdmin = 8,
  kAll = kRead | kWrite | kDelete | kAdmin,
};

inline Permission operator|(Permission a, Permission b) {
  return static_cast<Permission>(static_cast<uint32_t>(a) | 
                                  static_cast<uint32_t>(b));
}

inline bool HasPermission(Permission perms, Permission required) {
  return (static_cast<uint32_t>(perms) & static_cast<uint32_t>(required)) != 0;
}

// =============================================================================
// Role Definition
// =============================================================================

struct Role {
  std::string name;
  Permission permissions;
  std::vector<std::string> allowed_spaces;
  std::vector<std::string> allowed_operations;
};

// =============================================================================
// User/Service Identity
// =============================================================================

struct Identity {
  std::string id;
  std::string name;
  std::vector<std::string> roles;
  std::unordered_map<std::string, std::string> attributes;
  
  bool HasRole(const std::string& role) const;
};

// =============================================================================
// Authentication Token
// =============================================================================

struct AuthToken {
  std::string token_id;
  Identity identity;
  std::chrono::system_clock::time_point issued_at;
  std::chrono::system_clock::time_point expires_at;
  std::string signature;
  
  bool IsExpired() const;
  bool IsValid() const;
};

// =============================================================================
// Authentication Manager
// =============================================================================

class AuthManager {
 public:
  AuthManager();
  ~AuthManager();
  
  Status Initialize(const std::string& config_path);
  
  // Password authentication
  StatusOr<AuthToken> AuthenticatePassword(const std::string& username,
                                           const std::string& password);
  
  // Certificate authentication
  StatusOr<AuthToken> AuthenticateCertificate(const std::string& cert_pem);
  
  // Token validation
  StatusOr<Identity> ValidateToken(const std::string& token_str);
  
  // Authorization
  Status CheckPermission(const Identity& identity, 
                         Permission required,
                         const std::string& resource);
  
  // Role management
  Status CreateRole(const Role& role);
  Status DeleteRole(const std::string& role_name);
  Status AssignRole(const std::string& identity_id, 
                    const std::string& role_name);
  
  // TLS/SSL
  Status ConfigureTLS(const std::string& cert_path,
                      const std::string& key_path,
                      const std::string& ca_path);

 private:
  std::unordered_map<std::string, Role> roles_;
  std::unordered_map<std::string, Identity> identities_;
  std::unordered_map<std::string, std::string> password_hashes_;
};

// =============================================================================
// TLS Configuration
// =============================================================================

struct TLSConfig {
  bool enabled = false;
  std::string cert_file;
  std::string key_file;
  std::string ca_file;
  bool verify_client = true;
  std::string cipher_suites = "ECDHE-RSA-AES256-GCM-SHA384";
};

}  // namespace security
}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_SECURITY_AUTH_MANAGER_H_
