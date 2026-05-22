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
// Security Module - 安全模块
// =============================================================================
// Features:
// - TLS/SSL 加密通信
// - 基于 Token 的认证
// - 基于角色的访问控制 (RBAC)
// - 审计日志
// - 密钥管理
// =============================================================================

#ifndef CEDAR_DTX_SECURITY_H_
#define CEDAR_DTX_SECURITY_H_

#include <atomic>
#include <cstdint>
#include <chrono>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"

namespace cedar {
namespace dtx {
namespace security {

// =============================================================================
// TLS/SSL 配置
// =============================================================================

struct TLSConfig {
  bool enable_tls{true};
  std::string cert_file;        // 服务器证书
  std::string key_file;         // 服务器私钥
  std::string ca_file;          // CA 证书
  bool verify_client{false};    // 是否验证客户端证书
  std::string cipher_suites;    // 加密套件
  uint32_t session_timeout_sec{7200};  // 会话超时
};

// =============================================================================
// TLS 上下文管理
// =============================================================================

class TLSContext {
 public:
  TLSContext();
  ~TLSContext();
  
  // 禁止拷贝
  TLSContext(const TLSContext&) = delete;
  TLSContext& operator=(const TLSContext&) = delete;
  
  Status Initialize(const TLSConfig& config);
  void Shutdown();
  
  bool IsEnabled() const { return enabled_; }
  
  // 获取底层 SSL 上下文（供 gRPC 使用）
  void* GetNativeContext() const { return ssl_context_; }

 private:
  bool enabled_{false};
  void* ssl_context_{nullptr};
  TLSConfig config_;
};

// =============================================================================
// 认证 Token
// =============================================================================

struct AuthToken {
  std::string token_id;
  std::string user_id;
  std::string user_name;
  std::vector<std::string> roles;
  std::chrono::system_clock::time_point issued_at;
  std::chrono::system_clock::time_point expires_at;
  std::map<std::string, std::string> claims;
  
  bool IsExpired() const {
    return std::chrono::system_clock::now() > expires_at;
  }
  
  bool HasRole(const std::string& role) const {
    return std::find(roles.begin(), roles.end(), role) != roles.end();
  }
};

// =============================================================================
// 认证管理器
// =============================================================================

class Authenticator {
 public:
  struct Account {
    std::string username;
    std::string password;
    std::vector<std::string> roles;
  };

  struct Config {
    std::string jwt_secret;           // JWT 密钥
    std::chrono::seconds token_ttl{3600};  // Token 有效期
    bool enable_refresh_token{true};
    uint32_t max_login_attempts{5};   // 最大登录尝试次数
    std::chrono::seconds lockout_duration{300};  // 锁定时间
    std::vector<Account> accounts;    // Config-only user accounts
  };
  
  Authenticator();
  ~Authenticator();
  
  Status Initialize(const Config& config);
  void Shutdown();
  
  // 用户认证
  StatusOr<AuthToken> Authenticate(const std::string& username,
                                    const std::string& password);
  
  // Token 验证
  StatusOr<AuthToken> ValidateToken(const std::string& token);
  
  // Token 刷新
  StatusOr<AuthToken> RefreshToken(const std::string& refresh_token);
  
  // Token 撤销
  Status RevokeToken(const std::string& token);
  
  // 添加/删除用户
  Status AddUser(const std::string& username, const std::string& password,
                 const std::vector<std::string>& roles);
  Status RemoveUser(const std::string& username);
  Status UpdatePassword(const std::string& username,
                        const std::string& new_password);

  // JWT generation/validation (public for testing)
  std::string GenerateJWT(const AuthToken& token);
  StatusOr<AuthToken> ParseJWT(const std::string& jwt);

 private:
  struct UserInfo {
    std::string username;
    std::string password_hash;
    std::vector<std::string> roles;
    bool locked{false};
    uint32_t failed_attempts{0};
    std::chrono::system_clock::time_point locked_until;
  };
  
  Config config_;
  std::mutex users_mutex_;
  std::map<std::string, UserInfo> users_;
  
  std::mutex tokens_mutex_;
  std::map<std::string, AuthToken> active_tokens_;
  std::set<std::string> revoked_tokens_{};
  
  std::string HashPassword(const std::string& password);
  bool VerifyPassword(const std::string& password,
                      const std::string& hash);
};

// =============================================================================
// 权限定义
// =============================================================================

enum class Permission : uint32_t {
  kNone = 0,
  kRead = 1 << 0,           // 读取数据
  kWrite = 1 << 1,          // 写入数据
  kDelete = 1 << 2,         // 删除数据
  kAdmin = 1 << 3,          // 管理权限
  kMonitor = 1 << 4,        // 监控权限
  kConfig = 1 << 5,         // 配置权限
  kAll = 0xFFFFFFFF,
};

inline Permission operator|(Permission a, Permission b) {
  return static_cast<Permission>(static_cast<uint32_t>(a) | 
                                 static_cast<uint32_t>(b));
}

inline bool HasPermission(Permission granted, Permission required) {
  return (static_cast<uint32_t>(granted) & static_cast<uint32_t>(required)) 
         == static_cast<uint32_t>(required);
}

// =============================================================================
// 角色定义
// =============================================================================

struct Role {
  std::string name;
  std::string description;
  Permission permissions;
  std::vector<std::string> allowed_resources;  // 允许访问的资源
  std::vector<std::string> denied_resources;   // 拒绝访问的资源
};

// =============================================================================
// 授权管理器 (RBAC)
// =============================================================================

class Authorizer {
 public:
  Authorizer();
  ~Authorizer();
  
  // 初始化默认角色
  Status InitializeDefaultRoles();
  
  // 角色管理
  Status AddRole(const Role& role);
  Status RemoveRole(const std::string& role_name);
  StatusOr<Role> GetRole(const std::string& role_name) const;
  
  // 权限检查
  Status CheckPermission(const AuthToken& token, Permission permission,
                         const std::string& resource = "");
  
  // 检查特定操作
  bool CanRead(const AuthToken& token, const std::string& resource = "");
  bool CanWrite(const AuthToken& token, const std::string& resource = "");
  bool CanDelete(const AuthToken& token, const std::string& resource = "");
  bool CanAdmin(const AuthToken& token);

 private:
  bool CheckResourceAccess(const std::vector<std::string>& allowed,
                           const std::vector<std::string>& denied,
                           const std::string& resource);
  
  mutable std::mutex roles_mutex_;
  std::map<std::string, Role> roles_;
};

// =============================================================================
// 审计日志
// =============================================================================

enum class AuditAction : uint8_t {
  kLogin = 0,
  kLogout = 1,
  kAccess = 2,
  kModify = 3,
  kDelete = 4,
  kAdmin = 5,
};

struct AuditEntry {
  uint64_t entry_id{0};
  std::chrono::system_clock::time_point timestamp;
  std::string user_id;
  std::string user_name;
  AuditAction action;
  std::string resource;
  std::string operation;
  bool success{false};
  std::string details;
  std::string client_ip;
  std::string session_id;
};

class AuditLogger {
 public:
  struct Config {
    std::string log_file;
    bool log_to_console{true};
    uint32_t max_entries{1000000};
    std::vector<AuditAction> filtered_actions;  // 不记录的动作
    std::string allowed_log_prefix;  // Empty = no restriction (dev only)
  };
  
  AuditLogger();
  ~AuditLogger();
  
  Status Initialize(const Config& config);
  void Shutdown();
  
  void Log(const AuditEntry& entry);
  
  // 查询审计日志
  std::vector<AuditEntry> Query(const std::string& user_id = "",
                                 AuditAction action = static_cast<AuditAction>(255),
                                 std::chrono::system_clock::time_point from = {},
                                 std::chrono::system_clock::time_point to = {},
                                 uint32_t limit = 100);
  
  // 导出审计日志
  Status ExportToFile(const std::string& filename, 
                      std::chrono::system_clock::time_point from = {},
                      std::chrono::system_clock::time_point to = {});

 private:
  void WriteLoop();
  bool ShouldFilter(AuditAction action);
  std::string EntryToJson(const AuditEntry& entry);
  
  Config config_;
  std::atomic<bool> running_{false};
  std::atomic<uint64_t> next_entry_id_{1};
  
  std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::queue<AuditEntry> write_queue_;
  
  std::vector<AuditEntry> entries_;
  std::mutex entries_mutex_;
  
  std::ofstream log_file_;
  std::thread write_thread_;
};

// =============================================================================
// 安全管理器
// =============================================================================

class SecurityManager {
 public:
  struct Config {
    TLSConfig tls;
    Authenticator::Config auth;
    AuditLogger::Config audit;
    bool enable_encryption{true};
    bool enable_auth{true};
    bool enable_audit{true};
  };
  
  static SecurityManager* GetInstance();
  
  Status Initialize(const Config& config);
  void Shutdown();
  
  // TLS
  TLSContext* GetTLSContext() { return tls_context_.get(); }
  
  // 认证
  Authenticator* GetAuthenticator() { return authenticator_.get(); }
  
  // 授权
  Authorizer* GetAuthorizer() { return authorizer_.get(); }
  
  // 审计
  AuditLogger* GetAuditLogger() { return audit_logger_.get(); }
  
  // 便捷方法
  Status AuthenticateAndAuthorize(const std::string& token,
                                   Permission permission,
                                   const std::string& resource = "");

 private:
  SecurityManager() = default;
  ~SecurityManager();
  
  std::unique_ptr<TLSContext> tls_context_;
  std::unique_ptr<Authenticator> authenticator_;
  std::unique_ptr<Authorizer> authorizer_;
  std::unique_ptr<AuditLogger> audit_logger_;
  
  Config config_;
  std::atomic<bool> initialized_{false};
};

// =============================================================================
// 便捷宏
// =============================================================================

#define AUDIT_LOG(action, resource, op, success, details) \
  do { \
    auto* audit = ::cedar::dtx::security::SecurityManager::GetInstance()->GetAuditLogger(); \
    if (audit) { \
      ::cedar::dtx::security::AuditEntry entry; \
      entry.timestamp = std::chrono::system_clock::now(); \
      entry.action = action; \
      entry.resource = resource; \
      entry.operation = op; \
      entry.success = success; \
      entry.details = details; \
      audit->Log(entry); \
    } \
  } while (0)

}  // namespace security
}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_SECURITY_H_
