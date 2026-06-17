// Copyright 2025 The Cedar Authors
//
// JWT Manager for token-based authentication

#ifndef CEDAR_CLIENT_JWT_MANAGER_H_
#define CEDAR_CLIENT_JWT_MANAGER_H_

#include <chrono>
#include <functional>
#include <mutex>
#include <string>

namespace cedar {
namespace client {

// JWT configuration
struct JWTConfig {
  std::string secret_key;
  std::string issuer = "cedar-client";
  std::string audience = "cedar-server";
  int expiration_seconds = 3600;  // 1 hour
  int refresh_threshold_seconds = 300;  // Refresh 5 minutes before expiration
};

// JWT token information
struct JWTToken {
  std::string token;
  std::string user_id;
  std::string user_name;
  std::chrono::system_clock::time_point issued_at;
  std::chrono::system_clock::time_point expires_at;
  bool is_valid;
};

// JWT Manager
class JWTManager {
 public:
  JWTManager(const JWTConfig& config = JWTConfig());
  ~JWTManager();

  // Generate a new JWT token
  JWTToken GenerateToken(const std::string& user_id, 
                          const std::string& user_name);

  // Validate a JWT token
  bool ValidateToken(const std::string& token) const;

  // Get token information
  JWTToken GetTokenInfo(const std::string& token) const;

  // Check if token needs refresh
  bool NeedsRefresh(const std::string& token) const;

  // Refresh token
  JWTToken RefreshToken(const std::string& token);

  // Set token refresh callback
  void SetRefreshCallback(std::function<JWTToken(const std::string&)> callback);

  // Get current token
  std::string GetCurrentToken() const;

  // Set current token
  void SetCurrentToken(const std::string& token);

  // Clear current token
  void ClearCurrentToken();

 private:
  JWTConfig config_;
  std::string current_token_;
  mutable std::mutex mutex_;
  std::function<JWTToken(const std::string&)> refresh_callback_;

  // Helper methods
  std::string Base64Encode(const std::string& input) const;
  std::string Base64Decode(const std::string& input) const;
  std::string CreateSignature(const std::string& header, const std::string& payload) const;
  bool VerifySignature(const std::string& header, const std::string& payload, 
                        const std::string& signature) const;
  std::string GenerateRandomId() const;
};

}  // namespace client
}  // namespace cedar

#endif  // CEDAR_CLIENT_JWT_MANAGER_H_
