// Copyright 2025 The Cedar Authors
//
// Session Manager for user sessions and transactions

#ifndef CEDAR_CLIENT_SESSION_MANAGER_H_
#define CEDAR_CLIENT_SESSION_MANAGER_H_

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace cedar {
namespace client {

// Session information
struct SessionInfo {
  std::string session_id;
  std::string user_name;
  std::string current_space;
  int64_t created_at;
  int64_t last_active;
  bool in_transaction;
  std::string transaction_id;
};

// Session Manager
class SessionManager {
 public:
  SessionManager();
  ~SessionManager();

  // Create a new session
  std::string CreateSession(const std::string& user_name);

  // Get session information
  SessionInfo GetSession(const std::string& session_id) const;

  // Update session activity
  void UpdateSessionActivity(const std::string& session_id);

  // Set current space for session
  void SetSessionSpace(const std::string& session_id, const std::string& space_name);

  // Get current space for session
  std::string GetSessionSpace(const std::string& session_id) const;

  // Start transaction
  std::string StartTransaction(const std::string& session_id);

  // End transaction
  void EndTransaction(const std::string& session_id);

  // Check if session is in transaction
  bool IsInTransaction(const std::string& session_id) const;

  // Remove session
  void RemoveSession(const std::string& session_id);

  // Clean up expired sessions
  void CleanupExpiredSessions(int64_t max_idle_seconds);

  // Get session count
  int GetSessionCount() const;

 private:
  std::unordered_map<std::string, SessionInfo> sessions_;
  mutable std::mutex mutex_;

  // Generate unique session ID
  std::string GenerateSessionId() const;

  // Generate unique transaction ID
  std::string GenerateTransactionId() const;
};

}  // namespace client
}  // namespace cedar

#endif  // CEDAR_CLIENT_SESSION_MANAGER_H_
