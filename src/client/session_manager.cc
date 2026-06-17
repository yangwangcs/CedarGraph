// Copyright 2025 The Cedar Authors
//
// Session Manager implementation

#include "cedar/client/session_manager.h"

#include <chrono>
#include <random>
#include <sstream>
#include <iomanip>

namespace cedar {
namespace client {

SessionManager::SessionManager() = default;
SessionManager::~SessionManager() = default;

std::string SessionManager::CreateSession(const std::string& user_name) {
  std::lock_guard<std::mutex> lock(mutex_);

  std::string session_id = GenerateSessionId();
  auto now = std::chrono::system_clock::now().time_since_epoch().count();

  SessionInfo session;
  session.session_id = session_id;
  session.user_name = user_name;
  session.current_space = "default";
  session.created_at = now;
  session.last_active = now;
  session.in_transaction = false;

  sessions_[session_id] = session;

  return session_id;
}

SessionInfo SessionManager::GetSession(const std::string& session_id) const {
  std::lock_guard<std::mutex> lock(mutex_);

  auto it = sessions_.find(session_id);
  if (it != sessions_.end()) {
    return it->second;
  }

  return {};
}

void SessionManager::UpdateSessionActivity(const std::string& session_id) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto it = sessions_.find(session_id);
  if (it != sessions_.end()) {
    it->second.last_active = 
        std::chrono::system_clock::now().time_since_epoch().count();
  }
}

void SessionManager::SetSessionSpace(const std::string& session_id, 
                                       const std::string& space_name) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto it = sessions_.find(session_id);
  if (it != sessions_.end()) {
    it->second.current_space = space_name;
    it->second.last_active = 
        std::chrono::system_clock::now().time_since_epoch().count();
  }
}

std::string SessionManager::GetSessionSpace(const std::string& session_id) const {
  std::lock_guard<std::mutex> lock(mutex_);

  auto it = sessions_.find(session_id);
  if (it != sessions_.end()) {
    return it->second.current_space;
  }

  return "default";
}

std::string SessionManager::StartTransaction(const std::string& session_id) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto it = sessions_.find(session_id);
  if (it != sessions_.end()) {
    if (it->second.in_transaction) {
      return it->second.transaction_id;  // Already in transaction
    }

    std::string transaction_id = GenerateTransactionId();
    it->second.in_transaction = true;
    it->second.transaction_id = transaction_id;
    it->second.last_active = 
        std::chrono::system_clock::now().time_since_epoch().count();

    return transaction_id;
  }

  return "";
}

void SessionManager::EndTransaction(const std::string& session_id) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto it = sessions_.find(session_id);
  if (it != sessions_.end()) {
    it->second.in_transaction = false;
    it->second.transaction_id.clear();
    it->second.last_active = 
        std::chrono::system_clock::now().time_since_epoch().count();
  }
}

bool SessionManager::IsInTransaction(const std::string& session_id) const {
  std::lock_guard<std::mutex> lock(mutex_);

  auto it = sessions_.find(session_id);
  if (it != sessions_.end()) {
    return it->second.in_transaction;
  }

  return false;
}

void SessionManager::RemoveSession(const std::string& session_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  sessions_.erase(session_id);
}

void SessionManager::CleanupExpiredSessions(int64_t max_idle_seconds) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto now = std::chrono::system_clock::now().time_since_epoch().count();
  auto max_idle_ns = max_idle_seconds * 1000000000LL;

  for (auto it = sessions_.begin(); it != sessions_.end(); ) {
    if (now - it->second.last_active > max_idle_ns) {
      it = sessions_.erase(it);
    } else {
      ++it;
    }
  }
}

int SessionManager::GetSessionCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return sessions_.size();
}

std::string SessionManager::GenerateSessionId() const {
  static std::random_device rd;
  static std::mt19937 gen(rd());
  static std::uniform_int_distribution<> dis(0, 15);

  std::stringstream ss;
  ss << std::hex;
  for (int i = 0; i < 32; ++i) {
    ss << dis(gen);
  }

  return ss.str();
}

std::string SessionManager::GenerateTransactionId() const {
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
