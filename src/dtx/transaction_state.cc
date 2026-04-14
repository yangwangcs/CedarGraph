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

#include "cedar/dtx/transaction_state.h"

#include <cstring>
#include <fstream>
#include <iostream>

namespace cedar {

using dtx::TxnID;
using dtx::PartitionID;

// Simple WAL implementation for transaction states
class TransactionStateManager::TransactionWAL {
 public:
  TransactionWAL() = default;
  ~TransactionWAL() { Close(); }
  
  Status Open(const std::string& wal_dir) {
    wal_dir_ = wal_dir;
    // Ensure directory exists
    std::filesystem::create_directories(wal_dir);
    
    wal_file_ = wal_dir + "/txn_state.wal";
    log_.open(wal_file_, std::ios::app | std::ios::binary);
    if (!log_.is_open()) {
      return Status::IOError("TransactionWAL", "Failed to open WAL file");
    }
    return Status::OK();
  }
  
  void Close() {
    if (log_.is_open()) {
      log_.close();
    }
  }
  
  Status Append(const std::string& data) {
    if (!log_.is_open()) {
      return Status::IOError("TransactionWAL", "WAL not open");
    }
    
    // Write length prefix + data
    uint32_t len = static_cast<uint32_t>(data.size());
    log_.write(reinterpret_cast<const char*>(&len), sizeof(len));
    log_.write(data.data(), data.size());
    log_.flush();
    
    if (!log_.good()) {
      return Status::IOError("TransactionWAL", "Write failed");
    }
    return Status::OK();
  }
  
  Status Sync() {
    if (!log_.is_open()) {
      return Status::IOError("TransactionWAL", "WAL not open");
    }
    log_.flush();
    return Status::OK();
  }
  
  Status Replay(const std::function<bool(const std::string& record)>& callback) {
    std::ifstream input(wal_file_, std::ios::binary);
    if (!input.is_open()) {
      return Status::OK();  // No WAL file yet
    }
    
    while (input.good()) {
      uint32_t len;
      input.read(reinterpret_cast<char*>(&len), sizeof(len));
      if (!input.good()) break;
      
      std::string data(len, '\0');
      input.read(&data[0], len);
      if (!input.good()) {
        return Status::Corruption("TransactionWAL", "Incomplete record");
      }
      
      if (!callback(data)) {
        break;
      }
    }
    return Status::OK();
  }
  
 private:
  std::string wal_dir_;
  std::string wal_file_;
  std::ofstream log_;
};

// TransactionRecord serialization
void TransactionRecord::Serialize(std::string* output) const {
  // Simple binary serialization
  output->clear();
  
  // Header: txn_id(8) + state(4) + commit_ts(8) + participants_count(4)
  output->append(reinterpret_cast<const char*>(&txn_id), sizeof(txn_id));
  int state_int = static_cast<int>(state);
  output->append(reinterpret_cast<const char*>(&state_int), sizeof(state_int));
  uint64_t ts = commit_ts.value();
  output->append(reinterpret_cast<const char*>(&ts), sizeof(ts));
  
  uint32_t participant_count = static_cast<uint32_t>(participants.size());
  output->append(reinterpret_cast<const char*>(&participant_count), sizeof(participant_count));
  
  // Participants
  for (const auto& pid : participants) {
    output->append(reinterpret_cast<const char*>(&pid), sizeof(pid));
  }
  
  // Retry count
  output->append(reinterpret_cast<const char*>(&retry_count), sizeof(retry_count));
}

Status TransactionRecord::Deserialize(const std::string& input, TransactionRecord* record) {
  if (input.size() < 24) {
    return Status::Corruption("TransactionRecord", "Input too small");
  }
  
  const char* p = input.data();
  size_t pos = 0;
  
  // txn_id
  record->txn_id = *reinterpret_cast<const TxnID*>(p + pos);
  pos += sizeof(TxnID);
  
  // state
  int state_int = *reinterpret_cast<const int*>(p + pos);
  record->state = static_cast<TxnState>(state_int);
  pos += sizeof(int);
  
  // commit_ts
  uint64_t ts = *reinterpret_cast<const uint64_t*>(p + pos);
  record->commit_ts = Timestamp(ts);
  pos += sizeof(uint64_t);
  
  // participants
  uint32_t participant_count = *reinterpret_cast<const uint32_t*>(p + pos);
  pos += sizeof(uint32_t);
  
  record->participants.clear();
  for (uint32_t i = 0; i < participant_count && pos + sizeof(PartitionID) <= input.size(); ++i) {
    PartitionID pid = *reinterpret_cast<const PartitionID*>(p + pos);
    record->participants.push_back(pid);
    pos += sizeof(PartitionID);
  }
  
  // retry_count (if available)
  if (pos + sizeof(int) <= input.size()) {
    record->retry_count = *reinterpret_cast<const int*>(p + pos);
  }
  
  auto now = std::chrono::steady_clock::now();
  record->created_at = now;
  record->updated_at = now;
  
  return Status::OK();
}

// TransactionStateManager implementation
TransactionStateManager::TransactionStateManager() = default;

TransactionStateManager::~TransactionStateManager() {
  Shutdown();
}

Status TransactionStateManager::Initialize(const std::string& wal_dir) {
  if (initialized_.exchange(true)) {
    return Status::OK();  // Already initialized
  }
  
  wal_ = std::make_unique<TransactionWAL>();
  Status s = wal_->Open(wal_dir);
  if (!s.ok()) {
    initialized_ = false;
    return s;
  }
  
  // Recover from WAL
  s = RecoverFromWAL(wal_dir);
  if (!s.ok()) {
    initialized_ = false;
    return s;
  }
  
  return Status::OK();
}

void TransactionStateManager::Shutdown() {
  if (shutdown_.exchange(true)) {
    return;
  }
  
  if (wal_) {
    wal_->Close();
    wal_.reset();
  }
  
  std::lock_guard<std::mutex> lock(mutex_);
  transactions_.clear();
}

Status TransactionStateManager::CreateTransaction(
    TxnID txn_id, 
    const std::vector<PartitionID>& participants) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (transactions_.count(txn_id)) {
    return Status::InvalidArgument("TransactionStateManager", "Transaction already exists");
  }
  
  TransactionRecord record;
  record.txn_id = txn_id;
  record.state = TxnState::kActive;
  record.participants = participants;
  auto now = std::chrono::steady_clock::now();
  record.created_at = now;
  record.updated_at = now;
  
  // Initialize participant states
  for (const auto& pid : participants) {
    ParticipantState ps;
    ps.partition_id = pid;
    ps.state = ParticipantState::State::kUnknown;
    ps.last_update = now;
    record.participant_states[pid] = std::move(ps);
  }
  
  // Persist to WAL
  Status s = PersistState(record);
  if (!s.ok()) {
    return s;
  }
  
  transactions_[txn_id] = std::move(record);
  return Status::OK();
}

Status TransactionStateManager::UpdateState(TxnID txn_id, TxnState new_state) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto it = transactions_.find(txn_id);
  if (it == transactions_.end()) {
    return Status::NotFound("TransactionStateManager", "Transaction not found");
  }
  
  it->second.state = new_state;
  it->second.updated_at = std::chrono::steady_clock::now();
  
  return PersistState(it->second);
}

Status TransactionStateManager::UpdateParticipantState(
    TxnID txn_id, 
    PartitionID pid,
    ParticipantState::State state,
    const std::optional<std::string>& error) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto it = transactions_.find(txn_id);
  if (it == transactions_.end()) {
    return Status::NotFound("TransactionStateManager", "Transaction not found");
  }
  
  auto ps_it = it->second.participant_states.find(pid);
  if (ps_it == it->second.participant_states.end()) {
    return Status::NotFound("TransactionStateManager", "Participant not found");
  }
  
  ps_it->second.state = state;
  ps_it->second.last_update = std::chrono::steady_clock::now();
  if (error) {
    ps_it->second.error_msg = error;
  }
  
  it->second.updated_at = std::chrono::steady_clock::now();
  return PersistState(it->second);
}

std::optional<TransactionRecord> TransactionStateManager::GetTransaction(
    TxnID txn_id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto it = transactions_.find(txn_id);
  if (it != transactions_.end()) {
    return it->second;
  }
  return std::nullopt;
}

std::vector<TransactionRecord> TransactionStateManager::GetPendingTransactions() const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::vector<TransactionRecord> result;
  for (const auto& [txn_id, record] : transactions_) {
    if (record.state != TxnState::kCommitted && 
        record.state != TxnState::kAborted) {
      result.push_back(record);
    }
  }
  return result;
}

std::vector<TransactionRecord> TransactionStateManager::GetTransactionsByState(
    TxnState state) const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::vector<TransactionRecord> result;
  for (const auto& [txn_id, record] : transactions_) {
    if (record.state == state) {
      result.push_back(record);
    }
  }
  return result;
}

std::vector<TxnID> TransactionStateManager::GetTimedOutTransactions(
    std::chrono::milliseconds timeout) const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto now = std::chrono::steady_clock::now();
  std::vector<TxnID> result;
  
  for (const auto& [txn_id, record] : transactions_) {
    if (record.timeout_at && now > *record.timeout_at) {
      result.push_back(txn_id);
    }
  }
  return result;
}

void TransactionStateManager::SetTransactionTimeout(
    TxnID txn_id, 
    std::chrono::milliseconds timeout) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto it = transactions_.find(txn_id);
  if (it != transactions_.end()) {
    it->second.timeout_at = std::chrono::steady_clock::now() + timeout;
  }
}

Status TransactionStateManager::RecoverFromWAL(const std::string& wal_dir) {
  return wal_->Replay([this](const std::string& record_data) -> bool {
    TransactionRecord record;
    Status s = TransactionRecord::Deserialize(record_data, &record);
    if (!s.ok()) {
      // Log error but continue recovery
      return true;
    }
    
    transactions_[record.txn_id] = std::move(record);
    return true;
  });
}

Status TransactionStateManager::PersistState(const TransactionRecord& record) {
  std::string data;
  record.Serialize(&data);
  
  Status s = wal_->Append(data);
  if (!s.ok()) {
    return s;
  }
  
  return wal_->Sync();
}

}  // namespace cedar
