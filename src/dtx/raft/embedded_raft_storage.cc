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

#include "cedar/dtx/raft/embedded_raft.h"

#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>

namespace cedar {
namespace dtx {
namespace raft {

// Simple binary format for log entries:
// [8 bytes: term][8 bytes: index][4 bytes: data length][data bytes]
//
// State file format:
// [8 bytes: current_term][4 bytes: voted_for]
//
// Snapshot format:
// [8 bytes: last_included_term][8 bytes: last_included_index][4 bytes: data length][data bytes]

FileRaftStorage::FileRaftStorage(const std::string& data_dir)
    : data_dir_(data_dir),
      log_file_(data_dir + "/raft.log"),
      state_file_(data_dir + "/raft.state"),
      snapshot_file_(data_dir + "/raft.snapshot") {}

FileRaftStorage::~FileRaftStorage() = default;

Status FileRaftStorage::Initialize() {
  std::lock_guard<std::mutex> lock(mutex_);
  
  // Create directory if not exists
  std::filesystem::path dir(data_dir_);
  if (!std::filesystem::exists(dir)) {
    std::filesystem::create_directories(dir);
  }
  
  // Load existing log to determine last_log_index
  if (std::filesystem::exists(log_file_)) {
    std::ifstream file(log_file_, std::ios::binary);
    if (!file.is_open()) {
      return Status::IOError("Cannot open log file");
    }
    
    while (file.good()) {
      Term term;
      LogIndex index;
      uint32_t data_len;
      
      file.read(reinterpret_cast<char*>(&term), sizeof(term));
      if (!file.good()) break;
      
      file.read(reinterpret_cast<char*>(&index), sizeof(index));
      file.read(reinterpret_cast<char*>(&data_len), sizeof(data_len));
      
      if (data_len > 0) {
        std::string data(data_len, '\0');
        file.read(data.data(), data_len);
      }
      
      last_log_index_ = index;
      last_log_term_ = term;
    }
    
    file.close();
  }
  
  return Status::OK();
}

Status FileRaftStorage::AppendLog(const std::vector<LogEntry>& entries) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::ofstream file(log_file_, std::ios::binary | std::ios::app);
  if (!file.is_open()) {
    return Status::IOError("Cannot open log file for append");
  }
  
  for (const auto& entry : entries) {
    uint32_t data_len = static_cast<uint32_t>(entry.data.size());
    
    file.write(reinterpret_cast<const char*>(&entry.term), sizeof(entry.term));
    file.write(reinterpret_cast<const char*>(&entry.index), sizeof(entry.index));
    file.write(reinterpret_cast<const char*>(&data_len), sizeof(data_len));
    
    if (data_len > 0) {
      file.write(entry.data.data(), data_len);
    }
    
    last_log_index_ = entry.index;
    last_log_term_ = entry.term;
  }
  
  file.flush();
  
  if (!file.good()) {
    return Status::IOError("Failed to write log entry");
  }
  
  return Status::OK();
}

Status FileRaftStorage::TruncateLog(LogIndex from_index) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (!std::filesystem::exists(log_file_)) {
    return Status::OK();
  }
  
  // Read all entries up to from_index-1
  std::vector<LogEntry> entries;
  
  {
    std::ifstream file(log_file_, std::ios::binary);
    if (!file.is_open()) {
      return Status::IOError("Cannot open log file for read");
    }
    
    while (file.good()) {
      LogEntry entry;
      uint32_t data_len;
      
      file.read(reinterpret_cast<char*>(&entry.term), sizeof(entry.term));
      if (!file.good()) break;
      
      file.read(reinterpret_cast<char*>(&entry.index), sizeof(entry.index));
      file.read(reinterpret_cast<char*>(&data_len), sizeof(data_len));
      
      if (data_len > 0) {
        entry.data.resize(data_len);
        file.read(entry.data.data(), data_len);
      }
      
      if (entry.index < from_index) {
        entries.push_back(entry);
      } else {
        break;
      }
    }
  }
  
  // Rewrite log file
  std::ofstream file(log_file_, std::ios::binary | std::ios::trunc);
  if (!file.is_open()) {
    return Status::IOError("Cannot open log file for write");
  }
  
  for (const auto& entry : entries) {
    uint32_t data_len = static_cast<uint32_t>(entry.data.size());
    
    file.write(reinterpret_cast<const char*>(&entry.term), sizeof(entry.term));
    file.write(reinterpret_cast<const char*>(&entry.index), sizeof(entry.index));
    file.write(reinterpret_cast<const char*>(&data_len), sizeof(data_len));
    
    if (data_len > 0) {
      file.write(entry.data.data(), data_len);
    }
  }
  
  file.flush();
  
  if (!entries.empty()) {
    last_log_index_ = entries.back().index;
    last_log_term_ = entries.back().term;
  } else {
    last_log_index_ = 0;
    last_log_term_ = 0;
  }
  
  return Status::OK();
}

StatusOr<LogEntry> FileRaftStorage::GetLogEntry(LogIndex index) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (!std::filesystem::exists(log_file_)) {
    return Status::NotFound("Log file not found");
  }
  
  std::ifstream file(log_file_, std::ios::binary);
  if (!file.is_open()) {
    return Status::IOError("Cannot open log file");
  }
  
  while (file.good()) {
    LogEntry entry;
    uint32_t data_len;
    
    file.read(reinterpret_cast<char*>(&entry.term), sizeof(entry.term));
    if (!file.good()) break;
    
    file.read(reinterpret_cast<char*>(&entry.index), sizeof(entry.index));
    file.read(reinterpret_cast<char*>(&data_len), sizeof(data_len));
    
    if (data_len > 0) {
      entry.data.resize(data_len);
      file.read(entry.data.data(), data_len);
    }
    
    if (entry.index == index) {
      return entry;
    }
    
    if (entry.index > index) {
      break;
    }
  }
  
  return Status::NotFound("Entry not found");
}

StatusOr<std::vector<LogEntry>> FileRaftStorage::GetLogEntries(LogIndex start, 
                                                                 LogIndex end) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::vector<LogEntry> result;
  
  if (!std::filesystem::exists(log_file_)) {
    return result;
  }
  
  std::ifstream file(log_file_, std::ios::binary);
  if (!file.is_open()) {
    return Status::IOError("Cannot open log file");
  }
  
  while (file.good()) {
    LogEntry entry;
    uint32_t data_len;
    
    file.read(reinterpret_cast<char*>(&entry.term), sizeof(entry.term));
    if (!file.good()) break;
    
    file.read(reinterpret_cast<char*>(&entry.index), sizeof(entry.index));
    file.read(reinterpret_cast<char*>(&data_len), sizeof(data_len));
    
    if (data_len > 0) {
      entry.data.resize(data_len);
      file.read(entry.data.data(), data_len);
    }
    
    if (entry.index >= start && entry.index <= end) {
      result.push_back(entry);
    }
    
    if (entry.index > end) {
      break;
    }
  }
  
  return result;
}

LogIndex FileRaftStorage::GetLastLogIndex() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return last_log_index_;
}

Term FileRaftStorage::GetLastLogTerm() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return last_log_term_;
}

Status FileRaftStorage::SaveState(Term current_term, NodeId voted_for) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::string tmp_file = state_file_ + ".tmp";
  
  std::ofstream file(tmp_file, std::ios::binary);
  if (!file.is_open()) {
    return Status::IOError("Cannot open state file for write");
  }
  
  file.write(reinterpret_cast<const char*>(&current_term), sizeof(current_term));
  file.write(reinterpret_cast<const char*>(&voted_for), sizeof(voted_for));
  file.flush();
  
  if (!file.good()) {
    return Status::IOError("Failed to write state");
  }
  
  file.close();
  
  // Atomic rename
  std::filesystem::rename(tmp_file, state_file_);
  
  return Status::OK();
}

Status FileRaftStorage::LoadState(Term* current_term, NodeId* voted_for) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (!std::filesystem::exists(state_file_)) {
    *current_term = 0;
    *voted_for = 0;
    return Status::OK();
  }
  
  std::ifstream file(state_file_, std::ios::binary);
  if (!file.is_open()) {
    return Status::IOError("Cannot open state file for read");
  }
  
  file.read(reinterpret_cast<char*>(current_term), sizeof(*current_term));
  file.read(reinterpret_cast<char*>(voted_for), sizeof(*voted_for));
  
  if (!file.good()) {
    *current_term = 0;
    *voted_for = 0;
  }
  
  return Status::OK();
}

Status FileRaftStorage::SaveSnapshot(const SnapshotMetadata& snapshot) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::string tmp_file = snapshot_file_ + ".tmp";
  
  std::ofstream file(tmp_file, std::ios::binary);
  if (!file.is_open()) {
    return Status::IOError("Cannot open snapshot file for write");
  }
  
  uint32_t data_len = static_cast<uint32_t>(snapshot.data.size());
  
  file.write(reinterpret_cast<const char*>(&snapshot.last_included_term), 
             sizeof(snapshot.last_included_term));
  file.write(reinterpret_cast<const char*>(&snapshot.last_included_index),
             sizeof(snapshot.last_included_index));
  file.write(reinterpret_cast<const char*>(&data_len), sizeof(data_len));
  
  if (data_len > 0) {
    file.write(snapshot.data.data(), data_len);
  }
  
  file.flush();
  
  if (!file.good()) {
    return Status::IOError("Failed to write snapshot");
  }
  
  file.close();
  
  // Atomic rename
  std::filesystem::rename(tmp_file, snapshot_file_);
  
  return Status::OK();
}

StatusOr<SnapshotMetadata> FileRaftStorage::LoadSnapshot() {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (!std::filesystem::exists(snapshot_file_)) {
    return Status::NotFound("Snapshot not found");
  }
  
  std::ifstream file(snapshot_file_, std::ios::binary);
  if (!file.is_open()) {
    return Status::IOError("Cannot open snapshot file for read");
  }
  
  SnapshotMetadata snapshot;
  uint32_t data_len;
  
  file.read(reinterpret_cast<char*>(&snapshot.last_included_term),
            sizeof(snapshot.last_included_term));
  file.read(reinterpret_cast<char*>(&snapshot.last_included_index),
            sizeof(snapshot.last_included_index));
  file.read(reinterpret_cast<char*>(&data_len), sizeof(data_len));
  
  if (data_len > 0) {
    snapshot.data.resize(data_len);
    file.read(snapshot.data.data(), data_len);
  }
  
  if (!file.good()) {
    return Status::IOError("Failed to read snapshot");
  }
  
  return snapshot;
}

}  // namespace raft
}  // namespace dtx
}  // namespace cedar
