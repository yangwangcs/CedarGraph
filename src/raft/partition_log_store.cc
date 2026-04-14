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

#include "raft/partition_log_store.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <cstring>
#include <filesystem>

namespace cedar {
namespace raft {

namespace fs = std::filesystem;

// File format constants
constexpr const char* kLogFileName = "wal.log";
constexpr const char* kMetaFileName = "metadata";
constexpr size_t kMaxEntrySize = 64 * 1024 * 1024;  // 64MB max entry size

PartitionLogStore::PartitionLogStore(uint32_t partition_id,
                                     const std::string& data_dir)
    : partition_id_(partition_id),
      data_dir_(data_dir),
      log_fd_(-1) {}

PartitionLogStore::~PartitionLogStore() { Close().IgnoreError(); }

Status PartitionLogStore::Initialize() {
  std::lock_guard<std::mutex> lock(mutex_);

  // Create partition directory if not exists
  std::string partition_dir = data_dir_ + "/partition_" + std::to_string(partition_id_);
  if (!fs::exists(partition_dir)) {
    try {
      fs::create_directories(partition_dir);
    } catch (const std::exception& e) {
      return Status::IOError("Failed to create partition directory: " + std::string(e.what()));
    }
  }

  log_file_path_ = partition_dir + "/" + kLogFileName;
  meta_file_path_ = partition_dir + "/" + kMetaFileName;

  // Open log file (create if not exists, append mode)
  log_fd_ = open(log_file_path_.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
  if (log_fd_ < 0) {
    return Status::IOError("Failed to open log file: " + log_file_path_ +
                           ", error: " + std::strerror(errno));
  }

  // Load existing entries from disk
  struct stat st;
  if (fstat(log_fd_, &st) == 0 && st.st_size > 0) {
    // Read all existing entries
    off_t offset = 0;
    while (offset < st.st_size) {
      // Read entry size (4 bytes)
      uint32_t entry_size = 0;
      ssize_t bytes_read = pread(log_fd_, &entry_size, sizeof(entry_size), offset);
      if (bytes_read != sizeof(entry_size)) {
        return Status::Corruption("Failed to read entry size at offset " +
                                  std::to_string(offset));
      }

      if (entry_size == 0 || entry_size > kMaxEntrySize) {
        return Status::Corruption("Invalid entry size: " + std::to_string(entry_size));
      }

      // Read entry data
      std::string entry_data;
      entry_data.resize(entry_size);
      bytes_read = pread(log_fd_, &entry_data[0], entry_size, offset + sizeof(entry_size));
      if (bytes_read != static_cast<ssize_t>(entry_size)) {
        return Status::Corruption("Failed to read entry data at offset " +
                                  std::to_string(offset));
      }

      // Parse LogEntry
      LogEntry entry;
      if (!entry.ParseFromString(entry_data)) {
        return Status::Corruption("Failed to parse entry at offset " +
                                  std::to_string(offset));
      }

      entries_.push_back(std::move(entry));
      offset += sizeof(entry_size) + entry_size;
    }
  }

  return Status::OK();
}

Status PartitionLogStore::Close() {
  std::lock_guard<std::mutex> lock(mutex_);

  Status status = Status::OK();

  if (log_fd_ >= 0) {
    // Sync any pending data
    if (fsync(log_fd_) != 0) {
      status = Status::IOError("Failed to sync log file: " + std::string(strerror(errno)));
    }

    if (::close(log_fd_) != 0 && status.ok()) {
      status = Status::IOError("Failed to close log file: " + std::string(strerror(errno)));
    }
    log_fd_ = -1;
  }

  entries_.clear();
  committed_index_ = 0;

  return status;
}

Status PartitionLogStore::AppendEntry(const LogEntry& entry) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (log_fd_ < 0) {
    return Status::IOError("Log store not initialized");
  }

  // Serialize entry
  std::string entry_data;
  if (!entry.SerializeToString(&entry_data)) {
    return Status::Corruption("Failed to serialize log entry");
  }

  if (entry_data.size() > kMaxEntrySize) {
    return Status::InvalidArgument("Entry size exceeds maximum: " +
                                   std::to_string(entry_data.size()));
  }

  // Write entry size (4 bytes) followed by entry data
  uint32_t entry_size = static_cast<uint32_t>(entry_data.size());

  struct iovec iov[2];
  iov[0].iov_base = &entry_size;
  iov[0].iov_len = sizeof(entry_size);
  iov[1].iov_base = &entry_data[0];
  iov[1].iov_len = entry_data.size();

  ssize_t bytes_written = writev(log_fd_, iov, 2);
  if (bytes_written != static_cast<ssize_t>(sizeof(entry_size) + entry_data.size())) {
    return Status::IOError("Failed to write entry: " + std::string(strerror(errno)));
  }

  // Add to in-memory cache
  entries_.push_back(entry);

  return Status::OK();
}

Status PartitionLogStore::AppendEntries(const std::vector<LogEntry>& entries) {
  if (entries.empty()) {
    return Status::OK();
  }

  std::lock_guard<std::mutex> lock(mutex_);

  if (log_fd_ < 0) {
    return Status::IOError("Log store not initialized");
  }

  // Reserve space for entries in cache
  entries_.reserve(entries_.size() + entries.size());

  for (const auto& entry : entries) {
    // Serialize entry
    std::string entry_data;
    if (!entry.SerializeToString(&entry_data)) {
      return Status::Corruption("Failed to serialize log entry");
    }

    if (entry_data.size() > kMaxEntrySize) {
      return Status::InvalidArgument("Entry size exceeds maximum: " +
                                     std::to_string(entry_data.size()));
    }

    // Write entry size (4 bytes) followed by entry data
    uint32_t entry_size = static_cast<uint32_t>(entry_data.size());

    struct iovec iov[2];
    iov[0].iov_base = &entry_size;
    iov[0].iov_len = sizeof(entry_size);
    iov[1].iov_base = &entry_data[0];
    iov[1].iov_len = entry_data.size();

    ssize_t bytes_written = writev(log_fd_, iov, 2);
    if (bytes_written != static_cast<ssize_t>(sizeof(entry_size) + entry_data.size())) {
      return Status::IOError("Failed to write entry: " + std::string(strerror(errno)));
    }

    // Add to in-memory cache
    entries_.push_back(entry);
  }

  return Status::OK();
}

StatusOr<LogEntry> PartitionLogStore::GetEntry(uint64_t index) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (entries_.empty()) {
    return Status::NotFound("No entries in log");
  }

  uint64_t first_index = entries_.front().index();
  if (index < first_index || index > entries_.back().index()) {
    return Status::NotFound("Entry not found at index " + std::to_string(index));
  }

  // Calculate position in entries vector
  size_t pos = static_cast<size_t>(index - first_index);
  if (pos >= entries_.size()) {
    return Status::NotFound("Entry not found at index " + std::to_string(index));
  }

  return entries_[pos];
}

std::vector<LogEntry> PartitionLogStore::GetEntries(uint64_t start_index,
                                                    uint64_t end_index) {
  std::lock_guard<std::mutex> lock(mutex_);

  std::vector<LogEntry> result;

  if (entries_.empty() || start_index > end_index) {
    return result;
  }

  uint64_t first_index = entries_.front().index();
  uint64_t last_index = entries_.back().index();

  // Clamp to valid range
  if (start_index < first_index) {
    start_index = first_index;
  }
  if (end_index > last_index) {
    end_index = last_index;
  }

  size_t start_pos = static_cast<size_t>(start_index - first_index);
  size_t end_pos = static_cast<size_t>(end_index - first_index);

  if (start_pos < entries_.size() && end_pos < entries_.size()) {
    result.reserve(end_pos - start_pos + 1);
    for (size_t i = start_pos; i <= end_pos; ++i) {
      result.push_back(entries_[i]);
    }
  }

  return result;
}

Status PartitionLogStore::TruncateFrom(uint64_t from_index) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (entries_.empty() || from_index > entries_.back().index()) {
    return Status::OK();  // Nothing to truncate
  }

  uint64_t first_index = entries_.front().index();
  if (from_index < first_index) {
    return Status::InvalidArgument("Cannot truncate from index before first entry");
  }

  size_t keep_count = static_cast<size_t>(from_index - first_index);

  // Truncate in-memory entries
  entries_.resize(keep_count);

  // Truncate log file
  if (log_fd_ >= 0) {
    // Calculate new file size by re-reading entries
    off_t new_size = 0;
    for (size_t i = 0; i < keep_count; ++i) {
      std::string entry_data;
      if (!entries_[i].SerializeToString(&entry_data)) {
        return Status::Corruption("Failed to serialize entry during truncate");
      }
      new_size += sizeof(uint32_t) + entry_data.size();
    }

    if (ftruncate(log_fd_, new_size) != 0) {
      return Status::IOError("Failed to truncate log file: " + std::string(strerror(errno)));
    }

    // Sync to ensure truncation is persisted
    if (fsync(log_fd_) != 0) {
      return Status::IOError("Failed to sync after truncate: " + std::string(strerror(errno)));
    }

    // Update file offset after truncation
    lseek(log_fd_, 0, SEEK_END);
  }

  return Status::OK();
}

uint64_t PartitionLogStore::GetLastLogIndex() const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (entries_.empty()) {
    return 0;
  }
  return entries_.back().index();
}

uint64_t PartitionLogStore::GetLastLogTerm() const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (entries_.empty()) {
    return 0;
  }
  return entries_.back().term();
}

Status PartitionLogStore::SaveMetadata(uint64_t current_term,
                                       const std::string& voted_for) {
  std::lock_guard<std::mutex> lock(mutex_);

  // Write to temp file first for atomicity
  std::string temp_path = meta_file_path_ + ".tmp";
  int fd = open(temp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) {
    return Status::IOError("Failed to create temp metadata file: " +
                           std::string(strerror(errno)));
  }

  // Format: current_term (8 bytes) | voted_for length (4 bytes) | voted_for data
  uint64_t term_be = current_term;  // Already native endian, could use htobe64 if needed
  uint32_t voted_for_len = static_cast<uint32_t>(voted_for.size());

  struct iovec iov[3];
  iov[0].iov_base = &term_be;
  iov[0].iov_len = sizeof(term_be);
  iov[1].iov_base = &voted_for_len;
  iov[1].iov_len = sizeof(voted_for_len);
  iov[2].iov_base = const_cast<char*>(voted_for.data());
  iov[2].iov_len = voted_for.size();

  ssize_t bytes_written = writev(fd, iov, 3);
  if (bytes_written != static_cast<ssize_t>(sizeof(term_be) + sizeof(voted_for_len) +
                                             voted_for.size())) {
    close(fd);
    unlink(temp_path.c_str());
    return Status::IOError("Failed to write metadata");
  }

  if (fsync(fd) != 0) {
    close(fd);
    unlink(temp_path.c_str());
    return Status::IOError("Failed to sync metadata file");
  }

  close(fd);

  // Atomic rename
  if (rename(temp_path.c_str(), meta_file_path_.c_str()) != 0) {
    unlink(temp_path.c_str());
    return Status::IOError("Failed to rename metadata file: " + std::string(strerror(errno)));
  }

  return Status::OK();
}

Status PartitionLogStore::LoadMetadata(uint64_t* current_term,
                                       std::string* voted_for) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (!fs::exists(meta_file_path_)) {
    // No metadata file yet, return defaults
    *current_term = 0;
    *voted_for = "";
    return Status::OK();
  }

  int fd = open(meta_file_path_.c_str(), O_RDONLY);
  if (fd < 0) {
    return Status::IOError("Failed to open metadata file: " + std::string(strerror(errno)));
  }

  // Read current_term (8 bytes)
  uint64_t term_be = 0;
  ssize_t bytes_read = read(fd, &term_be, sizeof(term_be));
  if (bytes_read != sizeof(term_be)) {
    close(fd);
    return Status::Corruption("Failed to read term from metadata");
  }
  *current_term = term_be;

  // Read voted_for length (4 bytes)
  uint32_t voted_for_len = 0;
  bytes_read = read(fd, &voted_for_len, sizeof(voted_for_len));
  if (bytes_read != sizeof(voted_for_len)) {
    close(fd);
    return Status::Corruption("Failed to read voted_for length from metadata");
  }

  // Read voted_for data
  if (voted_for_len > 0) {
    voted_for->resize(voted_for_len);
    bytes_read = read(fd, &(*voted_for)[0], voted_for_len);
    if (bytes_read != static_cast<ssize_t>(voted_for_len)) {
      close(fd);
      return Status::Corruption("Failed to read voted_for from metadata");
    }
  } else {
    *voted_for = "";
  }

  close(fd);
  return Status::OK();
}

Status PartitionLogStore::FlushToDisk() {
  if (log_fd_ < 0) {
    return Status::IOError("Log store not initialized");
  }

  if (fsync(log_fd_) != 0) {
    return Status::IOError("Failed to sync log file: " + std::string(strerror(errno)));
  }

  return Status::OK();
}

}  // namespace raft
}  // namespace cedar
