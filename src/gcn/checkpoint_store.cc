// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#include "cedar/gcn/checkpoint_store.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <algorithm>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <sstream>
#include <utility>
#include <vector>

#include "cedar/core/crc32c.h"

namespace cedar::gcn {
namespace {

constexpr char kMagic[8] = {'C', 'G', 'C', 'N', 'C', 'P', 'K', '1'};
constexpr uint32_t kFormatVersion = 1;
constexpr uint32_t kMaxSnapshotIdBytes = 1 << 20;

void AppendFixed32(std::vector<char>* out, uint32_t value) {
  for (int shift = 0; shift < 32; shift += 8) {
    out->push_back(static_cast<char>((value >> shift) & 0xff));
  }
}

void AppendFixed64(std::vector<char>* out, uint64_t value) {
  for (int shift = 0; shift < 64; shift += 8) {
    out->push_back(static_cast<char>((value >> shift) & 0xff));
  }
}

bool ReadFixed32(const std::vector<char>& input, size_t* pos, uint32_t* value) {
  if (*pos + sizeof(uint32_t) > input.size()) {
    return false;
  }
  uint32_t decoded = 0;
  for (int shift = 0; shift < 32; shift += 8) {
    decoded |= static_cast<uint32_t>(
                   static_cast<unsigned char>(input[(*pos)++]))
               << shift;
  }
  *value = decoded;
  return true;
}

bool ReadFixed64(const std::vector<char>& input, size_t* pos, uint64_t* value) {
  if (*pos + sizeof(uint64_t) > input.size()) {
    return false;
  }
  uint64_t decoded = 0;
  for (int shift = 0; shift < 64; shift += 8) {
    decoded |= static_cast<uint64_t>(
                   static_cast<unsigned char>(input[(*pos)++]))
               << shift;
  }
  *value = decoded;
  return true;
}

Status IOErrorFromErrno(const std::string& message, int err = errno) {
  return Status::IOError(message, std::strerror(err));
}

StatusOr<int> OpenDirectoryNoFollow(const std::string& path) {
  int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
  if (fd < 0) {
    return IOErrorFromErrno("open checkpoint directory without symlinks failed: " +
                            path);
  }
  return fd;
}

Status CloseFd(int fd, const std::string& description) {
  if (::close(fd) != 0) {
    return IOErrorFromErrno("close failed for " + description);
  }
  return Status::OK();
}

Status FsyncFd(int fd, const std::string& description) {
  if (::fsync(fd) != 0) {
    return IOErrorFromErrno("fsync failed for " + description);
  }
  return Status::OK();
}

Status WriteAll(int fd, const std::vector<char>& data,
                const std::string& description) {
  size_t written = 0;
  while (written < data.size()) {
    ssize_t rc = ::write(fd, data.data() + written, data.size() - written);
    if (rc < 0) {
      if (errno == EINTR) {
        continue;
      }
      return IOErrorFromErrno("write failed for " + description);
    }
    if (rc == 0) {
      return Status::IOError("short write for " + description);
    }
    written += static_cast<size_t>(rc);
  }
  return Status::OK();
}

StatusOr<std::vector<char>> ReadAll(int fd, const std::string& description) {
  std::vector<char> data;
  char buffer[8192];
  while (true) {
    ssize_t rc = ::read(fd, buffer, sizeof(buffer));
    if (rc < 0) {
      if (errno == EINTR) {
        continue;
      }
      return IOErrorFromErrno("read failed for " + description);
    }
    if (rc == 0) {
      return data;
    }
    data.insert(data.end(), buffer, buffer + rc);
  }
}

std::string CheckpointFileName(uint32_t partition_id) {
  std::ostringstream name;
  name << "partition_" << std::setw(10) << std::setfill('0')
       << partition_id << ".chkpt";
  return name.str();
}

std::string TempFileName(uint32_t partition_id) {
  return CheckpointFileName(partition_id) + ".tmp";
}

std::vector<char> EncodeCheckpoint(const PartitionCheckpoint& checkpoint) {
  std::vector<char> encoded;
  encoded.insert(encoded.end(), std::begin(kMagic), std::end(kMagic));
  AppendFixed32(&encoded, kFormatVersion);
  AppendFixed32(&encoded, checkpoint.partition_id);
  AppendFixed64(&encoded, checkpoint.partition_epoch);
  AppendFixed64(&encoded, checkpoint.applied_offset);
  AppendFixed64(&encoded, checkpoint.applied_version);
  AppendFixed32(&encoded,
                static_cast<uint32_t>(checkpoint.tmv_snapshot_id.size()));
  encoded.insert(encoded.end(), checkpoint.tmv_snapshot_id.begin(),
                 checkpoint.tmv_snapshot_id.end());
  const uint32_t crc =
      crc32c::Value(encoded.data(), encoded.size());
  AppendFixed32(&encoded, crc);
  return encoded;
}

StatusOr<PartitionCheckpoint> DecodeCheckpoint(const std::vector<char>& bytes,
                                               uint32_t expected_partition_id) {
  constexpr size_t kMinimumSize =
      sizeof(kMagic) + sizeof(uint32_t) + sizeof(uint32_t) +
      sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t) +
      sizeof(uint32_t) + sizeof(uint32_t);
  if (bytes.size() < kMinimumSize) {
    return Status::Corruption("GCN checkpoint is truncated");
  }
  if (!std::equal(std::begin(kMagic), std::end(kMagic), bytes.begin())) {
    return Status::Corruption("GCN checkpoint magic mismatch");
  }

  size_t crc_pos = bytes.size() - sizeof(uint32_t);
  size_t read_crc_pos = crc_pos;
  uint32_t stored_crc = 0;
  if (!ReadFixed32(bytes, &read_crc_pos, &stored_crc) ||
      read_crc_pos != bytes.size()) {
    return Status::Corruption("GCN checkpoint CRC is malformed");
  }
  const uint32_t computed_crc = crc32c::Value(bytes.data(), crc_pos);
  if (stored_crc != computed_crc) {
    return Status::Corruption("GCN checkpoint checksum mismatch");
  }

  size_t pos = sizeof(kMagic);
  uint32_t version = 0;
  uint32_t partition_id = 0;
  uint64_t partition_epoch = 0;
  uint64_t applied_offset = 0;
  uint64_t applied_version = 0;
  uint32_t snapshot_len = 0;
  if (!ReadFixed32(bytes, &pos, &version) ||
      !ReadFixed32(bytes, &pos, &partition_id) ||
      !ReadFixed64(bytes, &pos, &partition_epoch) ||
      !ReadFixed64(bytes, &pos, &applied_offset) ||
      !ReadFixed64(bytes, &pos, &applied_version) ||
      !ReadFixed32(bytes, &pos, &snapshot_len)) {
    return Status::Corruption("GCN checkpoint fixed fields are truncated");
  }
  if (version != kFormatVersion) {
    return Status::Corruption("GCN checkpoint format version is unknown");
  }
  if (partition_id != expected_partition_id) {
    return Status::Corruption("GCN checkpoint partition mismatch");
  }
  if (snapshot_len > kMaxSnapshotIdBytes ||
      pos + snapshot_len != crc_pos) {
    return Status::Corruption("GCN checkpoint snapshot id is malformed");
  }

  PartitionCheckpoint checkpoint;
  checkpoint.partition_id = partition_id;
  checkpoint.partition_epoch = partition_epoch;
  checkpoint.applied_offset = applied_offset;
  checkpoint.applied_version = applied_version;
  checkpoint.tmv_snapshot_id.assign(bytes.data() + pos, snapshot_len);
  return checkpoint;
}

}  // namespace

CheckpointStore::CheckpointStore(std::string directory)
    : directory_(std::move(directory)) {}

StatusOr<std::optional<PartitionCheckpoint>> CheckpointStore::Load(
    uint32_t partition_id) const {
  auto dir = OpenDirectoryNoFollow(directory_);
  if (!dir.ok()) {
    if (errno == ENOENT) {
      return std::optional<PartitionCheckpoint>();
    }
    return dir.status();
  }
  const int dir_fd = dir.ValueOrDie();
  const std::string filename = CheckpointFileName(partition_id);
  int file_fd = ::openat(dir_fd, filename.c_str(), O_RDONLY | O_NOFOLLOW);
  if (file_fd < 0) {
    const int saved_errno = errno;
    Status close_status = CloseFd(dir_fd, directory_);
    if (saved_errno == ENOENT) {
      if (!close_status.ok()) return close_status;
      return std::optional<PartitionCheckpoint>();
    }
    return IOErrorFromErrno("open checkpoint without symlinks failed: " +
                            filename, saved_errno);
  }

  auto bytes = ReadAll(file_fd, filename);
  Status file_close = CloseFd(file_fd, filename);
  Status dir_close = CloseFd(dir_fd, directory_);
  if (!bytes.ok()) {
    if (!file_close.ok()) return file_close;
    if (!dir_close.ok()) return dir_close;
    return bytes.status();
  }
  if (!file_close.ok()) return file_close;
  if (!dir_close.ok()) return dir_close;

  auto decoded = DecodeCheckpoint(bytes.ValueOrDie(), partition_id);
  if (!decoded.ok()) {
    return decoded.status();
  }
  return std::optional<PartitionCheckpoint>(decoded.ValueOrDie());
}

Status CheckpointStore::Save(const PartitionCheckpoint& checkpoint) {
  if (checkpoint.tmv_snapshot_id.size() > kMaxSnapshotIdBytes) {
    return Status::InvalidArgument("GCN checkpoint snapshot id is too large");
  }
  std::error_code ec;
  std::filesystem::create_directories(directory_, ec);
  if (ec) {
    return Status::IOError("failed to create checkpoint directory: " +
                           directory_, ec.message());
  }

  auto dir = OpenDirectoryNoFollow(directory_);
  if (!dir.ok()) {
    return dir.status();
  }
  const int dir_fd = dir.ValueOrDie();
  const std::string filename = CheckpointFileName(checkpoint.partition_id);
  const std::string tmp_filename = TempFileName(checkpoint.partition_id);
  const auto encoded = EncodeCheckpoint(checkpoint);

  int file_fd = ::openat(dir_fd, tmp_filename.c_str(),
                         O_WRONLY | O_CREAT | O_TRUNC | O_NOFOLLOW, 0600);
  if (file_fd < 0) {
    const int saved_errno = errno;
    (void)CloseFd(dir_fd, directory_);
    return IOErrorFromErrno("open temporary checkpoint without symlinks failed: " +
                            tmp_filename, saved_errno);
  }
  Status status = WriteAll(file_fd, encoded, tmp_filename);
  if (status.ok()) {
    status = FsyncFd(file_fd, tmp_filename);
  }
  Status file_close = CloseFd(file_fd, tmp_filename);
  if (!status.ok()) {
    (void)::unlinkat(dir_fd, tmp_filename.c_str(), 0);
    (void)CloseFd(dir_fd, directory_);
    return status;
  }
  if (!file_close.ok()) {
    (void)::unlinkat(dir_fd, tmp_filename.c_str(), 0);
    (void)CloseFd(dir_fd, directory_);
    return file_close;
  }
  if (::renameat(dir_fd, tmp_filename.c_str(), dir_fd, filename.c_str()) != 0) {
    const int saved_errno = errno;
    (void)::unlinkat(dir_fd, tmp_filename.c_str(), 0);
    (void)CloseFd(dir_fd, directory_);
    return IOErrorFromErrno("failed to publish checkpoint: " + filename,
                            saved_errno);
  }
  status = FsyncFd(dir_fd, directory_);
  Status dir_close = CloseFd(dir_fd, directory_);
  if (!status.ok()) return status;
  return dir_close;
}

Status CheckpointStore::Remove(uint32_t partition_id) {
  auto dir = OpenDirectoryNoFollow(directory_);
  if (!dir.ok()) {
    if (errno == ENOENT) {
      return Status::OK();
    }
    return dir.status();
  }
  const int dir_fd = dir.ValueOrDie();
  const std::string filename = CheckpointFileName(partition_id);
  if (::unlinkat(dir_fd, filename.c_str(), 0) != 0) {
    const int saved_errno = errno;
    if (saved_errno != ENOENT) {
      (void)CloseFd(dir_fd, directory_);
      return IOErrorFromErrno("failed to remove checkpoint: " + filename,
                              saved_errno);
    }
  }
  Status status = FsyncFd(dir_fd, directory_);
  Status close_status = CloseFd(dir_fd, directory_);
  if (!status.ok()) return status;
  return close_status;
}

std::string CheckpointStore::CheckpointPath(uint32_t partition_id) const {
  return (std::filesystem::path(directory_) /
          CheckpointFileName(partition_id)).string();
}

std::string CheckpointStore::TempPath(uint32_t partition_id) const {
  return (std::filesystem::path(directory_) /
          TempFileName(partition_id)).string();
}

}  // namespace cedar::gcn
