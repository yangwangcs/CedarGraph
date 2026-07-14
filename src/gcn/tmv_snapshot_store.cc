// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#include "cedar/gcn/tmv_snapshot_store.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <algorithm>
#include <cstring>
#include <filesystem>
#include <sstream>
#include <utility>
#include <vector>

#include "cedar/core/crc32c.h"

namespace cedar::gcn {
namespace {

constexpr char kMagic[8] = {'C', 'G', 'T', 'M', 'V', 'S', 'N', '1'};
constexpr uint32_t kFormatVersion = 1;
constexpr uint32_t kSnapshotBlockEdges = 64;
constexpr uint64_t kMaxSnapshotEdges = 1ULL << 30;

Status IOErrorFromErrno(const std::string& message, int err = errno) {
  return Status::IOError(message, std::strerror(err));
}

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
  if (*pos + sizeof(uint32_t) > input.size()) return false;
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
  if (*pos + sizeof(uint64_t) > input.size()) return false;
  uint64_t decoded = 0;
  for (int shift = 0; shift < 64; shift += 8) {
    decoded |= static_cast<uint64_t>(
                   static_cast<unsigned char>(input[(*pos)++]))
               << shift;
  }
  *value = decoded;
  return true;
}

void AppendEdgeAppend(std::vector<char>* out,
                      const TMVEngine::EdgeAppend& append) {
  AppendFixed64(out, append.entity_id);
  AppendFixed32(out, append.dir == Direction::kOut ? 0 : 1);
  AppendFixed64(out, append.edge.target_id);
  AppendFixed32(out, append.edge.valid_from);
  AppendFixed32(out, append.edge.valid_to);
  AppendFixed64(out, append.edge.attr_offset);
  AppendFixed32(out, append.edge.edge_type);
  AppendFixed32(out, append.edge.reserved);
}

bool ReadEdgeAppend(const std::vector<char>& input, size_t* pos,
                    TMVEngine::EdgeAppend* append) {
  uint64_t entity_id = 0;
  uint32_t dir = 0;
  uint64_t target_id = 0;
  uint32_t valid_from = 0;
  uint32_t valid_to = 0;
  uint64_t attr_offset = 0;
  uint32_t edge_type = 0;
  uint32_t reserved = 0;
  if (!ReadFixed64(input, pos, &entity_id) ||
      !ReadFixed32(input, pos, &dir) ||
      !ReadFixed64(input, pos, &target_id) ||
      !ReadFixed32(input, pos, &valid_from) ||
      !ReadFixed32(input, pos, &valid_to) ||
      !ReadFixed64(input, pos, &attr_offset) ||
      !ReadFixed32(input, pos, &edge_type) ||
      !ReadFixed32(input, pos, &reserved)) {
    return false;
  }
  if (dir > 1) {
    return false;
  }
  append->entity_id = entity_id;
  append->dir = dir == 0 ? Direction::kOut : Direction::kIn;
  append->edge = TMVEdge{target_id, valid_from, valid_to, attr_offset,
                         edge_type, reserved};
  append->reverse = false;
  return true;
}

StatusOr<int> OpenDirectoryNoFollow(const std::string& path) {
  int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
  if (fd < 0) {
    return IOErrorFromErrno("open TMV snapshot directory failed: " + path);
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
      if (errno == EINTR) continue;
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
      if (errno == EINTR) continue;
      return IOErrorFromErrno("read failed for " + description);
    }
    if (rc == 0) return data;
    data.insert(data.end(), buffer, buffer + rc);
  }
}

std::string SnapshotFileName(uint32_t partition_id) {
  std::ostringstream name;
  name << "partition_" << partition_id << ".tmv";
  return name.str();
}

StatusOr<std::pair<TmvSnapshotMetadata, std::vector<TMVEngine::EdgeAppend>>>
DecodeSnapshot(const std::vector<char>& bytes, uint32_t expected_partition_id) {
  constexpr size_t kMinimumSize =
      sizeof(kMagic) + sizeof(uint32_t) + sizeof(uint32_t) +
      sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t) +
      sizeof(uint32_t);
  if (bytes.size() < kMinimumSize) {
    return Status::Corruption("TMV snapshot is truncated");
  }
  if (!std::equal(std::begin(kMagic), std::end(kMagic), bytes.begin())) {
    return Status::Corruption("TMV snapshot magic mismatch");
  }

  const size_t crc_pos = bytes.size() - sizeof(uint32_t);
  size_t crc_read_pos = crc_pos;
  uint32_t stored_crc = 0;
  if (!ReadFixed32(bytes, &crc_read_pos, &stored_crc) ||
      crc_read_pos != bytes.size()) {
    return Status::Corruption("TMV snapshot CRC is malformed");
  }
  const uint32_t computed_crc = crc32c::Value(bytes.data(), crc_pos);
  if (stored_crc != computed_crc) {
    return Status::Corruption("TMV snapshot checksum mismatch");
  }

  size_t pos = sizeof(kMagic);
  uint32_t version = 0;
  TmvSnapshotMetadata metadata;
  uint32_t block_edges = 0;
  uint64_t block_count = 0;
  if (!ReadFixed32(bytes, &pos, &version) ||
      !ReadFixed32(bytes, &pos, &metadata.partition_id) ||
      !ReadFixed64(bytes, &pos, &metadata.applied_version) ||
      !ReadFixed64(bytes, &pos, &metadata.applied_offset) ||
      !ReadFixed64(bytes, &pos, &metadata.edge_count) ||
      !ReadFixed32(bytes, &pos, &block_edges) ||
      !ReadFixed64(bytes, &pos, &block_count)) {
    return Status::Corruption("TMV snapshot metadata is truncated");
  }
  if (version != kFormatVersion) {
    return Status::Corruption("TMV snapshot format version is unknown");
  }
  if (metadata.partition_id != expected_partition_id) {
    return Status::Corruption("TMV snapshot partition mismatch");
  }
  if (metadata.edge_count > kMaxSnapshotEdges) {
    return Status::Corruption("TMV snapshot edge count is too large");
  }
  if (block_edges == 0 || block_edges > kSnapshotBlockEdges) {
    return Status::Corruption("TMV snapshot block size is invalid");
  }
  const uint64_t expected_block_count =
      (metadata.edge_count + block_edges - 1) / block_edges;
  if (block_count != expected_block_count) {
    return Status::Corruption("TMV snapshot block count mismatch");
  }

  std::vector<TMVEngine::EdgeAppend> appends;
  appends.reserve(static_cast<size_t>(metadata.edge_count));
  for (uint64_t block = 0; block < block_count; ++block) {
    uint32_t block_edge_count = 0;
    uint32_t stored_block_crc = 0;
    if (!ReadFixed32(bytes, &pos, &block_edge_count) ||
        !ReadFixed32(bytes, &pos, &stored_block_crc)) {
      return Status::Corruption("TMV snapshot block header is truncated");
    }
    if (block_edge_count == 0 || block_edge_count > block_edges ||
        appends.size() + block_edge_count > metadata.edge_count) {
      return Status::Corruption("TMV snapshot block edge count is invalid");
    }
    const size_t block_payload_start = pos;
    for (uint32_t i = 0; i < block_edge_count; ++i) {
      TMVEngine::EdgeAppend append;
      if (!ReadEdgeAppend(bytes, &pos, &append)) {
        return Status::Corruption("TMV snapshot edge payload is truncated");
      }
      if (append.edge.reserved != metadata.partition_id) {
        return Status::Corruption("TMV snapshot edge partition mismatch");
      }
      appends.push_back(append);
    }
    const uint32_t computed_block_crc =
        crc32c::Value(bytes.data() + block_payload_start,
                      pos - block_payload_start);
    if (stored_block_crc != computed_block_crc) {
      return Status::Corruption("TMV snapshot block checksum mismatch");
    }
  }
  if (appends.size() != metadata.edge_count) {
    return Status::Corruption("TMV snapshot edge count mismatch");
  }
  if (pos != crc_pos) {
    return Status::Corruption("TMV snapshot has trailing bytes");
  }
  return std::make_pair(metadata, std::move(appends));
}

}  // namespace

TmvSnapshotStore::TmvSnapshotStore(std::string directory)
    : directory_(std::move(directory)) {}

Status TmvSnapshotStore::SavePartition(const TMVEngine& engine,
                                       uint32_t partition_id,
                                       uint64_t applied_version,
                                       uint64_t applied_offset) const {
  std::error_code ec;
  std::filesystem::create_directories(directory_, ec);
  if (ec) {
    return Status::IOError("failed to create TMV snapshot directory: " +
                           directory_, ec.message());
  }

  auto appends = engine.ExportPartitionEdges(partition_id);
  std::vector<char> encoded;
  encoded.insert(encoded.end(), std::begin(kMagic), std::end(kMagic));
  AppendFixed32(&encoded, kFormatVersion);
  AppendFixed32(&encoded, partition_id);
  AppendFixed64(&encoded, applied_version);
  AppendFixed64(&encoded, applied_offset);
  AppendFixed64(&encoded, static_cast<uint64_t>(appends.size()));
  AppendFixed32(&encoded, kSnapshotBlockEdges);
  const uint64_t block_count =
      (appends.size() + kSnapshotBlockEdges - 1) / kSnapshotBlockEdges;
  AppendFixed64(&encoded, block_count);
  for (size_t block_start = 0; block_start < appends.size();
       block_start += kSnapshotBlockEdges) {
    const size_t block_end =
        std::min(block_start + kSnapshotBlockEdges, appends.size());
    std::vector<char> block_payload;
    for (size_t i = block_start; i < block_end; ++i) {
      AppendEdgeAppend(&block_payload, appends[i]);
    }
    AppendFixed32(&encoded,
                  static_cast<uint32_t>(block_end - block_start));
    AppendFixed32(&encoded,
                  crc32c::Value(block_payload.data(), block_payload.size()));
    encoded.insert(encoded.end(), block_payload.begin(), block_payload.end());
  }
  const uint32_t crc = crc32c::Value(encoded.data(), encoded.size());
  AppendFixed32(&encoded, crc);

  auto dir = OpenDirectoryNoFollow(directory_);
  if (!dir.ok()) return dir.status();
  const int dir_fd = dir.ValueOrDie();
  const std::string filename = SnapshotFileName(partition_id);
  const std::string tmp_filename = filename + ".tmp";
  int file_fd = ::openat(dir_fd, tmp_filename.c_str(),
                         O_WRONLY | O_CREAT | O_TRUNC | O_NOFOLLOW, 0600);
  if (file_fd < 0) {
    const int saved_errno = errno;
    (void)CloseFd(dir_fd, directory_);
    return IOErrorFromErrno("open TMV snapshot temporary failed: " +
                                tmp_filename,
                            saved_errno);
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
    return IOErrorFromErrno("publish TMV snapshot failed: " + filename,
                            saved_errno);
  }
  status = FsyncFd(dir_fd, directory_);
  Status dir_close = CloseFd(dir_fd, directory_);
  if (!status.ok()) return status;
  return dir_close;
}

StatusOr<TmvSnapshotMetadata> TmvSnapshotStore::RestorePartition(
    TMVEngine* engine,
    uint32_t partition_id) const {
  if (!engine) {
    return Status::InvalidArgument("TMVEngine is required for restore");
  }
  auto dir = OpenDirectoryNoFollow(directory_);
  if (!dir.ok()) return dir.status();
  const int dir_fd = dir.ValueOrDie();
  const std::string filename = SnapshotFileName(partition_id);
  int file_fd = ::openat(dir_fd, filename.c_str(), O_RDONLY | O_NOFOLLOW);
  if (file_fd < 0) {
    const int saved_errno = errno;
    (void)CloseFd(dir_fd, directory_);
    return IOErrorFromErrno("open TMV snapshot failed: " + filename,
                            saved_errno);
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

  auto decoded = DecodeSnapshot(bytes.ValueOrDie(), partition_id);
  if (!decoded.ok()) return decoded.status();

  const auto& metadata = decoded.ValueOrDie().first;
  const auto& appends = decoded.ValueOrDie().second;
  CEDAR_RETURN_IF_ERROR(engine->ReplacePartitionEdgesAtomic(partition_id,
                                                            appends));
  return metadata;
}

std::string TmvSnapshotStore::SnapshotPathForTest(uint32_t partition_id) const {
  return SnapshotPath(partition_id);
}

std::string TmvSnapshotStore::SnapshotPath(uint32_t partition_id) const {
  return (std::filesystem::path(directory_) / SnapshotFileName(partition_id))
      .string();
}

std::string TmvSnapshotStore::TempPath(uint32_t partition_id) const {
  return SnapshotPath(partition_id) + ".tmp";
}

}  // namespace cedar::gcn
