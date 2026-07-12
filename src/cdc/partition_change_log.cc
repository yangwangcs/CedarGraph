#include "cedar/cdc/partition_change_log.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <set>
#include <sstream>
#include <string>
#include <system_error>
#include <unordered_map>

#include <fcntl.h>
#include <unistd.h>

#include "cedar/core/crc32c.h"

namespace cedar::cdc {
namespace {

constexpr uint32_t kFrameMagic = 0x43444331;  // CDC1
constexpr uint16_t kFrameVersion = 1;
constexpr size_t kFrameHeaderBytes = 16;
constexpr const char* kManifestName = "MANIFEST";
constexpr const char* kSnapshotName = "SNAPSHOT";

std::string SnapshotLogicalKey(const ChangeRecord& record) {
  std::ostringstream key;
  key << record.partition_id() << ':' << record.entity_id() << ':'
      << record.target_id() << ':' << record.entity_type() << ':'
      << record.edge_type() << ':' << record.column_id();
  return key.str();
}

StatusOr<size_t> SerializedSize(const ChangeRecord& record) {
  std::string payload;
  if (!record.SerializeToString(&payload)) {
    return Status::Corruption("PartitionChangeLog",
                              "failed to size ChangeRecord");
  }
  return payload.size();
}

struct FrameHeader {
  uint32_t magic;
  uint16_t version;
  uint16_t flags;
  uint32_t payload_size;
  uint32_t crc32c;
};

struct ManifestInfo {
  struct Segment {
    std::string name;
    uint64_t first_offset = 0;
    uint64_t last_offset = 0;
    uint64_t bytes = 0;
  };

  bool present = false;
  uint32_t partition_id = 0;
  uint64_t partition_epoch = 0;
  uint64_t earliest_offset = 1;
  uint64_t high_watermark = 0;
  uint64_t committed_version = 0;
  std::vector<Segment> segments;
};

static_assert(sizeof(FrameHeader) == kFrameHeaderBytes,
              "frame header must stay 16 bytes");

Status PosixError(const std::string& context) {
  return Status::IOError(context, std::strerror(errno));
}

Status WriteAll(int fd, const void* data, size_t size) {
  const char* cursor = static_cast<const char*>(data);
  while (size > 0) {
    ssize_t written = ::write(fd, cursor, size);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      return PosixError("write");
    }
    cursor += written;
    size -= static_cast<size_t>(written);
  }
  return Status::OK();
}

Status FsyncPath(const std::filesystem::path& path) {
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    return PosixError("open for fsync: " + path.string());
  }
  if (::fsync(fd) != 0) {
    int saved = errno;
    ::close(fd);
    errno = saved;
    return PosixError("fsync: " + path.string());
  }
  if (::close(fd) != 0) {
    return PosixError("close: " + path.string());
  }
  return Status::OK();
}

Status FsyncDirectory(const std::filesystem::path& path) {
  int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY);
  if (fd < 0) {
    return PosixError("open directory for fsync: " + path.string());
  }
  if (::fsync(fd) != 0) {
    int saved = errno;
    ::close(fd);
    errno = saved;
    return PosixError("fsync directory: " + path.string());
  }
  if (::close(fd) != 0) {
    return PosixError("close directory: " + path.string());
  }
  return Status::OK();
}

std::string SegmentFileName(uint64_t first_offset) {
  std::ostringstream out;
  out << std::setw(20) << std::setfill('0') << first_offset << ".seg";
  return out.str();
}

bool IsSegmentFile(const std::filesystem::path& path) {
  return path.extension() == ".seg";
}

StatusOr<ManifestInfo> ReadManifest(const std::filesystem::path& dir) {
  ManifestInfo info;
  const auto path = dir / kManifestName;
  if (!std::filesystem::exists(path)) {
    return info;
  }
  std::ifstream in(path);
  if (!in.is_open()) {
    return Status::IOError("PartitionChangeLog",
                           "cannot open manifest " + path.string());
  }
  info.present = true;
  bool saw_version = false;
  bool saw_partition_id = false;
  bool saw_partition_epoch = false;
  bool saw_earliest_offset = false;
  bool saw_high_watermark = false;
  bool saw_committed_version = false;
  bool in_segments = false;
  std::string line;
  while (std::getline(in, line)) {
    if (line == "segments") {
      in_segments = true;
      continue;
    }
    if (in_segments) {
      continue;
    }
    std::istringstream parsed(line);
    std::string key;
    parsed >> key;
    if (key == "version") {
      saw_version = true;
      uint64_t version = 0;
      parsed >> version;
      if (version != 1) {
        return Status::Corruption("PartitionChangeLog",
                                  "unsupported manifest version");
      }
    }
    if (key == "partition_id") {
      saw_partition_id = true;
      parsed >> info.partition_id;
    }
    if (key == "partition_epoch") {
      saw_partition_epoch = true;
      parsed >> info.partition_epoch;
    }
    if (key == "earliest_offset") {
      saw_earliest_offset = true;
      parsed >> info.earliest_offset;
    }
    if (key == "high_watermark") {
      saw_high_watermark = true;
      parsed >> info.high_watermark;
    }
    if (key == "committed_version") {
      saw_committed_version = true;
      parsed >> info.committed_version;
    }
  }
  if (!info.present) {
    return info;
  }
  in.clear();
  in.seekg(0);
  in_segments = false;
  while (std::getline(in, line)) {
    if (line == "segments") {
      in_segments = true;
      continue;
    }
    if (!in_segments || line.empty()) {
      continue;
    }
    std::istringstream parsed(line);
    ManifestInfo::Segment segment;
    parsed >> segment.name;
    std::string label;
    parsed >> label >> segment.first_offset;
    if (label != "first_offset") {
      return Status::Corruption("PartitionChangeLog",
                                "invalid manifest segment first_offset");
    }
    parsed >> label >> segment.last_offset;
    if (label != "last_offset") {
      return Status::Corruption("PartitionChangeLog",
                                "invalid manifest segment last_offset");
    }
    parsed >> label >> segment.bytes;
    if (label != "bytes") {
      return Status::Corruption("PartitionChangeLog",
                                "invalid manifest segment bytes");
    }
    if (segment.name.empty()) {
      return Status::Corruption("PartitionChangeLog",
                                "empty manifest segment name");
    }
    info.segments.push_back(segment);
  }
  if (!saw_version || !saw_partition_id || !saw_partition_epoch ||
      !saw_earliest_offset || !saw_high_watermark ||
      !saw_committed_version) {
    return Status::Corruption("PartitionChangeLog",
                              "manifest missing required scalar");
  }
  return info;
}

Status TruncateFile(const std::filesystem::path& path, uint64_t size) {
  if (::truncate(path.c_str(), static_cast<off_t>(size)) != 0) {
    return PosixError("truncate: " + path.string());
  }
  CEDAR_RETURN_IF_ERROR(FsyncPath(path));
  return FsyncDirectory(path.parent_path());
}

Status AtomicWriteFile(const std::filesystem::path& path,
                       const std::string& data) {
  std::filesystem::create_directories(path.parent_path());
  std::filesystem::path tmp = path;
  tmp += ".tmp";
  int fd = ::open(tmp.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) {
    return PosixError("open temp file: " + tmp.string());
  }
  Status s = WriteAll(fd, data.data(), data.size());
  if (s.ok() && ::fsync(fd) != 0) {
    s = PosixError("fsync temp file: " + tmp.string());
  }
  if (::close(fd) != 0 && s.ok()) {
    s = PosixError("close temp file: " + tmp.string());
  }
  if (!s.ok()) {
    std::error_code ignored;
    std::filesystem::remove(tmp, ignored);
    return s;
  }
  if (::rename(tmp.c_str(), path.c_str()) != 0) {
    std::error_code ignored;
    std::filesystem::remove(tmp, ignored);
    return PosixError("rename " + tmp.string() + " to " + path.string());
  }
  return FsyncDirectory(path.parent_path());
}

StatusOr<std::vector<ChangeRecord>> ReadRecordFrameFile(
    const std::filesystem::path& path, uint32_t partition_id,
    uint64_t partition_epoch) {
  std::vector<ChangeRecord> records;
  if (!std::filesystem::exists(path)) {
    return records;
  }
  std::ifstream in(path, std::ios::binary);
  if (!in.is_open()) {
    return Status::IOError("PartitionChangeLog",
                           "cannot open record frame file " + path.string());
  }
  while (true) {
    FrameHeader header{};
    in.read(reinterpret_cast<char*>(&header), sizeof(header));
    if (in.gcount() == 0 && in.eof()) {
      break;
    }
    if (in.gcount() != static_cast<std::streamsize>(sizeof(header)) ||
        header.magic != kFrameMagic || header.version != kFrameVersion) {
      return Status::Corruption("PartitionChangeLog",
                                "invalid record frame file header");
    }
    std::string payload(header.payload_size, '\0');
    in.read(payload.data(), payload.size());
    if (in.gcount() != static_cast<std::streamsize>(payload.size())) {
      return Status::Corruption("PartitionChangeLog",
                                "truncated record frame file");
    }
    if (crc32c::Value(payload.data(), payload.size()) != header.crc32c) {
      return Status::Corruption("PartitionChangeLog",
                                "record frame file checksum mismatch");
    }
    ChangeRecord record;
    if (!record.ParseFromString(payload)) {
      return Status::Corruption("PartitionChangeLog",
                                "invalid record frame payload");
    }
    if (record.partition_id() != partition_id ||
        record.partition_epoch() != partition_epoch) {
      return Status::Corruption("PartitionChangeLog",
                                "snapshot record partition metadata mismatch");
    }
    records.push_back(std::move(record));
  }
  return records;
}

Status ReadSegmentRange(const std::filesystem::path& path, uint64_t* first,
                        uint64_t* last, uint64_t* bytes) {
  *first = 0;
  *last = 0;
  *bytes = 0;
  std::ifstream in(path, std::ios::binary);
  if (!in.is_open()) {
    return Status::IOError("PartitionChangeLog",
                           "cannot open segment " + path.string());
  }
  while (true) {
    FrameHeader header{};
    in.read(reinterpret_cast<char*>(&header), sizeof(header));
    if (in.gcount() == 0 && in.eof()) {
      break;
    }
    if (in.gcount() != static_cast<std::streamsize>(sizeof(header)) ||
        header.magic != kFrameMagic || header.version != kFrameVersion) {
      return Status::Corruption("PartitionChangeLog",
                                "invalid segment for manifest");
    }
    std::string payload(header.payload_size, '\0');
    in.read(payload.data(), payload.size());
    if (in.gcount() != static_cast<std::streamsize>(payload.size())) {
      return Status::Corruption("PartitionChangeLog",
                                "truncated segment for manifest");
    }
    if (crc32c::Value(payload.data(), payload.size()) != header.crc32c) {
      return Status::Corruption("PartitionChangeLog",
                                "segment checksum mismatch for manifest");
    }
    ChangeRecord record;
    if (!record.ParseFromString(payload)) {
      return Status::Corruption("PartitionChangeLog",
                                "invalid segment payload for manifest");
    }
    if (*first == 0) {
      *first = record.offset();
    }
    *last = record.offset();
    *bytes += kFrameHeaderBytes + payload.size();
  }
  return Status::OK();
}

}  // namespace

PartitionChangeLog::PartitionChangeLog(Options options)
    : options_(std::move(options)) {
  state_.partition_epoch = options_.partition_epoch;
}

StatusOr<std::unique_ptr<PartitionChangeLog>> PartitionChangeLog::Open(
    Options options) {
  if (options.directory.empty()) {
    return Status::InvalidArgument("PartitionChangeLog", "directory is empty");
  }
  if (options.max_segment_bytes <= kFrameHeaderBytes) {
    return Status::InvalidArgument("PartitionChangeLog",
                                   "max_segment_bytes too small");
  }
  if (options.max_fetch_records == 0 || options.max_fetch_bytes == 0) {
    return Status::InvalidArgument("PartitionChangeLog",
                                   "fetch limits must be non-zero");
  }
  auto log = std::unique_ptr<PartitionChangeLog>(
      new PartitionChangeLog(std::move(options)));
  CEDAR_RETURN_IF_ERROR(log->Recover());
  return log;
}

Status PartitionChangeLog::AppendCommittedBatch(
    uint64_t commit_version, std::vector<ChangeRecord> records) {
  std::lock_guard<std::mutex> lock(mu_);
  uint64_t next = state_.high_watermark + 1;
  for (size_t i = 0; i < records.size(); ++i) {
    records[i].set_partition_id(options_.partition_id);
    records[i].set_partition_epoch(options_.partition_epoch);
    records[i].set_offset(next + i);
    records[i].set_commit_version(commit_version);
    records[i].set_batch_index(static_cast<uint32_t>(i));
    records[i].set_batch_size(static_cast<uint32_t>(records.size()));
    CEDAR_RETURN_IF_ERROR(AppendRecordFrame(records[i]));
    records_.push_back(records[i]);
    snapshot_records_.push_back(records[i]);
  }
  if (!records.empty()) {
    state_.high_watermark += records.size();
    state_.committed_version = std::max(state_.committed_version,
                                        commit_version);
    if (state_.earliest_offset == 0) {
      state_.earliest_offset = records_.front().offset();
    }
  }
  CEDAR_RETURN_IF_ERROR(PersistSnapshotRecordsLocked());
  return PersistManifestLocked();
}

StatusOr<std::vector<ChangeRecord>> PartitionChangeLog::ReadAfter(
    uint64_t offset, size_t limit_records, size_t limit_bytes) const {
  std::lock_guard<std::mutex> lock(mu_);
  const size_t effective_records =
      std::min(limit_records, options_.max_fetch_records);
  const size_t effective_bytes = std::min(limit_bytes, options_.max_fetch_bytes);
  std::vector<ChangeRecord> out;
  if (effective_records == 0 || effective_bytes == 0) {
    return out;
  }
  size_t bytes = 0;
  for (const auto& record : records_) {
    if (record.offset() <= offset) {
      continue;
    }
    std::string payload;
    if (!record.SerializeToString(&payload)) {
      return Status::Corruption("PartitionChangeLog",
                                "failed to size ChangeRecord");
    }
    if (!out.empty() && bytes + payload.size() > effective_bytes) {
      break;
    }
    if (out.empty() && payload.size() > effective_bytes) {
      break;
    }
    out.push_back(record);
    bytes += payload.size();
    if (out.size() >= effective_records) {
      break;
    }
  }
  return out;
}

StatusOr<std::vector<ChangeRecord>> PartitionChangeLog::SnapshotRecords(
    uint64_t snapshot_version, uint64_t after_offset, size_t limit_records,
    size_t limit_bytes) const {
  std::lock_guard<std::mutex> lock(mu_);
  const size_t effective_records =
      std::min(limit_records, options_.max_fetch_records);
  const size_t effective_bytes = std::min(limit_bytes, options_.max_fetch_bytes);
  std::vector<ChangeRecord> out;
  if (effective_records == 0 || effective_bytes == 0) {
    return out;
  }

  std::unordered_map<std::string, ChangeRecord> latest_by_key;
  for (const auto& record : snapshot_records_) {
    if (record.commit_version() > snapshot_version) {
      continue;
    }
    latest_by_key[SnapshotLogicalKey(record)] = record;
  }

  std::vector<ChangeRecord> folded;
  folded.reserve(latest_by_key.size());
  for (auto& [key, record] : latest_by_key) {
    (void)key;
    if (record.operation() == CHANGE_OPERATION_DELETE) {
      continue;
    }
    folded.push_back(std::move(record));
  }
  std::sort(folded.begin(), folded.end(),
            [](const ChangeRecord& a, const ChangeRecord& b) {
              if (a.entity_id() != b.entity_id()) {
                return a.entity_id() < b.entity_id();
              }
              if (a.target_id() != b.target_id()) {
                return a.target_id() < b.target_id();
              }
              if (a.edge_type() != b.edge_type()) {
                return a.edge_type() < b.edge_type();
              }
              return a.column_id() < b.column_id();
            });

  size_t bytes = 0;
  uint64_t snapshot_offset = 0;
  for (auto record : folded) {
    ++snapshot_offset;
    if (snapshot_offset <= after_offset) {
      continue;
    }
    record.set_offset(snapshot_offset);
    record.set_txn_id(snapshot_offset);
    record.set_batch_index(0);
    record.set_batch_size(1);
    if (record.operation() == CHANGE_OPERATION_UNSPECIFIED) {
      record.set_operation(CHANGE_OPERATION_UPDATE);
    }
    auto record_bytes_or = SerializedSize(record);
    if (!record_bytes_or.ok()) {
      return record_bytes_or.status();
    }
    const size_t record_bytes = record_bytes_or.ValueOrDie();
    if (!out.empty() && bytes + record_bytes > effective_bytes) {
      break;
    }
    if (out.empty() && record_bytes > effective_bytes) {
      return Status::ResourceExhausted(
          "next snapshot record exceeds snapshot byte limit");
    }
    out.push_back(std::move(record));
    bytes += record_bytes;
    if (out.size() >= effective_records) {
      break;
    }
  }
  return out;
}

ChangeLogState PartitionChangeLog::GetState() const {
  std::lock_guard<std::mutex> lock(mu_);
  ChangeLogState state = state_;
  state.active_segment_first_offset = active_segment_first_offset_;
  state.segment_count = manifest_segment_names_.size();
  state.segment_bytes = 0;
  const std::filesystem::path dir(options_.directory);
  const auto now = std::filesystem::file_time_type::clock::now();
  bool saw_closed_segment = false;
  for (size_t i = 0; i < manifest_segment_names_.size(); ++i) {
    const auto& segment_name = manifest_segment_names_[i];
    std::error_code ec;
    const auto path = dir / segment_name;
    const auto bytes = std::filesystem::file_size(path, ec);
    if (!ec) {
      state.segment_bytes += bytes;
    }
    if (i + 1 < manifest_segment_names_.size()) {
      const auto mtime = std::filesystem::last_write_time(path, ec);
      if (!ec) {
        const auto age = std::chrono::duration_cast<std::chrono::hours>(
            now - mtime);
        state.oldest_closed_segment_age_hours = std::max(
            state.oldest_closed_segment_age_hours,
            static_cast<uint64_t>(std::max<int64_t>(0, age.count())));
        const auto bounded_age =
            static_cast<uint64_t>(std::max<int64_t>(0, age.count()));
        if (!saw_closed_segment ||
            bounded_age < state.youngest_closed_segment_age_hours) {
          state.youngest_closed_segment_age_hours = bounded_age;
        }
        saw_closed_segment = true;
      }
    }
  }
  return state;
}

Status PartitionChangeLog::Compact(uint64_t retain_from_offset) {
  std::lock_guard<std::mutex> lock(mu_);
  if (retain_from_offset == 0 || retain_from_offset <= state_.earliest_offset) {
    return Status::OK();
  }
  records_.erase(std::remove_if(records_.begin(), records_.end(),
                                [retain_from_offset](const ChangeRecord& r) {
                                  return r.offset() < retain_from_offset;
                                }),
                 records_.end());
  state_.earliest_offset = records_.empty() ? state_.high_watermark + 1
                                            : records_.front().offset();
  return RewriteSegmentsLocked();
}

Status PartitionChangeLog::Recover() {
  std::lock_guard<std::mutex> lock(mu_);
  std::filesystem::create_directories(options_.directory);
  state_ = ChangeLogState{};
  state_.partition_epoch = options_.partition_epoch;
  state_.earliest_offset = 1;
  records_.clear();
  snapshot_records_.clear();
  active_segment_first_offset_ = 1;
  active_segment_size_ = 0;

  std::vector<std::filesystem::path> segments;
  auto manifest = ReadManifest(options_.directory);
  if (!manifest.ok()) {
    return manifest.status();
  }
  if (manifest.ValueOrDie().present) {
    if (manifest.ValueOrDie().partition_id != options_.partition_id ||
        manifest.ValueOrDie().partition_epoch != options_.partition_epoch) {
      return Status::Corruption("PartitionChangeLog",
                                "manifest partition metadata mismatch");
    }
    for (const auto& segment : manifest.ValueOrDie().segments) {
      const auto path = std::filesystem::path(options_.directory) / segment.name;
      if (!std::filesystem::exists(path)) {
        return Status::Corruption("PartitionChangeLog",
                                  "manifest segment missing");
      }
      segments.push_back(path);
      manifest_segment_names_.push_back(segment.name);
    }
  } else {
    for (const auto& entry :
         std::filesystem::directory_iterator(options_.directory)) {
      if (entry.is_regular_file() && IsSegmentFile(entry.path())) {
        segments.push_back(entry.path());
      }
    }
  }
  std::sort(segments.begin(), segments.end());

  uint64_t expected_offset = 0;
  uint64_t last_segment_first_offset = 1;
  bool truncated_tail = false;
  for (size_t segment_index = 0; segment_index < segments.size();
       ++segment_index) {
    const auto& path = segments[segment_index];
    const bool is_last_segment = segment_index + 1 == segments.size();
    uint64_t segment_first_offset = 0;
    uint64_t segment_last_offset = 0;
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) {
      return Status::IOError("PartitionChangeLog",
                             "cannot open segment " + path.string());
    }
    uint64_t valid_bytes = 0;
    while (true) {
      FrameHeader header{};
      in.read(reinterpret_cast<char*>(&header), sizeof(header));
      const std::streamsize header_read = in.gcount();
      if (header_read == 0 && in.eof()) {
        break;
      }
      if (header_read != static_cast<std::streamsize>(sizeof(header))) {
        if (!is_last_segment ||
            (manifest.ValueOrDie().present &&
             manifest.ValueOrDie().high_watermark > expected_offset)) {
          return Status::Corruption("PartitionChangeLog",
                                    "partial frame before log tail");
        }
        CEDAR_RETURN_IF_ERROR(TruncateFile(path, valid_bytes));
        truncated_tail = true;
        break;
      }
      if (header.magic != kFrameMagic || header.version != kFrameVersion) {
        return Status::Corruption("PartitionChangeLog",
                                  "invalid frame header in " + path.string());
      }
      std::string payload(header.payload_size, '\0');
      in.read(payload.data(), payload.size());
      if (in.gcount() != static_cast<std::streamsize>(payload.size())) {
        if (!is_last_segment ||
            (manifest.ValueOrDie().present &&
             manifest.ValueOrDie().high_watermark > expected_offset)) {
          return Status::Corruption("PartitionChangeLog",
                                    "partial payload before log tail");
        }
        CEDAR_RETURN_IF_ERROR(TruncateFile(path, valid_bytes));
        truncated_tail = true;
        break;
      }
      const uint32_t actual_crc =
          crc32c::Value(payload.data(), payload.size());
      if (actual_crc != header.crc32c) {
        return Status::Corruption("PartitionChangeLog",
                                  "frame checksum mismatch in " +
                                      path.string());
      }
      ChangeRecord record;
      if (!record.ParseFromString(payload)) {
        return Status::Corruption("PartitionChangeLog",
                                  "invalid ChangeRecord payload");
      }
      if (record.partition_id() != options_.partition_id ||
          record.partition_epoch() != options_.partition_epoch) {
        return Status::Corruption("PartitionChangeLog",
                                  "record partition metadata mismatch");
      }
      if (expected_offset != 0 && record.offset() != expected_offset) {
        return Status::Corruption("PartitionChangeLog", "offset gap");
      }
      if (valid_bytes == 0) {
        last_segment_first_offset = record.offset();
        segment_first_offset = record.offset();
      }
      segment_last_offset = record.offset();
      records_.push_back(record);
      snapshot_records_.push_back(record);
      state_.high_watermark = record.offset();
      state_.committed_version =
          std::max<uint64_t>(state_.committed_version, record.commit_version());
      expected_offset = record.offset() + 1;
      valid_bytes += kFrameHeaderBytes + payload.size();
    }
    if (manifest.ValueOrDie().present) {
      const auto& expected_segment = manifest.ValueOrDie().segments[segment_index];
      const bool allow_truncated_last =
          truncated_tail && is_last_segment &&
          manifest.ValueOrDie().high_watermark == expected_offset;
      if (segment_first_offset != expected_segment.first_offset ||
          (!allow_truncated_last && valid_bytes != expected_segment.bytes) ||
          (!allow_truncated_last &&
           segment_last_offset != expected_segment.last_offset)) {
        return Status::Corruption("PartitionChangeLog",
                                  "manifest segment range mismatch");
      }
    }
  }

  if (!records_.empty()) {
    state_.earliest_offset = records_.front().offset();
    active_segment_first_offset_ = last_segment_first_offset;
    active_segment_size_ =
        segments.empty() ? 0 : std::filesystem::file_size(segments.back());
  } else {
    state_.earliest_offset = state_.high_watermark + 1;
    active_segment_first_offset_ = state_.high_watermark + 1;
    active_segment_size_ = 0;
  }
  if (manifest.ValueOrDie().present) {
    const bool allow_truncated_tail =
        truncated_tail && manifest.ValueOrDie().high_watermark == expected_offset;
    if (state_.earliest_offset != manifest.ValueOrDie().earliest_offset ||
        (!allow_truncated_tail &&
         state_.high_watermark != manifest.ValueOrDie().high_watermark) ||
        state_.committed_version != manifest.ValueOrDie().committed_version) {
      return Status::Corruption("PartitionChangeLog",
                                "manifest watermark mismatch");
    }
  }
  auto snapshot_records =
      ReadRecordFrameFile(std::filesystem::path(options_.directory) /
                              kSnapshotName,
                          options_.partition_id, options_.partition_epoch);
  if (!snapshot_records.ok()) {
    return snapshot_records.status();
  }
  if (!snapshot_records.ValueOrDie().empty()) {
    snapshot_records_ = std::move(snapshot_records.ValueOrDie());
  }
  if (!manifest.ValueOrDie().present) {
    manifest_segment_names_.clear();
  }
  return PersistManifestLocked();
}

Status PartitionChangeLog::AppendRecordFrame(const ChangeRecord& record) {
  std::string payload;
  if (!record.SerializeToString(&payload)) {
    return Status::Corruption("PartitionChangeLog",
                              "failed to serialize ChangeRecord");
  }
  const size_t frame_size = kFrameHeaderBytes + payload.size();
  if (active_segment_size_ > 0 &&
      active_segment_size_ + frame_size > options_.max_segment_bytes) {
    active_segment_first_offset_ = record.offset();
    active_segment_size_ = 0;
  }
  if (active_segment_size_ == 0) {
    const std::string segment_name = SegmentFileName(active_segment_first_offset_);
    if (std::find(manifest_segment_names_.begin(), manifest_segment_names_.end(),
                  segment_name) == manifest_segment_names_.end()) {
      manifest_segment_names_.push_back(segment_name);
    }
  }

  const std::filesystem::path dir(options_.directory);
  std::filesystem::create_directories(dir);
  const auto path = dir / SegmentFileName(active_segment_first_offset_);
  int fd = ::open(path.c_str(), O_CREAT | O_APPEND | O_WRONLY, 0644);
  if (fd < 0) {
    return PosixError("open segment: " + path.string());
  }
  FrameHeader header{kFrameMagic, kFrameVersion, 0,
                     static_cast<uint32_t>(payload.size()),
                     crc32c::Value(payload.data(), payload.size())};
  Status s = WriteAll(fd, &header, sizeof(header));
  if (s.ok()) {
    s = WriteAll(fd, payload.data(), payload.size());
  }
  if (s.ok() && ::fsync(fd) != 0) {
    s = PosixError("fsync segment: " + path.string());
  }
  if (::close(fd) != 0 && s.ok()) {
    s = PosixError("close segment: " + path.string());
  }
  if (!s.ok()) {
    return s;
  }
  active_segment_size_ += frame_size;
  return FsyncDirectory(dir);
}

Status PartitionChangeLog::PersistSnapshotRecordsLocked() const {
  const std::filesystem::path dir(options_.directory);
  std::filesystem::create_directories(dir);
  const auto path = dir / kSnapshotName;
  const auto tmp = dir / (std::string(kSnapshotName) + ".tmp");
  int fd = ::open(tmp.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) {
    return PosixError("open snapshot frame file: " + tmp.string());
  }
  Status s = Status::OK();
  for (const auto& record : snapshot_records_) {
    std::string payload;
    if (!record.SerializeToString(&payload)) {
      s = Status::Corruption("PartitionChangeLog",
                             "failed to serialize snapshot record");
      break;
    }
    FrameHeader header{kFrameMagic, kFrameVersion, 0,
                       static_cast<uint32_t>(payload.size()),
                       crc32c::Value(payload.data(), payload.size())};
    s = WriteAll(fd, &header, sizeof(header));
    if (!s.ok()) {
      break;
    }
    s = WriteAll(fd, payload.data(), payload.size());
    if (!s.ok()) {
      break;
    }
  }
  if (s.ok() && ::fsync(fd) != 0) {
    s = PosixError("fsync snapshot frame file: " + tmp.string());
  }
  if (::close(fd) != 0 && s.ok()) {
    s = PosixError("close snapshot frame file: " + tmp.string());
  }
  if (!s.ok()) {
    std::error_code ignored;
    std::filesystem::remove(tmp, ignored);
    return s;
  }
  if (::rename(tmp.c_str(), path.c_str()) != 0) {
    std::error_code ignored;
    std::filesystem::remove(tmp, ignored);
    return PosixError("rename snapshot frame file: " + path.string());
  }
  return FsyncDirectory(dir);
}

Status PartitionChangeLog::RewriteSegmentsLocked() {
  const std::filesystem::path dir(options_.directory);
  std::filesystem::create_directories(dir);
  std::vector<std::filesystem::path> old_segments;
  for (const auto& entry : std::filesystem::directory_iterator(dir)) {
    if (entry.is_regular_file() && IsSegmentFile(entry.path())) {
      old_segments.push_back(entry.path());
    }
  }
  manifest_segment_names_.clear();
  active_segment_size_ = 0;

  int fd = -1;
  auto close_current = [&]() -> Status {
    if (fd < 0) {
      return Status::OK();
    }
    if (::fsync(fd) != 0) {
      int saved = errno;
      ::close(fd);
      fd = -1;
      errno = saved;
      return PosixError("fsync compacted segment");
    }
    if (::close(fd) != 0) {
      fd = -1;
      return PosixError("close compacted segment");
    }
    fd = -1;
    return Status::OK();
  };

  std::vector<std::filesystem::path> temp_segments;
  for (const auto& record : records_) {
    std::string payload;
    if (!record.SerializeToString(&payload)) {
      return Status::Corruption("PartitionChangeLog",
                                "failed to serialize compacted record");
    }
    const size_t frame_size = kFrameHeaderBytes + payload.size();
    if (active_segment_size_ == 0 ||
        active_segment_size_ + frame_size > options_.max_segment_bytes) {
      CEDAR_RETURN_IF_ERROR(close_current());
      const std::string final_name = SegmentFileName(record.offset());
      manifest_segment_names_.push_back(final_name);
      const auto tmp = dir / (final_name + ".rewrite");
      fd = ::open(tmp.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
      if (fd < 0) {
        manifest_segment_names_.clear();
        return PosixError("open compacted segment: " + tmp.string());
      }
      temp_segments.push_back(tmp);
      active_segment_size_ = 0;
    }
    FrameHeader header{kFrameMagic, kFrameVersion, 0,
                       static_cast<uint32_t>(payload.size()),
                       crc32c::Value(payload.data(), payload.size())};
    CEDAR_RETURN_IF_ERROR(WriteAll(fd, &header, sizeof(header)));
    CEDAR_RETURN_IF_ERROR(WriteAll(fd, payload.data(), payload.size()));
    active_segment_size_ += frame_size;
  }
  CEDAR_RETURN_IF_ERROR(close_current());

  for (size_t i = 0; i < temp_segments.size(); ++i) {
    const auto final_path = dir / manifest_segment_names_[i];
    if (::rename(temp_segments[i].c_str(), final_path.c_str()) != 0) {
      manifest_segment_names_.clear();
      return PosixError("rename compacted segment: " + final_path.string());
    }
  }
  CEDAR_RETURN_IF_ERROR(FsyncDirectory(dir));
  CEDAR_RETURN_IF_ERROR(PersistManifestLocked());
  std::set<std::string> retained(manifest_segment_names_.begin(),
                                 manifest_segment_names_.end());
  for (const auto& path : old_segments) {
    if (retained.find(path.filename().string()) != retained.end()) {
      continue;
    }
    std::error_code ec;
    std::filesystem::remove(path, ec);
    if (ec) {
      manifest_segment_names_.clear();
      return Status::IOError("PartitionChangeLog", ec.message());
    }
  }
  if (records_.empty() || manifest_segment_names_.empty()) {
    active_segment_first_offset_ = state_.high_watermark + 1;
    active_segment_size_ = 0;
  } else {
    active_segment_first_offset_ =
        std::stoull(manifest_segment_names_.back().substr(0, 20));
    std::error_code ec;
    active_segment_size_ =
        std::filesystem::file_size(dir / manifest_segment_names_.back(), ec);
    if (ec) {
      active_segment_size_ = 0;
    }
  }
  return FsyncDirectory(dir);
}

Status PartitionChangeLog::PersistManifestLocked() const {
  std::ostringstream out;
  out << "version 1\n";
  out << "partition_id " << options_.partition_id << "\n";
  out << "partition_epoch " << state_.partition_epoch << "\n";
  out << "earliest_offset " << state_.earliest_offset << "\n";
  out << "high_watermark " << state_.high_watermark << "\n";
  out << "committed_version " << state_.committed_version << "\n";
  out << "segments\n";
  std::filesystem::path dir(options_.directory);
  if (std::filesystem::exists(dir)) {
    std::vector<std::filesystem::path> segments;
    if (!manifest_segment_names_.empty()) {
      for (const auto& name : manifest_segment_names_) {
        segments.push_back(name);
      }
    } else {
      for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        if (entry.is_regular_file() && IsSegmentFile(entry.path())) {
          segments.push_back(entry.path().filename());
        }
      }
    }
    std::sort(segments.begin(), segments.end());
    for (const auto& segment : segments) {
      uint64_t first = 0;
      uint64_t last = 0;
      uint64_t bytes = 0;
      CEDAR_RETURN_IF_ERROR(ReadSegmentRange(dir / segment, &first, &last,
                                             &bytes));
      out << segment.string() << " first_offset " << first << " last_offset "
          << last << " bytes " << bytes << "\n";
    }
  }
  return AtomicWriteFile(dir / kManifestName, out.str());
}

}  // namespace cedar::cdc
