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

#include "cedar/dtx/storage/braft_partition_raft.h"
#include "cedar/dtx/storage_service_impl.h"

#include <braft/raft.h>
#include <braft/util.h>
#include <butil/logging.h>

#include <gflags/gflags.h>

#include <cstring>
#include <filesystem>

DEFINE_int64(raft_propose_timeout_ms, 5000, "Raft proposal timeout");

namespace cedar {
namespace dtx {

namespace {

class RaftCircuitBreaker {
 public:
  bool IsOpen() const {
    return std::chrono::steady_clock::now() < open_until_;
  }

  void RecordSuccess() { consecutive_failures_ = 0; }

  void RecordFailure() {
    ++consecutive_failures_;
    if (consecutive_failures_ >= 5) {
      open_until_ = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    }
  }

 private:
  size_t consecutive_failures_ = 0;
  std::chrono::steady_clock::time_point open_until_;
};

}  // namespace

// =============================================================================
// StorageLogEntry Serialization
// =============================================================================

std::string StorageLogEntry::Serialize() const {
  std::string data;
  // Estimate size to avoid repeated reallocations
  size_t est = 1 + sizeof(txn_id) + sizeof(commit_ts);
  if (type == Type::kPrepare) {
    est += sizeof(uint32_t) + read_set.size() * (sizeof(uint32_t) + 32);
    est += sizeof(uint32_t) + write_set.size() * (sizeof(uint32_t) + 32);
    est += sizeof(uint32_t) + write_descriptors.size() * (sizeof(uint64_t) + sizeof(uint64_t));
  } else if (type == Type::kPut || type == Type::kDelete) {
    est += sizeof(uint32_t) + 32 + 1 + sizeof(uint32_t) + 32 + sizeof(uint64_t);
  } else if (type == Type::kBatch) {
    est += sizeof(uint32_t) + batch_data.size() * (sizeof(uint32_t) + 32 + sizeof(uint32_t) + 32);
  }
  data.reserve(est);

  // type: 1 byte
  data.push_back(static_cast<char>(type));

  // 2PC entries use different serialization
  if (type == Type::kPrepare || type == Type::kCommit || type == Type::kAbort) {
    // txn_id: 8 bytes
    data.append(reinterpret_cast<const char*>(&txn_id), sizeof(txn_id));
    // commit_ts: 8 bytes
    uint64_t cts = commit_ts.value();
    data.append(reinterpret_cast<const char*>(&cts), sizeof(cts));

    if (type == Type::kPrepare) {
      // read_set
      uint32_t read_count = static_cast<uint32_t>(read_set.size());
      data.append(reinterpret_cast<const char*>(&read_count), sizeof(read_count));
      for (const auto& k : read_set) {
        uint32_t k_len = sizeof(CedarKey);
        data.append(reinterpret_cast<const char*>(&k_len), sizeof(k_len));
        data.append(reinterpret_cast<const char*>(&k), sizeof(CedarKey));
      }
      // write_set
      uint32_t write_count = static_cast<uint32_t>(write_set.size());
      data.append(reinterpret_cast<const char*>(&write_count), sizeof(write_count));
      for (const auto& k : write_set) {
        uint32_t k_len = sizeof(CedarKey);
        data.append(reinterpret_cast<const char*>(&k_len), sizeof(k_len));
        data.append(reinterpret_cast<const char*>(&k), sizeof(CedarKey));
      }
      // write_descriptors
      uint32_t desc_count = static_cast<uint32_t>(write_descriptors.size());
      data.append(reinterpret_cast<const char*>(&desc_count), sizeof(desc_count));
      for (const auto& [kh, d] : write_descriptors) {
        data.append(reinterpret_cast<const char*>(&kh), sizeof(kh));
        uint64_t desc_raw = d.AsRaw();
        data.append(reinterpret_cast<const char*>(&desc_raw), sizeof(desc_raw));
      }
    }
    return data;
  }

  // key serialization (for kPut/kDelete/kBatch)
  uint32_t key_len = sizeof(CedarKey);
  data.append(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
  data.append(reinterpret_cast<const char*>(&key), sizeof(CedarKey));

  // descriptor presence + data
  if (descriptor.has_value()) {
    data.push_back(1);
    uint64_t desc_raw = descriptor.value().AsRaw();
    data.append(reinterpret_cast<const char*>(&desc_raw), sizeof(desc_raw));
  } else {
    data.push_back(0);
  }

  // txn_version: 8 bytes
  uint64_t ts = txn_version.value();
  data.append(reinterpret_cast<const char*>(&ts), sizeof(ts));

  // batch_data for kBatch
  if (type == Type::kBatch) {
    uint32_t batch_size = static_cast<uint32_t>(batch_data.size());
    data.append(reinterpret_cast<const char*>(&batch_size), sizeof(batch_size));
    for (const auto& [k, d] : batch_data) {
      std::string k_str = k.Encode();
      uint32_t k_len = static_cast<uint32_t>(k_str.size());
      data.append(reinterpret_cast<const char*>(&k_len), sizeof(k_len));
      data.append(k_str);
      std::string d_str = d.Encode();
      uint32_t d_len = static_cast<uint32_t>(d_str.size());
      data.append(reinterpret_cast<const char*>(&d_len), sizeof(d_len));
      data.append(d_str);
    }
  }

  return data;
}

StatusOr<StorageLogEntry> StorageLogEntry::Deserialize(
    const std::string& data) {
  if (data.size() < 1) {
    return Status::InvalidArgument("Empty log entry");
  }

  StorageLogEntry entry;
  size_t pos = 0;

  // type
  entry.type = static_cast<Type>(data[pos++]);

  // 2PC entries
  if (entry.type == Type::kPrepare || entry.type == Type::kCommit || entry.type == Type::kAbort) {
    if (pos + sizeof(uint64_t) > data.size()) {
      return Status::InvalidArgument("Corrupt log entry: txn_id");
    }
    std::memcpy(&entry.txn_id, &data[pos], sizeof(entry.txn_id));
    pos += sizeof(entry.txn_id);

    if (pos + sizeof(uint64_t) > data.size()) {
      return Status::InvalidArgument("Corrupt log entry: commit_ts");
    }
    uint64_t cts;
    std::memcpy(&cts, &data[pos], sizeof(cts));
    entry.commit_ts = Timestamp(cts);
    pos += sizeof(cts);

    if (entry.type == Type::kPrepare) {
      // read_set
      if (pos + sizeof(uint32_t) > data.size()) {
        return Status::InvalidArgument("Corrupt log entry: read count");
      }
      uint32_t read_count;
      std::memcpy(&read_count, &data[pos], sizeof(read_count));
      pos += sizeof(read_count);
      for (uint32_t i = 0; i < read_count; ++i) {
        if (pos + sizeof(uint32_t) > data.size()) {
          return Status::InvalidArgument("Corrupt log entry: read key length");
        }
        uint32_t k_len;
        std::memcpy(&k_len, &data[pos], sizeof(k_len));
        pos += sizeof(k_len);
        if (pos + k_len > data.size()) {
          return Status::InvalidArgument("Corrupt log entry: read key data");
        }
        auto key_opt = CedarKey::Decode(std::string_view(data.data() + pos, k_len));
        if (!key_opt.has_value()) return Status::InvalidArgument("Invalid read CedarKey");
        entry.read_set.push_back(key_opt.value());
        pos += k_len;
      }
      // write_set
      if (pos + sizeof(uint32_t) > data.size()) {
        return Status::InvalidArgument("Corrupt log entry: write count");
      }
      uint32_t write_count;
      std::memcpy(&write_count, &data[pos], sizeof(write_count));
      pos += sizeof(write_count);
      for (uint32_t i = 0; i < write_count; ++i) {
        if (pos + sizeof(uint32_t) > data.size()) {
          return Status::InvalidArgument("Corrupt log entry: write key length");
        }
        uint32_t k_len;
        std::memcpy(&k_len, &data[pos], sizeof(k_len));
        pos += sizeof(k_len);
        if (pos + k_len > data.size()) {
          return Status::InvalidArgument("Corrupt log entry: write key data");
        }
        auto key_opt = CedarKey::Decode(std::string_view(data.data() + pos, k_len));
        if (!key_opt.has_value()) return Status::InvalidArgument("Invalid write CedarKey");
        entry.write_set.push_back(key_opt.value());
        pos += k_len;
      }
      // write_descriptors
      if (pos + sizeof(uint32_t) > data.size()) {
        return Status::InvalidArgument("Corrupt log entry: descriptor count");
      }
      uint32_t desc_count;
      std::memcpy(&desc_count, &data[pos], sizeof(desc_count));
      pos += sizeof(desc_count);
      for (uint32_t i = 0; i < desc_count; ++i) {
        if (pos + sizeof(uint64_t) > data.size()) {
          return Status::InvalidArgument("Corrupt log entry: key_hash");
        }
        uint64_t kh;
        std::memcpy(&kh, &data[pos], sizeof(kh));
        pos += sizeof(kh);
        if (pos + sizeof(uint64_t) > data.size()) {
          return Status::InvalidArgument("Corrupt log entry: descriptor raw");
        }
        uint64_t desc_raw;
        std::memcpy(&desc_raw, &data[pos], sizeof(desc_raw));
        entry.write_descriptors[kh] = Descriptor(desc_raw);
        pos += sizeof(desc_raw);
      }
    }
    return entry;
  }

  // key (for kPut/kDelete/kBatch)
  if (pos + sizeof(uint32_t) > data.size()) {
    return Status::InvalidArgument("Corrupt log entry: key length");
  }
  uint32_t key_len;
  std::memcpy(&key_len, &data[pos], sizeof(key_len));
  pos += sizeof(key_len);
  if (pos + key_len > data.size()) {
    return Status::InvalidArgument("Corrupt log entry: key data");
  }
  auto key_opt = CedarKey::Decode(std::string_view(data.data() + pos, key_len));
  if (!key_opt.has_value()) return Status::InvalidArgument("Invalid CedarKey");
  entry.key = key_opt.value();
  pos += key_len;

  // descriptor
  if (pos >= data.size()) {
    return Status::InvalidArgument("Corrupt log entry: descriptor flag");
  }
  bool has_desc = data[pos++] != 0;
  if (has_desc) {
    if (pos + sizeof(uint64_t) > data.size()) {
      return Status::InvalidArgument("Corrupt log entry: desc raw");
    }
    uint64_t desc_raw;
    std::memcpy(&desc_raw, &data[pos], sizeof(desc_raw));
    entry.descriptor = Descriptor(desc_raw);
    pos += sizeof(desc_raw);
  }

  // txn_version
  if (pos + sizeof(uint64_t) > data.size()) {
    return Status::InvalidArgument("Corrupt log entry: timestamp");
  }
  uint64_t ts;
  std::memcpy(&ts, &data[pos], sizeof(ts));
  entry.txn_version = Timestamp(ts);
  pos += sizeof(ts);

  // batch_data
  if (entry.type == Type::kBatch) {
    if (pos + sizeof(uint32_t) > data.size()) {
      return Status::InvalidArgument("Corrupt log entry: batch size");
    }
    uint32_t batch_size;
    std::memcpy(&batch_size, &data[pos], sizeof(batch_size));
    pos += sizeof(batch_size);
    for (uint32_t i = 0; i < batch_size; ++i) {
      if (pos + sizeof(uint32_t) > data.size()) {
        return Status::InvalidArgument("Corrupt log entry: batch key length");
      }
      uint32_t k_len;
      std::memcpy(&k_len, &data[pos], sizeof(k_len));
      pos += sizeof(k_len);
      if (pos + k_len > data.size()) {
        return Status::InvalidArgument("Corrupt log entry: batch key data");
      }
      if (pos + k_len > data.size()) {
        return Status::InvalidArgument("Corrupt log entry: batch key data");
      }
      auto k_opt = CedarKey::Decode(std::string_view(data.data() + pos, k_len));
      if (!k_opt.has_value()) return Status::InvalidArgument("Invalid CedarKey in batch");
      pos += k_len;

      if (pos + sizeof(uint32_t) > data.size()) {
        return Status::InvalidArgument("Corrupt log entry: batch desc length");
      }
      uint32_t d_len;
      std::memcpy(&d_len, &data[pos], sizeof(d_len));
      pos += sizeof(d_len);
      if (pos + d_len > data.size()) {
        return Status::InvalidArgument("Corrupt log entry: batch desc data");
      }
      auto d_opt = Descriptor::Decode(Slice(data.data() + pos, d_len));
      if (!d_opt.has_value()) return Status::InvalidArgument("Invalid Descriptor in batch");
      pos += d_len;

      entry.batch_data.emplace_back(k_opt.value(), d_opt.value());
    }
  }

  return entry;
}

// =============================================================================
// StoragePartitionStateMachine
// =============================================================================

StoragePartitionStateMachine::StoragePartitionStateMachine(
    PartitionStorage* storage)
    : storage_(storage) {}

void StoragePartitionStateMachine::on_apply(braft::Iterator& iter) {
  for (; iter.valid(); iter.next()) {
    braft::AsyncClosureGuard closure_guard(iter.done());

    std::string data = iter.data().to_string();
    if (data.size() < 1) {
      LOG(ERROR) << "Corrupt log entry: too small at index=" << iter.index()
                 << " — stepping down";
      iter.set_error_and_rollback();
      return;
    }

    auto entry_result = StorageLogEntry::Deserialize(data);
    if (!entry_result.ok()) {
      LOG(ERROR) << "Failed to deserialize log entry: "
                 << entry_result.status().ToString()
                 << " — stepping down";
      iter.set_error_and_rollback();
      return;
    }

    const auto& entry = entry_result.value();
    if (!storage_) {
      LOG(ERROR) << "No storage available for apply at index=" << iter.index();
      iter.set_error_and_rollback();
      return;
    }
    if (entry.type == StorageLogEntry::Type::kPut) {
      storage_->Put(entry.key, entry.descriptor.value_or(Descriptor()),
                    entry.txn_version, 0);
    } else if (entry.type == StorageLogEntry::Type::kDelete) {
      storage_->Put(entry.key, Descriptor(), entry.txn_version, 0);
    } else if (entry.type == StorageLogEntry::Type::kBatch) {
      for (const auto& [k, d] : entry.batch_data) {
        storage_->Put(k, d, entry.txn_version, 0);
      }
    } else if (entry.type == StorageLogEntry::Type::kPrepare) {
      auto status = storage_->Prepare(entry.txn_id, entry.read_set, entry.write_set,
                                      entry.write_descriptors, entry.commit_ts);
      if (!status.ok()) {
        LOG(ERROR) << "Apply PREPARE failed at index=" << iter.index()
                   << " txn_id=" << entry.txn_id << ": " << status.ToString()
                   << " — stepping down";
        iter.set_error_and_rollback();
        return;
      }
    } else if (entry.type == StorageLogEntry::Type::kCommit) {
      auto status = storage_->Commit(entry.txn_id, entry.commit_ts);
      if (!status.ok()) {
        LOG(ERROR) << "Apply COMMIT failed at index=" << iter.index()
                   << " txn_id=" << entry.txn_id << ": " << status.ToString()
                   << " — stepping down";
        iter.set_error_and_rollback();
        return;
      }
    } else if (entry.type == StorageLogEntry::Type::kAbort) {
      auto status = storage_->Abort(entry.txn_id);
      if (!status.ok()) {
        LOG(ERROR) << "Apply ABORT failed at index=" << iter.index()
                   << " txn_id=" << entry.txn_id << ": " << status.ToString()
                   << " — stepping down";
        iter.set_error_and_rollback();
        return;
      }
    } else {
      LOG(ERROR) << "Unknown log entry type: " << static_cast<int>(entry.type)
                 << " at index=" << iter.index() << " — stepping down";
      iter.set_error_and_rollback();
      return;
    }

    last_term_ = iter.term();
  }
}

// Helper: Recursively copy all files from src_dir to dst_dir
static std::vector<std::string> CopySnapshotFiles(
    const std::string& src_dir,
    const std::string& dst_dir) {
  std::vector<std::string> copied_files;
  if (!std::filesystem::exists(src_dir)) {
    return copied_files;
  }
  std::filesystem::create_directories(dst_dir);
  for (const auto& entry : std::filesystem::recursive_directory_iterator(src_dir)) {
    if (entry.is_regular_file()) {
      std::string relative_path = std::filesystem::relative(entry.path(), src_dir).string();
      std::string dst_path = dst_dir + "/" + relative_path;
      std::filesystem::create_directories(std::filesystem::path(dst_path).parent_path());
      try {
        std::filesystem::copy_file(entry.path(), dst_path,
                                   std::filesystem::copy_options::overwrite_existing);
        copied_files.push_back(relative_path);
      } catch (const std::filesystem::filesystem_error& e) {
        LOG(WARNING) << "Failed to copy file " << entry.path().string()
                     << " to " << dst_path << ": " << e.what();
      }
    }
  }
  return copied_files;
}

void StoragePartitionStateMachine::on_snapshot_save(
    braft::SnapshotWriter* writer, braft::Closure* done) {
  braft::AsyncClosureGuard done_guard(done);
  
  if (!storage_) {
    LOG(WARNING) << "Storage not available for snapshot save";
    return;
  }
  
  // Step 1: Flush underlying storage to ensure all data is on disk
  auto* shared_storage = storage_->GetSharedStorage();
  if (shared_storage) {
    auto flush_status = shared_storage->ForceFlush();
    if (!flush_status.ok()) {
      LOG(WARNING) << "ForceFlush failed during snapshot: " << flush_status.ToString();
    }
  }
  
  // Step 2: Copy data files from storage data_root to snapshot directory
  std::string data_root = storage_->GetDataRoot();
  std::string snapshot_path = writer->get_path();
  auto copied_files = CopySnapshotFiles(data_root, snapshot_path + "/data");
  
  // Register copied data files with snapshot writer
  for (const auto& rel_path : copied_files) {
    std::string snapshot_file = "data/" + rel_path;
    if (writer->add_file(snapshot_file, nullptr) != 0) {
      LOG(ERROR) << "Failed to add file to snapshot: " << snapshot_file;
      if (done) {
        done->status().set_error(EIO, "Failed to add snapshot file");
      }
      return;
    }
  }
  
  // Step 3: Serialize prepared transaction state (2PC)
  std::string txn_state_path = snapshot_path + "/txn_state";
  auto status = storage_->SavePreparedTxns(txn_state_path);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to save prepared txns for snapshot: " << status.ToString();
    if (done) {
      done->status().set_error(EIO, "Failed to save txn state");
    }
    return;
  }
  if (writer->add_file("txn_state", nullptr) != 0) {
    LOG(ERROR) << "Failed to add txn_state to snapshot";
    if (done) {
      done->status().set_error(EIO, "Failed to add txn_state");
    }
    return;
  }
  
  LOG(INFO) << "Snapshot saved for partition " << storage_->GetPartitionId()
            << " with " << copied_files.size() << " data files";
}

int StoragePartitionStateMachine::on_snapshot_load(
    braft::SnapshotReader* reader) {
  if (!storage_) {
    LOG(WARNING) << "Storage not available for snapshot load";
    return 0;
  }
  
  std::string snapshot_path = reader->get_path();
  std::string data_root = storage_->GetDataRoot();
  
  // Step 1: Restore data files from snapshot to storage data_root
  std::string snapshot_data_dir = snapshot_path + "/data";
  if (std::filesystem::exists(snapshot_data_dir)) {
    auto copied_files = CopySnapshotFiles(snapshot_data_dir, data_root);
    LOG(INFO) << "Restored " << copied_files.size() << " data files from snapshot"
              << " for partition " << storage_->GetPartitionId();
  }
  
  // Step 2: Restore prepared transaction state (2PC)
  std::string txn_state_path = snapshot_path + "/txn_state";
  if (std::filesystem::exists(txn_state_path)) {
    auto status = storage_->LoadPreparedTxns(txn_state_path);
    if (!status.ok()) {
      LOG(ERROR) << "Failed to load prepared txns from snapshot: " << status.ToString();
      return -1;
    }
    LOG(INFO) << "Loaded prepared txns from snapshot for partition "
              << storage_->GetPartitionId();
  }
  
  return 0;
}

void StoragePartitionStateMachine::on_leader_start(int64_t term) {
  last_term_ = term;
  LOG(INFO) << "Leader started, term=" << term;
  if (lease_callback_) {
    lease_callback_(term, true);
  }
}

void StoragePartitionStateMachine::on_leader_stop(
    const butil::Status& status) {
  LOG(INFO) << "Leader stopped: " << status.error_str();
  if (lease_callback_) {
    lease_callback_(0, false);
  }
}

void StoragePartitionStateMachine::on_shutdown() {
  LOG(INFO) << "StateMachine shutdown";
}

void StoragePartitionStateMachine::on_error(const braft::Error& e) {
  LOG(ERROR) << "StateMachine error: " << e.status().error_str();
}

void StoragePartitionStateMachine::on_configuration_committed(
    const braft::Configuration& conf) {
  LOG(INFO) << "Configuration committed: " << conf;
}

// =============================================================================
// ProposeClosure
// =============================================================================

class ProposeClosure : public braft::Closure {
 public:
  ProposeClosure(std::shared_ptr<std::promise<::cedar::Status>> promise,
                 std::shared_ptr<butil::IOBuf> data)
      : promise_(std::move(promise)), data_(std::move(data)) {}

  void Run() override {
    bool expected = false;
    if (!done_.compare_exchange_strong(expected, true)) {
      delete this;
      return;
    }
    if (status().ok()) {
      promise_->set_value(::cedar::Status::OK());
    } else {
      promise_->set_value(
          ::cedar::Status::IOError("BRaftPartitionNode", status().error_str()));
    }
    delete this;
  }

 private:
  std::shared_ptr<std::promise<::cedar::Status>> promise_;
  std::shared_ptr<butil::IOBuf> data_;  // Keep IOBuf alive until braft is done
  std::atomic<bool> done_{false};
};

// =============================================================================
// BraftPartitionNode::Impl
// =============================================================================

class BraftPartitionNode::Impl {
 public:
  Impl() = default;
  ~Impl() { Shutdown(); }

  Status Init(const Options& options, PartitionStorage* storage) {
    std::lock_guard<std::mutex> lock(node_mutex_);

    butil::EndPoint ep;
    if (butil::str2endpoint(options.listen_address.c_str(), &ep) != 0) {
      return Status::InvalidArgument("Invalid listen address: " +
                                      options.listen_address);
    }

    std::string group_name =
        "partition_" + std::to_string(options.partition_id);

    braft::NodeOptions node_options;
    node_options.election_timeout_ms = options.election_timeout_ms;
    election_timeout_ms_ = options.election_timeout_ms;
    for (const auto& peer : options.initial_peers) {
      node_options.initial_conf.add_peer(peer);
    }
    auto* fsm = new StoragePartitionStateMachine(storage);
    // Register lease callback so the state machine can notify us of leader changes
    fsm->SetLeaderLeaseCallback([this](int64_t term, bool is_leader) {
      std::lock_guard<std::mutex> lease_lock(lease_mutex_);
      if (is_leader) {
        // Grant leader lease: valid for 2x election timeout
        leader_lease_expiry_ = std::chrono::steady_clock::now() +
            std::chrono::milliseconds(election_timeout_ms_ * 2);
        LOG(INFO) << "[LeaseRead] Leader lease granted, term=" << term
                  << " expiry=" << election_timeout_ms_ * 2 << "ms";
      } else {
        // Revoke lease immediately on leader step down
        leader_lease_expiry_ = std::chrono::steady_clock::time_point::min();
        LOG(INFO) << "[LeaseRead] Leader lease revoked";
      }
    });
    node_options.fsm = fsm;
    node_options.node_owns_fsm = true;
    node_options.snapshot_interval_s = 3600;

    // Configure Raft log persistence
    std::string raft_data_path = options.data_path + "/raft_data";
    butil::FilePath(raft_data_path).value();  // ensure valid path
    node_options.log_uri = "local://" + raft_data_path + "/log";
    node_options.raft_meta_uri = "local://" + raft_data_path + "/meta";
    node_options.snapshot_uri = "local://" + raft_data_path + "/snapshot";

    peer_node_ids_ = options.peer_node_ids;

    if (node_options.initial_conf.empty()) {
      delete fsm;  // fsm not yet owned by braft
      return Status::InvalidArgument("No initial peers configured");
    }

    node_.reset(new braft::Node(group_name, braft::PeerId(ep)));
    if (node_->init(node_options) != 0) {
      delete fsm;  // init failed, braft did not take ownership
      return Status::IOError("Failed to init braft node for partition " +
                              std::to_string(options.partition_id));
    }

    // Start background lease renewal thread
    shutdown_lease_thread_ = false;
    lease_renewal_thread_ = std::thread(&Impl::RenewLeaseLoop, this);

    return Status::OK();
  }

  void Shutdown() {
    {
      std::lock_guard<std::mutex> lock(node_mutex_);
      if (node_) {
        node_->shutdown(nullptr);
        node_->join();
        node_.reset();
      }
    }
    // Stop lease renewal thread
    shutdown_lease_thread_ = true;
    if (lease_renewal_thread_.joinable()) {
      lease_renewal_thread_.join();
    }
  }

  bool IsLeader() const {
    std::lock_guard<std::mutex> lock(node_mutex_);
    return node_ && node_->is_leader();
  }

  bool IsLeaseValid() const {
    std::lock_guard<std::mutex> lock(lease_mutex_);
    if (leader_lease_expiry_ == std::chrono::steady_clock::time_point::min()) {
      return false;
    }
    return std::chrono::steady_clock::now() < leader_lease_expiry_;
  }

  std::optional<NodeID> GetLeaderId() const {
    std::lock_guard<std::mutex> lock(node_mutex_);
    if (!node_) return std::nullopt;

    braft::PeerId leader = node_->leader_id();
    if (leader.is_empty()) return std::nullopt;

    std::string addr = leader.to_string();
    // Remove braft index suffix (:0) if present
    size_t last_colon = addr.rfind(':');
    if (last_colon != std::string::npos) {
      size_t idx_colon = addr.rfind(':', last_colon - 1);
      if (idx_colon != std::string::npos) {
        addr = addr.substr(0, idx_colon);
      }
    }

    auto it = peer_node_ids_.find(addr);
    if (it != peer_node_ids_.end()) return it->second;
    return std::nullopt;
  }

  std::optional<std::string> GetLeaderAddress() const {
    std::lock_guard<std::mutex> lock(node_mutex_);
    if (!node_) return std::nullopt;

    braft::PeerId leader = node_->leader_id();
    if (leader.is_empty()) return std::nullopt;

    std::string addr = leader.to_string();
    // Remove braft index suffix (:0) if present
    size_t last_colon = addr.rfind(':');
    if (last_colon != std::string::npos) {
      size_t idx_colon = addr.rfind(':', last_colon - 1);
      if (idx_colon != std::string::npos) {
        addr = addr.substr(0, idx_colon);
      }
    }
    return addr;
  }

  void RenewLeaseLoop() {
    while (!shutdown_lease_thread_.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(election_timeout_ms_) / 2);
      if (shutdown_lease_thread_.load()) break;

      try {
        std::lock_guard<std::mutex> lock(node_mutex_);
        if (node_ && node_->is_leader()) {
          std::lock_guard<std::mutex> lease_lock(lease_mutex_);
          // Renew lease: extend by 2x election timeout from now
          leader_lease_expiry_ = std::chrono::steady_clock::now() +
              std::chrono::milliseconds(election_timeout_ms_ * 2);
        }
      } catch (const std::exception& e) {
        LOG(ERROR) << "[LeaseRead] RenewLeaseLoop exception: " << e.what();
      }
    }
  }

  Status Propose(const StorageLogEntry& entry) {
    if (circuit_breaker_.IsOpen()) {
      return Status::IOError("BRaftPartitionNode", "circuit breaker open");
    }

    std::lock_guard<std::mutex> lock(node_mutex_);
    if (!node_ || !node_->is_leader()) {
      return Status::NotLeader("Not leader");
    }

    auto data = std::make_shared<butil::IOBuf>();
    std::string serialized = entry.Serialize();
    data->append(serialized);

    braft::Task task;
    task.data = data.get();

    auto promise = std::make_shared<std::promise<::cedar::Status>>();
    task.done = new ProposeClosure(promise, data);

    node_->apply(task);

    auto future = promise->get_future();
    if (future.wait_for(std::chrono::milliseconds(FLAGS_raft_propose_timeout_ms)) ==
        std::future_status::timeout) {
      circuit_breaker_.RecordFailure();
      return Status::IOError("BRaftPartitionNode", "propose timeout");
    }

    auto status = future.get();
    if (!status.ok()) {
      circuit_breaker_.RecordFailure();
    } else {
      circuit_breaker_.RecordSuccess();
    }
    return status;
  }

  StatusOr<uint64_t> ReadIndex(std::chrono::milliseconds timeout) {
    // This version of braft does not support read_index API.
    // For linearizable reads, fallback to leader read.
    (void)timeout;
    return Status::NotSupported("ReadIndex not supported in this braft version");
  }

  Status WaitForApplied(uint64_t index, std::chrono::milliseconds timeout) {
    std::lock_guard<std::mutex> lock(node_mutex_);
    if (!node_) return Status::IOError("Node not initialized");

    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      braft::NodeStatus node_status;
      node_->get_status(&node_status);
      if (node_status.known_applied_index >= static_cast<int64_t>(index)) {
        return Status::OK();
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return Status::IOError("WaitForApplied timeout");
  }

  Status TransferLeadershipTo(NodeID target_node_id) {
    std::lock_guard<std::mutex> lock(node_mutex_);
    if (!node_) return Status::IOError("Node not initialized");
    
    // Find peer address for target NodeID
    std::string target_addr;
    for (const auto& [addr, nid] : peer_node_ids_) {
      if (nid == target_node_id) {
        target_addr = addr;
        break;
      }
    }
    if (target_addr.empty()) {
      return Status::NotFound("Target node not found in peer list");
    }
    
    braft::PeerId peer_id;
    if (peer_id.parse(target_addr) != 0) {
      return Status::InvalidArgument("Invalid peer address: " + target_addr);
    }
    
    int rc = node_->transfer_leadership_to(peer_id);
    if (rc != 0) {
      return Status::IOError("Failed to transfer leadership: " + std::string(berror(rc)));
    }
    return Status::OK();
  }

  NodeStatus GetStatus() const {
    std::lock_guard<std::mutex> lock(node_mutex_);
    NodeStatus status{};
    if (!node_) return status;

    status.is_leader = node_->is_leader();

    braft::NodeStatus node_status;
    node_->get_status(&node_status);
    status.term = node_status.term;
    status.committed_index = node_status.committed_index;
    status.applied_index = node_status.known_applied_index;

    braft::PeerId leader = node_->leader_id();
    if (!leader.is_empty()) {
      status.leader_address = leader.to_string();
    }

    std::vector<braft::PeerId> peers;
    if (node_->list_peers(&peers).ok()) {
      status.peer_count = peers.size();
    }

    return status;
  }

 private:
  mutable std::mutex node_mutex_;
  std::unique_ptr<braft::Node> node_;
  std::unordered_map<std::string, NodeID> peer_node_ids_;
  int election_timeout_ms_ = 1000;

  // Leader lease for linearizable reads (LeaseRead)
  mutable std::mutex lease_mutex_;
  std::chrono::steady_clock::time_point leader_lease_expiry_{
      std::chrono::steady_clock::time_point::min()};
  std::atomic<bool> shutdown_lease_thread_{false};
  std::thread lease_renewal_thread_;

  RaftCircuitBreaker circuit_breaker_;
};

// =============================================================================
// BraftPartitionNode
// =============================================================================

BraftPartitionNode::BraftPartitionNode() : impl_(std::make_unique<Impl>()) {}

BraftPartitionNode::~BraftPartitionNode() = default;

Status BraftPartitionNode::Init(const Options& options,
                                PartitionStorage* storage) {
  return impl_->Init(options, storage);
}

void BraftPartitionNode::Shutdown() { impl_->Shutdown(); }

bool BraftPartitionNode::IsLeader() const { return impl_->IsLeader(); }

bool BraftPartitionNode::IsLeaseValid() const { return impl_->IsLeaseValid(); }

std::optional<NodeID> BraftPartitionNode::GetLeaderId() const {
  return impl_->GetLeaderId();
}

std::optional<std::string> BraftPartitionNode::GetLeaderAddress() const {
  return impl_->GetLeaderAddress();
}

Status BraftPartitionNode::Propose(const StorageLogEntry& entry) {
  return impl_->Propose(entry);
}

StatusOr<uint64_t> BraftPartitionNode::ReadIndex(
    std::chrono::milliseconds timeout) {
  return impl_->ReadIndex(timeout);
}

Status BraftPartitionNode::WaitForApplied(uint64_t index,
                                           std::chrono::milliseconds timeout) {
  return impl_->WaitForApplied(index, timeout);
}

Status BraftPartitionNode::TransferLeadershipTo(NodeID target_node_id) {
  return impl_->TransferLeadershipTo(target_node_id);
}

BraftPartitionNode::NodeStatus BraftPartitionNode::GetStatus() const {
  return impl_->GetStatus();
}

}  // namespace dtx
}  // namespace cedar
