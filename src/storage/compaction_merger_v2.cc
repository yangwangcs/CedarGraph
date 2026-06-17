//===----------------------------------------------------------------------===//
// Compaction Merger V2 - Zone-Synchronized K-Way Merge
//===----------------------------------------------------------------------===//

#include "cedar/storage/compaction_merger.h"

#include <algorithm>
#include <queue>
#include <vector>

#include "cedar/sst/zone_columnar_builder_v2.h"
#include "cedar/sst/zone_columnar_reader.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include "cedar/core/env.h"

namespace cedar {

//===----------------------------------------------------------------------===//
// Merge Heap Item
//===----------------------------------------------------------------------===//
struct MergeItem {
  CedarKey key;
  Descriptor descriptor;
  size_t input_idx;
  uint32_t row_idx;  // 当前行号，用于读取下一个
  
  // 用于排序：entity_id ASC, type ASC, col ASC, target ASC, ts DESC
  bool operator>(const MergeItem& other) const {
    return key.LessForSorting(other.key);
  }
};

//===----------------------------------------------------------------------===//
// CompactionMergerV2 实现
//===----------------------------------------------------------------------===//

class CompactionMergerV2::Impl {
 public:
  Impl(const CompactionOptions& options, const std::vector<std::string>& input_files,
       const std::string& output_path, Env* fs)
      : options_(options),
        input_files_(input_files),
        output_path_(output_path),
        fs_(fs),
        min_entity_id_(UINT64_MAX),
        max_entity_id_(0) {}
  
  std::unique_ptr<ZoneSstMeta> Run() {
    if (!Initialize()) {
      return nullptr;
    }
    
    if (!Merge()) {
      return nullptr;
    }
    
    return Finalize();
  }
  
 private:
  bool Initialize() {
    // 打开所有输入文件
    for (const auto& path : input_files_) {
      auto reader = std::make_unique<SstReader>(path);
      if (!reader->Open().ok()) {
        last_error_ = "Failed to open: " + path;
        return false;
      }
      
      readers_.push_back(std::move(reader));
    }
    
    // 打开输出文件
    WritableFile* file_ptr = nullptr;
    Status s = fs_->NewWritableFile(output_path_, &file_ptr);
    if (!s.ok()) {
      last_error_ = s.ToString();
      return false;
    }
    output_file_.reset(file_ptr);
    
    // 创建 V2 Builder
    ZoneColumnarSstBuilder::Options builder_options;
    builder_options.target_block_size = options_.target_block_size;
    builder_ = std::make_unique<ZoneColumnarSstBuilder>(builder_options, output_file_.get());
    
    // 初始化堆：从每个输入读取第一个条目
    current_row_.resize(readers_.size(), 0);
    for (size_t i = 0; i < readers_.size(); ++i) {
      if (readers_[i]->NumEntries() > 0) {
        CedarKey key = readers_[i]->ReconstructKey(0);
        auto desc = readers_[i]->GetValueByRow(0);
        if (desc) {
          MergeItem item{key, *desc, i, 0};
          heap_.push(item);
        }
      }
    }
    
    return true;
  }
  
  bool Merge() {
    while (!heap_.empty()) {
      // 弹出最小元素
      auto current = heap_.top();
      heap_.pop();
      
      stats_.input_entries++;
      
      // 跟踪 key range
      uint64_t eid = current.key.entity_id();
      if (eid < min_entity_id_) min_entity_id_ = eid;
      if (eid > max_entity_id_) max_entity_id_ = eid;
      
      // 检查是否为墓碑
      if (ShouldFilter(current.key, current.descriptor)) {
        //  Tombstone，根据策略决定是否删除
        if (options_.remove_tombstones) {
          stats_.deleted_entries++;
          // 继续读取下一个
          if (ReadNextFromInput(current.input_idx, current.row_idx + 1)) {
            // ReadNextFromInput 会将新条目推入堆
          }
          continue;
        }
      }
      
      // 输出到 Builder
      builder_->Add(current.key, current.descriptor, Timestamp(0));
      stats_.output_entries++;
      
      // 从同一输入读取下一个
      if (ReadNextFromInput(current.input_idx, current.row_idx + 1)) {
        // ReadNextFromInput 会将新条目推入堆
      }
    }
    
    return true;
  }
  
  // 从指定输入读取指定行号的条目并推入堆
  bool ReadNextFromInput(size_t input_idx, uint32_t row_idx) {
    if (input_idx >= readers_.size()) return false;
    
    const auto& reader = readers_[input_idx];
    if (row_idx >= reader->NumEntries()) {
      return false;
    }
    
    CedarKey key = reader->ReconstructKey(row_idx);
    auto desc = reader->GetValueByRow(row_idx);
    if (desc) {
      MergeItem item{key, *desc, input_idx, row_idx};
      heap_.push(item);
      current_row_[input_idx] = row_idx;
      return true;
    }
    return false;
  }
  
  std::unique_ptr<ZoneSstMeta> Finalize() {
    Status s = builder_->Finish();
    if (!s.ok()) {
      last_error_ = s.ToString();
      return nullptr;
    }
    
    stats_.output_file_size = builder_->FileSize();
    
    // 构建元数据
    auto meta = std::make_unique<ZoneSstMeta>();
    meta->file_size = stats_.output_file_size;
    meta->num_entries = stats_.output_entries;
    meta->min_entity_id = min_entity_id_;
    meta->max_entity_id = max_entity_id_;
    
    return meta;
  }
  
  bool ShouldFilter(const CedarKey& key, const Descriptor& desc) {
    // Tombstone filtering:
    // 1. Check if descriptor is a tombstone
    // 2. Check if key has physical tombstone marker (bit 7)
    // 3. Only remove if we're at a deep enough level (L3+)
    
    // Physical tombstone (bit 7) - set by compaction for cleanup
    if (key.IsTombstone()) {
      return options_.remove_tombstones;
    }
    
    // Descriptor tombstone - business DELETE
    if (desc.IsTombstone()) {
      return options_.remove_tombstones;
    }
    
    return false;
  }
  
 private:
  CompactionOptions options_;
  std::vector<std::string> input_files_;
  std::string output_path_;
  Env* fs_;
  
  std::vector<std::unique_ptr<SstReader>> readers_;
  std::unique_ptr<WritableFile> output_file_;
  std::unique_ptr<SstBuilder> builder_;
  
  std::priority_queue<MergeItem, std::vector<MergeItem>, std::greater<>> heap_;
  std::vector<uint32_t> current_row_;  // 每个 reader 的当前行号
  
  uint64_t min_entity_id_;  // 跟踪合并过程中的最小 entity_id
  uint64_t max_entity_id_;  // 跟踪合并过程中的最大 entity_id
  
  struct Stats {
    size_t input_entries = 0;
    size_t output_entries = 0;
    size_t deleted_entries = 0;
    size_t output_file_size = 0;
  } stats_;
  
  std::string last_error_;
};

//===----------------------------------------------------------------------===//
// CompactionMergerV2 Public API
//===----------------------------------------------------------------------===//

CompactionMergerV2::CompactionMergerV2(const CompactionOptions& options,
                                        const std::vector<std::string>& input_files,
                                        const std::string& output_path,
                                        Env* fs)
    : impl_(std::make_unique<Impl>(options, input_files, output_path, fs)) {}

CompactionMergerV2::~CompactionMergerV2() = default;

std::unique_ptr<ZoneSstMeta> CompactionMergerV2::Run() {
  return impl_->Run();
}

}  // namespace cedar
