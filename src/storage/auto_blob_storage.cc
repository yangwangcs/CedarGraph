// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// 自动Blob存储管理器实现

#include "cedar/storage/auto_blob_storage.h"

#include <cstring>

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/types/cedar_key.h"
#include "cedar/core/slice.h"

namespace cedar {

AutoBlobStorage::AutoBlobStorage(LsmEngine* engine,
                                  BlobFileManager* blob_mgr,
                                  const AutoBlobConfig& config)
    : engine_(engine),
      blob_mgr_(blob_mgr),
      config_(config),
      stats_{} {}

AutoBlobStorage::~AutoBlobStorage() = default;

Status AutoBlobStorage::PutString(uint64_t entity_id, uint16_t col_id, const std::string& value) {
    if (!engine_) {
        return Status::InvalidArgument("Storage not initialized");
    }
    
    // 决策：小字符串内联，大字符串Blob
    bool use_blob = config_.enable_auto_blob && 
                    blob_mgr_ &&
                    value.size() > config_.inline_string_max_len &&
                    value.size() >= config_.min_blob_size;
    
    if (use_blob) {
        return PutBlobString(entity_id, col_id, value);
    } else {
        return PutInlineString(entity_id, col_id, value);
    }
}

Status AutoBlobStorage::PutInlineString(uint64_t entity_id, uint16_t col_id, const std::string& value) {
    // 拒绝超过4字节的字符串，防止静默截断
    if (value.size() > 4) {
        return Status::InvalidArgument("AutoBlobStorage",
            "inline string too long (" + std::to_string(value.size()) +
            " > 4 bytes); use Blob storage for large strings");
    }
    
    // 将字符串编码到整数（最多4字节）
    uint32_t encoded = 0;
    if (!value.empty()) {
        memcpy(&encoded, value.data(), value.size());
    }
    
    CedarKey key = CedarKey::Vertex(entity_id, col_id, Timestamp(0));
    Descriptor desc = Descriptor::InlineInt(col_id, static_cast<int32_t>(encoded));
    
    Status s = engine_->Put(key, desc, Timestamp(0));
    if (s.ok()) {
        stats_.inline_stores++;
    }
    return s;
}

Status AutoBlobStorage::PutBlobString(uint64_t entity_id, uint16_t col_id, const std::string& value) {
    if (!blob_mgr_) {
        return Status::NotSupported("Blob manager not available");
    }
    
    // 写入Blob
    uint32_t file_id, offset, size;
    Status s = blob_mgr_->WriteBlob(Slice(value), &file_id, &offset, &size);
    if (!s.ok()) {
        // Blob失败，回退到内联
        return PutInlineString(entity_id, col_id, value);
    }
    
    // 编码 file_id + offset 到 32-bit payload
    // file_id 占高 4 位 (支持 16 个 blob 文件), offset 占低 28 位 (每文件 256MB)
    constexpr uint32_t kMaxFileId = 0x0F;
    constexpr uint32_t kMaxOffset = 0x0FFFFFFF;
    if (file_id > kMaxFileId || offset > kMaxOffset) {
        return PutInlineString(entity_id, col_id, value);
    }
    uint32_t packed = (file_id << 28) | offset;
    
    // 存储单个 ExternalRef Descriptor 到主列
    uint8_t len = static_cast<uint8_t>(std::min(size, static_cast<uint32_t>(255)));
    CedarKey key = CedarKey::Vertex(entity_id, col_id, Timestamp(0));
    Descriptor desc(EntryKind::ExternalRef, col_id, packed, len);
    Status put_s = engine_->Put(key, desc, Timestamp(0));
    if (!put_s.ok()) {
        return Status::IOError("AutoBlobStorage", "Failed to write ExternalRef: " + put_s.ToString());
    }
    
    stats_.blob_stores++;
    return Status::OK();
}

std::optional<std::string> AutoBlobStorage::GetString(uint64_t entity_id, uint16_t col_id) {
    if (!engine_) {
        return std::nullopt;
    }
    
    // 先尝试读取Blob
    auto blob_result = GetBlobString(entity_id, col_id);
    if (blob_result.has_value()) {
        return blob_result;
    }
    
    // 回退到内联读取
    return GetInlineString(entity_id, col_id);
}

std::optional<std::string> AutoBlobStorage::GetInlineString(uint64_t entity_id, uint16_t col_id) {
    auto versions = engine_->GetAll(entity_id, EntityType::Vertex, col_id);
    if (versions.empty()) {
        return std::nullopt;
    }
    
    auto val = versions[0].descriptor.AsInlineInt();
    if (!val) {
        return std::nullopt;
    }
    
    // 解码为字符串 - 使用实际长度而非 null 终止
    uint32_t payload = static_cast<uint32_t>(*val);
    char buf[4];
    memcpy(buf, &payload, 4);
    
    // 计算实际长度（最多4字节，去除尾部 null）
    size_t len = 4;
    while (len > 0 && buf[len - 1] == '\0') {
      --len;
    }
    
    stats_.inline_reads++;
    return std::string(buf, len);
}

std::optional<std::string> AutoBlobStorage::GetBlobString(uint64_t entity_id, uint16_t col_id) {
    if (!blob_mgr_ || !engine_) {
        return std::nullopt;
    }
    
    // 读取主列 Descriptor
    auto versions = engine_->GetAll(entity_id, EntityType::Vertex, col_id);
    if (versions.empty()) {
        return std::nullopt;
    }
    
    const auto& desc = versions[0].descriptor;
    
    // 新格式: ExternalRef — payload 编码了 (file_id<<28 | offset)
    if (desc.GetKind() == EntryKind::ExternalRef) {
        uint32_t packed = desc.GetPayload();
        uint32_t file_id = packed >> 28;
        uint32_t offset = packed & 0x0FFFFFFF;
        
        // 从 blob header 读取实际大小
        std::string data;
        // 先尝试用 length 字段读取
        uint8_t hdr_len = desc.GetLength();
        if (hdr_len > 0) {
            Status s = blob_mgr_->ReadBlob(file_id, offset, hdr_len, &data);
            if (s.ok()) {
                stats_.blob_reads++;
                return data;
            }
        }
        // length=0 或读取失败，尝试读取 blob header 获取真实 size
        // BlobEntryHeader::kHeaderSize = 12, size 在前 4 字节
        std::string header;
        Status s = blob_mgr_->ReadBlob(file_id, offset, 12, &header);
        if (s.ok() && header.size() >= 4) {
            uint32_t real_size;
            memcpy(&real_size, header.data(), 4);
            if (real_size > 0 && real_size < 100 * 1024 * 1024) {  // 合理性检查 <100MB
                data.clear();
                s = blob_mgr_->ReadBlob(file_id, offset, real_size, &data);
                if (s.ok()) {
                    stats_.blob_reads++;
                    return data;
                }
            }
        }
        return std::nullopt;
    }
    
    // 旧格式兼容: InlineInt 存 size，辅助列存 file_id/offset
    if (desc.GetKind() == EntryKind::InlineInt) {
        auto size_val = desc.AsInlineInt();
        if (!size_val || *size_val <= 0) {
            return std::nullopt;
        }
        
        uint16_t blob_hi_col = GetBlobHiCol(col_id);
        uint16_t blob_lo_col = GetBlobLoCol(col_id);
        
        auto hi_versions = engine_->GetAll(entity_id, EntityType::Vertex, blob_hi_col);
        auto lo_versions = engine_->GetAll(entity_id, EntityType::Vertex, blob_lo_col);
        
        if (hi_versions.empty() || lo_versions.empty()) {
            return std::nullopt;
        }
        
        auto hi_val = hi_versions[0].descriptor.AsInlineInt();
        auto lo_val = lo_versions[0].descriptor.AsInlineInt();
        if (!hi_val || !lo_val) {
            return std::nullopt;
        }
        
        uint32_t file_id = static_cast<uint32_t>(*hi_val);
        uint32_t offset = static_cast<uint32_t>(*lo_val);
        
        std::string data;
        Status s = blob_mgr_->ReadBlob(file_id, offset, *size_val, &data);
        if (s.ok()) {
            stats_.blob_reads++;
            return data;
        }
    }
    
    return std::nullopt;
}

Status AutoBlobStorage::PutBinary(uint64_t entity_id, uint16_t col_id, const void* data, size_t size) {
    std::string str(static_cast<const char*>(data), size);
    return PutString(entity_id, col_id, str);
}

std::vector<uint8_t> AutoBlobStorage::GetBinary(uint64_t entity_id, uint16_t col_id) {
    auto str_opt = GetString(entity_id, col_id);
    if (!str_opt) {
        return {};
    }
    
    std::vector<uint8_t> result(str_opt->begin(), str_opt->end());
    return result;
}

Status AutoBlobStorage::SyncBlob() {
    if (blob_mgr_) {
        return blob_mgr_->Sync();
    }
    return Status::OK();
}

}  // namespace cedar
