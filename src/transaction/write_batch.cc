// Copyright (c) 2024 Cedar Storage Engine Authors.
// WriteBatch implementation for Cedar.

#include "cedar/transaction/write_batch.h"

#include <cstring>

namespace cedar {

// WriteBatch 格式:
// [header: 1 byte][count: 4 bytes][entries...]
// header: 0x01 = Cedar format with CedarKey
// entry: [type: 1 byte][key_size: 2 bytes][key: 32 bytes][value_size: 4 bytes][value: variable]

static const char kCedarHeader = 0x01;

WriteBatch::WriteBatch() {
  rep_.push_back(kCedarHeader);
  // Reserve space for count (4 bytes, little endian)
  rep_.append(4, 0);
}

WriteBatch::~WriteBatch() = default;

void WriteBatch::Put(const Slice& key, const Slice& value) {
  // Simple key-value put (legacy format)
  // Not used for CedarKey operations
  (void)key;
  (void)value;
}

void WriteBatch::Put(const CedarKey& key, const Descriptor& value) {
  // Cedar-specific: Store with CedarKey (32 bytes) and Descriptor
  // Entry format:
  // [type: 1 byte = 0x01]
  // [key: 32 bytes (CedarKey::Encode())]
  // [value_type: 1 byte]
  // [value: variable based on type]
  
  rep_.push_back(0x01);  // Type: CedarKey entry
  
  // Encode CedarKey (32 bytes)
  std::string key_bytes = key.Encode();
  rep_.append(key_bytes);
  
  // Encode Descriptor (8 bytes raw value)
  // Descriptor format: [raw_value: 8 bytes]
  uint64_t raw_value = value.AsRaw();
  rep_.append(reinterpret_cast<const char*>(&raw_value), sizeof(raw_value));
  
  // Update count
  int count = Count() + 1;
  rep_[1] = static_cast<char>(count & 0xFF);
  rep_[2] = static_cast<char>((count >> 8) & 0xFF);
  rep_[3] = static_cast<char>((count >> 16) & 0xFF);
  rep_[4] = static_cast<char>((count >> 24) & 0xFF);
}

void WriteBatch::PutVertex(uint64_t entity_id, uint16_t col_id,
                           Timestamp timestamp, const Descriptor& value) {
  // Create CedarKey for vertex
  CedarKey key = CedarKey::Vertex(entity_id, col_id, timestamp);
  Put(key, value);
}

void WriteBatch::PutEdge(uint64_t src_id, uint64_t dst_id, uint16_t edge_type,
                         Timestamp timestamp, const Descriptor& value) {
  // Create CedarKey for edge
  CedarKey key = CedarKey::EdgeOut(src_id, dst_id, edge_type, timestamp);
  Put(key, value);
}

void WriteBatch::Delete(const Slice& key) {
  (void)key;
  // Legacy delete, not used for CedarKey
}

void WriteBatch::Delete(const CedarKey& key) {
  // Cedar-specific delete
  rep_.push_back(0x02);  // Type: CedarKey delete
  
  std::string key_bytes = key.Encode();
  rep_.append(key_bytes);
  
  // Update count
  int count = Count() + 1;
  rep_[1] = static_cast<char>(count & 0xFF);
  rep_[2] = static_cast<char>((count >> 8) & 0xFF);
  rep_[3] = static_cast<char>((count >> 16) & 0xFF);
  rep_[4] = static_cast<char>((count >> 24) & 0xFF);
}

void WriteBatch::Clear() {
  rep_.clear();
  rep_.push_back(kCedarHeader);
  rep_.append(4, 0);
}

Status WriteBatch::Iterate(Handler* handler) const {
  if (rep_.size() < 5) {
    return Status::OK();
  }
  
  const char* ptr = rep_.data() + 5;  // Skip header and count
  const char* end = rep_.data() + rep_.size();
  
  while (ptr < end) {
    char type = *ptr++;
    
    if (type == 0x01) {
      // CedarKey put
      if (ptr + 32 > end) {
        return Status::Corruption("WriteBatch", "truncated key");
      }
      CedarKey key = CedarKey::Decode(ptr);
      ptr += 32;
      
      // Read 8-byte descriptor value (matches Put() format)
      if (ptr + 8 > end) {
        return Status::Corruption("WriteBatch", "truncated value");
      }
      uint64_t raw_value;
      std::memcpy(&raw_value, ptr, sizeof(uint64_t));
      ptr += 8;
      
      Descriptor desc(raw_value);
      handler->Put(key.Encode(), Slice(reinterpret_cast<const char*>(&raw_value), sizeof(raw_value)));
    } else if (type == 0x02) {
      // CedarKey delete
      if (ptr + 32 > end) {
        return Status::Corruption("WriteBatch", "truncated delete key");
      }
      CedarKey key = CedarKey::Decode(ptr);
      ptr += 32;
      
      handler->Delete(key.Encode());
    }
  }
  
  return Status::OK();
}

std::string WriteBatch::Encode() const {
  return rep_;
}

Status WriteBatch::Decode(const Slice& data) {
  if (data.size() < 5) {
    return Status::Corruption("WriteBatch", "truncated");
  }
  
  rep_.assign(data.data(), data.size());
  return Status::OK();
}

size_t WriteBatch::GetDataSize() const {
  return rep_.size();
}

int WriteBatch::Count() const {
  if (rep_.size() < 5) return 0;
  return static_cast<unsigned char>(rep_[1]) |
         (static_cast<unsigned char>(rep_[2]) << 8) |
         (static_cast<unsigned char>(rep_[3]) << 16) |
         (static_cast<unsigned char>(rep_[4]) << 24);
}

bool WriteBatch::HasTemporalOps() const {
  // Check if batch contains CedarKey operations
  if (rep_.size() < 5) return false;
  
  const char* ptr = rep_.data() + 5;
  const char* end = rep_.data() + rep_.size();
  
  while (ptr < end) {
    char type = *ptr;
    if (type == 0x01 || type == 0x02) {
      return true;
    }
    ptr++;
  }
  
  return false;
}

size_t WriteBatch::ApproximateSize() const {
  return rep_.size();
}

std::vector<std::pair<CedarKey, Descriptor>> WriteBatch::GetTemporalOps() const {
  std::vector<std::pair<CedarKey, Descriptor>> result;
  
  if (rep_.size() < 5) return result;
  
  const char* ptr = rep_.data() + 5;
  const char* end = rep_.data() + rep_.size();
  
  while (ptr < end) {
    char type = *ptr++;
    
    if (type == 0x01 && ptr + 32 <= end) {
      CedarKey key = CedarKey::Decode(ptr);
      ptr += 32;
      
      // Skip descriptor for now
      if (ptr < end) {
        ptr++;  // Skip type
        ptr += 8;  // Skip data
      }
      
      result.emplace_back(key, Descriptor());
    }
  }
  
  return result;
}

// Handler base class implementation
WriteBatch::Handler::~Handler() = default;

}  // namespace cedar
