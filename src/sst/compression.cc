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

#include "cedar/sst/compression.h"

#include <cstring>

// LZ4
#if __has_include(<lz4.h>)
  #include <lz4.h>
  #define FERN_HAS_LZ4 1
#else
  #define FERN_HAS_LZ4 0
  #warning "LZ4 not found, compression will be disabled"
#endif

// Zstd
#if __has_include(<zstd.h>)
  #include <zstd.h>
  #define FERN_HAS_ZSTD 1
#else
  #define FERN_HAS_ZSTD 0
#endif

namespace cedar {

Status Compression::Compress(CedarCompressionType type,
                             const Slice& input,
                             std::string* output,
                             CedarCompressionType* actual_type) {
  if (type == CedarCompressionType::None || !IsSupported(type)) {
    output->assign(input.data(), input.size());
    *actual_type = CedarCompressionType::None;
    return Status::OK();
  }
  
#if FERN_HAS_LZ4
  if (type == CedarCompressionType::LZ4) {
    // LZ4 压缩
    int max_compressed_size = LZ4_compressBound(static_cast<int>(input.size()));
    if (max_compressed_size <= 0) {
      return Status::InvalidArgument("Compression", "input too large");
    }
    
    output->resize(max_compressed_size);
    int compressed_size = LZ4_compress_default(
        input.data(),
        &(*output)[0],
        static_cast<int>(input.size()),
        max_compressed_size);
    
    if (compressed_size <= 0) {
      return Status::Corruption("Compression", "LZ4 compress failed");
    }
    
    // 如果压缩后更大，使用原始数据
    if (static_cast<size_t>(compressed_size) >= input.size()) {
      output->assign(input.data(), input.size());
      *actual_type = CedarCompressionType::None;
    } else {
      output->resize(compressed_size);
      *actual_type = CedarCompressionType::LZ4;
    }
    
    return Status::OK();
  }
#endif
  
#if FERN_HAS_ZSTD
  if (type == CedarCompressionType::Zstd) {
    size_t max_compressed = ZSTD_compressBound(input.size());
    output->resize(max_compressed);
    size_t compressed_size = ZSTD_compress(
        &(*output)[0], max_compressed,
        input.data(), input.size(),
        1);  // Level 1 = fast mode (~780 MB/s)
    
    if (ZSTD_isError(compressed_size)) {
      return Status::Corruption("Compression", "Zstd compress failed");
    }
    
    if (compressed_size >= input.size()) {
      output->assign(input.data(), input.size());
      *actual_type = CedarCompressionType::None;
    } else {
      output->resize(compressed_size);
      *actual_type = CedarCompressionType::Zstd;
    }
    
    return Status::OK();
  }
#endif
  
  // 不支持的其他压缩类型，返回未压缩
  output->assign(input.data(), input.size());
  *actual_type = CedarCompressionType::None;
  return Status::OK();
}

Status Compression::Decompress(CedarCompressionType type,
                               const Slice& input,
                               std::string* output,
                               size_t uncompressed_size) {
  if (type == CedarCompressionType::None) {
    output->assign(input.data(), input.size());
    return Status::OK();
  }
  
#if FERN_HAS_LZ4
  if (type == CedarCompressionType::LZ4) {
    // Use generous bound: LZ4 compressBound gives max compressed size for given input,
    // but for decompression we need the actual output size. Use 4x compressed as estimate.
    size_t alloc_size = std::max(uncompressed_size, input.size() * 4);
    output->resize(alloc_size);
    int result = LZ4_decompress_safe(
        input.data(),
        &(*output)[0],
        static_cast<int>(input.size()),
        static_cast<int>(alloc_size));
    
    if (result < 0) {
      return Status::Corruption("Compression", "LZ4 decompress failed");
    }
    output->resize(static_cast<size_t>(result));
    return Status::OK();
  }
#endif
  
#if FERN_HAS_ZSTD
  if (type == CedarCompressionType::Zstd) {
    // Get actual uncompressed size from Zstd frame header
    unsigned long long frame_size = ZSTD_getFrameContentSize(input.data(), input.size());
    size_t actual_size;
    if (frame_size != ZSTD_CONTENTSIZE_UNKNOWN && frame_size != ZSTD_CONTENTSIZE_ERROR) {
      actual_size = static_cast<size_t>(frame_size);
    } else {
      actual_size = std::max(uncompressed_size, input.size() * 4);
    }
    output->resize(actual_size);
    size_t result = ZSTD_decompress(
        &(*output)[0], actual_size,
        input.data(), input.size());
    
    if (ZSTD_isError(result)) {
      return Status::Corruption("Compression", "Zstd decompress failed");
    }
    output->resize(result);
    return Status::OK();
  }
#endif
  
  return Status::NotSupported("Compression", "unsupported compression type");
}

const char* Compression::TypeName(CedarCompressionType type) {
  switch (type) {
    case CedarCompressionType::None:
      return "none";
    case CedarCompressionType::LZ4:
      return "lz4";
    case CedarCompressionType::Zstd:
      return "zstd";
    default:
      return "unknown";
  }
}

size_t Compression::MaxCompressedSize(CedarCompressionType type, size_t uncompressed_size) {
#if FERN_HAS_LZ4
  if (type == CedarCompressionType::LZ4) {
    return LZ4_compressBound(static_cast<int>(uncompressed_size));
  }
#endif
#if FERN_HAS_ZSTD
  if (type == CedarCompressionType::Zstd) {
    return ZSTD_compressBound(uncompressed_size);
  }
#endif
  return uncompressed_size;
}

bool Compression::IsSupported(CedarCompressionType type) {
  switch (type) {
    case CedarCompressionType::None:
      return true;
    case CedarCompressionType::LZ4:
#if FERN_HAS_LZ4
      return true;
#else
      return false;
#endif
    case CedarCompressionType::Zstd:
#if FERN_HAS_ZSTD
      return true;
#else
      return false;
#endif
    default:
      return false;
  }
}

// BlockStats 实现

void BlockStats::EncodeTo(std::string* dst) const {
  char buf[40];
  // 8 bytes each
  memcpy(buf, &min_entity_id, 8);
  memcpy(buf + 8, &max_entity_id, 8);
  memcpy(buf + 16, &min_timestamp, 8);
  memcpy(buf + 24, &max_timestamp, 8);
  // 4 bytes each
  memcpy(buf + 32, &null_count, 4);
  memcpy(buf + 36, &distinct_count, 4);
  dst->append(buf, 40);
}

Status BlockStats::DecodeFrom(Slice* input) {
  if (input->size() < 40) {
    return Status::Corruption("BlockStats", "truncated");
  }
  
  const char* p = input->data();
  memcpy(&min_entity_id, p, 8);
  memcpy(&max_entity_id, p + 8, 8);
  memcpy(&min_timestamp, p + 16, 8);
  memcpy(&max_timestamp, p + 24, 8);
  memcpy(&null_count, p + 32, 4);
  memcpy(&distinct_count, p + 36, 4);
  
  input->remove_prefix(40);
  return Status::OK();
}

}  // namespace cedar
