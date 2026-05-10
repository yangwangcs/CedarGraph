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

// =============================================================================
// Portable Little-Endian Serialization Utilities
// =============================================================================
// Explicit little-endian read/write to ensure snapshot/serialization
// portability across big-endian and little-endian architectures.
// =============================================================================

#ifndef CEDAR_CORE_ENDIAN_H_
#define CEDAR_CORE_ENDIAN_H_

#include <cstdint>
#include <cstring>
#include <string>

namespace cedar {

inline void WriteLE32(std::string& out, uint32_t value) {
  out.push_back(static_cast<char>(value & 0xFF));
  out.push_back(static_cast<char>((value >> 8) & 0xFF));
  out.push_back(static_cast<char>((value >> 16) & 0xFF));
  out.push_back(static_cast<char>((value >> 24) & 0xFF));
}

inline void WriteLE64(std::string& out, uint64_t value) {
  out.push_back(static_cast<char>(value & 0xFF));
  out.push_back(static_cast<char>((value >> 8) & 0xFF));
  out.push_back(static_cast<char>((value >> 16) & 0xFF));
  out.push_back(static_cast<char>((value >> 24) & 0xFF));
  out.push_back(static_cast<char>((value >> 32) & 0xFF));
  out.push_back(static_cast<char>((value >> 40) & 0xFF));
  out.push_back(static_cast<char>((value >> 48) & 0xFF));
  out.push_back(static_cast<char>((value >> 56) & 0xFF));
}

inline uint32_t ReadLE32(const char* data, size_t& pos) {
  uint32_t value = static_cast<uint8_t>(data[pos]);
  value |= static_cast<uint32_t>(static_cast<uint8_t>(data[pos + 1])) << 8;
  value |= static_cast<uint32_t>(static_cast<uint8_t>(data[pos + 2])) << 16;
  value |= static_cast<uint32_t>(static_cast<uint8_t>(data[pos + 3])) << 24;
  pos += 4;
  return value;
}

inline uint64_t ReadLE64(const char* data, size_t& pos) {
  uint64_t value = static_cast<uint8_t>(data[pos]);
  value |= static_cast<uint64_t>(static_cast<uint8_t>(data[pos + 1])) << 8;
  value |= static_cast<uint64_t>(static_cast<uint8_t>(data[pos + 2])) << 16;
  value |= static_cast<uint64_t>(static_cast<uint8_t>(data[pos + 3])) << 24;
  value |= static_cast<uint64_t>(static_cast<uint8_t>(data[pos + 4])) << 32;
  value |= static_cast<uint64_t>(static_cast<uint8_t>(data[pos + 5])) << 40;
  value |= static_cast<uint64_t>(static_cast<uint8_t>(data[pos + 6])) << 48;
  value |= static_cast<uint64_t>(static_cast<uint8_t>(data[pos + 7])) << 56;
  pos += 8;
  return value;
}

}  // namespace cedar

#endif  // CEDAR_CORE_ENDIAN_H_
