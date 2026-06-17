// Copyright 2025 The Cedar Authors
// Shared hash utilities

#ifndef CEDAR_UTILS_HASH_UTILS_H_
#define CEDAR_UTILS_HASH_UTILS_H_

#include <cstdint>
#include <string>
#include <functional>

namespace cedar {
namespace utils {

// Convert property name to column ID (12-bit hash)
inline uint16_t PropertyNameToColumnId(const std::string& name) {
  return static_cast<uint16_t>(std::hash<std::string>{}(name) & 0x0FFF);
}

} // namespace utils
} // namespace cedar

#endif // CEDAR_UTILS_HASH_UTILS_H_
