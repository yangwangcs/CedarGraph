// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#ifndef CEDAR_CDC_RPC_LIMITS_H_
#define CEDAR_CDC_RPC_LIMITS_H_

#include <cstdint>

namespace cedar::cdc {

inline constexpr uint32_t kMaxCdcRpcRecords = 4096;
inline constexpr uint64_t kMaxCdcRpcBytes = 4ULL * 1024ULL * 1024ULL;

}  // namespace cedar::cdc

#endif  // CEDAR_CDC_RPC_LIMITS_H_
