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
// MetaService Implementation
// =============================================================================

#ifndef CEDAR_DTX_META_SERVICE_IMPL_H_
#define CEDAR_DTX_META_SERVICE_IMPL_H_

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <vector>
#include <string>
#include <chrono>

#include "cedar/core/status.h"
#include "cedar/dtx/types.h"
#include "cedar/dtx/meta_service.h"

namespace cedar {
namespace dtx {

// =============================================================================
// Metadata Command Types (for Raft replication)
// =============================================================================

enum class MetaCommandType : uint8_t {
  kCreateSpace = 1,
  kDropSpace = 2,
  kRegisterNode = 3,
  kUpdateNodeStatus = 4,
  kUpdatePartitionLeader = 5,
  kUpdatePartitionAssignment = 6,
};

struct MetaCommand {
  MetaCommandType type;
  std::string data;  // Serialized command data
  
  std::string Serialize() const;
  static StatusOr<MetaCommand> Deserialize(const std::string& data);
};

// =============================================================================
// Metadata Service (Raft-aware wrapper - to be reimplemented with braft)
// =============================================================================
// NOTE: The old RaftMetaService and MetadataStore have been removed along with
// the custom raft layer. MetaCommand is retained for future braft-based
// reimplementation.
// =============================================================================

}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_META_SERVICE_IMPL_H_
