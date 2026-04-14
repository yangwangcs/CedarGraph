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

#ifndef CEDAR_DTX_RAFT_RAFT_NODE_FACTORY_H_
#define CEDAR_DTX_RAFT_RAFT_NODE_FACTORY_H_

#include <memory>

#include "cedar/dtx/raft/raft_interface.h"

namespace cedar {
namespace dtx {
namespace raft {

// =============================================================================
// Raft Node Types
// =============================================================================

enum class RaftNodeType {
  kDefault = 0,    // Use default (set via SetDefaultRaftNodeType)
  kEmbedded = 1,   // Self-contained embedded Raft (recommended)
  kBraft = 2,      // braft-based (requires external library)
  kMemory = 3,     // Memory-only (for testing only)
};

// =============================================================================
// Factory Functions
// =============================================================================

// Set the default Raft node type for all new nodes
void SetDefaultRaftNodeType(RaftNodeType type);

// Create a Raft node with the specified type (or default if kDefault)
std::unique_ptr<RaftNode> CreateRaftNode(const RaftConfig& config,
                                          RaftNodeType type = RaftNodeType::kDefault);

}  // namespace raft
}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_RAFT_RAFT_NODE_FACTORY_H_
