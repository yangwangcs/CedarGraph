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

#include "cedar/dtx/raft/raft_node_factory.h"

#include <memory>

#include "cedar/dtx/raft/raft_interface.h"

#ifdef CEDAR_WITH_BRAFT
#include "cedar/dtx/raft/braft_node.h"
#endif

namespace cedar {
namespace dtx {
namespace raft {

// =============================================================================
// Factory Implementation
// =============================================================================

RaftNodeType g_default_node_type = RaftNodeType::kBraft;

void SetDefaultRaftNodeType(RaftNodeType type) {
  g_default_node_type = type;
}

std::unique_ptr<RaftNode> CreateRaftNode(const RaftConfig& config, 
                                          RaftNodeType type) {
  // Use default type if not specified
  if (type == RaftNodeType::kDefault) {
    type = g_default_node_type;
  }
  
  std::unique_ptr<RaftNode> node;
  
  switch (type) {
#ifdef CEDAR_WITH_BRAFT
    case RaftNodeType::kBraft: {
      // Legacy unified RaftNode interface is deprecated.
      // MetaD uses BRaftNode directly (see tools/metad.cc).
      // StorageD uses BraftPartitionNode directly (see src/dtx/storage/).
      // CreateRaftNode() is kept for backward compatibility only.
      break;
    }
#endif
    default: {
      break;
    }
  }
  
  return node;
}

}  // namespace raft
}  // namespace dtx
}  // namespace cedar
