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

#include "cedar/dtx/meta_service_impl.h"

#include <chrono>

namespace cedar {
namespace dtx {

// =============================================================================
// MetaCommand Serialization
// =============================================================================

std::string MetaCommand::Serialize() const {
  // Simple binary format: [1 byte type][4 bytes data length][data]
  std::string result;
  result.push_back(static_cast<char>(type));
  
  uint32_t len = static_cast<uint32_t>(data.size());
  result.append(reinterpret_cast<const char*>(&len), sizeof(len));
  result.append(data);
  
  return result;
}

StatusOr<MetaCommand> MetaCommand::Deserialize(const std::string& data) {
  if (data.size() < 5) {
    return Status::InvalidArgument("Command data too short");
  }
  
  MetaCommand cmd;
  cmd.type = static_cast<MetaCommandType>(data[0]);
  
  uint32_t len;
  memcpy(&len, data.data() + 1, sizeof(len));
  
  if (data.size() < 5 + len) {
    return Status::InvalidArgument("Command data length mismatch");
  }
  
  cmd.data = data.substr(5, len);
  return cmd;
}

}  // namespace dtx
}  // namespace cedar
