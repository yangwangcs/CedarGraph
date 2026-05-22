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

#include <iostream>

#include "cedar/driver/transaction_types.h"

namespace cedar {
namespace driver {

std::string ConflictInfo::ToString() const {
  std::string result;
  switch (type) {
    case ConflictType::kNone:
      result = "No conflict";
      break;
    case ConflictType::kReadWrite:
      result = "Read-write conflict (OCC validation failed)";
      break;
    case ConflictType::kWriteWrite:
      result = "Write-write conflict on entity " + std::to_string(entity_id);
      break;
    case ConflictType::kConstraintViolation:
      result = "Constraint violation: " + message;
      break;
    case ConflictType::kTimeout:
      result = "Transaction timeout";
      break;
    default:
      std::cerr << "[ConflictInfo] Unknown conflict type" << std::endl;
      break;
  }
  
  if (!message.empty() && type != ConflictType::kConstraintViolation) {
    result += " - " + message;
  }
  
  return result;
}

}  // namespace driver
}  // namespace cedar
