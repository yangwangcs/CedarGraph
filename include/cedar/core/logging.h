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

#ifndef FERN_CORE_LOGGING_H_
#define FERN_CORE_LOGGING_H_

#include <iostream>

// Minimal logging macros for CedarGraph.
// No external logging dependency (glog/spdlog) is used.
// These write to stderr with a consistent prefix.

#define CEDAR_LOG_ERROR() \
  std::cerr << "[CEDAR ERROR] " << __FILE__ << ":" << __LINE__ << " "

#define CEDAR_LOG_WARN() \
  std::cerr << "[CEDAR WARN]  " << __FILE__ << ":" << __LINE__ << " "

#define CEDAR_LOG_INFO() \
  std::cerr << "[CEDAR INFO]  " << __FILE__ << ":" << __LINE__ << " "

#endif  // FERN_CORE_LOGGING_H_
