// Copyright 2026 CedarGraph Authors
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

#include "cedar/dtx/security.h"

#include <gtest/gtest.h>
#include <iostream>

namespace cedar {
namespace dtx {
namespace security {

TEST(AuditLog, RejectsDirectoryTraversal) {
  AuditLogger logger;
  AuditLogger::Config config;
  config.log_file = "/tmp/../etc/passwd";
  config.log_to_console = false;

  auto status = logger.Initialize(config);
  EXPECT_FALSE(status.ok());
  EXPECT_NE(status.ToString().find("directory traversal"), std::string::npos);
}

TEST(AuditLog, RejectsPathOutsidePrefix) {
  AuditLogger logger;
  AuditLogger::Config config;
  config.log_file = "/tmp/audit.log";
  config.allowed_log_prefix = "/var/log/cedar";
  config.log_to_console = false;

  auto status = logger.Initialize(config);
  EXPECT_FALSE(status.ok());
  EXPECT_NE(status.ToString().find("must start with"), std::string::npos);
}

}  // namespace security
}  // namespace dtx
}  // namespace cedar
