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

TEST(RBAC, ExactMatchAllowsAccess) {
  Authorizer auth;
  ASSERT_TRUE(auth.InitializeDefaultRoles().ok());

  Role role;
  role.name = "exact";
  role.permissions = Permission::kRead;
  role.allowed_resources = {"space1"};
  ASSERT_TRUE(auth.AddRole(role).ok());

  AuthToken token;
  token.roles = {"exact"};

  EXPECT_TRUE(auth.CanRead(token, "space1"));
  EXPECT_FALSE(auth.CanRead(token, "space1_extra"));
  EXPECT_FALSE(auth.CanRead(token, "extra_space1"));
}

TEST(RBAC, SubstringNoLongerMatches) {
  Authorizer auth;
  ASSERT_TRUE(auth.InitializeDefaultRoles().ok());

  Role role;
  role.name = "substr";
  role.permissions = Permission::kRead;
  role.allowed_resources = {"space"};
  ASSERT_TRUE(auth.AddRole(role).ok());

  AuthToken token;
  token.roles = {"substr"};

  EXPECT_TRUE(auth.CanRead(token, "space"));
  EXPECT_FALSE(auth.CanRead(token, "myspace"));
  EXPECT_FALSE(auth.CanRead(token, "space2"));
}

TEST(RBAC, GlobWildcardMatches) {
  Authorizer auth;
  ASSERT_TRUE(auth.InitializeDefaultRoles().ok());

  Role role;
  role.name = "glob";
  role.permissions = Permission::kRead;
  role.allowed_resources = {"space*"};
  ASSERT_TRUE(auth.AddRole(role).ok());

  AuthToken token;
  token.roles = {"glob"};

  EXPECT_TRUE(auth.CanRead(token, "space1"));
  EXPECT_TRUE(auth.CanRead(token, "spaceABC"));
  EXPECT_TRUE(auth.CanRead(token, "space"));
  EXPECT_FALSE(auth.CanRead(token, "myspace"));
}

}  // namespace security
}  // namespace dtx
}  // namespace cedar
