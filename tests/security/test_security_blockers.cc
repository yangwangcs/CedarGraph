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

namespace cedar {
namespace dtx {
namespace security {

TEST(SecurityBlockers, EmptyAccountsRejected) {
  Authenticator auth;
  Authenticator::Config config;
  config.jwt_secret = "test-secret";
  config.accounts.clear();

  auto status = auth.Initialize(config);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.ToString(), "Invalid argument: No accounts configured");
}

TEST(SecurityBlockers, JWTRoundTrip) {
  Authenticator auth;
  Authenticator::Config config;
  config.jwt_secret = "test-secret-key";
  config.accounts.push_back({"admin", "adminpass", {"admin"}});

  auto status = auth.Initialize(config);
  EXPECT_TRUE(status.ok());

  // Authenticate to get a token
  auto token_result = auth.Authenticate("admin", "adminpass");
  EXPECT_TRUE(token_result.ok());

  auto token = token_result.value();
  EXPECT_FALSE(token.token_id.empty());
  EXPECT_EQ(token.user_id, "admin");

  // Generate JWT
  std::string jwt = auth.GenerateJWT(token);
  EXPECT_FALSE(jwt.empty());

  // Verify JWT has three parts separated by dots
  size_t first_dot = jwt.find('.');
  size_t second_dot = jwt.find('.', first_dot + 1);
  EXPECT_NE(first_dot, std::string::npos);
  EXPECT_NE(second_dot, std::string::npos);
  EXPECT_EQ(jwt.find('.', second_dot + 1), std::string::npos);

  // Parse and validate JWT
  auto parsed_result = auth.ParseJWT(jwt);
  EXPECT_TRUE(parsed_result.ok());

  auto parsed = parsed_result.value();
  EXPECT_EQ(parsed.token_id, token.token_id);
  EXPECT_EQ(parsed.user_id, token.user_id);
  EXPECT_EQ(parsed.user_name, token.user_name);
}

TEST(SecurityBlockers, JWTTamperedSignatureFails) {
  Authenticator auth;
  Authenticator::Config config;
  config.jwt_secret = "test-secret-key";
  config.accounts.push_back({"admin", "adminpass", {"admin"}});

  auto status = auth.Initialize(config);
  EXPECT_TRUE(status.ok());

  auto token_result = auth.Authenticate("admin", "adminpass");
  EXPECT_TRUE(token_result.ok());

  std::string jwt = auth.GenerateJWT(token_result.value());

  // Tamper with the last character of the signature
  jwt.back() = (jwt.back() == 'x') ? 'y' : 'x';

  auto parsed_result = auth.ParseJWT(jwt);
  EXPECT_FALSE(parsed_result.ok());
}

TEST(SecurityBlockers, ConfigAccountsLoaded) {
  Authenticator auth;
  Authenticator::Config config;
  config.jwt_secret = "test-secret";
  config.accounts.push_back({"user1", "pass1", {"readwrite"}});
  config.accounts.push_back({"user2", "pass2", {"readonly"}});

  auto status = auth.Initialize(config);
  EXPECT_TRUE(status.ok());

  // user1 should authenticate successfully
  auto t1 = auth.Authenticate("user1", "pass1");
  EXPECT_TRUE(t1.ok());
  EXPECT_TRUE(t1.value().HasRole("readwrite"));

  // user2 should authenticate successfully
  auto t2 = auth.Authenticate("user2", "pass2");
  EXPECT_TRUE(t2.ok());
  EXPECT_TRUE(t2.value().HasRole("readonly"));

  // Unknown user should fail
  auto t3 = auth.Authenticate("unknown", "pass");
  EXPECT_FALSE(t3.ok());
}

}  // namespace security
}  // namespace dtx
}  // namespace cedar
