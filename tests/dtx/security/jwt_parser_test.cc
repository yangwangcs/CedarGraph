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

TEST(JWTParser, RejectsMalformedJWT) {
  Authenticator auth;
  Authenticator::Config config;
  config.jwt_secret = "test-secret";
  config.accounts.push_back({"admin", "adminpass", {"admin"}});

  ASSERT_TRUE(auth.Initialize(config).ok());

  // Missing dots entirely
  auto result1 = auth.ParseJWT("notajwt");
  EXPECT_FALSE(result1.ok());

  // Only one dot
  auto result2 = auth.ParseJWT("part1.part2");
  EXPECT_FALSE(result2.ok());

  // Empty header
  auto result3 = auth.ParseJWT(".payload.signature");
  EXPECT_FALSE(result3.ok());

  // Empty payload
  auto result4 = auth.ParseJWT("header..signature");
  EXPECT_FALSE(result4.ok());
}

TEST(JWTParser, RejectsEmptySignature) {
  Authenticator auth;
  Authenticator::Config config;
  config.jwt_secret = "test-secret";
  config.accounts.push_back({"admin", "adminpass", {"admin"}});

  ASSERT_TRUE(auth.Initialize(config).ok());

  // Trailing dot with empty signature
  auto result1 = auth.ParseJWT("header.payload.");
  EXPECT_FALSE(result1.ok());

  // No signature at all (only two parts)
  auto result2 = auth.ParseJWT("header.payload");
  EXPECT_FALSE(result2.ok());
}

TEST(JWTParser, RoundTripValidToken) {
  Authenticator auth;
  Authenticator::Config config;
  config.jwt_secret = "test-secret";
  config.accounts.push_back({"admin", "adminpass", {"admin"}});

  ASSERT_TRUE(auth.Initialize(config).ok());

  // Create a valid token and generate JWT
  auto token_result = auth.Authenticate("admin", "adminpass");
  ASSERT_TRUE(token_result.ok());

  std::string jwt = auth.GenerateJWT(token_result.value());
  EXPECT_FALSE(jwt.empty());

  // Parse the valid JWT
  auto parsed = auth.ParseJWT(jwt);
  EXPECT_TRUE(parsed.ok());
}

TEST(JWTParser, HandlesQuotesInUsername) {
  Authenticator auth;
  Authenticator::Config config;
  config.jwt_secret = "test-secret";
  config.accounts.push_back({"user\"quote", "password", {"user"}});

  ASSERT_TRUE(auth.Initialize(config).ok());

  auto token_result = auth.Authenticate("user\"quote", "password");
  ASSERT_TRUE(token_result.ok());

  std::string jwt = auth.GenerateJWT(token_result.value());
  EXPECT_FALSE(jwt.empty());

  auto parsed = auth.ParseJWT(jwt);
  ASSERT_TRUE(parsed.ok());
  EXPECT_EQ(parsed.value().user_name, "user\"quote");
}

TEST(JWTParser, HandlesBackslashInUsername) {
  Authenticator auth;
  Authenticator::Config config;
  config.jwt_secret = "test-secret";
  config.accounts.push_back({"user\\backslash", "password", {"user"}});

  ASSERT_TRUE(auth.Initialize(config).ok());

  auto token_result = auth.Authenticate("user\\backslash", "password");
  ASSERT_TRUE(token_result.ok());

  std::string jwt = auth.GenerateJWT(token_result.value());
  EXPECT_FALSE(jwt.empty());

  auto parsed = auth.ParseJWT(jwt);
  ASSERT_TRUE(parsed.ok());
  EXPECT_EQ(parsed.value().user_name, "user\\backslash");
}

TEST(JWTParser, RejectsInjectionPayload) {
  Authenticator auth;
  Authenticator::Config config;
  config.jwt_secret = "test-secret";
  config.accounts.push_back({R"(",\"roles\":[\"admin\"])", "password", {"user"}});

  ASSERT_TRUE(auth.Initialize(config).ok());

  auto token_result = auth.Authenticate(R"(",\"roles\":[\"admin\"])", "password");
  ASSERT_TRUE(token_result.ok());
  EXPECT_FALSE(token_result.value().HasRole("admin"));

  std::string jwt = auth.GenerateJWT(token_result.value());
  EXPECT_FALSE(jwt.empty());

  auto parsed = auth.ParseJWT(jwt);
  ASSERT_TRUE(parsed.ok());
  EXPECT_EQ(parsed.value().user_name, R"(",\"roles\":[\"admin\"])");
  EXPECT_FALSE(parsed.value().HasRole("admin"));
}

}  // namespace security
}  // namespace dtx
}  // namespace cedar
