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
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>

namespace cedar {
namespace dtx {
namespace security {

static std::string Base64UrlEncode(const std::string& input) {
  BIO* b64 = BIO_new(BIO_f_base64());
  BIO* bio = BIO_new(BIO_s_mem());
  b64 = BIO_push(b64, bio);
  BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
  BIO_write(b64, input.data(), static_cast<int>(input.size()));
  BIO_flush(b64);

  BUF_MEM* buffer_ptr;
  BIO_get_mem_ptr(b64, &buffer_ptr);

  std::string encoded(buffer_ptr->data, buffer_ptr->length);
  BIO_free_all(b64);

  for (size_t i = 0; i < encoded.size(); ++i) {
    if (encoded[i] == '+') encoded[i] = '-';
    else if (encoded[i] == '/') encoded[i] = '_';
  }
  while (!encoded.empty() && encoded.back() == '=') {
    encoded.pop_back();
  }
  return encoded;
}

static std::string MakeTestJWT(const std::string& payload_json,
                               const std::string& secret) {
  std::string header = R"({"alg":"HS256","typ":"JWT"})";
  std::string encoded_header = Base64UrlEncode(header);
  std::string encoded_payload = Base64UrlEncode(payload_json);

  std::string signature_input = encoded_header + "." + encoded_payload;
  unsigned char signature[EVP_MAX_MD_SIZE];
  unsigned int signature_len;
  HMAC(EVP_sha256(), secret.data(), static_cast<int>(secret.size()),
       reinterpret_cast<const unsigned char*>(signature_input.data()),
       static_cast<int>(signature_input.size()), signature, &signature_len);

  std::string encoded_signature = Base64UrlEncode(
      std::string(reinterpret_cast<const char*>(signature), signature_len));

  return encoded_header + "." + encoded_payload + "." + encoded_signature;
}

class JWTParserNlohmannTest : public ::testing::Test {
 protected:
  void SetUp() override {
    config_.jwt_secret = "test-secret";
    config_.accounts.push_back({"admin", "adminpass", {"admin"}});
    ASSERT_TRUE(auth_.Initialize(config_).ok());
  }

  Authenticator auth_;
  Authenticator::Config config_;
};

TEST_F(JWTParserNlohmannTest, ParsesValidPayloadWithAllFields) {
  std::string jwt = MakeTestJWT(
      R"({"sub":"user123","name":"Test User","exp":1893456000,"jti":"token-abc"})",
      config_.jwt_secret);

  auto result = auth_.ParseJWT(jwt);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_EQ(result.value().user_id, "user123");
  EXPECT_EQ(result.value().user_name, "Test User");
  EXPECT_EQ(result.value().token_id, "token-abc");
  EXPECT_FALSE(result.value().IsExpired());
}

TEST_F(JWTParserNlohmannTest, FallsBackToUserIdWhenSubMissing) {
  std::string jwt = MakeTestJWT(
      R"({"user_id":"fallback_user","name":"Fallback","jti":"token-xyz"})",
      config_.jwt_secret);

  auto result = auth_.ParseJWT(jwt);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_EQ(result.value().user_id, "fallback_user");
  EXPECT_EQ(result.value().user_name, "Fallback");
}

TEST_F(JWTParserNlohmannTest, PrefersSubOverUserId) {
  std::string jwt = MakeTestJWT(
      R"({"sub":"sub_user","user_id":"uid_user","jti":"token-123"})",
      config_.jwt_secret);

  auto result = auth_.ParseJWT(jwt);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_EQ(result.value().user_id, "sub_user");
}

TEST_F(JWTParserNlohmannTest, HandlesMissingOptionalFields) {
  std::string jwt = MakeTestJWT(
      R"({"sub":"minimal_user"})",
      config_.jwt_secret);

  auto result = auth_.ParseJWT(jwt);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_EQ(result.value().user_id, "minimal_user");
  EXPECT_TRUE(result.value().user_name.empty());
  EXPECT_TRUE(result.value().token_id.empty());
}

TEST_F(JWTParserNlohmannTest, HandlesNumericExpField) {
  std::string jwt = MakeTestJWT(
      R"({"sub":"num_user","exp":1893456000})",
      config_.jwt_secret);

  auto result = auth_.ParseJWT(jwt);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_EQ(result.value().user_id, "num_user");
  EXPECT_FALSE(result.value().IsExpired());
}

TEST_F(JWTParserNlohmannTest, RejectsInvalidJSONPayload) {
  std::string jwt = MakeTestJWT(
      R"({"sub":"bad_user", invalid })",
      config_.jwt_secret);

  auto result = auth_.ParseJWT(jwt);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.status().ToString().find("parse error"), std::string::npos);
}

TEST_F(JWTParserNlohmannTest, HandlesComplexJSONEscapes) {
  std::string jwt = MakeTestJWT(
      R"({"sub":"user\u0041","name":"Test\nLine","jti":"id-1"})",
      config_.jwt_secret);

  auto result = auth_.ParseJWT(jwt);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_EQ(result.value().user_id, "userA");  // \u0041 = 'A'
  EXPECT_EQ(result.value().user_name, "Test\nLine");
}

TEST_F(JWTParserNlohmannTest, HandlesBooleanAndNullValues) {
  std::string jwt = MakeTestJWT(
      R"({"sub":"bool_user","active":true,"deleted":false,"meta":null,"jti":"id-2"})",
      config_.jwt_secret);

  auto result = auth_.ParseJWT(jwt);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_EQ(result.value().user_id, "bool_user");
  EXPECT_EQ(result.value().token_id, "id-2");
}

TEST_F(JWTParserNlohmannTest, RoundTripViaGenerateJWT) {
  auto token_result = auth_.Authenticate("admin", "adminpass");
  ASSERT_TRUE(token_result.ok());

  std::string jwt = auth_.GenerateJWT(token_result.value());
  EXPECT_FALSE(jwt.empty());

  auto parsed = auth_.ParseJWT(jwt);
  ASSERT_TRUE(parsed.ok()) << parsed.status().ToString();
  EXPECT_EQ(parsed.value().user_id, token_result.value().user_id);
  EXPECT_EQ(parsed.value().user_name, token_result.value().user_name);
  EXPECT_EQ(parsed.value().token_id, token_result.value().token_id);
}

TEST_F(JWTParserNlohmannTest, RejectsTamperedPayload) {
  std::string jwt = MakeTestJWT(
      R"({"sub":"user1"})",
      config_.jwt_secret);

  // Tamper with the payload portion
  size_t first_dot = jwt.find('.');
  size_t second_dot = jwt.find('.', first_dot + 1);
  std::string tampered = jwt.substr(0, first_dot + 1) +
                         Base64UrlEncode(R"({"sub":"hacker"})") +
                         jwt.substr(second_dot);

  auto result = auth_.ParseJWT(tampered);
  EXPECT_FALSE(result.ok());
}

}  // namespace security
}  // namespace dtx
}  // namespace cedar
