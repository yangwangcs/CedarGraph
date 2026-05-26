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

#include <gtest/gtest.h>
#include "cedar/dtx/raft/grpc_tls.h"

using cedar::dtx::raft::TlsConfig;
using cedar::dtx::raft::TlsCredentialFactory;

TEST(TlsCredentialFactory, MissingServerCertReturnsError) {
  TlsConfig config;
  config.enabled = true;
  config.server_cert_file = "/nonexistent/cert.pem";
  config.server_key_file = "/nonexistent/key.pem";
  auto result = TlsCredentialFactory::CreateServerCredentials(config);
  EXPECT_FALSE(result.ok());
}

TEST(TlsCredentialFactory, DisabledReturnsInsecure) {
  TlsConfig config;
  config.enabled = false;
  auto result = TlsCredentialFactory::CreateServerCredentials(config);
  EXPECT_TRUE(result.ok());
  EXPECT_NE(result.ValueOrDie(), nullptr);
}

TEST(TlsCredentialFactory, MissingClientCertReturnsError) {
  TlsConfig config;
  config.enabled = true;
  config.mtls_enabled = true;
  config.client_cert_file = "/nonexistent/client_cert.pem";
  config.client_key_file = "/nonexistent/client_key.pem";
  auto result = TlsCredentialFactory::CreateClientCredentials(config);
  EXPECT_FALSE(result.ok());
}

TEST(TlsCredentialFactory, DisabledClientReturnsInsecure) {
  TlsConfig config;
  config.enabled = false;
  auto result = TlsCredentialFactory::CreateClientCredentials(config);
  EXPECT_TRUE(result.ok());
  EXPECT_NE(result.ValueOrDie(), nullptr);
}
