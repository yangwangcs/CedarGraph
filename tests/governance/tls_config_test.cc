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

TEST(TlsConfigTest, EmptyTlsConfigReturnsNullCredentials) {
  TlsConfig config;
  config.enabled = true;  // TLS enabled but no certs provided

  auto server_creds = TlsCredentialFactory::CreateServerCredentials(config);
  EXPECT_EQ(server_creds, nullptr);
}

TEST(TlsConfigTest, DevModeExplicitlyDisabledTls) {
  TlsConfig config;
  config.enabled = false;  // Explicitly disabled for dev/test

  auto server_creds = TlsCredentialFactory::CreateServerCredentials(config);
  EXPECT_NE(server_creds, nullptr);

  auto client_creds = TlsCredentialFactory::CreateClientCredentials(config);
  EXPECT_NE(client_creds, nullptr);
}
