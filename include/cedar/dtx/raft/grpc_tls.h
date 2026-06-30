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

// =============================================================================
// gRPC TLS Support for Embedded Raft
// =============================================================================

#ifndef CEDAR_DTX_RAFT_GRPC_TLS_H_
#define CEDAR_DTX_RAFT_GRPC_TLS_H_

#include <grpcpp/grpcpp.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/security/credentials.h>
#include <memory>
#include <string>

#include "cedar/core/status.h"

namespace cedar {
namespace dtx {
namespace raft {

// =============================================================================
// TLS Configuration
// =============================================================================

struct TlsConfig {
  // Enable TLS
  bool enabled = true;
  
  // Certificate files
  std::string server_cert_file;   // Server certificate
  std::string server_key_file;    // Server private key
  std::string ca_cert_file;       // CA certificate for client verification
  
  // mTLS (mutual TLS) - verify client certificates
  bool mtls_enabled = false;
  
  // Client certificate (for mTLS)
  std::string client_cert_file;
  std::string client_key_file;
  
  // Skip hostname verification (for testing only)
  bool skip_hostname_verification = false;
};

// =============================================================================
// TLS Credential Factory
// =============================================================================

class TlsCredentialFactory {
 public:
  // Create server credentials with TLS/mTLS
  static StatusOr<std::shared_ptr<grpc::ServerCredentials>> CreateServerCredentials(
      const TlsConfig& config);
  
  // Create client credentials with TLS/mTLS
  static StatusOr<std::shared_ptr<grpc::ChannelCredentials>> CreateClientCredentials(
      const TlsConfig& config);
  
  // Load file contents
  static std::string LoadFile(const std::string& path);
  
  // Validate TLS configuration
  static bool ValidateConfig(const TlsConfig& config, std::string* error_msg);
  
  // Create credentials from environment variables
  // Reads CEDAR_GRPC_TLS_ENABLED, CEDAR_GRPC_CA_CERT, CEDAR_GRPC_SERVER_CERT,
  // CEDAR_GRPC_SERVER_KEY, CEDAR_GRPC_CLIENT_CERT, CEDAR_GRPC_CLIENT_KEY
  static StatusOr<std::shared_ptr<grpc::ServerCredentials>> CreateServerCredentialsFromEnv();
  static StatusOr<std::shared_ptr<grpc::ChannelCredentials>> CreateClientCredentialsFromEnv();
  static StatusOr<std::shared_ptr<grpc::ServerCredentials>> CreateServerCredentialsFromEnvStrict();
  static StatusOr<std::shared_ptr<grpc::ChannelCredentials>> CreateClientCredentialsFromEnvStrict();
  static bool EnvTlsEnabled();
  static bool EnvAllowsInsecure();
};

}  // namespace raft
}  // namespace dtx
}  // namespace cedar

#endif  // CEDAR_DTX_RAFT_GRPC_TLS_H_
