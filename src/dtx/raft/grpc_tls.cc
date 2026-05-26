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

#include "cedar/dtx/raft/grpc_tls.h"

#include <cstdlib>
#include <fstream>
#include <sstream>
#include <iostream>

namespace cedar {
namespace dtx {
namespace raft {

// Use global grpc namespace for credential types
using ::grpc::ServerCredentials;
using ::grpc::ChannelCredentials;
using ::grpc::SslServerCredentialsOptions;
using ::grpc::SslCredentialsOptions;
using ::grpc::InsecureServerCredentials;
using ::grpc::InsecureChannelCredentials;
using ::grpc::SslServerCredentials;
using ::grpc::SslCredentials;

std::string TlsCredentialFactory::LoadFile(const std::string& path) {
  std::ifstream file(path);
  if (!file.is_open()) {
    std::cerr << "Failed to open file: " << path << std::endl;
    return "";
  }
  std::stringstream buffer;
  buffer << file.rdbuf();
  return buffer.str();
}

StatusOr<std::shared_ptr<ServerCredentials>> TlsCredentialFactory::CreateServerCredentials(
    const TlsConfig& config) {
  if (!config.enabled) {
    return InsecureServerCredentials();
  }

  // Load certificate and key
  std::string server_cert = LoadFile(config.server_cert_file);
  std::string server_key = LoadFile(config.server_key_file);

  if (server_cert.empty()) {
    return Status::IOError("Failed to load server certificate: " + config.server_cert_file);
  }
  if (server_key.empty()) {
    return Status::IOError("Failed to load server key: " + config.server_key_file);
  }

  SslServerCredentialsOptions::PemKeyCertPair key_cert_pair;
  key_cert_pair.private_key = server_key;
  key_cert_pair.cert_chain = server_cert;

  SslServerCredentialsOptions ssl_opts;
  ssl_opts.pem_key_cert_pairs.push_back(key_cert_pair);

  if (config.mtls_enabled && !config.ca_cert_file.empty()) {
    // mTLS: verify client certificates
    std::string ca_cert = LoadFile(config.ca_cert_file);
    if (ca_cert.empty()) {
      return Status::IOError("Failed to load CA certificate for mTLS: " + config.ca_cert_file);
    }
    ssl_opts.pem_root_certs = ca_cert;
    ssl_opts.client_certificate_request = 
        GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY;
  }

  return SslServerCredentials(ssl_opts);
}

StatusOr<std::shared_ptr<ChannelCredentials>> TlsCredentialFactory::CreateClientCredentials(
    const TlsConfig& config) {
  if (!config.enabled) {
    return InsecureChannelCredentials();
  }

  SslCredentialsOptions ssl_opts;

  if (config.mtls_enabled) {
    // mTLS: load client certificate
    std::string client_cert = LoadFile(config.client_cert_file);
    std::string client_key = LoadFile(config.client_key_file);
    std::string ca_cert = LoadFile(config.ca_cert_file);

    if (client_cert.empty()) {
      return Status::IOError("Failed to load client certificate: " + config.client_cert_file);
    }
    if (client_key.empty()) {
      return Status::IOError("Failed to load client key: " + config.client_key_file);
    }

    ssl_opts.pem_cert_chain = client_cert;
    ssl_opts.pem_private_key = client_key;
    ssl_opts.pem_root_certs = ca_cert;
  } else {
    // TLS only: just need CA cert to verify server
    if (!config.ca_cert_file.empty()) {
      std::string ca_cert = LoadFile(config.ca_cert_file);
      if (ca_cert.empty()) {
        return Status::IOError("Failed to load CA certificate for TLS: " + config.ca_cert_file);
      }
      ssl_opts.pem_root_certs = ca_cert;
    }
  }

  return grpc::SslCredentials(ssl_opts);
}

bool TlsCredentialFactory::ValidateConfig(const TlsConfig& config, 
                                           std::string* error_msg) {
  if (!config.enabled) {
    return true;  // TLS disabled is valid
  }

  if (config.server_cert_file.empty() || config.server_key_file.empty()) {
    if (error_msg) *error_msg = "Server certificate and key files are required";
    return false;
  }

  if (config.mtls_enabled) {
    if (config.ca_cert_file.empty()) {
      if (error_msg) *error_msg = "CA certificate is required for mTLS";
      return false;
    }
    if (config.client_cert_file.empty() || config.client_key_file.empty()) {
      if (error_msg) *error_msg = "Client certificate and key are required for mTLS";
      return false;
    }
  }

  return true;
}

StatusOr<std::shared_ptr<ServerCredentials>> TlsCredentialFactory::CreateServerCredentialsFromEnv() {
  TlsConfig config;
  const char* enabled = std::getenv("CEDAR_GRPC_TLS_ENABLED");
  if (enabled && std::string(enabled) == "1") {
    config.enabled = true;
    const char* ca_cert = std::getenv("CEDAR_GRPC_CA_CERT");
    if (ca_cert) config.ca_cert_file = ca_cert;
    const char* server_cert = std::getenv("CEDAR_GRPC_SERVER_CERT");
    if (server_cert) config.server_cert_file = server_cert;
    const char* server_key = std::getenv("CEDAR_GRPC_SERVER_KEY");
    if (server_key) config.server_key_file = server_key;
    const char* mtls = std::getenv("CEDAR_GRPC_MTLS_ENABLED");
    if (mtls && std::string(mtls) == "1") {
      config.mtls_enabled = true;
      const char* client_cert = std::getenv("CEDAR_GRPC_CLIENT_CERT");
      if (client_cert) config.client_cert_file = client_cert;
      const char* client_key = std::getenv("CEDAR_GRPC_CLIENT_KEY");
      if (client_key) config.client_key_file = client_key;
    }
    return CreateServerCredentials(config);
  }
  return InsecureServerCredentials();
}

StatusOr<std::shared_ptr<ChannelCredentials>> TlsCredentialFactory::CreateClientCredentialsFromEnv() {
  TlsConfig config;
  const char* enabled = std::getenv("CEDAR_GRPC_TLS_ENABLED");
  if (enabled && std::string(enabled) == "1") {
    config.enabled = true;
    const char* ca_cert = std::getenv("CEDAR_GRPC_CA_CERT");
    if (ca_cert) config.ca_cert_file = ca_cert;
    const char* mtls = std::getenv("CEDAR_GRPC_MTLS_ENABLED");
    if (mtls && std::string(mtls) == "1") {
      config.mtls_enabled = true;
      const char* client_cert = std::getenv("CEDAR_GRPC_CLIENT_CERT");
      if (client_cert) config.client_cert_file = client_cert;
      const char* client_key = std::getenv("CEDAR_GRPC_CLIENT_KEY");
      if (client_key) config.client_key_file = client_key;
    }
    return CreateClientCredentials(config);
  }
  return InsecureChannelCredentials();
}

}  // namespace raft
}  // namespace dtx
}  // namespace cedar
