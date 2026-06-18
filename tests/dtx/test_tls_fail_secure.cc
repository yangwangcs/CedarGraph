// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");

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

TEST(TlsCredentialFactory, DisabledReturnsInsecureCredentials) {
  TlsConfig config;
  config.enabled = false;
  auto result = TlsCredentialFactory::CreateServerCredentials(config);
  EXPECT_TRUE(result.ok());  // Falls back to insecure credentials
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

TEST(TlsCredentialFactory, DisabledClientReturnsInsecureCredentials) {
  TlsConfig config;
  config.enabled = false;
  auto result = TlsCredentialFactory::CreateClientCredentials(config);
  EXPECT_TRUE(result.ok());  // Falls back to insecure credentials
}

TEST(TlsCredentialFactory, ValidateConfigRejectsDisabled) {
  TlsConfig config;
  config.enabled = false;
  std::string err;
  EXPECT_FALSE(TlsCredentialFactory::ValidateConfig(config, &err));
  EXPECT_NE(err.find("insecure"), std::string::npos);
}

TEST(TlsCredentialFactory, EnvUnsetReturnsInsecureCredentials) {
  unsetenv("CEDAR_GRPC_TLS_ENABLED");
  auto srv = TlsCredentialFactory::CreateServerCredentialsFromEnv();
  EXPECT_TRUE(srv.ok());  // Falls back to insecure credentials

  auto cli = TlsCredentialFactory::CreateClientCredentialsFromEnv();
  EXPECT_TRUE(cli.ok());  // Falls back to insecure credentials
}
