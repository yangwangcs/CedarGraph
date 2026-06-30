// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");

#include <gtest/gtest.h>
#include "cedar/dtx/raft/grpc_tls.h"
#include "test_tls_certs.h"

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

TEST(TlsCredentialFactory, ServerMtlsWithoutCaReturnsError) {
  ASSERT_TRUE(cedar::test::SetupTestTlsEnvironment("server_mtls_missing_ca"));
  TlsConfig config;
  config.enabled = true;
  config.mtls_enabled = true;
  config.server_cert_file = std::getenv("CEDAR_GRPC_SERVER_CERT");
  config.server_key_file = std::getenv("CEDAR_GRPC_SERVER_KEY");

  auto result = TlsCredentialFactory::CreateServerCredentials(config);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.status().ToString().find("CA certificate"), std::string::npos);
}

TEST(TlsCredentialFactory, ClientMtlsWithoutCaReturnsError) {
  TlsConfig config;
  config.enabled = true;
  config.mtls_enabled = true;
  config.client_cert_file = "/nonexistent/client_cert.pem";
  config.client_key_file = "/nonexistent/client_key.pem";

  auto result = TlsCredentialFactory::CreateClientCredentials(config);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.status().ToString().find("CA certificate"), std::string::npos);
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

TEST(TlsCredentialFactory, StrictEnvUnsetRejectsImplicitInsecure) {
  unsetenv("CEDAR_GRPC_TLS_ENABLED");
  unsetenv("CEDAR_GRPC_ALLOW_INSECURE");

  auto srv = TlsCredentialFactory::CreateServerCredentialsFromEnvStrict();
  EXPECT_FALSE(srv.ok());
  EXPECT_NE(srv.status().ToString().find("CEDAR_GRPC_ALLOW_INSECURE"), std::string::npos);

  auto cli = TlsCredentialFactory::CreateClientCredentialsFromEnvStrict();
  EXPECT_FALSE(cli.ok());
  EXPECT_NE(cli.status().ToString().find("CEDAR_GRPC_ALLOW_INSECURE"), std::string::npos);
}

TEST(TlsCredentialFactory, StrictEnvAllowsExplicitDevelopmentInsecure) {
  unsetenv("CEDAR_GRPC_TLS_ENABLED");
  setenv("CEDAR_GRPC_ALLOW_INSECURE", "1", 1);

  auto srv = TlsCredentialFactory::CreateServerCredentialsFromEnvStrict();
  EXPECT_TRUE(srv.ok());

  auto cli = TlsCredentialFactory::CreateClientCredentialsFromEnvStrict();
  EXPECT_TRUE(cli.ok());

  unsetenv("CEDAR_GRPC_ALLOW_INSECURE");
}
