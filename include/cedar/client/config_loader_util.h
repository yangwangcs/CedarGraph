// Copyright 2025 The Cedar Authors
//
// Configuration loader utility

#ifndef CEDAR_CLIENT_CONFIG_LOADER_UTIL_H_
#define CEDAR_CLIENT_CONFIG_LOADER_UTIL_H_

#include "cedar/client/config_loader.h"
#include "cedar/client/cedar_client.h"

namespace cedar {
namespace client {

// Load CedarClientConfig from config file
inline CedarClientConfig LoadClientConfig(const std::string& file_path) {
  ConfigLoader loader;
  CedarClientConfig config;

  if (!loader.LoadFromFile(file_path)) {
    // Return default config if file not found
    return config;
  }

  // MetaD configuration
  config.metad_host = loader.GetString("metad", "host", config.metad_host);
  config.metad_port = loader.GetInt("metad", "port", config.metad_port);

  // GraphD configuration
  config.graphd_host = loader.GetString("graphd", "host", config.graphd_host);
  config.graphd_port = loader.GetInt("graphd", "port", config.graphd_port);

  // StorageD configuration
  config.storaged_host = loader.GetString("storaged", "host", config.storaged_host);
  config.storaged_port = loader.GetInt("storaged", "port", config.storaged_port);

  // Connection pool settings
  config.max_connections = loader.GetInt("connection_pool", "max_connections", config.max_connections);
  config.timeout_ms = loader.GetInt("connection_pool", "timeout_ms", config.timeout_ms);

  // Service discovery settings
  config.enable_service_discovery = loader.GetBool("service_discovery", "enable", config.enable_service_discovery);
  config.refresh_interval_ms = loader.GetInt("service_discovery", "refresh_interval_ms", config.refresh_interval_ms);

  // TLS settings
  config.enable_tls = loader.GetBool("tls", "enable", config.enable_tls);
  config.ca_cert_path = loader.GetString("tls", "ca_cert_path", config.ca_cert_path);
  config.client_cert_path = loader.GetString("tls", "client_cert_path", config.client_cert_path);
  config.client_key_path = loader.GetString("tls", "client_key_path", config.client_key_path);

  // JWT settings
  config.enable_jwt = loader.GetBool("jwt", "enable", config.enable_jwt);
  config.jwt_secret_key = loader.GetString("jwt", "secret_key", config.jwt_secret_key);
  config.jwt_issuer = loader.GetString("jwt", "issuer", config.jwt_issuer);
  config.jwt_audience = loader.GetString("jwt", "audience", config.jwt_audience);
  config.jwt_expiration_seconds = loader.GetInt("jwt", "expiration_seconds", config.jwt_expiration_seconds);
  config.jwt_token = loader.GetString("jwt", "token", config.jwt_token);

  // Logging settings
  config.enable_logging = loader.GetBool("logging", "enable", config.enable_logging);
  std::string log_level_str = loader.GetString("logging", "level", "INFO");
  if (log_level_str == "DEBUG") config.log_level = LogLevel::DEBUG;
  else if (log_level_str == "INFO") config.log_level = LogLevel::INFO;
  else if (log_level_str == "WARN") config.log_level = LogLevel::WARN;
  else if (log_level_str == "ERROR") config.log_level = LogLevel::ERROR;
  else if (log_level_str == "FATAL") config.log_level = LogLevel::FATAL;
  config.log_file_path = loader.GetString("logging", "file_path", config.log_file_path);
  config.enable_console_logging = loader.GetBool("logging", "console", config.enable_console_logging);
  config.enable_file_logging = loader.GetBool("logging", "file", config.enable_file_logging);

  return config;
}

// Save CedarClientConfig to config file
inline bool SaveClientConfig(const CedarClientConfig& config, const std::string& file_path) {
  ConfigLoader loader;

  // MetaD configuration
  loader.SetString("metad", "host", config.metad_host);
  loader.SetInt("metad", "port", config.metad_port);

  // GraphD configuration
  loader.SetString("graphd", "host", config.graphd_host);
  loader.SetInt("graphd", "port", config.graphd_port);

  // StorageD configuration
  loader.SetString("storaged", "host", config.storaged_host);
  loader.SetInt("storaged", "port", config.storaged_port);

  // Connection pool settings
  loader.SetInt("connection_pool", "max_connections", config.max_connections);
  loader.SetInt("connection_pool", "timeout_ms", config.timeout_ms);

  // Service discovery settings
  loader.SetBool("service_discovery", "enable", config.enable_service_discovery);
  loader.SetInt("service_discovery", "refresh_interval_ms", config.refresh_interval_ms);

  // TLS settings
  loader.SetBool("tls", "enable", config.enable_tls);
  loader.SetString("tls", "ca_cert_path", config.ca_cert_path);
  loader.SetString("tls", "client_cert_path", config.client_cert_path);
  loader.SetString("tls", "client_key_path", config.client_key_path);

  // JWT settings
  loader.SetBool("jwt", "enable", config.enable_jwt);
  loader.SetString("jwt", "secret_key", config.jwt_secret_key);
  loader.SetString("jwt", "issuer", config.jwt_issuer);
  loader.SetString("jwt", "audience", config.jwt_audience);
  loader.SetInt("jwt", "expiration_seconds", config.jwt_expiration_seconds);
  loader.SetString("jwt", "token", config.jwt_token);

  // Logging settings
  loader.SetBool("logging", "enable", config.enable_logging);
  std::string log_level_str;
  switch (config.log_level) {
    case LogLevel::DEBUG: log_level_str = "DEBUG"; break;
    case LogLevel::INFO: log_level_str = "INFO"; break;
    case LogLevel::WARN: log_level_str = "WARN"; break;
    case LogLevel::ERROR: log_level_str = "ERROR"; break;
    case LogLevel::FATAL: log_level_str = "FATAL"; break;
    default: log_level_str = "INFO"; break;
  }
  loader.SetString("logging", "level", log_level_str);
  loader.SetString("logging", "file_path", config.log_file_path);
  loader.SetBool("logging", "console", config.enable_console_logging);
  loader.SetBool("logging", "file", config.enable_file_logging);

  return loader.SaveToFile(file_path);
}

}  // namespace client
}  // namespace cedar

#endif  // CEDAR_CLIENT_CONFIG_LOADER_UTIL_H_
