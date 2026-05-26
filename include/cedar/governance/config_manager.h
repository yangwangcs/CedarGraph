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

#ifndef CEDAR_GOVERNANCE_CONFIG_MANAGER_H_
#define CEDAR_GOVERNANCE_CONFIG_MANAGER_H_

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "cedar/core/status.h"

// Forward declaration for yaml-cpp
namespace YAML {
class Node;
}

namespace cedar {
namespace governance {

// =============================================================================
// Configuration Change Event Types
// =============================================================================

enum class ConfigChangeType {
  kValueChanged = 0,   // Configuration value was changed
  kValueAdded = 1,     // New configuration value was added
  kValueRemoved = 2    // Configuration value was removed
};

// Convert ConfigChangeType to string
std::string ConfigChangeTypeToString(ConfigChangeType type);

// =============================================================================
// Configuration Change Event
// =============================================================================

struct ConfigChangeEvent {
  ConfigChangeType type;           // Type of change
  std::string key;                 // Configuration key that changed
  std::string old_value;           // Previous value (empty if kValueAdded)
  std::string new_value;           // New value (empty if kValueRemoved)
  int64_t timestamp_ms;            // When the change occurred
};

// =============================================================================
// Configuration Change Callback Type
// =============================================================================

using ConfigChangeCallback = std::function<void(const ConfigChangeEvent& event)>;

// =============================================================================
// Configuration Validation Result
// =============================================================================

struct ValidationResult {
  bool valid = true;
  std::string error_message;
  std::string error_key;

  static ValidationResult Ok() { return ValidationResult{}; }

  static ValidationResult Error(const std::string& key, const std::string& message) {
    ValidationResult result;
    result.valid = false;
    result.error_key = key;
    result.error_message = message;
    return result;
  }
};

// =============================================================================
// ConfigManager Implementation
// =============================================================================

// Forward declaration of implementation class (Pimpl idiom)
class ConfigManagerImpl;

class ConfigManager {
 public:
  // Constructor and Destructor
  ConfigManager();
  ~ConfigManager();

  // Disable copy (use Merge for combining configs)
  ConfigManager(const ConfigManager&) = delete;
  ConfigManager& operator=(const ConfigManager&) = delete;

  // Enable move
  ConfigManager(ConfigManager&&) noexcept;
  ConfigManager& operator=(ConfigManager&&) noexcept;

  // ---------------------------------------------------------------------------
  // Configuration Loading
  // ---------------------------------------------------------------------------

  // Load configuration from a YAML file
  // Returns OK on success, IOError if file cannot be read,
  // InvalidArgument if YAML is malformed
  Status LoadFromFile(const std::string& filepath);

  // Load configuration from a YAML string
  // Returns OK on success, InvalidArgument if YAML is malformed
  Status LoadFromString(const std::string& content);

  // Apply environment variable overrides
  // Variables with CEDAR_ prefix are mapped to config keys
  // Example: CEDAR_CLUSTER_NAME -> cluster.name
  // This is automatically called after LoadFromFile/LoadFromString
  Status ApplyEnvironmentOverrides();

  // ---------------------------------------------------------------------------
  // Type-Safe Configuration Getters
  // ---------------------------------------------------------------------------

  // Get string value with default
  std::string GetString(const std::string& key,
                        const std::string& default_val = "") const;

  // Get integer value with default
  int GetInt(const std::string& key, int default_val = 0) const;

  // Get 64-bit integer value with default
  int64_t GetInt64(const std::string& key, int64_t default_val = 0) const;

  // Get double value with default
  double GetDouble(const std::string& key, double default_val = 0.0) const;

  // Get boolean value with default
  bool GetBool(const std::string& key, bool default_val = false) const;

  // Get string array value with default
  std::vector<std::string> GetStringArray(
      const std::string& key,
      const std::vector<std::string>& default_val = {}) const;

  // Check if a key exists
  bool HasKey(const std::string& key) const;

  // ---------------------------------------------------------------------------
  // Configuration Setters
  // ---------------------------------------------------------------------------

  // Set string value (creates or updates)
  void SetString(const std::string& key, const std::string& value);

  // Set integer value (creates or updates)
  void SetInt(const std::string& key, int value);

  // Set 64-bit integer value (creates or updates)
  void SetInt64(const std::string& key, int64_t value);

  // Set double value (creates or updates)
  void SetDouble(const std::string& key, double value);

  // Set boolean value (creates or updates)
  void SetBool(const std::string& key, bool value);

  // Set string array value (creates or updates)
  void SetStringArray(const std::string& key,
                      const std::vector<std::string>& value);

  // Remove a key
  bool RemoveKey(const std::string& key);

  // ---------------------------------------------------------------------------
  // Configuration Validation
  // ---------------------------------------------------------------------------

  // Validate configuration against a JSON schema file
  // Returns OK if valid, InvalidArgument if validation fails
  Status Validate(const std::string& schema_path);

  // Validate using built-in rules
  // Checks for required keys and valid value ranges
  Status ValidateBasic(const std::vector<std::string>& required_keys);

  // ---------------------------------------------------------------------------
  // Configuration Merging
  // ---------------------------------------------------------------------------

  // Merge another ConfigManager into this one
  // Values from 'other' take precedence over existing values
  Status Merge(const ConfigManager& other);

  // Merge another ConfigManager with prefix
  // All keys from 'other' are prefixed before merging
  Status MergeWithPrefix(const ConfigManager& other,
                         const std::string& prefix);

  // ---------------------------------------------------------------------------
  // Configuration Serialization
  // ---------------------------------------------------------------------------

  // Dump configuration to YAML string
  std::string Dump() const;

  // Dump configuration to YAML string with comments
  std::string DumpWithComments() const;

  // Dump a specific section to YAML string
  std::string DumpSection(const std::string& section_key) const;

  // ---------------------------------------------------------------------------
  // Watch Mechanism (Hot Reload Support)
  // ---------------------------------------------------------------------------

  // Watch a specific key for changes
  // Returns a watch ID that can be used to unregister
  StatusOr<int64_t> Watch(const std::string& key, ConfigChangeCallback callback);

  // Watch all configuration changes
  StatusOr<int64_t> WatchAll(ConfigChangeCallback callback);

  // Unregister a watch callback by ID
  Status Unwatch(int64_t watch_id);

  // ---------------------------------------------------------------------------
  // File Watching (Hot Reload)
  // ---------------------------------------------------------------------------

  // Enable hot reload for a file
  // Automatically reloads configuration when the file changes
  // interval_ms: how often to check for file changes
  Status EnableHotReload(const std::string& filepath, int interval_ms = 5000);

  // Disable hot reload
  void DisableHotReload();

  // Check if hot reload is enabled
  bool IsHotReloadEnabled() const;

  // Manually trigger a reload (if hot reload is enabled)
  Status Reload();

  // ---------------------------------------------------------------------------
  // Utility Methods
  // ---------------------------------------------------------------------------

  // Get all keys (flattened, dot-notation)
  std::vector<std::string> GetAllKeys() const;

  // Get all keys with a specific prefix
  std::vector<std::string> GetKeysWithPrefix(const std::string& prefix) const;

  // Clear all configuration
  void Clear();

  // Check if configuration is empty
  bool IsEmpty() const;

  // Get the number of configuration entries
  size_t Size() const;

  // Get the source file path (if loaded from file)
  std::string GetSourceFile() const;

  // Get the last modification time of the source file
  int64_t GetLastModifiedTime() const;

 private:
  void LoadYamlNode(const std::string& prefix, const YAML::Node& node);

  // Pimpl idiom - implementation details are hidden
  std::unique_ptr<ConfigManagerImpl> impl_;
};

// =============================================================================
// Helper Functions
// =============================================================================

// Convert environment variable name to config key
// Example: CEDAR_CLUSTER_NAME -> cluster.name
std::string EnvVarToConfigKey(const std::string& env_var);

// Convert config key to environment variable name
// Example: cluster.name -> CEDAR_CLUSTER_NAME
std::string ConfigKeyToEnvVar(const std::string& key);

// Parse a dot-notation key into parts
// Example: "cluster.name" -> ["cluster", "name"]
std::vector<std::string> ParseKey(const std::string& key);

}  // namespace governance
}  // namespace cedar

#endif  // CEDAR_GOVERNANCE_CONFIG_MANAGER_H_
