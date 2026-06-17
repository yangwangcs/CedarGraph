// Copyright 2025 The Cedar Authors
//
// Configuration file loader (INI-style)

#ifndef CEDAR_CLIENT_CONFIG_LOADER_H_
#define CEDAR_CLIENT_CONFIG_LOADER_H_

#include <string>
#include <unordered_map>
#include <vector>

namespace cedar {
namespace client {

// Configuration section
using ConfigSection = std::unordered_map<std::string, std::string>;

// Configuration file loader
class ConfigLoader {
 public:
  ConfigLoader();
  ~ConfigLoader();

  // Load configuration from file
  bool LoadFromFile(const std::string& file_path);

  // Load configuration from string
  bool LoadFromString(const std::string& content);

  // Get value
  std::string GetString(const std::string& section, const std::string& key,
                         const std::string& default_value = "") const;
  int GetInt(const std::string& section, const std::string& key,
             int default_value = 0) const;
  bool GetBool(const std::string& section, const std::string& key,
               bool default_value = false) const;
  float GetFloat(const std::string& section, const std::string& key,
                 float default_value = 0.0f) const;

  // Set value
  void SetString(const std::string& section, const std::string& key,
                 const std::string& value);
  void SetInt(const std::string& section, const std::string& key, int value);
  void SetBool(const std::string& section, const std::string& key, bool value);
  void SetFloat(const std::string& section, const std::string& key, float value);

  // Check if key exists
  bool HasKey(const std::string& section, const std::string& key) const;

  // Get all sections
  std::vector<std::string> GetSections() const;

  // Get all keys in a section
  std::vector<std::string> GetKeys(const std::string& section) const;

  // Get all key-value pairs in a section
  ConfigSection GetSection(const std::string& section) const;

  // Save configuration to file
  bool SaveToFile(const std::string& file_path) const;

  // Clear all configuration
  void Clear();

 private:
  std::unordered_map<std::string, ConfigSection> sections_;

  // Parse helpers
  std::string Trim(const std::string& str) const;
  bool IsComment(const std::string& line) const;
  bool IsSection(const std::string& line) const;
  std::string ParseSection(const std::string& line) const;
  bool ParseKeyValue(const std::string& line, std::string& key, std::string& value) const;
  std::string ExpandEnvironmentVariables(const std::string& str) const;
};

}  // namespace client
}  // namespace cedar

#endif  // CEDAR_CLIENT_CONFIG_LOADER_H_
