// Copyright 2025 The Cedar Authors
//
// Configuration file loader implementation

#include "cedar/client/config_loader.h"

#include <algorithm>
#include <fstream>
#include <sstream>
#include <stdexcept>

#ifdef _WIN32
#include <windows.h>
#else
#include <cstdlib>
#endif

namespace cedar {
namespace client {

ConfigLoader::ConfigLoader() = default;
ConfigLoader::~ConfigLoader() = default;

bool ConfigLoader::LoadFromFile(const std::string& file_path) {
  std::ifstream file(file_path);
  if (!file.is_open()) {
    return false;
  }

  std::stringstream buffer;
  buffer << file.rdbuf();
  return LoadFromString(buffer.str());
}

bool ConfigLoader::LoadFromString(const std::string& content) {
  std::istringstream stream(content);
  std::string line;
  std::string current_section;

  while (std::getline(stream, line)) {
    // Trim whitespace
    line = Trim(line);

    // Skip empty lines and comments
    if (line.empty() || IsComment(line)) {
      continue;
    }

    // Check if this is a section header
    if (IsSection(line)) {
      current_section = ParseSection(line);
      continue;
    }

    // Parse key-value pair
    std::string key, value;
    if (ParseKeyValue(line, key, value)) {
      // Expand environment variables
      value = ExpandEnvironmentVariables(value);

      // Store in current section
      sections_[current_section][key] = value;
    }
  }

  return true;
}

std::string ConfigLoader::GetString(const std::string& section, const std::string& key,
                                      const std::string& default_value) const {
  auto section_it = sections_.find(section);
  if (section_it == sections_.end()) {
    return default_value;
  }

  auto key_it = section_it->second.find(key);
  if (key_it == section_it->second.end()) {
    return default_value;
  }

  return key_it->second;
}

int ConfigLoader::GetInt(const std::string& section, const std::string& key,
                           int default_value) const {
  std::string value = GetString(section, key);
  if (value.empty()) {
    return default_value;
  }

  try {
    return std::stoi(value);
  } catch (const std::exception&) {
    return default_value;
  }
}

bool ConfigLoader::GetBool(const std::string& section, const std::string& key,
                             bool default_value) const {
  std::string value = GetString(section, key);
  if (value.empty()) {
    return default_value;
  }

  // Convert to lowercase
  std::string lower_value = value;
  std::transform(lower_value.begin(), lower_value.end(), lower_value.begin(), ::tolower);

  if (lower_value == "true" || lower_value == "1" || lower_value == "yes" || lower_value == "on") {
    return true;
  } else if (lower_value == "false" || lower_value == "0" || lower_value == "no" || lower_value == "off") {
    return false;
  }

  return default_value;
}

float ConfigLoader::GetFloat(const std::string& section, const std::string& key,
                               float default_value) const {
  std::string value = GetString(section, key);
  if (value.empty()) {
    return default_value;
  }

  try {
    return std::stof(value);
  } catch (const std::exception&) {
    return default_value;
  }
}

void ConfigLoader::SetString(const std::string& section, const std::string& key,
                               const std::string& value) {
  sections_[section][key] = value;
}

void ConfigLoader::SetInt(const std::string& section, const std::string& key, int value) {
  sections_[section][key] = std::to_string(value);
}

void ConfigLoader::SetBool(const std::string& section, const std::string& key, bool value) {
  sections_[section][key] = value ? "true" : "false";
}

void ConfigLoader::SetFloat(const std::string& section, const std::string& key, float value) {
  sections_[section][key] = std::to_string(value);
}

bool ConfigLoader::HasKey(const std::string& section, const std::string& key) const {
  auto section_it = sections_.find(section);
  if (section_it == sections_.end()) {
    return false;
  }

  return section_it->second.find(key) != section_it->second.end();
}

std::vector<std::string> ConfigLoader::GetSections() const {
  std::vector<std::string> sections;
  for (const auto& pair : sections_) {
    sections.push_back(pair.first);
  }
  return sections;
}

std::vector<std::string> ConfigLoader::GetKeys(const std::string& section) const {
  std::vector<std::string> keys;
  
  auto section_it = sections_.find(section);
  if (section_it == sections_.end()) {
    return keys;
  }

  for (const auto& pair : section_it->second) {
    keys.push_back(pair.first);
  }
  return keys;
}

ConfigSection ConfigLoader::GetSection(const std::string& section) const {
  auto section_it = sections_.find(section);
  if (section_it == sections_.end()) {
    return {};
  }

  return section_it->second;
}

bool ConfigLoader::SaveToFile(const std::string& file_path) const {
  std::ofstream file(file_path);
  if (!file.is_open()) {
    return false;
  }

  for (const auto& section_pair : sections_) {
    if (!section_pair.first.empty()) {
      file << "[" << section_pair.first << "]" << std::endl;
    }

    for (const auto& key_pair : section_pair.second) {
      file << key_pair.first << " = " << key_pair.second << std::endl;
    }

    file << std::endl;
  }

  return true;
}

void ConfigLoader::Clear() {
  sections_.clear();
}

std::string ConfigLoader::Trim(const std::string& str) const {
  size_t start = str.find_first_not_of(" \t\r\n");
  if (start == std::string::npos) {
    return "";
  }

  size_t end = str.find_last_not_of(" \t\r\n");
  return str.substr(start, end - start + 1);
}

bool ConfigLoader::IsComment(const std::string& line) const {
  return !line.empty() && (line[0] == '#' || line[0] == ';');
}

bool ConfigLoader::IsSection(const std::string& line) const {
  return !line.empty() && line.front() == '[' && line.back() == ']';
}

std::string ConfigLoader::ParseSection(const std::string& line) const {
  return Trim(line.substr(1, line.size() - 2));
}

bool ConfigLoader::ParseKeyValue(const std::string& line, std::string& key, std::string& value) const {
  size_t pos = line.find('=');
  if (pos == std::string::npos) {
    return false;
  }

  key = Trim(line.substr(0, pos));
  value = Trim(line.substr(pos + 1));

  // Remove quotes if present
  if (value.size() >= 2 && 
      ((value.front() == '"' && value.back() == '"') ||
       (value.front() == '\'' && value.back() == '\''))) {
    value = value.substr(1, value.size() - 2);
  }

  return !key.empty();
}

std::string ConfigLoader::ExpandEnvironmentVariables(const std::string& str) const {
  std::string result;
  size_t pos = 0;

  while (pos < str.size()) {
    if (str[pos] == '$' && pos + 1 < str.size() && str[pos + 1] == '{') {
      // Find closing brace
      size_t end = str.find('}', pos + 2);
      if (end != std::string::npos) {
        // Extract variable name
        std::string var_name = str.substr(pos + 2, end - pos - 2);

        // Get environment variable
        const char* env_value = nullptr;
#ifdef _WIN32
        char buffer[4096];
        if (GetEnvironmentVariableA(var_name.c_str(), buffer, sizeof(buffer)) > 0) {
          env_value = buffer;
        }
#else
        env_value = std::getenv(var_name.c_str());
#endif

        if (env_value) {
          result += env_value;
        }

        pos = end + 1;
      } else {
        result += str[pos++];
      }
    } else {
      result += str[pos++];
    }
  }

  return result;
}

}  // namespace client
}  // namespace cedar
