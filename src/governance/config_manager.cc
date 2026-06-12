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

#include "cedar/governance/config_manager.h"

#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>
#include <sys/stat.h>
#include <thread>
#include <yaml-cpp/yaml.h>

// For environment variable access
#ifdef __APPLE__
#include <crt_externs.h>
#define environ (*_NSGetEnviron())
#elif defined(__linux__)
extern char** environ;
#endif

namespace cedar {
namespace governance {

// =============================================================================
// Simple Config Tree Node
// =============================================================================

struct ConfigNode {
  std::string value;
  std::map<std::string, std::shared_ptr<ConfigNode>> children;
  bool is_scalar = true;

  bool HasChild(const std::string& key) const {
    return children.find(key) != children.end();
  }

  std::shared_ptr<ConfigNode> GetOrCreateChild(const std::string& key) {
    if (!HasChild(key)) {
      children[key] = std::make_shared<ConfigNode>();
    }
    return children[key];
  }
};

static void DumpNode(const std::shared_ptr<ConfigNode>& node, std::ostream& out,
                     int indent = 0) {
  std::string prefix(indent, ' ');
  for (const auto& [key, child] : node->children) {
    if (child->is_scalar && child->children.empty()) {
      bool is_number = !child->value.empty() &&
                       std::all_of(child->value.begin(), child->value.end(),
                                   [](char c) {
                                     return std::isdigit(static_cast<unsigned char>(c)) ||
                                            c == '.' || c == '-';
                                   });
      if (is_number || child->value == "true" || child->value == "false") {
        out << prefix << key << ": " << child->value << "\n";
      } else {
        out << prefix << key << ": \"" << child->value << "\"\n";
      }
    } else {
      out << prefix << key << ":\n";
      DumpNode(child, out, indent + 2);
    }
  }
}

static void FlattenNode(const std::shared_ptr<ConfigNode>& node,
                        const std::string& prefix,
                        std::map<std::string, std::string>& result) {
  for (const auto& [key, child] : node->children) {
    std::string full_key = prefix.empty() ? key : prefix + "." + key;
    if (child->is_scalar && !child->value.empty()) {
      result[full_key] = child->value;
    }
    FlattenNode(child, full_key, result);
  }
}

static std::vector<std::string> ParseKeyInternal(const std::string& key) {
  std::vector<std::string> parts;
  std::istringstream stream(key);
  std::string part;
  while (std::getline(stream, part, '.')) {
    if (!part.empty()) {
      parts.push_back(part);
    }
  }
  return parts;
}

static std::shared_ptr<ConfigNode> FindNode(const std::shared_ptr<ConfigNode>& root,
                                            const std::string& key) {
  auto parts = ParseKeyInternal(key);
  auto current = root;
  for (const auto& part : parts) {
    if (!current->HasChild(part)) {
      return nullptr;
    }
    current = current->children[part];
  }
  return current;
}

static void SetNodeValue(const std::shared_ptr<ConfigNode>& root,
                         const std::string& key, const std::string& value) {
  auto parts = ParseKeyInternal(key);
  auto current = root;
  for (size_t i = 0; i < parts.size(); ++i) {
    if (i == parts.size() - 1) {
      auto child = current->GetOrCreateChild(parts[i]);
      child->value = value;
      child->is_scalar = true;
    } else {
      current = current->GetOrCreateChild(parts[i]);
      current->is_scalar = false;
    }
  }
}

static void BuildConfigNode(const YAML::Node& yaml_node,
                            std::shared_ptr<ConfigNode> tree_node) {
  if (yaml_node.IsMap()) {
    tree_node->is_scalar = false;
    for (const auto& kv : yaml_node) {
      std::string key = kv.first.as<std::string>();
      auto child = std::make_shared<ConfigNode>();
      BuildConfigNode(kv.second, child);
      tree_node->children[key] = child;
    }
  } else if (yaml_node.IsScalar()) {
    tree_node->value = yaml_node.as<std::string>();
    tree_node->is_scalar = true;
  } else if (yaml_node.IsSequence()) {
    tree_node->is_scalar = false;
    for (size_t i = 0; i < yaml_node.size(); ++i) {
      auto child = std::make_shared<ConfigNode>();
      BuildConfigNode(yaml_node[i], child);
      tree_node->children[std::to_string(i)] = child;
    }
  }
}

// =============================================================================
// ConfigManager Implementation
// =============================================================================

class ConfigManagerImpl {
 public:
  ConfigManagerImpl() : root_(std::make_shared<ConfigNode>()) {}

  mutable std::mutex mutex_;
  std::shared_ptr<ConfigNode> root_;
  std::string source_file_;
  int64_t last_modified_time_ = 0;

  // Watchers
  std::unordered_map<int64_t, ConfigChangeCallback> watchers_;
  std::unordered_map<int64_t, std::string> watch_keys_;
  int64_t next_watch_id_ = 1;

  // Hot reload
  std::unique_ptr<std::thread> hot_reload_thread_;
  std::atomic<bool> hot_reload_enabled_{false};
  int hot_reload_interval_ms_ = 5000;
  time_t hot_reload_last_mtime_{0};

  // Flattened config for fast lookups
  std::map<std::string, std::string> flattened_;

  void UpdateFlattened() {
    flattened_.clear();
    FlattenNode(root_, "", flattened_);
  }

  void NotifyWatchers(const std::string& key, const std::string& old_value,
                      const std::string& new_value,
                      ConfigChangeType change_type) {
    ConfigChangeEvent event;
    event.type = change_type;
    event.key = key;
    event.old_value = old_value;
    event.new_value = new_value;
    event.timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();

    std::vector<ConfigChangeCallback> callbacks_to_invoke;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      for (const auto& [watch_id, watch_key] : watch_keys_) {
        if (watch_key.empty() || key == watch_key ||
            key.find(watch_key + ".") == 0) {
          auto it = watchers_.find(watch_id);
          if (it != watchers_.end()) {
            callbacks_to_invoke.push_back(it->second);
          }
        }
      }
    }

    for (auto& callback : callbacks_to_invoke) {
      callback(event);
    }
  }
};

// =============================================================================
// Helper Functions
// =============================================================================

std::string ConfigChangeTypeToString(ConfigChangeType type) {
  switch (type) {
    case ConfigChangeType::kValueChanged:
      return "changed";
    case ConfigChangeType::kValueAdded:
      return "added";
    case ConfigChangeType::kValueRemoved:
      return "removed";
    default:
      return "unknown";
  }
}

std::string EnvVarToConfigKey(const std::string& env_var) {
  if (env_var.substr(0, 6) != "CEDAR_") {
    return "";
  }

  std::string key = env_var.substr(6);
  std::string result;
  bool found_first_underscore = false;

  for (size_t i = 0; i < key.size(); ++i) {
    if (key[i] == '_' && !found_first_underscore) {
      // First underscore separates section from key
      result += '.';
      found_first_underscore = true;
    } else {
      result += std::tolower(key[i]);
    }
  }

  return result;
}

std::string ConfigKeyToEnvVar(const std::string& key) {
  std::string result = "CEDAR_";

  for (char c : key) {
    if (c == '.') {
      result += '_';
    } else {
      result += std::toupper(c);
    }
  }

  return result;
}

std::vector<std::string> ParseKey(const std::string& key) {
  return ParseKeyInternal(key);
}

// =============================================================================
// ConfigManager Public Interface
// =============================================================================

ConfigManager::ConfigManager() : impl_(std::make_unique<ConfigManagerImpl>()) {}

ConfigManager::~ConfigManager() {
  DisableHotReload();
}

ConfigManager::ConfigManager(ConfigManager&&) noexcept = default;

ConfigManager& ConfigManager::operator=(ConfigManager&&) noexcept = default;

Status ConfigManager::LoadFromFile(const std::string& filepath) {
  std::unique_lock<std::mutex> lock(impl_->mutex_);

  std::ifstream file(filepath);
  if (!file.is_open()) {
    return Status::IOError("Cannot open config file: " + filepath);
  }

  try {
    YAML::Node yaml_root = YAML::LoadFile(filepath);
    auto new_root = std::make_shared<ConfigNode>();
    BuildConfigNode(yaml_root, new_root);
    impl_->root_ = new_root;
    impl_->source_file_ = filepath;
    impl_->last_modified_time_ =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
    impl_->flattened_.clear();
    LoadYamlNode("", yaml_root);
  } catch (const YAML::Exception& e) {
    return Status::InvalidArgument(std::string("YAML parse error: ") + e.what());
  }

  // Apply environment overrides
  lock.unlock();
  return ApplyEnvironmentOverrides();
}

Status ConfigManager::LoadFromString(const std::string& content) {
  std::unique_lock<std::mutex> lock(impl_->mutex_);

  try {
    YAML::Node yaml_root = YAML::Load(content);
    auto new_root = std::make_shared<ConfigNode>();
    BuildConfigNode(yaml_root, new_root);
    impl_->root_ = new_root;
    impl_->flattened_.clear();
    LoadYamlNode("", yaml_root);
  } catch (const YAML::Exception& e) {
    return Status::InvalidArgument(std::string("YAML parse error: ") + e.what());
  }

  // Apply environment overrides
  lock.unlock();
  return ApplyEnvironmentOverrides();
}

void ConfigManager::LoadYamlNode(const std::string& prefix,
                                 const YAML::Node& node) {
  if (node.IsMap()) {
    for (const auto& kv : node) {
      std::string key = kv.first.as<std::string>();
      std::string full_key = prefix.empty() ? key : prefix + "." + key;
      LoadYamlNode(full_key, kv.second);
    }
  } else if (node.IsScalar()) {
    impl_->flattened_[prefix] = node.as<std::string>();
  } else if (node.IsSequence()) {
    for (size_t i = 0; i < node.size(); ++i) {
      LoadYamlNode(prefix + "[" + std::to_string(i) + "]", node[i]);
    }
  }
}

Status ConfigManager::ApplyEnvironmentOverrides() {
  std::vector<std::tuple<std::string, std::string, std::string>> notifications;
  {
    std::lock_guard<std::mutex> lock(impl_->mutex_);

    // Iterate through environment variables
    char** env_ptr = environ;
    while (env_ptr && *env_ptr) {
      std::string env_var(*env_ptr);
      size_t eq_pos = env_var.find('=');
      if (eq_pos == std::string::npos) {
        ++env_ptr;
        continue;
      }

      std::string var_name = env_var.substr(0, eq_pos);
      std::string var_value = env_var.substr(eq_pos + 1);

      std::string key = EnvVarToConfigKey(var_name);
      if (!key.empty()) {
        // Get old value before updating
        std::string old_value;
        auto node = FindNode(impl_->root_, key);
        if (node && node->is_scalar) {
          old_value = node->value;
        }
        SetNodeValue(impl_->root_, key, var_value);
        impl_->UpdateFlattened();
        notifications.emplace_back(key, old_value, var_value);
      }
      ++env_ptr;
    }
  }

  for (auto& [key, old_value, new_value] : notifications) {
    impl_->NotifyWatchers(key, old_value, new_value,
                          ConfigChangeType::kValueChanged);
  }
  return Status::OK();
}

std::string ConfigManager::GetString(const std::string& key,
                                     const std::string& default_val) const {
  std::lock_guard<std::mutex> lock(impl_->mutex_);

  auto it = impl_->flattened_.find(key);
  if (it != impl_->flattened_.end()) {
    return it->second;
  }

  auto node = FindNode(impl_->root_, key);
  if (node && node->is_scalar) {
    return node->value;
  }

  return default_val;
}

int ConfigManager::GetInt(const std::string& key, int default_val) const {
  std::string str_val = GetString(key, "");
  if (str_val.empty()) {
    return default_val;
  }

  try {
    return std::stoi(str_val);
  } catch (const std::invalid_argument&) {
    std::cerr << "[ConfigManager] Invalid integer value: '" << str_val << "', using default: " << default_val << std::endl;
    return default_val;
  } catch (const std::out_of_range&) {
    std::cerr << "[ConfigManager] Out of range integer value: '" << str_val << "', using default: " << default_val << std::endl;
    return default_val;
  }
}

int64_t ConfigManager::GetInt64(const std::string& key,
                                int64_t default_val) const {
  std::string str_val = GetString(key, "");
  if (str_val.empty()) {
    return default_val;
  }

  try {
    return std::stoll(str_val);
  } catch (const std::invalid_argument&) {
    std::cerr << "[ConfigManager] Invalid integer value: '" << str_val << "', using default: " << default_val << std::endl;
    return default_val;
  } catch (const std::out_of_range&) {
    std::cerr << "[ConfigManager] Out of range integer value: '" << str_val << "', using default: " << default_val << std::endl;
    return default_val;
  }
}

double ConfigManager::GetDouble(const std::string& key,
                                double default_val) const {
  std::string str_val = GetString(key, "");
  if (str_val.empty()) {
    return default_val;
  }

  try {
    return std::stod(str_val);
  } catch (const std::invalid_argument&) {
    std::cerr << "[ConfigManager] Invalid double value: '" << str_val << "', using default: " << default_val << std::endl;
    return default_val;
  } catch (const std::out_of_range&) {
    std::cerr << "[ConfigManager] Out of range double value: '" << str_val << "', using default: " << default_val << std::endl;
    return default_val;
  }
}

bool ConfigManager::GetBool(const std::string& key, bool default_val) const {
  std::string str_val = GetString(key, "");
  if (str_val.empty()) {
    return default_val;
  }

  // Convert to lowercase for comparison
  std::transform(str_val.begin(), str_val.end(), str_val.begin(), ::tolower);

  if (str_val == "true" || str_val == "yes" || str_val == "1" ||
      str_val == "on") {
    return true;
  }
  if (str_val == "false" || str_val == "no" || str_val == "0" ||
      str_val == "off") {
    return false;
  }

  return default_val;
}

std::vector<std::string> ConfigManager::GetStringArray(
    const std::string& key,
    const std::vector<std::string>& default_val) const {
  std::lock_guard<std::mutex> lock(impl_->mutex_);

  auto node = FindNode(impl_->root_, key);
  if (!node) {
    return default_val;
  }

  std::vector<std::string> result;
  for (const auto& [idx, child] : node->children) {
    if (child->is_scalar) {
      result.push_back(child->value);
    }
  }

  return result.empty() ? default_val : result;
}

bool ConfigManager::HasKey(const std::string& key) const {
  std::lock_guard<std::mutex> lock(impl_->mutex_);

  if (impl_->flattened_.find(key) != impl_->flattened_.end()) {
    return true;
  }

  auto node = FindNode(impl_->root_, key);
  return node != nullptr;
}

void ConfigManager::SetString(const std::string& key,
                              const std::string& value) {
  std::string old_value;
  ConfigChangeType change_type;
  {
    std::lock_guard<std::mutex> lock(impl_->mutex_);

    auto node = FindNode(impl_->root_, key);
    if (node && node->is_scalar) {
      old_value = node->value;
    }

    SetNodeValue(impl_->root_, key, value);
    impl_->UpdateFlattened();

    change_type = old_value.empty()
                      ? ConfigChangeType::kValueAdded
                      : ConfigChangeType::kValueChanged;
  }
  impl_->NotifyWatchers(key, old_value, value, change_type);
}

void ConfigManager::SetInt(const std::string& key, int value) {
  SetString(key, std::to_string(value));
}

void ConfigManager::SetInt64(const std::string& key, int64_t value) {
  SetString(key, std::to_string(value));
}

void ConfigManager::SetDouble(const std::string& key, double value) {
  SetString(key, std::to_string(value));
}

void ConfigManager::SetBool(const std::string& key, bool value) {
  SetString(key, value ? "true" : "false");
}

void ConfigManager::SetStringArray(const std::string& key,
                                   const std::vector<std::string>& value) {
  // Clear existing
  {
    std::lock_guard<std::mutex> lock(impl_->mutex_);
    auto node = FindNode(impl_->root_, key);
    if (node) {
      node->children.clear();
    }
  }

  // Set new values
  for (size_t i = 0; i < value.size(); ++i) {
    SetString(key + "." + std::to_string(i), value[i]);
  }
}

bool ConfigManager::RemoveKey(const std::string& key) {
  std::string old_value;
  bool removed = false;
  {
    std::lock_guard<std::mutex> lock(impl_->mutex_);

    // Note: Full removal from tree structure is complex with the current
    // implementation. For now, we'll just remove from flattened map and
    // mark the node as non-scalar.
    auto it = impl_->flattened_.find(key);
    if (it != impl_->flattened_.end()) {
      old_value = it->second;
      impl_->flattened_.erase(it);

      auto node = FindNode(impl_->root_, key);
      if (node) {
        node->is_scalar = false;
        node->value.clear();
      }
      removed = true;
    }
  }

  if (removed) {
    impl_->NotifyWatchers(key, old_value, "", ConfigChangeType::kValueRemoved);
  }
  return removed;
}

Status ConfigManager::Validate(const std::string& schema_path) {
  // For now, just check if file exists
  std::ifstream file(schema_path);
  if (!file.is_open()) {
    return Status::IOError("Cannot open schema file: " + schema_path);
  }
  file.close();

  // JSON Schema validation requires a JSON Schema library (e.g., nlohmann/json-schema).
  // For now, schema file existence is the only check.
  return Status::OK();
}

Status ConfigManager::ValidateBasic(
    const std::vector<std::string>& required_keys) {
  for (const auto& key : required_keys) {
    if (!HasKey(key)) {
      return Status::InvalidArgument("Missing required config key: " + key);
    }
  }
  return Status::OK();
}

Status ConfigManager::Merge(const ConfigManager& other) {
  std::vector<std::tuple<std::string, std::string, std::string>> notifications;
  {
    // Use scoped_lock to avoid deadlock when two ConfigManagers merge each other simultaneously
    std::scoped_lock lock(impl_->mutex_, other.impl_->mutex_);

    for (const auto& [key, value] : other.impl_->flattened_) {
      std::string old_value;
      auto it = impl_->flattened_.find(key);
      if (it != impl_->flattened_.end()) {
        old_value = it->second;
      }

      SetNodeValue(impl_->root_, key, value);
      notifications.emplace_back(key, old_value, value);
    }

    impl_->UpdateFlattened();
  }

  for (auto& [key, old_value, new_value] : notifications) {
    ConfigChangeType change_type = old_value.empty()
                                       ? ConfigChangeType::kValueAdded
                                       : ConfigChangeType::kValueChanged;
    impl_->NotifyWatchers(key, old_value, new_value, change_type);
  }
  return Status::OK();
}

Status ConfigManager::MergeWithPrefix(const ConfigManager& other,
                                      const std::string& prefix) {
  std::vector<std::tuple<std::string, std::string, std::string>> notifications;
  {
    std::lock_guard<std::mutex> lock(impl_->mutex_);
    std::lock_guard<std::mutex> other_lock(other.impl_->mutex_);

    for (const auto& [key, value] : other.impl_->flattened_) {
      std::string prefixed_key = prefix + "." + key;
      std::string old_value;
      auto it = impl_->flattened_.find(prefixed_key);
      if (it != impl_->flattened_.end()) {
        old_value = it->second;
      }

      SetNodeValue(impl_->root_, prefixed_key, value);
      notifications.emplace_back(prefixed_key, old_value, value);
    }

    impl_->UpdateFlattened();
  }

  for (auto& [key, old_value, new_value] : notifications) {
    ConfigChangeType change_type = old_value.empty()
                                       ? ConfigChangeType::kValueAdded
                                       : ConfigChangeType::kValueChanged;
    impl_->NotifyWatchers(key, old_value, new_value, change_type);
  }
  return Status::OK();
}

std::string ConfigManager::Dump() const {
  std::lock_guard<std::mutex> lock(impl_->mutex_);

  std::ostringstream out;
  DumpNode(impl_->root_, out, 0);
  return out.str();
}

std::string ConfigManager::DumpWithComments() const {
  // For now, just add a header comment
  std::string result = "# CedarGraph Configuration\n";
  result += "# Generated automatically\n";
  result += "\n";
  result += Dump();
  return result;
}

std::string ConfigManager::DumpSection(const std::string& section_key) const {
  std::lock_guard<std::mutex> lock(impl_->mutex_);

  auto node = FindNode(impl_->root_, section_key);
  if (!node) {
    return "";
  }

  std::ostringstream out;
  out << section_key << ":\n";
  DumpNode(node, out, 2);
  return out.str();
}

StatusOr<int64_t> ConfigManager::Watch(const std::string& key,
                                       ConfigChangeCallback callback) {
  std::lock_guard<std::mutex> lock(impl_->mutex_);

  int64_t watch_id = impl_->next_watch_id_++;
  impl_->watchers_[watch_id] = callback;
  impl_->watch_keys_[watch_id] = key;

  return watch_id;
}

StatusOr<int64_t> ConfigManager::WatchAll(ConfigChangeCallback callback) {
  return Watch("", callback);
}

Status ConfigManager::Unwatch(int64_t watch_id) {
  std::lock_guard<std::mutex> lock(impl_->mutex_);

  auto it = impl_->watchers_.find(watch_id);
  if (it == impl_->watchers_.end()) {
    return Status::NotFound("Watch ID not found: " + std::to_string(watch_id));
  }

  impl_->watchers_.erase(it);
  impl_->watch_keys_.erase(watch_id);

  return Status::OK();
}

Status ConfigManager::EnableHotReload(const std::string& filepath,
                                      int interval_ms) {
  DisableHotReload();

  impl_->hot_reload_interval_ms_ = interval_ms;
  impl_->hot_reload_enabled_ = true;
  impl_->source_file_ = filepath;

  impl_->hot_reload_last_mtime_ = 0;
  {
    struct stat st;
    if (stat(impl_->source_file_.c_str(), &st) == 0) {
      impl_->hot_reload_last_mtime_ = st.st_mtime;
    }
  }

  impl_->hot_reload_thread_ = std::make_unique<std::thread>([this]() {
    while (impl_->hot_reload_enabled_) {
      std::this_thread::sleep_for(
          std::chrono::milliseconds(impl_->hot_reload_interval_ms_));
      if (!impl_->hot_reload_enabled_) {
        break;
      }
      if (!impl_->source_file_.empty()) {
        struct stat st;
        if (stat(impl_->source_file_.c_str(), &st) == 0) {
          if (st.st_mtime != impl_->hot_reload_last_mtime_) {
            impl_->hot_reload_last_mtime_ = st.st_mtime;
            Reload();
          }
        }
      }
    }
  });

  return Status::OK();
}

void ConfigManager::DisableHotReload() {
  impl_->hot_reload_enabled_ = false;

  if (impl_->hot_reload_thread_ && impl_->hot_reload_thread_->joinable()) {
    impl_->hot_reload_thread_->join();
    impl_->hot_reload_thread_.reset();
  }
}

bool ConfigManager::IsHotReloadEnabled() const {
  return impl_->hot_reload_enabled_;
}

Status ConfigManager::Reload() {
  std::unique_lock<std::mutex> lock(impl_->mutex_);
  if (impl_->source_file_.empty()) {
    return Status::InvalidArgument("No source file configured for reload");
  }

  // Release lock before calling LoadFromFile to avoid deadlock
  lock.unlock();
  return LoadFromFile(impl_->source_file_);
}

std::vector<std::string> ConfigManager::GetAllKeys() const {
  std::lock_guard<std::mutex> lock(impl_->mutex_);

  std::vector<std::string> keys;
  for (const auto& [key, _] : impl_->flattened_) {
    keys.push_back(key);
  }

  return keys;
}

std::vector<std::string> ConfigManager::GetKeysWithPrefix(
    const std::string& prefix) const {
  std::lock_guard<std::mutex> lock(impl_->mutex_);

  std::vector<std::string> keys;
  for (const auto& [key, _] : impl_->flattened_) {
    if (key.find(prefix) == 0) {
      keys.push_back(key);
    }
  }

  return keys;
}

void ConfigManager::Clear() {
  std::lock_guard<std::mutex> lock(impl_->mutex_);

  impl_->root_ = std::make_shared<ConfigNode>();
  impl_->flattened_.clear();
}

bool ConfigManager::IsEmpty() const {
  std::lock_guard<std::mutex> lock(impl_->mutex_);
  return impl_->flattened_.empty();
}

size_t ConfigManager::Size() const {
  std::lock_guard<std::mutex> lock(impl_->mutex_);
  return impl_->flattened_.size();
}

std::string ConfigManager::GetSourceFile() const {
  return impl_->source_file_;
}

int64_t ConfigManager::GetLastModifiedTime() const {
  return impl_->last_modified_time_;
}

}  // namespace governance
}  // namespace cedar
