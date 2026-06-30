// Copyright 2026 CedarGraph Authors
//
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

namespace {

std::string ReadFile(const std::filesystem::path& path) {
  std::ifstream in(path);
  std::ostringstream ss;
  ss << in.rdbuf();
  return ss.str();
}

std::vector<std::string> SplitLines(const std::string& text) {
  std::vector<std::string> lines;
  std::istringstream in(text);
  std::string line;
  while (std::getline(in, line)) {
    lines.push_back(line);
  }
  return lines;
}

bool IsTestSource(const std::filesystem::path& path) {
  const auto ext = path.extension().string();
  return ext == ".cc" || ext == ".cpp" || ext == ".h";
}

}  // namespace

TEST(DisabledTests, EveryDisabledTestDocumentsBlockedReason) {
  const std::filesystem::path tests_dir =
      std::filesystem::path(__FILE__).parent_path();
  ASSERT_TRUE(std::filesystem::exists(tests_dir));

  std::vector<std::string> undocumented;
  for (const auto& entry :
       std::filesystem::recursive_directory_iterator(tests_dir)) {
    if (!entry.is_regular_file() || !IsTestSource(entry.path())) {
      continue;
    }
    if (entry.path().filename() == "test_disabled_tests_documented.cc") {
      continue;
    }

    auto lines = SplitLines(ReadFile(entry.path()));
    for (size_t i = 0; i < lines.size(); ++i) {
      if (lines[i].find("DISABLED_") == std::string::npos) {
        continue;
      }

      bool has_blocked_reason = false;
      const size_t begin = i > 5 ? i - 5 : 0;
      for (size_t j = begin; j <= i; ++j) {
        if (lines[j].find("BLOCKED:") != std::string::npos) {
          has_blocked_reason = true;
          break;
        }
      }

      if (!has_blocked_reason) {
        undocumented.push_back(entry.path().string() + ":" +
                               std::to_string(i + 1));
      }
    }
  }

  EXPECT_TRUE(undocumented.empty())
      << "DISABLED_ tests must document a nearby BLOCKED: reason. First hit: "
      << (undocumented.empty() ? "" : undocumented.front());
}
