#ifndef CEDAR_TEST_TEMP_DIR_H_
#define CEDAR_TEST_TEMP_DIR_H_

#include <filesystem>
#include <string>
#include <random>
#include <sstream>
#include <chrono>
#include <functional>
#include <thread>

namespace cedar {
namespace test {

// Generate a unique temporary directory path for tests.
inline std::string MakeTempDir(const std::string& prefix) {
  static std::mt19937 rng(std::random_device{}());
  std::uniform_int_distribution<uint32_t> dist(0, 0xFFFFFFFF);
  std::ostringstream oss;
  oss << std::filesystem::temp_directory_path().string()
      << "/" << prefix << "_" << std::hex << dist(rng);
  return oss.str();
}

// RAII wrapper: creates temp dir on construction, removes on destruction.
class ScopedTempDir {
 public:
  explicit ScopedTempDir(const std::string& prefix)
      : path_(MakeTempDir(prefix)) {
    std::filesystem::create_directories(path_);
  }

  ~ScopedTempDir() {
    std::error_code ec;
    std::filesystem::remove_all(path_, ec);
  }

  const std::string& Path() const { return path_; }
  operator std::string() const { return path_; }

  ScopedTempDir(const ScopedTempDir&) = delete;
  ScopedTempDir& operator=(const ScopedTempDir&) = delete;

 private:
  std::string path_;
};

// Poll a condition with timeout. Returns true if condition met, false on timeout.
// Replaces fragile sleep_for in tests.
inline bool WaitForCondition(std::function<bool()> condition,
                             std::chrono::milliseconds timeout,
                             std::chrono::milliseconds poll_interval = std::chrono::milliseconds(10)) {
  auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (condition()) return true;
    std::this_thread::sleep_for(poll_interval);
  }
  return condition();  // Final check
}

}  // namespace test
}  // namespace cedar

#endif  // CEDAR_TEST_TEMP_DIR_H_
