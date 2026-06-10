// Copyright 2025 The Cedar Authors
//
// End-to-end test for StorageD node registration and advertise address
// configuration. Launches the actual cedar-storaged binary as a subprocess.

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>

class NodeRegistrationTest : public ::testing::Test {
 protected:
  std::string test_dir_;
  std::string storaged_binary_;

  void SetUp() override {
    test_dir_ = "/tmp/test_node_reg_" + std::to_string(getpid()) + "_" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(test_dir_);

    std::vector<std::string> candidates = {
        "./cedar-storaged",
        "../cedar-storaged",
        "../../cedar-storaged",
    };
    storaged_binary_.clear();
    for (const auto& cand : candidates) {
      if (std::filesystem::exists(cand)) {
        storaged_binary_ = std::filesystem::absolute(cand).string();
        break;
      }
    }
  }

  void TearDown() override {
    std::filesystem::remove_all(test_dir_);
  }

  // Run storaged with given CLI args, capture combined stdout+stderr, return it.
  std::string RunStoraged(const std::vector<std::string>& extra_args,
                          int timeout_ms = 1200) {
    if (storaged_binary_.empty()) {
      return "BINARY_NOT_FOUND";
    }

    int pipefd[2];
    if (pipe(pipefd) != 0) {
      return "PIPE_ERROR";
    }

    pid_t pid = fork();
    if (pid < 0) {
      close(pipefd[0]);
      close(pipefd[1]);
      return "FORK_ERROR";
    }

    if (pid == 0) {
      // Child
      close(pipefd[0]);
      dup2(pipefd[1], STDOUT_FILENO);
      dup2(pipefd[1], STDERR_FILENO);
      close(pipefd[1]);

      std::vector<const char*> argv;
      argv.push_back("cedar-storaged");
      argv.push_back("--test_mode");
      argv.push_back("--data_dir");
      argv.push_back(test_dir_.c_str());
      argv.push_back("--heartbeat_interval");
      argv.push_back("3600");
      for (const auto& a : extra_args) {
        argv.push_back(a.c_str());
      }
      argv.push_back(nullptr);

      execv(storaged_binary_.c_str(), const_cast<char* const*>(argv.data()));
      _exit(127);
    }

    // Parent
    close(pipefd[1]);

    // Read with timeout
    std::string output;
    char buffer[1024];
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);

    while (std::chrono::steady_clock::now() < deadline) {
      fd_set fds;
      FD_ZERO(&fds);
      FD_SET(pipefd[0], &fds);
      auto remaining = deadline - std::chrono::steady_clock::now();
      long ms = std::max(0L, static_cast<long>(
          std::chrono::duration_cast<std::chrono::milliseconds>(remaining).count()));
      struct timeval tv;
      tv.tv_sec = ms / 1000;
      tv.tv_usec = (ms % 1000) * 1000;

      int ret = select(pipefd[0] + 1, &fds, nullptr, nullptr, &tv);
      if (ret > 0 && FD_ISSET(pipefd[0], &fds)) {
        ssize_t n = read(pipefd[0], buffer, sizeof(buffer) - 1);
        if (n > 0) {
          buffer[n] = '\0';
          output.append(buffer, static_cast<size_t>(n));
        } else {
          break;
        }
      } else {
        break;
      }
    }

    close(pipefd[0]);
    kill(pid, SIGTERM);
    int status = 0;
    waitpid(pid, &status, 0);
    return output;
  }
};

TEST_F(NodeRegistrationTest, AdvertiseAddressCliFlag) {
  if (storaged_binary_.empty()) GTEST_SKIP() << "storaged binary not found";

  std::string output = RunStoraged({"--advertise_address", "10.0.0.5"});

  EXPECT_NE(output.find("\"advertise_address\":\"10.0.0.5\""), std::string::npos)
      << "Expected advertise address 10.0.0.5 in output:\n" << output;
  EXPECT_EQ(output.find("127.0.0.1:9779"), std::string::npos)
      << "Expected NO 127.0.0.1:9779 fallback in output:\n" << output;
}

TEST_F(NodeRegistrationTest, AdvertiseAddressEnvOverride) {
  if (storaged_binary_.empty()) GTEST_SKIP() << "storaged binary not found";

  setenv("CEDAR_ADVERTISE_ADDRESS", "10.0.0.6", 1);
  std::string output = RunStoraged({});
  unsetenv("CEDAR_ADVERTISE_ADDRESS");

  EXPECT_NE(output.find("\"advertise_address\":\"10.0.0.6\""), std::string::npos)
      << "Expected CEDAR_ADVERTISE_ADDRESS=10.0.0.6 in output:\n" << output;
}

TEST_F(NodeRegistrationTest, BindAddressFallbackWhenNotWildcard) {
  if (storaged_binary_.empty()) GTEST_SKIP() << "storaged binary not found";

  std::string output = RunStoraged({"--bind", "192.168.1.10"});

  EXPECT_NE(output.find("192.168.1.10:9779"), std::string::npos)
      << "Expected bind address fallback 192.168.1.10:9779 in output:\n" << output;
}

TEST_F(NodeRegistrationTest, LocalhostFallbackWarning) {
  if (storaged_binary_.empty()) GTEST_SKIP() << "storaged binary not found";

  std::string output = RunStoraged({"--bind", "0.0.0.0"});

  EXPECT_NE(output.find("127.0.0.1"), std::string::npos)
      << "Expected localhost fallback in output:\n" << output;
  EXPECT_NE(output.find("advertise_address not set"), std::string::npos)
      << "Expected warning about missing advertise_address:\n" << output;
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
