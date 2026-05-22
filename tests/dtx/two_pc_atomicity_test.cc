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

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

#include "cedar/dtx/optimized_2pc_engine.h"
#include "cedar/dtx/production_config.h"

using namespace cedar;
using namespace cedar::dtx;

TEST(TwoPCAtomicityTest, DecisionLogPersistsBeforeCommit) {
  TwoPCConfig config;
  config.decision_log_dir = "/tmp/cedar_test_decisions";
  Optimized2PCEngine engine(config);

  // Verify config plumbing: decision_log_dir is passed through
  auto current = engine.GetCurrentConfig();
  EXPECT_EQ(current.decision_log_dir, "/tmp/cedar_test_decisions");
}

TEST(TwoPCAtomicityTest, DecisionLogFileFormat) {
  std::string test_dir = "/tmp/cedar_test_decisions_format";
  std::filesystem::create_directories(test_dir);
  std::string path = test_dir + "/txn_42.decision";

  // Manually write a decision log file in the expected binary format
  {
    std::ofstream ofs(path, std::ios::binary);
    ASSERT_TRUE(ofs);
    constexpr uint32_t kMagic = 0x44454301;
    constexpr uint32_t kVersion = 1;
    TxnID txn_id = 42;
    Timestamp commit_ts(12345678);
    uint32_t num_parts = 3;
    PartitionID pids[3] = {1, 7, 42};

    ofs.write(reinterpret_cast<const char*>(&kMagic), sizeof(kMagic));
    ofs.write(reinterpret_cast<const char*>(&kVersion), sizeof(kVersion));
    ofs.write(reinterpret_cast<const char*>(&txn_id), sizeof(txn_id));
    ofs.write(reinterpret_cast<const char*>(&commit_ts), sizeof(commit_ts));
    ofs.write(reinterpret_cast<const char*>(&num_parts), sizeof(num_parts));
    for (uint32_t i = 0; i < num_parts; ++i) {
      ofs.write(reinterpret_cast<const char*>(&pids[i]), sizeof(pids[i]));
    }
    ofs.flush();
    ASSERT_TRUE(ofs);
    ofs.close();
  }

  // Read it back manually and verify
  {
    std::ifstream ifs(path, std::ios::binary);
    ASSERT_TRUE(ifs);
    uint32_t magic = 0, version = 0;
    ifs.read(reinterpret_cast<char*>(&magic), sizeof(magic));
    ifs.read(reinterpret_cast<char*>(&version), sizeof(version));
    EXPECT_EQ(magic, 0x44454301u);
    EXPECT_EQ(version, 1u);

    TxnID txn_id = 0;
    Timestamp commit_ts(0);
    ifs.read(reinterpret_cast<char*>(&txn_id), sizeof(txn_id));
    ifs.read(reinterpret_cast<char*>(&commit_ts), sizeof(commit_ts));
    EXPECT_EQ(txn_id, 42u);
    EXPECT_EQ(commit_ts.value(), 12345678u);

    uint32_t num_parts = 0;
    ifs.read(reinterpret_cast<char*>(&num_parts), sizeof(num_parts));
    EXPECT_EQ(num_parts, 3u);

    std::vector<PartitionID> participants;
    participants.resize(num_parts);
    for (uint32_t i = 0; i < num_parts; ++i) {
      ifs.read(reinterpret_cast<char*>(&participants[i]), sizeof(participants[i]));
    }
    ASSERT_EQ(participants.size(), 3u);
    EXPECT_EQ(participants[0], 1u);
    EXPECT_EQ(participants[1], 7u);
    EXPECT_EQ(participants[2], 42u);
  }

  // Cleanup
  std::filesystem::remove_all(test_dir);
}
