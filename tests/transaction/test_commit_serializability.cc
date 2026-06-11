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
#include <thread>
#include <atomic>
#include <chrono>

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/transaction/occ_transaction.h"

using namespace cedar;

// P0-6: Two transactions write the same key concurrently.
// With proper serialization, exactly one must commit.
TEST(CommitSerializabilityTest, ConcurrentWritesToSameKey) {
  std::string data_dir = "/tmp/test_commit_serializability_" +
      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
  std::filesystem::remove_all(data_dir);
  std::filesystem::create_directories(data_dir);

  CedarOptions options;
  options.create_if_missing = true;
  CedarGraphStorage* storage = nullptr;
  ASSERT_TRUE(CedarGraphStorage::Open(options, data_dir, &storage).ok());
  ASSERT_NE(storage, nullptr);

  LsmEngine* lsm = storage->GetLsmEngine();
  ASSERT_NE(lsm, nullptr);

  TransactionManager txn_mgr;
  TransactionOptions txn_opts;
  txn_opts.isolation = IsolationLevel::kSnapshot;

  std::atomic<int> commits{0};
  std::atomic<int> aborts{0};
  const int kThreads = 4;
  const int kIterations = 50;

  auto worker = [&](int worker_id) {
    for (int i = 0; i < kIterations; ++i) {
      uint64_t key = 1000 + i;  // same key across threads per iteration

      OCCTransaction txn(&txn_mgr, lsm->GetMemTable(), lsm,
                         lsm->GetWalWriter(), txn_opts);
      Status s = txn.Begin();
      if (!s.ok()) { aborts.fetch_add(1); continue; }

      s = txn.Put(key, EntityType::Vertex, 0,
                  Descriptor::InlineInt(0, worker_id),
                  Timestamp(static_cast<uint64_t>(i)), 0);
      if (!s.ok()) { txn.Abort(); aborts.fetch_add(1); continue; }

      s = txn.Commit();
      if (s.ok()) {
        commits.fetch_add(1);
      } else {
        aborts.fetch_add(1);
      }
    }
  };

  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back(worker, t);
  }
  for (auto& th : threads) th.join();

  // With proper serialization, for each key exactly one of the 4 threads
  // should succeed. Total commits == number of unique keys written.
  EXPECT_EQ(commits.load(), kIterations)
      << "Expected exactly one commit per key (" << kIterations
      << "), got " << commits.load();

  // Verify no key has more than one committed version visible
  for (int i = 0; i < kIterations; ++i) {
    uint64_t key = 1000 + i;
    OCCTransaction read_txn(&txn_mgr, lsm->GetMemTable(), lsm,
                            nullptr, txn_opts);
    Status s = read_txn.Begin();
    ASSERT_TRUE(s.ok());

    Descriptor desc;
    Timestamp ts;
    s = read_txn.Get(key, EntityType::Vertex, 0, &desc, &ts);
    ASSERT_TRUE(s.ok()) << "Key " << key << " should exist";

    auto val = desc.AsInlineInt();
    ASSERT_TRUE(val.has_value());
    // Value must be from one of the worker_ids (0..3)
    EXPECT_GE(val.value(), 0);
    EXPECT_LT(val.value(), kThreads);
  }

  delete storage;
  std::filesystem::remove_all(data_dir);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
