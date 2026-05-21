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
#include <chrono>

#include "cedar/transaction/occ_transaction.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/lsm_engine.h"

using namespace cedar;

TEST(WalOrderingTest, WalFailurePreservesOldValue) {
  std::string data_dir = "/tmp/test_wal_ordering_" +
      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
  std::filesystem::remove_all(data_dir);
  std::filesystem::create_directories(data_dir);

  CedarOptions options;
  options.create_if_missing = true;
  CedarGraphStorage* storage = nullptr;
  ASSERT_TRUE(CedarGraphStorage::Open(options, data_dir, &storage).ok());
  ASSERT_NE(storage, nullptr);

  LsmEngine* lsm_engine = storage->GetLsmEngine();
  ASSERT_NE(lsm_engine, nullptr);

  TransactionManager txn_manager;
  TransactionOptions txn_options;

  // Step 1: Write an initial committed value (no WAL needed).
  {
    OCCTransaction txn(&txn_manager, lsm_engine->GetMemTable(),
                       lsm_engine, nullptr, txn_options);
    ASSERT_TRUE(txn.Begin().ok());

    std::vector<WriteSetEntry> entries;
    CedarKey key = CedarKey::Vertex(1, VertexColumnId(0), Timestamp(100), 0, 0);
    entries.emplace_back(1, EntityType::Vertex, 0,
                         Descriptor::InlineInt(0, 42),
                         key, Timestamp(100), Timestamp(0), 0);
    ASSERT_TRUE(txn.PutBatch(entries).ok());
    ASSERT_TRUE(txn.Commit().ok());
  }

  // Step 2: Attempt to overwrite with a WAL writer that has NOT been opened,
  // so WriteBatch will fail with "not opened".
  {
    WalOptions wal_options;
    wal_options.group_commit_timeout_us = 0;  // direct write path
    WalWriter failing_wal(data_dir + "/wal", cedar::Env::Default(), wal_options);
    // Deliberately do NOT call failing_wal.Open() so current_file_ == nullptr.

    OCCTransaction txn(&txn_manager, lsm_engine->GetMemTable(),
                       lsm_engine, &failing_wal, txn_options);
    ASSERT_TRUE(txn.Begin().ok());

    std::vector<WriteSetEntry> entries;
    CedarKey key = CedarKey::Vertex(1, VertexColumnId(0), Timestamp(200), 0, 0);
    entries.emplace_back(1, EntityType::Vertex, 0,
                         Descriptor::InlineInt(0, 99),
                         key, Timestamp(200), Timestamp(0), 0);
    ASSERT_TRUE(txn.PutBatch(entries).ok());

    Status commit_status = txn.Commit();
    EXPECT_FALSE(commit_status.ok())
        << "Commit should fail because WAL is not opened";
  }

  // Step 3: Read back with a fresh transaction; must still see the old value.
  {
    OCCTransaction txn(&txn_manager, lsm_engine->GetMemTable(),
                       lsm_engine, nullptr, txn_options);
    ASSERT_TRUE(txn.Begin().ok());

    Descriptor desc;
    Timestamp ts;
    Status get_status = txn.Get(1, EntityType::Vertex, 0, &desc, &ts);
    ASSERT_TRUE(get_status.ok()) << get_status.ToString();

    auto val = desc.AsInlineInt();
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(val.value(), 42)
        << "Old value should be preserved when WAL fails before memtable write";
  }

  delete storage;
  std::filesystem::remove_all(data_dir);
}
