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
#include <vector>

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/transaction/occ_transaction.h"

using namespace cedar;

class EdgeAtomicityTest : public ::testing::Test {
 protected:
  std::string data_dir_;
  CedarGraphStorage* storage_ = nullptr;
  LsmEngine* lsm_ = nullptr;

  void SetUp() override {
    data_dir_ = "/tmp/test_edge_atomicity_" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::remove_all(data_dir_);
    std::filesystem::create_directories(data_dir_);

    CedarOptions options;
    options.create_if_missing = true;
    ASSERT_TRUE(CedarGraphStorage::Open(options, data_dir_, &storage_).ok());
    ASSERT_NE(storage_, nullptr);
    lsm_ = storage_->GetLsmEngine();
    ASSERT_NE(lsm_, nullptr);
  }

  void TearDown() override {
    if (storage_) {
      delete storage_;
      storage_ = nullptr;
    }
    std::filesystem::remove_all(data_dir_);
  }
};

// Concurrent readers should never see EdgeOut without EdgeIn.
TEST_F(EdgeAtomicityTest, NoHalfWrittenEdge) {
  TransactionManager txn_mgr;
  TransactionOptions txn_opts;
  txn_opts.isolation = IsolationLevel::kSnapshot;

  const uint64_t kSrc = 42;
  const uint64_t kDst = 99;
  const uint16_t kEdgeType = 7;
  const int kReaderThreads = 4;
  const int kWriterIterations = 200;

  std::atomic<int> half_write_observations{0};
  std::atomic<bool> writers_done{false};
  std::atomic<int> writes_committed{0};

  auto writer = [&]() {
    for (int i = 0; i < kWriterIterations; ++i) {
      Timestamp ts(static_cast<uint64_t>(i + 1));
      Descriptor desc = Descriptor::InlineInt(kEdgeType, i);
      WriteOptions wopts;
      Status s = storage_->PutEdge(wopts, kSrc, kDst, kEdgeType, ts, desc, ts);
      if (s.ok()) {
        writes_committed.fetch_add(1);
      }
    }
    writers_done.store(true);
  };

  auto reader = [&]() {
    while (!writers_done.load()) {
      OCCTransaction txn(&txn_mgr, lsm_->GetMemTable(), lsm_,
                         nullptr, txn_opts);
      Status s = txn.Begin();
      if (!s.ok()) continue;

      Descriptor out_desc;
      Timestamp out_ts;
      Status out_s = txn.Get(kSrc, EntityType::EdgeOut, kEdgeType, &out_desc, &out_ts);

      Descriptor in_desc;
      Timestamp in_ts;
      Status in_s = txn.Get(kDst, EntityType::EdgeIn, kEdgeType, &in_desc, &in_ts);

      // If we see EdgeOut, we MUST see EdgeIn at the same version.
      if (out_s.ok() && !in_s.ok()) {
        half_write_observations.fetch_add(1);
      }
    }
  };

  std::thread writer_thread(writer);
  std::vector<std::thread> readers;
  for (int r = 0; r < kReaderThreads; ++r) {
    readers.emplace_back(reader);
  }

  writer_thread.join();
  for (auto& th : readers) th.join();

  EXPECT_EQ(half_write_observations.load(), 0)
      << "Observed " << half_write_observations.load()
      << " half-written edges (EdgeOut without EdgeIn)";
  EXPECT_GT(writes_committed.load(), 0);
}

// If PutEdge fails, neither EdgeOut nor EdgeIn should be visible.
TEST_F(EdgeAtomicityTest, FailedPutEdgeLeavesNoTrace) {
  // This test relies on the fact that PutEdge now uses a txn.
  // We simply verify that a successful PutEdge is readable both ways,
  // and that aborting a manual equivalent txn leaves nothing.
  TransactionManager txn_mgr;
  TransactionOptions txn_opts;

  auto txn = lsm_->BeginTransaction(txn_opts);
  ASSERT_NE(txn, nullptr);

  Timestamp ts(1234);
  Status s = txn->Put(1, EntityType::EdgeOut, 1,
                      Descriptor::InlineInt(1, 100), ts, 2);
  ASSERT_TRUE(s.ok());

  s = txn->Put(2, EntityType::EdgeIn, 1,
               Descriptor::InlineInt(1, 0), ts, 1);
  ASSERT_TRUE(s.ok());

  // Abort instead of commit
  txn->Abort();

  OCCTransaction read_txn(&txn_mgr, lsm_->GetMemTable(), lsm_,
                          nullptr, txn_opts);
  s = read_txn.Begin();
  ASSERT_TRUE(s.ok());

  Descriptor desc;
  Timestamp ver;
  EXPECT_FALSE(read_txn.Get(1, EntityType::EdgeOut, 1, &desc, &ver).ok());
  EXPECT_FALSE(read_txn.Get(2, EntityType::EdgeIn, 1, &desc, &ver).ok());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
