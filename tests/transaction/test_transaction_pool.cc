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

#include "cedar/transaction/transaction_pool.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/lsm_engine.h"

using namespace cedar;

class TransactionPoolTest : public ::testing::Test {
 protected:
  std::string data_dir_;
  cedar::CedarGraphStorage* storage_ = nullptr;
  cedar::LsmEngine* lsm_engine_ = nullptr;
  std::unique_ptr<TransactionManager> txn_manager_;
  std::unique_ptr<TransactionPool> pool_;

  void SetUp() override {
    data_dir_ = "/tmp/test_txn_pool_" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::remove_all(data_dir_);
    std::filesystem::create_directories(data_dir_);

    cedar::CedarOptions options;
    options.create_if_missing = true;
    options.enable_accumulated_flush = false;
    auto status = cedar::CedarGraphStorage::Open(options, data_dir_, &storage_);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_NE(storage_, nullptr);

    lsm_engine_ = storage_->GetLsmEngine();
    ASSERT_NE(lsm_engine_, nullptr);

    txn_manager_ = std::make_unique<TransactionManager>();
    pool_ = std::make_unique<TransactionPool>(
        txn_manager_.get(), lsm_engine_->GetMemTable(), lsm_engine_, nullptr, 2);
  }

  void TearDown() override {
    pool_.reset();
    txn_manager_.reset();
    if (storage_) {
      delete storage_;
      storage_ = nullptr;
    }
    std::filesystem::remove_all(data_dir_);
  }
};

TEST_F(TransactionPoolTest, AcquireReleaseDoesNotCrash) {
  auto* txn = pool_->Acquire(TransactionOptions{});
  ASSERT_NE(txn, nullptr);
  EXPECT_EQ(txn->GetState(), TransactionState::kActive);
  pool_->Release(txn);
}

TEST_F(TransactionPoolTest, ReusedTransactionHasEmptySets) {
  auto* txn1 = pool_->Acquire(TransactionOptions{});
  ASSERT_NE(txn1, nullptr);

  Status s = txn1->Put(1, EntityType::Vertex, 0, Descriptor::InlineInt(0, 42));
  ASSERT_TRUE(s.ok());

  pool_->Release(txn1);

  auto* txn2 = pool_->Acquire(TransactionOptions{});
  ASSERT_NE(txn2, nullptr);
  EXPECT_EQ(txn2->GetState(), TransactionState::kActive);

  // Without Cleanup, the old write_set_ would leak and Commit might fail
  s = txn2->Put(2, EntityType::Vertex, 0, Descriptor::InlineInt(0, 99));
  ASSERT_TRUE(s.ok());

  s = txn2->Begin();
  ASSERT_TRUE(s.ok());

  s = txn2->Commit();
  EXPECT_TRUE(s.ok()) << "Commit failed with stale state: " << s.ToString();
}

TEST_F(TransactionPoolTest, PoolCreatesNewWhenExhausted) {
  auto* txn1 = pool_->Acquire(TransactionOptions{});
  auto* txn2 = pool_->Acquire(TransactionOptions{});
  ASSERT_NE(txn1, nullptr);
  ASSERT_NE(txn2, nullptr);

  auto* txn3 = pool_->Acquire(TransactionOptions{});
  ASSERT_NE(txn3, nullptr);
  EXPECT_NE(txn3, txn1);
  EXPECT_NE(txn3, txn2);

  pool_->Release(txn1);
  pool_->Release(txn2);
  pool_->Release(txn3);
}
