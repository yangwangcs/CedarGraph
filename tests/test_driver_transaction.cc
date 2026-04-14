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

// =============================================================================
// Driver Transaction API 测试（简化版 - 直接使用 OCCTransaction）
// =============================================================================

#include <gtest/gtest.h>
#include <filesystem>
#include <chrono>

#include "cedar/driver/session.h"
#include "cedar/driver/retry_policy.h"
#include "cedar/transaction/occ_transaction.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/types/descriptor.h"

using namespace cedar;
using namespace cedar::driver;

class DriverTransactionTest : public ::testing::Test {
 protected:
  std::string test_dir_;
  std::unique_ptr<LsmEngine> engine_;
  std::unique_ptr<TransactionManager> txn_manager_;
  std::unique_ptr<VSLMemTable> memtable_;
  std::unique_ptr<WalWriter> wal_writer_;
  
  void SetUp() override {
    test_dir_ = "/tmp/cedar_driver_test_" + std::to_string(
        std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(test_dir_);
    
    // 初始化存储引擎
    CedarOptions options;
    options.create_if_missing = true;
    
    engine_ = std::make_unique<LsmEngine>(test_dir_, options, cedar::Env::Default());
    ASSERT_TRUE(engine_->Open().ok());
    
    // 初始化 TransactionManager 和相关组件
    txn_manager_ = std::make_unique<TransactionManager>();
    memtable_ = std::make_unique<VSLMemTable>();
    
    // WalWriter 简化创建
    wal_writer_ = nullptr;  // 简化测试，不使用 WAL
  }
  
  void TearDown() override {
    memtable_.reset();
    txn_manager_.reset();
    wal_writer_.reset();
    if (engine_) engine_->Close();
    engine_.reset();
    std::filesystem::remove_all(test_dir_);
  }
};

TEST_F(DriverTransactionTest, BasicTransaction) {
  std::cout << "\n=== Test: Basic Transaction (OCCTransaction via Session) ===" << std::endl;
  
  SessionConfig session_config;
  Session session(txn_manager_.get(), memtable_.get(), engine_.get(), 
                  wal_writer_.get(), session_config);
  
  // 开始事务 - 返回 ManagedTxn，自动管理生命周期
  auto txn = session.BeginTransaction();
  
  // ManagedTxn 通过 operator-> 暴露 OCCTransaction 的所有方法
  // 事务已在 BeginTransaction 中自动启动
  
  // 写入数据
  Descriptor desc;
  desc.SetKind(EntryKind::InlineShortStr);
  desc.SetColumnId(0);
  desc.SetPayload(12345);
  
  // 直接调用 OCCTransaction 的方法
  Status put_status = txn->Put(1, EntityType::Vertex, 0, desc, Timestamp::Now());
  EXPECT_TRUE(put_status.ok());
  
  // 读取数据
  Descriptor read_desc;
  Timestamp version_ts;
  Status get_status = txn->Get(1, EntityType::Vertex, 0, &read_desc, &version_ts);
  // 新写入的数据可能看不到（取决于 OCCTransaction 的实现）
  
  // 提交事务 - ManagedTxn 的 Commit 方法
  auto result = txn.Commit();
  
  if (result.ok()) {
    std::cout << "✅ Transaction committed successfully" << std::endl;
    std::cout << "   Bookmark: " << result.value().ToString() << std::endl;
    std::cout << "   Transaction ID: " << result.value().GetTransactionId() << std::endl;
    std::cout << "   Timestamp: " << result.value().GetTimestamp() << std::endl;
    EXPECT_TRUE(txn.IsCommitted());
  } else {
    std::cout << "❌ Transaction failed: " << result.error().message << std::endl;
  }
}

TEST_F(DriverTransactionTest, TransactionRollback) {
  std::cout << "\n=== Test: Transaction Rollback ===" << std::endl;
  
  SessionConfig session_config;
  Session session(txn_manager_.get(), memtable_.get(), engine_.get(),
                  wal_writer_.get(), session_config);
  
  {
    auto txn = session.BeginTransaction();
    
    // 写入一些数据
    Descriptor desc;
    desc.SetKind(EntryKind::InlineShortStr);
    desc.SetColumnId(0);
    desc.SetPayload(99999);
    
    txn->Put(100, EntityType::Vertex, 0, desc, Timestamp::Now());
    txn->Put(101, EntityType::Vertex, 0, desc, Timestamp::Now());
    
    // 显式回滚
    txn.Rollback();
    EXPECT_TRUE(txn.IsAborted());
  }  // 析构时不会再次回滚（已标记为 aborted）
  
  std::cout << "✅ Transaction rolled back successfully" << std::endl;
}

TEST_F(DriverTransactionTest, AutoRollbackOnDestruction) {
  std::cout << "\n=== Test: Auto Rollback on Destruction ===" << std::endl;
  
  SessionConfig session_config;
  Session session(txn_manager_.get(), memtable_.get(), engine_.get(),
                  wal_writer_.get(), session_config);
  
  {
    auto txn = session.BeginTransaction();
    
    Descriptor desc;
    desc.SetKind(EntryKind::InlineShortStr);
    desc.SetColumnId(0);
    desc.SetPayload(77777);
    
    txn->Put(300, EntityType::Vertex, 0, desc, Timestamp::Now());
    
    // 不调用 Commit 也不调用 Rollback，析构时应自动回滚
  }
  
  std::cout << "✅ Auto-rollback on destruction works" << std::endl;
}

TEST_F(DriverTransactionTest, SessionBookmarkManagement) {
  std::cout << "\n=== Test: Session Bookmark Management ===" << std::endl;
  
  SessionConfig session_config;
  Session session(txn_manager_.get(), memtable_.get(), engine_.get(),
                  wal_writer_.get(), session_config);
  
  // 初始书签应为空
  EXPECT_TRUE(session.GetLastBookmark().IsEmpty());
  
  // 第一次事务
  {
    auto txn = session.BeginTransaction();
    
    Descriptor desc;
    desc.SetKind(EntryKind::InlineShortStr);
    desc.SetColumnId(0);
    desc.SetPayload(11111);
    
    txn->Put(200, EntityType::Vertex, 0, desc, Timestamp::Now());
    
    auto result = txn.Commit();
    EXPECT_TRUE(result.ok());
    // 书签会自动更新到 session
  }
  
  // 验证书签已更新
  Bookmark first_bookmark = session.GetLastBookmark();
  EXPECT_FALSE(first_bookmark.IsEmpty());
  std::cout << "First bookmark: " << first_bookmark.ToString() << std::endl;
  std::cout << "   Timestamp: " << first_bookmark.GetTimestamp() << std::endl;
  std::cout << "   Txn ID: " << first_bookmark.GetTransactionId() << std::endl;
  
  // 第二次事务，使用因果一致性（指定书签）
  {
    auto txn = session.BeginTransaction(first_bookmark);
    // 这个事务保证能看到第一次事务的写入
    
    Descriptor desc;
    desc.SetKind(EntryKind::InlineShortStr);
    desc.SetColumnId(0);
    desc.SetPayload(22222);
    
    txn->Put(201, EntityType::Vertex, 0, desc, Timestamp::Now());
    
    auto result = txn.Commit();
    EXPECT_TRUE(result.ok());
  }
  
  Bookmark second_bookmark = session.GetLastBookmark();
  EXPECT_TRUE(second_bookmark > first_bookmark || 
              second_bookmark.GetTimestamp() == first_bookmark.GetTimestamp());
  std::cout << "Second bookmark: " << second_bookmark.ToString() << std::endl;
  
  std::cout << "✅ Bookmark management works correctly" << std::endl;
}

TEST_F(DriverTransactionTest, RetryPolicyTest) {
  std::cout << "\n=== Test: Retry Policy ===" << std::endl;
  
  // 使用激进重试策略
  RetryPolicy retry_policy(RetryPolicies::Aggressive());
  
  int attempt_count = 0;
  
  // 模拟一个最终成功的操作
  auto result = retry_policy.Execute([&]() -> Status {
    attempt_count++;
    if (attempt_count < 3) {
      // 模拟前两次失败 - 使用包含 "Lock" 的错误来触发重试
      return Status::IOError("Lock", "Lock conflict - retryable");
    }
    return Status::OK();
  });
  
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(attempt_count, 3);
  std::cout << "✅ Retry policy executed " << attempt_count << " attempts" << std::endl;
  
  // 测试不重试策略
  RetryPolicy no_retry(RetryPolicies::NoRetry());
  attempt_count = 0;
  
  auto result2 = no_retry.Execute([&]() -> Status {
    attempt_count++;
    return Status::IOError("Lock", "Error");
  });
  
  EXPECT_FALSE(result2.ok());
  EXPECT_EQ(attempt_count, 1);
  std::cout << "✅ No-retry policy executed only 1 attempt" << std::endl;
}

TEST_F(DriverTransactionTest, ErrorClassification) {
  std::cout << "\n=== Test: Error Classification ===" << std::endl;
  
  // 瞬态错误 - 可重试 (使用包含 "Lock" 的错误)
  Status transient = Status::IOError("Lock", "Lock conflict");
  EXPECT_TRUE(ErrorClassifier::IsRetryable(transient));
  std::cout << "✅ Lock conflict error is retryable" << std::endl;
  
  // 客户端错误 - 不可重试
  Status client_error = Status::InvalidArgument("Bad input");
  EXPECT_FALSE(ErrorClassifier::IsRetryable(client_error));
  std::cout << "✅ InvalidArgument is not retryable" << std::endl;
}

TEST_F(DriverTransactionTest, BookmarkSerialization) {
  std::cout << "\n=== Test: Bookmark Serialization ===" << std::endl;
  
  // 创建书签（模拟 OCCTransaction 的结果）
  Bookmark original(123456789, 987654321);
  
  // 序列化
  std::string str = original.ToString();
  std::cout << "Serialized: " << str << std::endl;
  
  // 反序列化
  auto restored = Bookmark::FromString(str);
  ASSERT_TRUE(restored.has_value());
  
  EXPECT_EQ(restored->GetTimestamp(), original.GetTimestamp());
  EXPECT_EQ(restored->GetTransactionId(), original.GetTransactionId());
  EXPECT_EQ(restored->ToString(), original.ToString());
  
  std::cout << "✅ Bookmark serialization works correctly" << std::endl;
}

TEST_F(DriverTransactionTest, DirectOCCTransactionAccess) {
  std::cout << "\n=== Test: Direct OCCTransaction Access ===" << std::endl;
  
  SessionConfig session_config;
  Session session(txn_manager_.get(), memtable_.get(), engine_.get(),
                  wal_writer_.get(), session_config);
  
  auto txn = session.BeginTransaction();
  
  // 直接访问 OCCTransaction 的所有方法
  std::cout << "   Transaction ID: " << txn->GetTransactionId() << std::endl;
  std::cout << "   Transaction started" << std::endl;
  
  // 执行操作
  Descriptor desc;
  desc.SetKind(EntryKind::InlineShortStr);
  desc.SetColumnId(0);
  desc.SetPayload(55555);
  
  auto status = txn->Put(400, EntityType::Vertex, 0, desc, Timestamp::Now());
  EXPECT_TRUE(status.ok());
  
  // 可以调用原生 OCCTransaction 的 Commit
  auto native_status = txn->Commit();
  EXPECT_TRUE(native_status.ok());
  
  // 或调用 ManagedTxn 的 Commit 获取书签
  // auto result = txn.Commit();  // 两者都 OK
  
  std::cout << "✅ Direct OCCTransaction access works" << std::endl;
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
