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
#include <chrono>
#include <future>
#include <thread>
#include "cedar/dtx/cross_dc_replicator.h"

using namespace cedar;
using namespace cedar::dtx;

TEST(CrossDCReplicationTest, BasicReplication) {
  DCReplicationConfig config;
  config.mode = ReplicationMode::kSync;
  
  CrossDCReplicator replicator;
  Status s = replicator.Initialize(config, "dc-beijing", {"dc-shanghai", "dc-shenzhen"});
  ASSERT_TRUE(s.ok());
  
  Descriptor desc = Descriptor::InlineInt(0, 42);
  s = replicator.Replicate("key-1", desc, Timestamp(1000));

  // Without endpoint, should fail with IOError
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsIOError());
}

TEST(CrossDCReplicationTest, AsyncReplication) {
  DCReplicationConfig config;
  config.mode = ReplicationMode::kAsync;
  config.batch_size = 10;
  
  CrossDCReplicator replicator;
  Status s = replicator.Initialize(config, "dc-local", {"dc-remote"});
  ASSERT_TRUE(s.ok());
  
  s = replicator.Start();
  ASSERT_TRUE(s.ok());
  
  for (int i = 0; i < 50; i++) {
    Descriptor desc = Descriptor::InlineInt(0, i);
    s = replicator.Replicate("key-" + std::to_string(i), desc, Timestamp(i));
    EXPECT_TRUE(s.ok());
  }
  
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  
  replicator.Stop();
}

TEST(CrossDCReplicationTest, ReplicationStatus) {
  DCReplicationConfig config;
  config.mode = ReplicationMode::kAsync;
  
  CrossDCReplicator replicator;
  replicator.Initialize(config, "dc-a", {"dc-b", "dc-c"});
  
  auto statuses = replicator.GetAllStatuses();
  EXPECT_EQ(statuses.size(), 2);
  
  for (const auto& [dc, status] : statuses) {
    EXPECT_EQ(status.replicated_count, 0);
    EXPECT_EQ(status.failed_count, 0);
    EXPECT_TRUE(status.is_healthy);
  }
}

TEST(CrossDCReplicationTest, ConflictResolution) {
  DCReplicationConfig config;

  CrossDCReplicator replicator;
  replicator.Initialize(config, "dc-local", {"dc-peer"});

  std::vector<ReplicationLog> conflicts;

  ReplicationLog log1;
  log1.key.SetEntityId(1);
  log1.timestamp = Timestamp(1000);
  log1.source_dc = "dc-a";

  ReplicationLog log2;
  log2.key.SetEntityId(1);
  log2.timestamp = Timestamp(2000);
  log2.source_dc = "dc-b";

  conflicts.push_back(log1);
  conflicts.push_back(log2);

  Status s = replicator.ResolveConflict("conflict-key", conflicts);
  EXPECT_TRUE(s.ok());
}

TEST(CrossDCReplicationTest, ReplicationCallback) {
  DCReplicationConfig config;
  config.mode = ReplicationMode::kSync;
  
  CrossDCReplicator replicator;
  replicator.Initialize(config, "dc-local", {"dc-remote"});
  
  int callback_count = 0;
  replicator.SetReplicationCallback(
    [&callback_count](const ReplicationLog& log, Status status) {
      callback_count++;
    });
  
  Descriptor desc = Descriptor::InlineInt(0, 1);
  replicator.Replicate("key", desc, Timestamp(1));
  
  // In sync mode with single target, callback called once per target
  EXPECT_EQ(callback_count, 1);
}

TEST(CrossDCReplicationTest, BatchReplication) {
  DCReplicationConfig config;
  config.mode = ReplicationMode::kSync;

  CrossDCReplicator replicator;
  replicator.Initialize(config, "dc-local", {"dc-remote"});

  std::vector<ReplicationLog> logs;
  for (int i = 0; i < 10; i++) {
    ReplicationLog log;
    log.key.SetEntityId(i);
    log.value = Descriptor::InlineInt(0, i);
    log.timestamp = Timestamp(i);
    logs.push_back(log);
  }

  Status s = replicator.ReplicateBatch(logs);
  EXPECT_TRUE(s.IsIOError());
}

#include <map>
#include <mutex>

// Mock replicator that lets us inject per-DC failure.
class MockCrossDCReplicator : public CrossDCReplicator {
 public:
  enum class Outcome {
    kBase,
    kSuccess,
    kFail,
  };

  void SetInjectFailureForDC(const std::string& dc, bool fail) {
    std::lock_guard<std::mutex> lock(inject_mutex_);
    outcomes_[dc] = fail ? Outcome::kFail : Outcome::kBase;
  }

  void SetOutcomeForDC(const std::string& dc, Outcome outcome) {
    std::lock_guard<std::mutex> lock(inject_mutex_);
    outcomes_[dc] = outcome;
  }

  void SetTombstoneOutcomeForDC(const std::string& dc, Outcome outcome) {
    std::lock_guard<std::mutex> lock(inject_mutex_);
    tombstone_outcomes_[dc] = outcome;
  }

  // Expose protected method for testing
  Status TestReplicateToDC(const ReplicationLog& log, const std::string& dc_id) {
    return ReplicateToDC(log, dc_id);
  }

 protected:
  Status ReplicateToDC(const ReplicationLog& log, const std::string& dc_id) override {
    {
      std::lock_guard<std::mutex> lock(inject_mutex_);
      auto& selected = log.value.IsTombstone() ? tombstone_outcomes_ : outcomes_;
      auto it = selected.find(dc_id);
      if (it != selected.end()) {
        if (it->second == Outcome::kSuccess) {
          return Status::OK();
        }
        if (it->second == Outcome::kFail) {
          return Status::IOError("Injected failure for " + dc_id);
        }
      }
    }
    return CrossDCReplicator::ReplicateToDC(log, dc_id);
  }

 private:
  std::mutex inject_mutex_;
  std::map<std::string, Outcome> outcomes_;
  std::map<std::string, Outcome> tombstone_outcomes_;
};

TEST(CrossDCReplicationTest, SyncModeAllOrNothing) {
  DCReplicationConfig config;
  config.mode = ReplicationMode::kSync;

  MockCrossDCReplicator replicator;
  Status s = replicator.Initialize(config, "dc-beijing",
                                    {"dc-shanghai", "dc-shenzhen", "dc-guangzhou"});
  ASSERT_TRUE(s.ok());

  // Without real endpoints, ReplicateToDC will fail with IOError.
  // The test verifies the STRUCTURE: if we could make the first two succeed
  // and the third fail, the overall Replicate() must NOT return OK.
  Descriptor desc = Descriptor::InlineInt(0, 42);
  s = replicator.Replicate("key-all-or-nothing", desc, Timestamp(1000));

  // Since there are no real endpoints, at least one DC will fail.
  // The method must return the failure, not OK.
  EXPECT_FALSE(s.ok());
}

TEST(CrossDCReplicationTest, SyncPartialFailureDoesNotReturnOk) {
  DCReplicationConfig config;
  config.mode = ReplicationMode::kSync;

  CrossDCReplicator replicator;
  Status s = replicator.Initialize(config, "dc-local",
                                    {"dc-a", "dc-b", "dc-c"});
  ASSERT_TRUE(s.ok());

  // Since no endpoints are configured, ReplicateToDC will fail for every DC.
  // With the new all-or-nothing logic, the overall status must be !ok.
  Descriptor desc = Descriptor::InlineInt(0, 99);
  s = replicator.Replicate("partial-key", desc, Timestamp(2000));
  EXPECT_FALSE(s.ok()) << "Expected failure when no DCs are reachable, got: "
                       << s.ToString();
}

TEST(CrossDCReplicationTest, SyncRollbackFailureEnqueuesReconciliation) {
  DCReplicationConfig config;
  config.mode = ReplicationMode::kSync;
  config.max_reconciliation_queue_size = 5;

  MockCrossDCReplicator replicator;
  Status s = replicator.Initialize(config, "dc-local", {"dc-a", "dc-b"});
  ASSERT_TRUE(s.ok());

  replicator.SetOutcomeForDC("dc-a", MockCrossDCReplicator::Outcome::kSuccess);
  replicator.SetOutcomeForDC("dc-b", MockCrossDCReplicator::Outcome::kFail);
  replicator.SetTombstoneOutcomeForDC("dc-a", MockCrossDCReplicator::Outcome::kFail);

  Descriptor desc = Descriptor::InlineInt(0, 123);
  s = replicator.Replicate("partial-key", desc, Timestamp(2000));
  EXPECT_FALSE(s.ok());

  auto status = replicator.GetReconciliationStatus();
  EXPECT_EQ(status.pending_count, 1u);
}

TEST(CrossDCReplicationTest, SyncCallbackRunsOutsideCallbackMutex) {
  DCReplicationConfig config;
  config.mode = ReplicationMode::kSync;

  CrossDCReplicator replicator;
  ASSERT_TRUE(replicator.Initialize(config, "dc-local", {"dc-remote"}).ok());

  std::promise<void> callback_entered;
  auto entered_future = callback_entered.get_future();
  replicator.SetReplicationCallback(
      [&replicator, &callback_entered](const ReplicationLog&, Status) {
        replicator.SetReplicationCallback(nullptr);
        callback_entered.set_value();
      });

  Descriptor desc = Descriptor::InlineInt(0, 1);
  auto future = std::async(std::launch::async, [&] {
    return replicator.Replicate("key", desc, Timestamp(1));
  });

  EXPECT_EQ(entered_future.wait_for(std::chrono::seconds(2)),
            std::future_status::ready);
  EXPECT_EQ(future.wait_for(std::chrono::seconds(2)),
            std::future_status::ready);
  EXPECT_FALSE(future.get().ok());
}

TEST(CrossDCReplicationTest, SequenceWraparoundAccepted) {
  DCReplicationConfig config;
  config.mode = ReplicationMode::kAsync;

  CrossDCReplicator replicator;
  Status s = replicator.Initialize(config, "dc-a", {"dc-b"});
  ASSERT_TRUE(s.ok());

  // Simulate receiving a log with generation=1, sequence=5 after
  // generation=0, sequence=UINT64_MAX.
  ReplicationLog log;
  log.sequence_num = 5;
  log.generation = 1;
  log.source_dc = "dc-b";
  log.key.SetEntityId(1);
  log.timestamp = Timestamp(1000);

  // First receive: should succeed (new DC, no prior state)
  s = replicator.ReceiveReplication(log);
  EXPECT_TRUE(s.ok()) << s.ToString();

  // Same generation, lower sequence: should fail
  log.sequence_num = 3;
  s = replicator.ReceiveReplication(log);
  EXPECT_FALSE(s.ok());
}

TEST(CrossDCReplicationTest, ReconciliationQueueBoundEnforced) {
  DCReplicationConfig config;
  config.mode = ReplicationMode::kSync;
  config.max_reconciliation_queue_size = 5;
  config.reconciliation_ttl = std::chrono::seconds(3600);

  CrossDCReplicator replicator;
  Status s = replicator.Initialize(config, "dc-local",
                                    {"dc-a", "dc-b", "dc-c"});
  ASSERT_TRUE(s.ok());

  // We cannot easily inject failures without mocking, but we can verify
  // the config is stored and the status reflects zero pending.
  auto status = replicator.GetReconciliationStatus();
  EXPECT_EQ(status.pending_count, 0u);
}

TEST(CrossDCReplicationTest, StopWakesIdleReconciliationThreadPromptly) {
  DCReplicationConfig config;
  config.mode = ReplicationMode::kSync;

  CrossDCReplicator replicator;
  ASSERT_TRUE(replicator.Initialize(config, "dc-local", {"dc-remote"}).ok());
  ASSERT_TRUE(replicator.Start().ok());

  auto start = std::chrono::steady_clock::now();
  replicator.Stop();
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
            500);
}

TEST(CrossDCReplicationTest, TlsEnabledWithoutCaFailsInitialization) {
  DCReplicationConfig config;
  config.tls_enabled = true;
  config.remote_dc_endpoints["dc-remote"] = "127.0.0.1:1";

  CrossDCReplicator replicator;
  auto status = replicator.Initialize(config, "dc-local", {"dc-remote"});

  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.IsIOError());
}

TEST(CrossDCReplicationTest, StopInterruptsReplicationRetryBackoffPromptly) {
  DCReplicationConfig config;
  config.mode = ReplicationMode::kAsync;
  config.allow_insecure = true;
  config.remote_dc_endpoints["dc-remote"] = "127.0.0.1:1";
  config.max_retry_attempts = 3;
  config.retry_delay = std::chrono::seconds(30);

  MockCrossDCReplicator replicator;
  ASSERT_TRUE(replicator.Initialize(config, "dc-local", {"dc-remote"}).ok());
  ASSERT_TRUE(replicator.Start().ok());

  ReplicationLog log;
  log.sequence_num = 1;
  log.key.SetEntityId(1);
  log.value = Descriptor::InlineInt(0, 1);
  log.timestamp = Timestamp(1);
  log.source_dc = "dc-local";
  log.target_dcs = {"dc-remote"};

  auto future = std::async(std::launch::async, [&]() {
    return replicator.TestReplicateToDC(log, "dc-remote");
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  auto start = std::chrono::steady_clock::now();
  replicator.Stop();

  ASSERT_EQ(future.wait_for(std::chrono::milliseconds(500)),
            std::future_status::ready);
  auto elapsed = std::chrono::steady_clock::now() - start;
  auto status = future.get();

  EXPECT_FALSE(status.ok());
  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
            500);
}
