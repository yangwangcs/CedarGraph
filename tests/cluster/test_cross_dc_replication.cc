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
