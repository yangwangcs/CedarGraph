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

#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "cedar/dtx/meta_service.h"

namespace cedar {
namespace dtx {
namespace {

uint64_t NowMs();

void AppendFixed32(std::string& out, uint32_t value) {
  for (int i = 0; i < 4; ++i) {
    out.push_back(static_cast<char>((value >> (i * 8)) & 0xFF));
  }
}

void AppendFixed64(std::string& out, uint64_t value) {
  for (int i = 0; i < 8; ++i) {
    out.push_back(static_cast<char>((value >> (i * 8)) & 0xFF));
  }
}

void AppendString(std::string& out, const std::string& value) {
  AppendFixed32(out, static_cast<uint32_t>(value.size()));
  out.append(value);
}

std::string SerializeRegistrationCommand(const GcnRegistration& registration) {
  std::string out;
  AppendFixed64(out, registration.gcn_id);
  AppendString(out, registration.endpoint);
  AppendFixed64(out, registration.incarnation);
  AppendFixed64(out, registration.last_heartbeat_ms);
  return out;
}

std::string SerializeRenewCommand(
    uint64_t gcn_id,
    uint64_t incarnation,
    uint64_t renew_now_ms,
    const std::vector<GcnPartitionProgress>& progress) {
  std::string out;
  AppendFixed64(out, gcn_id);
  AppendFixed64(out, incarnation);
  AppendFixed64(out, renew_now_ms);
  AppendFixed32(out, static_cast<uint32_t>(progress.size()));
  for (const auto& item : progress) {
    std::string encoded_progress;
    AppendFixed32(encoded_progress, item.partition_id);
    AppendFixed64(encoded_progress, item.partition_epoch);
    AppendFixed64(encoded_progress, item.applied_offset);
    AppendFixed64(encoded_progress, item.applied_version);
    encoded_progress.push_back(item.query_ready ? 1 : 0);
    AppendString(out, encoded_progress);
  }
  return out;
}

std::string SerializeRenewCommand(
    uint64_t gcn_id,
    uint64_t incarnation,
    const std::vector<GcnPartitionProgress>& progress) {
  return SerializeRenewCommand(gcn_id, incarnation, NowMs(), progress);
}

uint64_t NowMs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());
}

std::unique_ptr<MetadataService> CreateTestMetadataService() {
  auto meta = std::make_unique<MetadataService>();
  MetaServiceConfig config;
  config.node_id = 1;
  config.listen_address = "127.0.0.1:2379";
  config.advertise_address = "127.0.0.1:2379";
  config.test_mode = true;
  auto status = meta->Initialize(config);
  EXPECT_TRUE(status.ok()) << status.ToString();
  return meta;
}

uint64_t OwnerFor(const std::vector<GcnLease>& leases, uint32_t partition_id) {
  for (const auto& lease : leases) {
    if (lease.partition_id == partition_id) {
      return lease.gcn_id;
    }
  }
  ADD_FAILURE() << "missing lease for partition " << partition_id;
  return 0;
}

uint64_t EpochFor(const std::vector<GcnLease>& leases, uint32_t partition_id) {
  for (const auto& lease : leases) {
    if (lease.partition_id == partition_id) {
      return lease.lease_epoch;
    }
  }
  ADD_FAILURE() << "missing lease for partition " << partition_id;
  return 0;
}

std::unique_ptr<MetadataService> ReadyMetaWithLease(uint32_t partition_id,
                                                    uint64_t version) {
  auto meta = CreateTestMetadataService();
  EXPECT_TRUE(meta->RegisterGcn({1, "gcn-a:9780", 10, NowMs()}).ok());
  auto leases = meta->RenewGcnLeases(
      1, 10,
      {GcnPartitionProgress{partition_id, 1, 123, version, true}});
  EXPECT_TRUE(leases.ok()) << leases.status().ToString();
  EXPECT_EQ(OwnerFor(leases.value(), partition_id), 1);
  return meta;
}

TEST(MetaGcnLeaseTest, AssignsSingleOwnerAndMonotonicEpoch) {
  auto meta = CreateTestMetadataService();
  auto status = meta->RegisterGcn({1, "gcn-a:9780", 10, NowMs()});
  ASSERT_TRUE(status.ok()) << status.ToString();
  status = meta->RegisterGcn({2, "gcn-b:9780", 20, NowMs()});
  ASSERT_TRUE(status.ok()) << status.ToString();

  auto first = meta->RenewGcnLeases(1, 10, {});
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  ASSERT_EQ(OwnerFor(first.value(), 3), 1);

  meta->ExpireGcnForTest(1);
  auto second = meta->RenewGcnLeases(2, 20, {});
  ASSERT_TRUE(second.ok()) << second.status().ToString();
  EXPECT_EQ(OwnerFor(second.value(), 3), 2);
  EXPECT_GT(EpochFor(second.value(), 3), EpochFor(first.value(), 3));
}

TEST(MetaGcnLeaseTest, LocateRequiresAppliedVersionAndLiveLease) {
  auto meta = ReadyMetaWithLease(/*partition_id=*/3, /*version=*/99);
  EXPECT_TRUE(meta->LocateGcn(3, 99).ok());
  EXPECT_TRUE(meta->LocateGcn(3, 100).status().IsNotFound());
}

TEST(MetaGcnLeaseTest, RetainsLiveOwnerToAvoidChurn) {
  auto meta = CreateTestMetadataService();
  ASSERT_TRUE(meta->RegisterGcn({1, "gcn-a:9780", 10, NowMs()}).ok());
  ASSERT_TRUE(meta->RegisterGcn({2, "gcn-b:9780", 20, NowMs()}).ok());

  auto first = meta->RenewGcnLeases(1, 10, {});
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  auto second = meta->RenewGcnLeases(2, 20, {});
  ASSERT_TRUE(second.ok()) << second.status().ToString();

  EXPECT_EQ(OwnerFor(first.value(), 3), 1);
  EXPECT_EQ(OwnerFor(second.value(), 3), 1);
  EXPECT_EQ(EpochFor(second.value(), 3), EpochFor(first.value(), 3));
}

TEST(MetaGcnLeaseTest, OwnerChangeClearsStaleProgressUntilNewOwnerReports) {
  auto meta = CreateTestMetadataService();
  ASSERT_TRUE(meta->RegisterGcn({1, "gcn-a:9780", 10, NowMs()}).ok());
  ASSERT_TRUE(meta->RegisterGcn({2, "gcn-b:9780", 20, NowMs()}).ok());

  auto first = meta->RenewGcnLeases(
      1, 10, {GcnPartitionProgress{3, 1, 123, 99, true}});
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  ASSERT_TRUE(meta->LocateGcn(3, 99).ok());

  meta->ExpireGcnForTest(1);
  auto second = meta->RenewGcnLeases(2, 20, {});
  ASSERT_TRUE(second.ok()) << second.status().ToString();
  const uint64_t new_epoch = EpochFor(second.value(), 3);
  ASSERT_GT(new_epoch, EpochFor(first.value(), 3));
  EXPECT_TRUE(meta->LocateGcn(3, 99).status().IsNotFound());

  auto renewed = meta->RenewGcnLeases(
      2, 20, {GcnPartitionProgress{3, new_epoch, 124, 100, true}});
  ASSERT_TRUE(renewed.ok()) << renewed.status().ToString();
  auto route = meta->LocateGcn(3, 99);
  ASSERT_TRUE(route.ok()) << route.status().ToString();
  EXPECT_EQ(route.value().gcn_id, 2u);
  EXPECT_EQ(route.value().lease_epoch, new_epoch);
}

TEST(MetaGcnLeaseTest, IncarnationChangeClearsStaleProgressUntilRestartReports) {
  auto meta = CreateTestMetadataService();
  ASSERT_TRUE(meta->RegisterGcn({1, "gcn-a:9780", 10, NowMs()}).ok());

  auto first = meta->RenewGcnLeases(
      1, 10, {GcnPartitionProgress{3, 1, 123, 99, true}});
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  const uint64_t lease_epoch = EpochFor(first.value(), 3);
  ASSERT_TRUE(meta->LocateGcn(3, 99).ok());

  ASSERT_TRUE(meta->RegisterGcn({1, "gcn-a-restarted:9780", 11, NowMs()}).ok());
  auto restarted = meta->RenewGcnLeases(1, 11, {});
  ASSERT_TRUE(restarted.ok()) << restarted.status().ToString();
  EXPECT_EQ(EpochFor(restarted.value(), 3), lease_epoch);
  EXPECT_TRUE(meta->LocateGcn(3, 99).status().IsNotFound());

  auto renewed = meta->RenewGcnLeases(
      1, 11, {GcnPartitionProgress{3, lease_epoch, 124, 100, true}});
  ASSERT_TRUE(renewed.ok()) << renewed.status().ToString();
  auto route = meta->LocateGcn(3, 99);
  ASSERT_TRUE(route.ok()) << route.status().ToString();
  EXPECT_EQ(route.value().gcn_id, 1u);
  EXPECT_EQ(route.value().endpoint, "gcn-a-restarted:9780");
  EXPECT_EQ(route.value().lease_epoch, lease_epoch);
}

TEST(MetaGcnLeaseTest, ExpiredOwnerCannotReviveStaleProgressWithoutFreshProgress) {
  auto meta = CreateTestMetadataService();
  ASSERT_TRUE(meta->RegisterGcn({1, "gcn-a:9780", 10, NowMs()}).ok());

  auto first = meta->RenewGcnLeases(
      1, 10, {GcnPartitionProgress{3, 1, 123, 99, true}});
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  ASSERT_TRUE(meta->LocateGcn(3, 99).ok());

  meta->ExpireGcnForTest(1);
  auto revived = meta->RenewGcnLeases(1, 10, {});
  ASSERT_TRUE(revived.ok()) << revived.status().ToString();
  EXPECT_GT(EpochFor(revived.value(), 3), EpochFor(first.value(), 3));
  EXPECT_TRUE(meta->LocateGcn(3, 99).status().IsNotFound());
}

TEST(MetaGcnLeaseTest, LeaseStateSurvivesSnapshotRoundtrip) {
  auto meta = ReadyMetaWithLease(/*partition_id=*/3, /*version=*/99);
  auto snapshot = meta->SerializeState();

  auto restored = CreateTestMetadataService();
  ASSERT_TRUE(restored->DeserializeState(snapshot));

  auto route = restored->LocateGcn(3, 99);
  ASSERT_TRUE(route.ok()) << route.status().ToString();
  EXPECT_EQ(route.value().gcn_id, 1);
  EXPECT_EQ(route.value().endpoint, "gcn-a:9780");
}

TEST(MetaGcnLeaseTest, GcnRaftCommandsReplayRegistrationAndRenewProgress) {
  auto meta = CreateTestMetadataService();

  RaftCommand register_cmd;
  register_cmd.type = RaftCommandType::kRegisterGcn;
  register_cmd.payload =
      SerializeRegistrationCommand({1, "gcn-a:9780", 10, NowMs()});
  ASSERT_TRUE(meta->ApplyRaftCommand(register_cmd));

  RaftCommand renew_without_progress;
  renew_without_progress.type = RaftCommandType::kRenewGcnLeases;
  renew_without_progress.payload = SerializeRenewCommand(1, 10, {});
  ASSERT_TRUE(meta->ApplyRaftCommand(renew_without_progress));
  EXPECT_TRUE(meta->LocateGcn(3, 99).status().IsNotFound());

  RaftCommand renew_with_progress;
  renew_with_progress.type = RaftCommandType::kRenewGcnLeases;
  renew_with_progress.payload = SerializeRenewCommand(
      1, 10, {GcnPartitionProgress{3, 1, 123, 99, true}});
  ASSERT_TRUE(meta->ApplyRaftCommand(renew_with_progress));

  auto route = meta->LocateGcn(3, 99);
  ASSERT_TRUE(route.ok()) << route.status().ToString();
  EXPECT_EQ(route.value().gcn_id, 1u);
  EXPECT_EQ(route.value().endpoint, "gcn-a:9780");
}

TEST(MetaGcnLeaseTest, GcnRenewRaftCommandUsesReplicatedTimestamp) {
  auto left = CreateTestMetadataService();
  auto right = CreateTestMetadataService();
  const uint64_t replicated_now_ms = NowMs();

  RaftCommand register_cmd;
  register_cmd.type = RaftCommandType::kRegisterGcn;
  register_cmd.payload =
      SerializeRegistrationCommand({1, "gcn-a:9780", 10, replicated_now_ms});
  ASSERT_TRUE(left->ApplyRaftCommand(register_cmd));
  ASSERT_TRUE(right->ApplyRaftCommand(register_cmd));

  RaftCommand renew_cmd;
  renew_cmd.type = RaftCommandType::kRenewGcnLeases;
  renew_cmd.payload = SerializeRenewCommand(
      1, 10, replicated_now_ms,
      {GcnPartitionProgress{3, 1, 123, 99, true}});
  ASSERT_TRUE(left->ApplyRaftCommand(renew_cmd));
  ASSERT_TRUE(right->ApplyRaftCommand(renew_cmd));

  auto left_route = left->LocateGcn(3, 99);
  auto right_route = right->LocateGcn(3, 99);
  ASSERT_TRUE(left_route.ok()) << left_route.status().ToString();
  ASSERT_TRUE(right_route.ok()) << right_route.status().ToString();
  EXPECT_EQ(left_route.value().expires_at_ms, right_route.value().expires_at_ms);
}

TEST(MetaGcnLeaseTest, CorruptGcnRenewRaftCommandIsRejected) {
  auto meta = CreateTestMetadataService();

  RaftCommand corrupt;
  corrupt.type = RaftCommandType::kRenewGcnLeases;
  corrupt.payload.assign(3, '\0');

  EXPECT_FALSE(meta->ApplyRaftCommand(corrupt));
}

}  // namespace
}  // namespace dtx
}  // namespace cedar
