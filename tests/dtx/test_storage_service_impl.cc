// Copyright 2025 The Cedar Authors
//
// Test: Proto CedarKey round-trip preserves entity_type correctly.
// Previously entity_type was corrupted because flags (which holds OpType
// in the low bits) was stored in type_flags and then cast to EntityType.

#include <gtest/gtest.h>
#include "cedar/dtx/storage_service_impl.h"

using namespace cedar;

TEST(ProtoCedarKeyTest, RoundTripPreservesEntityType) {
  CedarKey original(42, EntityType::Vertex, 1, Timestamp(1000), 0, 0, 0, 0);
  auto proto = cedar::dtx::StorageServiceImpl::CedarKeyToProto(original);
  auto decoded = cedar::dtx::StorageServiceImpl::ProtoToCedarKey(proto);
  EXPECT_EQ(decoded.entity_type(), EntityType::Vertex);
}

TEST(ProtoCedarKeyTest, RoundTripPreservesEdgeOutEntityType) {
  CedarKey original(42, EntityType::EdgeOut, 1, Timestamp(1000), 0, 100, 0, 0);
  auto proto = cedar::dtx::StorageServiceImpl::CedarKeyToProto(original);
  auto decoded = cedar::dtx::StorageServiceImpl::ProtoToCedarKey(proto);
  EXPECT_EQ(decoded.entity_type(), EntityType::EdgeOut);
}

TEST(ProtoCedarKeyTest, RoundTripPreservesEdgeInEntityType) {
  CedarKey original(42, EntityType::EdgeIn, 1, Timestamp(1000), 0, 100, 0, 0);
  auto proto = cedar::dtx::StorageServiceImpl::CedarKeyToProto(original);
  auto decoded = cedar::dtx::StorageServiceImpl::ProtoToCedarKey(proto);
  EXPECT_EQ(decoded.entity_type(), EntityType::EdgeIn);
}

TEST(ProtoCedarKeyTest, UpdateFlagDoesNotCorruptEntityType) {
  // OpType::UPDATE = 1. When stored in flags, the low bit is set.
  // Old bug: casting flags (1) to EntityType gave EdgeOut instead of Vertex.
  CedarKey original(42, EntityType::Vertex, 1, Timestamp(1000), 0, 0,
                    key_flags::kOpUpdate, 0);
  auto proto = cedar::dtx::StorageServiceImpl::CedarKeyToProto(original);
  auto decoded = cedar::dtx::StorageServiceImpl::ProtoToCedarKey(proto);
  EXPECT_EQ(decoded.entity_type(), EntityType::Vertex);
  EXPECT_TRUE(decoded.IsUpdate());
}
