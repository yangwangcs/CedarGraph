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
// Temporal Constraint Check Test
// =============================================================================
// Verify that CheckTemporalConstraint correctly rejects edges whose timestamp
// is earlier than the endpoint's creation time.
// =============================================================================

#include <gtest/gtest.h>
#include <filesystem>

#include "cedar/update/cedar_update.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/types/descriptor.h"

using namespace cedar;

class TemporalConstraintTest : public ::testing::Test {
 protected:
  std::string test_dir_;
  CedarGraphStorage* storage_ = nullptr;

  void SetUp() override {
    test_dir_ = "/tmp/cedar_temporal_test_" + std::to_string(getpid());
    std::filesystem::remove_all(test_dir_);
    std::filesystem::create_directories(test_dir_);

    CedarOptions options;
    options.create_if_missing = true;

    Status s = CedarGraphStorage::Open(options, test_dir_, &storage_);
    EXPECT_TRUE(s.ok()) << "Failed to open storage: " << s.ToString();
  }

  void TearDown() override {
    delete storage_;
    std::filesystem::remove_all(test_dir_);
  }
};

TEST_F(TemporalConstraintTest, RejectsEdgeBeforeNodeCreation) {
  // Create nodes at T=100
  Timestamp node_time(100);
  {
    CEDAR_UPDATE(update, StrictLevel::PERMISSIVE);
    update.At(node_time)
          .CreateVertex(1, 1, Descriptor::InlineInt(1, 42))
          .CreateVertex(2, 1, Descriptor::InlineInt(1, 43));
    auto status = update.Apply(storage_);
    EXPECT_TRUE(status.ok()) << status.ToString();
  }

  // Try to create edge at T=50 (before node creation)
  Timestamp edge_time(50);
  CEDAR_UPDATE(update, StrictLevel::STRICT_TEMPORAL);
  update.At(edge_time).CreateEdge(1, 2, 1, Descriptor::InlineInt(2, 1));
  auto status = update.Apply(storage_);

  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), CedarCode::kTemporalAnachronism);
  EXPECT_TRUE(status.IsTemporalError());
}

TEST_F(TemporalConstraintTest, AllowsEdgeAfterNodeCreation) {
  // Create nodes at T=100
  Timestamp node_time(100);
  {
    CEDAR_UPDATE(update, StrictLevel::PERMISSIVE);
    update.At(node_time)
          .CreateVertex(1, 1, Descriptor::InlineInt(1, 42))
          .CreateVertex(2, 1, Descriptor::InlineInt(1, 43));
    auto status = update.Apply(storage_);
    EXPECT_TRUE(status.ok()) << status.ToString();
  }

  // Create edge at T=150 (after node creation)
  Timestamp edge_time(150);
  CEDAR_UPDATE(update, StrictLevel::STRICT_TEMPORAL);
  update.At(edge_time).CreateEdge(1, 2, 1, Descriptor::InlineInt(2, 1));
  auto status = update.Apply(storage_);

  EXPECT_TRUE(status.ok()) << status.ToString();
}
