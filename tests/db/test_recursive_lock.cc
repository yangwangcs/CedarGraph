#include <gtest/gtest.h>
#include <filesystem>

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/types/descriptor.h"

using namespace cedar;

class DeadlockTest : public ::testing::Test {
 protected:
  std::string test_dir_;

  void SetUp() override {
    test_dir_ = "/tmp/deadlock_test_" + std::to_string(getpid());
    std::filesystem::remove_all(test_dir_);
  }

  void TearDown() override {
    std::filesystem::remove_all(test_dir_);
  }
};

TEST_F(DeadlockTest, GetAtTimeDoesNotDeadlock) {
  CedarOptions opts;
  opts.create_if_missing = true;

  CedarGraphStorage* storage = nullptr;
  ASSERT_TRUE(CedarGraphStorage::Open(opts, test_dir_, &storage).ok());
  ASSERT_NE(storage, nullptr);

  ASSERT_TRUE(storage->Put(1, 100, Descriptor::InlineInt(0, 42), Timestamp(1)).ok());
  auto result = storage->Get(1, EntityType::Vertex, 0, Timestamp(100));
  EXPECT_TRUE(result.has_value());

  delete storage;
}
