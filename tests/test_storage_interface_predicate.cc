#include <gtest/gtest.h>
#include <filesystem>
#include <chrono>
#include <limits>

#include "cedar/storage/storage_interface.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/cypher/value.h"
#include "cedar/raft/partition_router.h"

using namespace cedar;
using namespace cedar::storage;
using namespace cedar::cypher;
using namespace cedar::raft;

class StorageInterfacePredicateTest : public ::testing::Test {
 protected:
  void SetUp() override {
    tmp_dir_ = "/tmp/si_pred_test_" + std::to_string(
        std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(tmp_dir_);
    CedarOptions options;
    options.create_if_missing = true;
    CedarGraphStorage* raw = nullptr;
    auto s = CedarGraphStorage::Open(options, tmp_dir_, &raw);
    ASSERT_TRUE(s.ok()) << s.ToString();
    storage_.reset(raw);

    // Initialize partition router (required for Put operations)
    PartitionRouterConfig router_config;
    router_config.default_replica_count = 1;
    router_config.enable_read_from_follower = true;
    s = storage_->InitializePartitionRouter(router_config);
    ASSERT_TRUE(s.ok()) << s.ToString();

    s = storage_->RegisterPartitionNode("local-node", "127.0.0.1", 9779, "dc1");
    ASSERT_TRUE(s.ok()) << s.ToString();

    std::vector<std::string> replicas = {"local-node"};
    s = storage_->CreatePartition(0, replicas);
    ASSERT_TRUE(s.ok()) << s.ToString();

    iface_ = std::make_unique<StorageInterface>(storage_.get());
  }

  void TearDown() override {
    iface_.reset();
    storage_.reset();
    std::filesystem::remove_all(tmp_dir_);
  }

  std::string tmp_dir_;
  std::unique_ptr<CedarGraphStorage> storage_;
  std::unique_ptr<StorageInterface> iface_;
};

TEST_F(StorageInterfacePredicateTest, ScanVertexWithMatchingPredicate) {
  Vertex v;
  v.id = 100;
  v.properties["value"] = Value(static_cast<int64_t>(42));
  ASSERT_TRUE(iface_->InsertVertex(v, 0).ok());

  std::vector<std::pair<Timestamp, Descriptor>> results;
  PropertyPredicateItem pred;
  pred.property_name = "value";
  pred.op = PropertyPredicateItem::EQ;
  pred.value = Value(static_cast<int64_t>(42));

  ASSERT_TRUE(iface_->ScanVertices(100, 0, Timestamp::Max(), {pred}, &results).ok());
  EXPECT_EQ(results.size(), 1);
}

TEST_F(StorageInterfacePredicateTest, ScanVertexWithNonMatchingPredicate) {
  Vertex v;
  v.id = 101;
  v.properties["value"] = Value(static_cast<int64_t>(42));
  ASSERT_TRUE(iface_->InsertVertex(v, 0).ok());

  std::vector<std::pair<Timestamp, Descriptor>> results;
  PropertyPredicateItem pred;
  pred.property_name = "value";
  pred.op = PropertyPredicateItem::EQ;
  pred.value = Value(static_cast<int64_t>(99));

  ASSERT_TRUE(iface_->ScanVertices(101, 0, Timestamp::Max(), {pred}, &results).ok());
  EXPECT_EQ(results.size(), 0);
}

TEST_F(StorageInterfacePredicateTest, ScanVertexWithStringPredicate) {
  Vertex v;
  v.id = 102;
  v.properties["value"] = Value(std::string("bob"));
  ASSERT_TRUE(iface_->InsertVertex(v, 0).ok());

  std::vector<std::pair<Timestamp, Descriptor>> results;
  PropertyPredicateItem pred;
  pred.property_name = "value";
  pred.op = PropertyPredicateItem::EQ;
  pred.value = Value(std::string("bob"));

  ASSERT_TRUE(iface_->ScanVertices(102, 0, Timestamp::Max(), {pred}, &results).ok());
  EXPECT_EQ(results.size(), 1);
}

TEST_F(StorageInterfacePredicateTest, ScanVertexWithNumericComparison) {
  Vertex v;
  v.id = 103;
  v.properties["value"] = Value(static_cast<int64_t>(10));
  ASSERT_TRUE(iface_->InsertVertex(v, 0).ok());

  std::vector<std::pair<Timestamp, Descriptor>> results;
  PropertyPredicateItem pred;
  pred.property_name = "value";
  pred.op = PropertyPredicateItem::GT;
  pred.value = Value(static_cast<int64_t>(5));

  ASSERT_TRUE(iface_->ScanVertices(103, 0, Timestamp::Max(), {pred}, &results).ok());
  EXPECT_EQ(results.size(), 1);

  results.clear();
  pred.op = PropertyPredicateItem::LT;
  pred.value = Value(static_cast<int64_t>(5));
  ASSERT_TRUE(iface_->ScanVertices(103, 0, Timestamp::Max(), {pred}, &results).ok());
  EXPECT_EQ(results.size(), 0);
}
