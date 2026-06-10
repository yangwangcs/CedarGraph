#include <gtest/gtest.h>
#include <filesystem>

#include "cedar/cypher/execution_plan.h"
#include "cedar/graph/cedar_graph.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/types/descriptor.h"

using namespace cedar;
using namespace cedar::cypher;

TEST(NodeScanRealDataTest, ReturnsOnlyExistingVertices) {
  char buf[] = "/tmp/cedar_nodescan_test_XXXXXX";
  char* dir = mkdtemp(buf);
  ASSERT_NE(dir, nullptr);
  std::string db_path = dir;

  CedarOptions options;
  options.create_if_missing = true;

  CedarGraphStorage* storage = nullptr;
  Status s = CedarGraphStorage::Open(options, db_path, &storage);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_NE(storage, nullptr);

  // Create 5 real vertices with distinct IDs
  for (uint64_t id : {10, 20, 30, 40, 50}) {
    Descriptor desc = Descriptor::InlineInt(1, static_cast<int32_t>(id));
    s = storage->PutStaticVertex(id, 1, desc);
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  // Wrap in CedarGraph
  CedarGraph graph(storage);

  // Set up execution context
  ExecutionContext ctx;
  ctx.graph = &graph;

  // Create NodeScan
  NodeScan scan("n");
  bool init_ok = scan.Init(&ctx);
  ASSERT_TRUE(init_ok);

  // Count returned nodes
  size_t count = 0;
  while (scan.Next() != nullptr) {
    ++count;
  }

  EXPECT_EQ(count, 5);

  delete storage;
  std::filesystem::remove_all(db_path);
}

TEST(NodeScanRealDataTest, PointLookupReturnsOnlyExistingVertices) {
  char buf[] = "/tmp/cedar_nodescan_pointlookup_test_XXXXXX";
  char* dir = mkdtemp(buf);
  ASSERT_NE(dir, nullptr);
  std::string db_path = dir;

  CedarOptions options;
  options.create_if_missing = true;

  CedarGraphStorage* storage = nullptr;
  Status s = CedarGraphStorage::Open(options, db_path, &storage);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_NE(storage, nullptr);

  // Create only vertex 10 (vertex 999 does NOT exist)
  Descriptor desc = Descriptor::InlineInt(1, 42);
  s = storage->PutStaticVertex(10, 1, desc);
  ASSERT_TRUE(s.ok()) << s.ToString();

  CedarGraph graph(storage);

  // Test 1: Point lookup for existing vertex 10
  {
    ExecutionContext ctx;
    ctx.graph = &graph;

    std::map<std::string, std::shared_ptr<Expression>> props;
    props["id"] = std::make_shared<LiteralExpr>(Value(static_cast<int64_t>(10)));

    NodeScan scan("n", std::nullopt, std::move(props));
    bool init_ok = scan.Init(&ctx);
    ASSERT_TRUE(init_ok);

    auto record = scan.Next();
    ASSERT_NE(record, nullptr);
    auto val = record->Get("n");
    ASSERT_TRUE(val.has_value());
    ASSERT_EQ(val->Type(), ValueType::kNode);
    EXPECT_EQ(val->GetNode().id, 10u);

    EXPECT_EQ(scan.Next(), nullptr);
  }

  // Test 2: Point lookup for non-existent vertex 999
  {
    ExecutionContext ctx;
    ctx.graph = &graph;

    std::map<std::string, std::shared_ptr<Expression>> props;
    props["id"] = std::make_shared<LiteralExpr>(Value(static_cast<int64_t>(999)));

    NodeScan scan("n", std::nullopt, std::move(props));
    bool init_ok = scan.Init(&ctx);
    ASSERT_TRUE(init_ok);

    EXPECT_EQ(scan.Next(), nullptr);
  }

  delete storage;
  std::filesystem::remove_all(db_path);
}
