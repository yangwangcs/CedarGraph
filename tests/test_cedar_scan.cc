#include <gtest/gtest.h>
#include "cedar/storage/cedar_graph_storage.h"

using namespace cedar;

TEST(CedarScanStub, CompileAndLink) {
  CedarOptions options;
  options.create_if_missing = true;
  EXPECT_TRUE(options.create_if_missing);
}
