#include <gtest/gtest.h>
#include "cedar/types/cedar_key.h"

using namespace cedar;

TEST(PartitionRouterLeaderOnlyStub, CompileAndLink) {
  CedarKey key = CedarKey::Vertex(42, 0, Timestamp::Now());
  EXPECT_EQ(key.entity_id(), 42);
}
