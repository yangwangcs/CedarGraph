#include <gtest/gtest.h>
#include "cedar/types/cedar_key.h"

using namespace cedar;

TEST(StorageInterfacePredicateStub, CompileAndLink) {
  CedarKey key = CedarKey::Vertex(99, 0, Timestamp::Now());
  EXPECT_EQ(key.entity_id(), 99);
}
