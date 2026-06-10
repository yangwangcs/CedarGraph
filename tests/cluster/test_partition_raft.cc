#include <gtest/gtest.h>
#include "cedar/dtx/types.h"

using namespace cedar::dtx;

TEST(PartitionRaftStub, CompileAndLink) {
  EXPECT_EQ(kInvalidTxnID, 0);
}
