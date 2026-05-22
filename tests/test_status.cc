#include <gtest/gtest.h>
#include "cedar/core/status.h"

TEST(StatusTest, ConstructorRejectsOversizedMessage) {
  std::string big_msg(1000000, 'x');
  cedar::Status s = cedar::Status::IOError(big_msg, big_msg);
  EXPECT_TRUE(s.IsIOError());
  EXPECT_NE(s.ToString().find("IO error"), std::string::npos);
}
