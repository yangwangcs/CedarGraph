#include <gtest/gtest.h>
#include <string>
#include <vector>

// Re-declare the helper (defined in security_manager.cc)
namespace cedar {
namespace dtx {
namespace security {
extern bool ConstantTimeCompare(const std::string& a, const std::string& b);
}
}
}

using namespace cedar::dtx::security;

TEST(ConstantTimeCompareTest, EqualStringsReturnTrue) {
  EXPECT_TRUE(ConstantTimeCompare("abc", "abc"));
  EXPECT_TRUE(ConstantTimeCompare(std::string(256, 'x'), std::string(256, 'x')));
}

TEST(ConstantTimeCompareTest, DifferentStringsReturnFalse) {
  EXPECT_FALSE(ConstantTimeCompare("abc", "abd"));
  EXPECT_FALSE(ConstantTimeCompare("abc", "ab"));
  EXPECT_FALSE(ConstantTimeCompare("ab", "abc"));
  EXPECT_FALSE(ConstantTimeCompare("", "a"));
}

TEST(ConstantTimeCompareTest, DifferentLengthReturnsFalse) {
  EXPECT_FALSE(ConstantTimeCompare("short", "longer_string"));
}
