#include <gtest/gtest.h>
#include "cedar/cypher/parser.h"

using namespace cedar::cypher;

TEST(CypherValidatorStub, CompileAndLink) {
  CypherParser parser("MATCH (n) RETURN n");
  auto stmt = parser.ParseStatement();
  EXPECT_NE(stmt, nullptr);
}
