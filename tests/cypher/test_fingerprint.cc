// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include "cedar/cypher/fingerprint.h"

using namespace cedar::cypher;

TEST(FingerprintTest, SameQueryDifferentLiterals) {
  std::string fp1 = ComputeFingerprint(
      "MATCH (n:Person {name: 'Alice'}) RETURN n.age");
  std::string fp2 = ComputeFingerprint(
      "MATCH (n:Person {name: 'Bob'}) RETURN n.age");

  EXPECT_EQ(fp1, fp2);
}

TEST(FingerprintTest, DifferentStructureDiffers) {
  std::string fp1 = ComputeFingerprint(
      "MATCH (n:Person) RETURN n.name");
  std::string fp2 = ComputeFingerprint(
      "MATCH (n:Company) RETURN n.name");

  EXPECT_NE(fp1, fp2);
}

TEST(FingerprintTest, CaseNormalization) {
  std::string fp1 = ComputeFingerprint(
      "MATCH (n:Person) WHERE n.age > 30 RETURN n");
  std::string fp2 = ComputeFingerprint(
      "match (n:Person) where n.age > 30 return n");

  EXPECT_EQ(fp1, fp2);
}

TEST(FingerprintTest, NumericLiteralsReplaced) {
  std::string fp1 = ComputeFingerprint(
      "MATCH (n) WHERE n.x = 42 RETURN n");
  std::string fp2 = ComputeFingerprint(
      "MATCH (n) WHERE n.x = 99 RETURN n");

  EXPECT_EQ(fp1, fp2);
}

TEST(FingerprintTest, BooleanLiteralsReplaced) {
  std::string fp1 = ComputeFingerprint(
      "MATCH (n) WHERE n.active = true RETURN n");
  std::string fp2 = ComputeFingerprint(
      "MATCH (n) WHERE n.active = false RETURN n");

  EXPECT_EQ(fp1, fp2);
}

TEST(FingerprintTest, WhitespaceNormalization) {
  std::string fp1 = ComputeFingerprint(
      "MATCH   (n:Person)   RETURN   n");
  std::string fp2 = ComputeFingerprint(
      "MATCH (n:Person) RETURN n");

  EXPECT_EQ(fp1, fp2);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
