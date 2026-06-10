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
#include "cedar/cypher/parser.h"

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

TEST(FingerprintTest, PreservePropertyKeys) {
  cedar::cypher::CypherParser parser("MATCH (n:Person {id: 100}) RETURN n");
  auto ast = parser.ParseStatement();
  ASSERT_TRUE(ast != nullptr);

  // Default: id replaced with ?
  std::string fp_default = ComputeFingerprint(*ast);
  EXPECT_EQ(fp_default, "match (n:Person{id:?}) return n as n");

  // With preserve_property_keys={"id"}: id value kept
  FingerprintOptions opts;
  opts.preserve_property_keys.insert("id");
  std::string fp_preserve = ComputeFingerprint(*ast, opts);
  EXPECT_EQ(fp_preserve, "match (n:Person{id:100}) return n as n");

  // Different id values produce different fingerprints
  cedar::cypher::CypherParser parser2("MATCH (n:Person {id: 200}) RETURN n");
  auto ast2 = parser2.ParseStatement();
  ASSERT_TRUE(ast2 != nullptr);
  std::string fp2 = ComputeFingerprint(*ast2, opts);
  EXPECT_NE(fp_preserve, fp2);
  EXPECT_EQ(fp2, "match (n:Person{id:200}) return n as n");
}

TEST(FingerprintTest, PreservePropertyKeysNonLiteral) {
  cedar::cypher::CypherParser parser("MATCH (n:Person {id: $param}) RETURN n");
  auto ast = parser.ParseStatement();
  ASSERT_TRUE(ast != nullptr);

  FingerprintOptions opts;
  opts.preserve_property_keys.insert("id");
  std::string fp = ComputeFingerprint(*ast, opts);
  // Parameters are still replaced with ? even when key is preserved
  EXPECT_EQ(fp, "match (n:Person{id:?}) return n as n");
}

TEST(FingerprintTest, PreservePropertyKeysOnlyForDesignatedKeys) {
  cedar::cypher::CypherParser parser(
      "MATCH (n:Person {id: 100, name: 'Alice'}) RETURN n");
  auto ast = parser.ParseStatement();
  ASSERT_TRUE(ast != nullptr);

  FingerprintOptions opts;
  opts.preserve_property_keys.insert("id");
  std::string fp = ComputeFingerprint(*ast, opts);
  // id is preserved, name is still replaced with ?
  EXPECT_EQ(fp, "match (n:Person{id:100,name:?}) return n as n");
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
