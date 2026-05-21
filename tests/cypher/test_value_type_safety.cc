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

// =============================================================================
// Value Type-Safety Test — StatusOr-guarded accessors
// =============================================================================
// Verifies that TryGetXxx() returns a value on type match and a non-ok Status
// on type mismatch, replacing naked std::get that throws std::bad_variant_access.
// =============================================================================

#include <gtest/gtest.h>

#include "cedar/cypher/value.h"

using namespace cedar::cypher;

// Helper to verify a StatusOr holds the expected value
#define EXPECT_STATUS_OK(val) EXPECT_TRUE((val).ok())
#define EXPECT_STATUS_NOT_OK(val) EXPECT_FALSE((val).ok())

TEST(ValueTypeSafetyTest, TryGetBool_Success) {
  Value v(true);
  auto result = v.TryGetBool();
  EXPECT_STATUS_OK(result);
  EXPECT_TRUE(result.ValueOrDie());
}

TEST(ValueTypeSafetyTest, TryGetBool_WrongType) {
  Value v(42);
  auto result = v.TryGetBool();
  EXPECT_STATUS_NOT_OK(result);
  EXPECT_TRUE(result.status().ToString().find("Expected bool") != std::string::npos);
}

TEST(ValueTypeSafetyTest, TryGetInt_Success) {
  Value v(int64_t{42});
  auto result = v.TryGetInt();
  EXPECT_STATUS_OK(result);
  EXPECT_EQ(result.ValueOrDie(), 42);
}

TEST(ValueTypeSafetyTest, TryGetInt_WrongType) {
  Value v("hello");
  auto result = v.TryGetInt();
  EXPECT_STATUS_NOT_OK(result);
  EXPECT_TRUE(result.status().ToString().find("Expected int") != std::string::npos);
}

TEST(ValueTypeSafetyTest, TryGetFloat_Success) {
  Value v(3.14);
  auto result = v.TryGetFloat();
  EXPECT_STATUS_OK(result);
  EXPECT_DOUBLE_EQ(result.ValueOrDie(), 3.14);
}

TEST(ValueTypeSafetyTest, TryGetFloat_WrongType) {
  Value v(int64_t{1});
  auto result = v.TryGetFloat();
  EXPECT_STATUS_NOT_OK(result);
  EXPECT_TRUE(result.status().ToString().find("Expected float") != std::string::npos);
}

TEST(ValueTypeSafetyTest, TryGetString_Success) {
  Value v(std::string("hello"));
  auto result = v.TryGetString();
  EXPECT_STATUS_OK(result);
  EXPECT_EQ(result.ValueOrDie(), "hello");
}

TEST(ValueTypeSafetyTest, TryGetString_WrongType) {
  Value v(3.14);
  auto result = v.TryGetString();
  EXPECT_STATUS_NOT_OK(result);
  EXPECT_TRUE(result.status().ToString().find("Expected string") != std::string::npos);
}

TEST(ValueTypeSafetyTest, TryGetTimestamp_Success) {
  Value v(cedar::Timestamp(12345));
  auto result = v.TryGetTimestamp();
  EXPECT_STATUS_OK(result);
  EXPECT_EQ(result.ValueOrDie().value(), 12345);
}

TEST(ValueTypeSafetyTest, TryGetTimestamp_WrongType) {
  Value v(std::string("not a timestamp"));
  auto result = v.TryGetTimestamp();
  EXPECT_STATUS_NOT_OK(result);
  EXPECT_TRUE(result.status().ToString().find("Expected timestamp") != std::string::npos);
}

TEST(ValueTypeSafetyTest, TryGetDate_Success) {
  Value v = Value::DateValue(2025, 5, 20);
  auto result = v.TryGetDate();
  EXPECT_STATUS_OK(result);
  EXPECT_EQ(result.ValueOrDie().days_since_epoch,
            Date::FromYMD(2025, 5, 20).days_since_epoch);
}

TEST(ValueTypeSafetyTest, TryGetDate_WrongType) {
  Value v(true);
  auto result = v.TryGetDate();
  EXPECT_STATUS_NOT_OK(result);
  EXPECT_TRUE(result.status().ToString().find("Expected date") != std::string::npos);
}

TEST(ValueTypeSafetyTest, TryGetTime_Success) {
  Value v = Value::TimeValue(14, 30, 0);
  auto result = v.TryGetTime();
  EXPECT_STATUS_OK(result);
  // 14:30:00 = 14*3600 + 30*60 = 52200 seconds = 52200000000000 nanoseconds
  EXPECT_EQ(result.ValueOrDie().nanos_since_midnight, 52200000000000LL);
}

TEST(ValueTypeSafetyTest, TryGetTime_WrongType) {
  Value v(int64_t{0});
  auto result = v.TryGetTime();
  EXPECT_STATUS_NOT_OK(result);
  EXPECT_TRUE(result.status().ToString().find("Expected time") != std::string::npos);
}

TEST(ValueTypeSafetyTest, TryGetDateTime_Success) {
  Value v = Value::DateTimeValue(2025, 5, 20, 14, 30, 0);
  auto result = v.TryGetDateTime();
  EXPECT_STATUS_OK(result);
  EXPECT_STATUS_OK(result);
}

TEST(ValueTypeSafetyTest, TryGetDateTime_WrongType) {
  Value v(std::vector<Value>{});
  auto result = v.TryGetDateTime();
  EXPECT_STATUS_NOT_OK(result);
  EXPECT_TRUE(result.status().ToString().find("Expected datetime") != std::string::npos);
}

TEST(ValueTypeSafetyTest, TryGetDuration_Success) {
  Value v = Value::DurationValue(1000000);
  auto result = v.TryGetDuration();
  EXPECT_STATUS_OK(result);
  EXPECT_EQ(result.ValueOrDie().microseconds, 1000000);
}

TEST(ValueTypeSafetyTest, TryGetDuration_WrongType) {
  Value v(std::map<std::string, Value>{});
  auto result = v.TryGetDuration();
  EXPECT_STATUS_NOT_OK(result);
  EXPECT_TRUE(result.status().ToString().find("Expected duration") != std::string::npos);
}

TEST(ValueTypeSafetyTest, TryGetNode_Success) {
  Value v = Value::MakeNode(1, {"Person"}, {{"name", Value("Alice")}});
  auto result = v.TryGetNode();
  EXPECT_STATUS_OK(result);
  EXPECT_EQ(result.ValueOrDie().id, 1);
}

TEST(ValueTypeSafetyTest, TryGetNode_WrongType) {
  Value v(std::string("not a node"));
  auto result = v.TryGetNode();
  EXPECT_STATUS_NOT_OK(result);
  EXPECT_TRUE(result.status().ToString().find("Expected node") != std::string::npos);
}

TEST(ValueTypeSafetyTest, TryGetRelationship_Success) {
  Value v = Value::MakeRelationship(1, 10, 20, "KNOWS", {});
  auto result = v.TryGetRelationship();
  EXPECT_STATUS_OK(result);
  EXPECT_EQ(result.ValueOrDie().type, "KNOWS");
}

TEST(ValueTypeSafetyTest, TryGetRelationship_WrongType) {
  Value v(int64_t{1});
  auto result = v.TryGetRelationship();
  EXPECT_STATUS_NOT_OK(result);
  EXPECT_TRUE(result.status().ToString().find("Expected relationship") != std::string::npos);
}

TEST(ValueTypeSafetyTest, TryGetPath_Success) {
  Node n1(1, {}, {});
  Relationship r1(1, 1, 2, "KNOWS", {});
  Node n2(2, {}, {});
  Path p;
  p.elements = {n1, r1, n2};
  Value v(p);
  auto result = v.TryGetPath();
  EXPECT_STATUS_OK(result);
  EXPECT_EQ(result.ValueOrDie().Length(), 1);
}

TEST(ValueTypeSafetyTest, TryGetPath_WrongType) {
  Value v(true);
  auto result = v.TryGetPath();
  EXPECT_STATUS_NOT_OK(result);
  EXPECT_TRUE(result.status().ToString().find("Expected path") != std::string::npos);
}

TEST(ValueTypeSafetyTest, TryGetList_Success) {
  Value v(std::vector<Value>{Value(1), Value(2), Value(3)});
  auto result = v.TryGetList();
  EXPECT_STATUS_OK(result);
  EXPECT_EQ(result.ValueOrDie().size(), 3);
}

TEST(ValueTypeSafetyTest, TryGetList_WrongType) {
  Value v(3.14);
  auto result = v.TryGetList();
  EXPECT_STATUS_NOT_OK(result);
  EXPECT_TRUE(result.status().ToString().find("Expected list") != std::string::npos);
}

TEST(ValueTypeSafetyTest, TryGetMap_Success) {
  Value v(std::map<std::string, Value>{{"key", Value("value")}});
  auto result = v.TryGetMap();
  EXPECT_STATUS_OK(result);
  EXPECT_EQ(result.ValueOrDie().size(), 1);
}

TEST(ValueTypeSafetyTest, TryGetMap_WrongType) {
  Value v(int64_t{0});
  auto result = v.TryGetMap();
  EXPECT_STATUS_NOT_OK(result);
  EXPECT_TRUE(result.status().ToString().find("Expected map") != std::string::npos);
}

TEST(ValueTypeSafetyTest, AllWrongTypeOnNullValue) {
  Value v;
  EXPECT_STATUS_NOT_OK(v.TryGetBool());
  EXPECT_STATUS_NOT_OK(v.TryGetInt());
  EXPECT_STATUS_NOT_OK(v.TryGetFloat());
  EXPECT_STATUS_NOT_OK(v.TryGetString());
  EXPECT_STATUS_NOT_OK(v.TryGetTimestamp());
  EXPECT_STATUS_NOT_OK(v.TryGetDate());
  EXPECT_STATUS_NOT_OK(v.TryGetTime());
  EXPECT_STATUS_NOT_OK(v.TryGetDateTime());
  EXPECT_STATUS_NOT_OK(v.TryGetDuration());
  EXPECT_STATUS_NOT_OK(v.TryGetNode());
  EXPECT_STATUS_NOT_OK(v.TryGetRelationship());
  EXPECT_STATUS_NOT_OK(v.TryGetPath());
  EXPECT_STATUS_NOT_OK(v.TryGetList());
  EXPECT_STATUS_NOT_OK(v.TryGetMap());
}
