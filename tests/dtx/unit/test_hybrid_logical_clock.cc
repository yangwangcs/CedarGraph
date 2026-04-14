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
#include "cedar/dtx/hybrid_logical_clock.h"

namespace cedar {
namespace dtx {

class HybridLogicalClockTest : public ::testing::Test {
 protected:
  void SetUp() override {
    clock_.ResetForTesting();
  }
  
  HybridLogicalClock clock_;
};

// Test basic Now() functionality
TEST_F(HybridLogicalClockTest, BasicNow) {
  auto ts1 = clock_.Now();
  EXPECT_GT(ts1.physical, 0);
  EXPECT_EQ(ts1.logical, 0);
  
  auto ts2 = clock_.Now();
  EXPECT_GE(ts2.physical, ts1.physical);
  if (ts2.physical == ts1.physical) {
    EXPECT_EQ(ts2.logical, ts1.logical + 1);
  } else {
    EXPECT_EQ(ts2.logical, 0);
  }
}

// Test monotonicity
TEST_F(HybridLogicalClockTest, Monotonicity) {
  std::vector<HlcTimestamp> timestamps;
  for (int i = 0; i < 100; i++) {
    timestamps.push_back(clock_.Now());
  }
  
  // Verify strict monotonic increase
  for (size_t i = 1; i < timestamps.size(); i++) {
    EXPECT_LT(timestamps[i-1], timestamps[i])
        << "Timestamp " << (i-1) << " should be less than " << i;
  }
}

// Test Update() functionality
TEST_F(HybridLogicalClockTest, Update) {
  // Get local timestamp
  auto local = clock_.Now();
  
  // Simulate receiving a message with a much larger timestamp
  HlcTimestamp remote(local.physical + 1000000, 5);  // 1 second ahead
  
  clock_.Update(remote);
  
  // After update, our clock should reflect the remote timestamp
  auto new_local = clock_.Now();
  EXPECT_GE(new_local.physical, remote.physical);
  EXPECT_GE(new_local.logical, remote.logical + 1);
}

// Test comparison operators
TEST_F(HybridLogicalClockTest, Comparison) {
  HlcTimestamp ts1(100, 0);
  HlcTimestamp ts2(100, 1);
  HlcTimestamp ts3(101, 0);
  HlcTimestamp ts4(100, 0);
  
  EXPECT_TRUE(ts1 < ts2);
  EXPECT_TRUE(ts2 < ts3);
  EXPECT_TRUE(ts1 < ts3);
  
  EXPECT_TRUE(ts2 > ts1);
  EXPECT_TRUE(ts3 > ts1);
  
  EXPECT_TRUE(ts1 == ts4);
  EXPECT_TRUE(ts1 != ts2);
  
  EXPECT_TRUE(ts1 <= ts2);
  EXPECT_TRUE(ts1 <= ts4);
  EXPECT_TRUE(ts2 >= ts1);
  EXPECT_TRUE(ts4 >= ts1);
}

// Test causality tracking
TEST_F(HybridLogicalClockTest, Causality) {
  // Event A happens
  auto ts_a = clock_.Now();
  
  // Event B happens after A
  auto ts_b = clock_.Now();
  
  EXPECT_TRUE(ts_a.HappensBefore(ts_b));
  EXPECT_FALSE(ts_b.HappensBefore(ts_a));
  
  // Create a truly concurrent timestamp (same physical, same logical)
  // In distributed systems, concurrent means no causal relationship
  HlcTimestamp ts_c(ts_a.physical, ts_a.logical);
  
  // ts_a and ts_c are equal (special case of concurrent)
  EXPECT_TRUE(ts_a == ts_c);
  EXPECT_FALSE(ts_a.HappensBefore(ts_c));
  EXPECT_FALSE(ts_c.HappensBefore(ts_a));
  
  // Create timestamp that happened after but same physical time
  auto ts_d = clock_.Now();
  if (ts_d.physical == ts_a.physical) {
    // If same physical time, higher logical means happens-after
    EXPECT_TRUE(ts_a.HappensBefore(ts_d));
  }
}

// Test serialization/deserialization
TEST_F(HybridLogicalClockTest, Serialization) {
  HlcTimestamp original(123456789, 42);
  
  uint8_t buffer[HlcTimestamp::kSerializedSize];
  original.Serialize(buffer);
  
  auto recovered = HlcTimestamp::Deserialize(buffer);
  
  EXPECT_EQ(original, recovered);
}

// Test Peek() doesn't advance logical counter
TEST_F(HybridLogicalClockTest, Peek) {
  auto ts1 = clock_.Now();
  auto peek1 = clock_.Peek();
  auto peek2 = clock_.Peek();
  auto ts2 = clock_.Now();
  
  // Peeks should return the same timestamp
  EXPECT_EQ(peek1, peek2);
  EXPECT_EQ(peek1.physical, ts1.physical);
  EXPECT_EQ(peek1.logical, ts1.logical);
  
  // But Now() should advance
  EXPECT_GT(ts2, ts1);
}

// Test rapid Now() calls at same physical time
TEST_F(HybridLogicalClockTest, RapidCalls) {
  // Call Now() rapidly many times
  auto ts_first = clock_.Now();
  
  for (int i = 0; i < 1000; i++) {
    auto ts = clock_.Now();
    EXPECT_GE(ts, ts_first);
  }
  
  auto ts_last = clock_.Now();
  EXPECT_GT(ts_last, ts_first);
}

// Test update with older timestamp (should not go backwards)
TEST_F(HybridLogicalClockTest, UpdateWithOlder) {
  // Advance clock
  for (int i = 0; i < 10; i++) {
    clock_.Now();
  }
  
  auto current = clock_.Peek();
  
  // Update with older timestamp
  HlcTimestamp old(current.physical - 1000, 0);
  clock_.Update(old);
  
  // Clock should not go backwards
  auto after = clock_.Now();
  EXPECT_GE(after.physical, current.physical);
}

}  // namespace dtx
}  // namespace cedar
