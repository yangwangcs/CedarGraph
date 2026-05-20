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

#include "cedar/gcn/backpressure_controller.h"

using namespace cedar::gcn;

TEST(BackpressureControllerTest, AcquireAndRelease) {
  BackpressureController ctrl;
  const std::string gcn = "gcn-1";

  EXPECT_TRUE(ctrl.AcquireSlot(gcn));
  EXPECT_EQ(ctrl.InFlight(gcn), 1u);

  ctrl.ReleaseSlot(gcn);
  EXPECT_EQ(ctrl.InFlight(gcn), 0u);
}

TEST(BackpressureControllerTest, RejectsAtDefaultLimit) {
  BackpressureController ctrl;
  const std::string gcn = "gcn-1";

  // Default max_concurrency is 16
  for (int i = 0; i < 16; ++i) {
    EXPECT_TRUE(ctrl.AcquireSlot(gcn));
  }
  EXPECT_EQ(ctrl.InFlight(gcn), 16u);
  EXPECT_FALSE(ctrl.AcquireSlot(gcn));

  ctrl.ReleaseSlot(gcn);
  EXPECT_TRUE(ctrl.AcquireSlot(gcn));
}

TEST(BackpressureControllerTest, DegradedHealthReducesLimit) {
  BackpressureController ctrl;
  const std::string gcn = "gcn-1";

  // Fill to normal limit
  for (int i = 0; i < 16; ++i) {
    EXPECT_TRUE(ctrl.AcquireSlot(gcn));
  }

  // Health drops to degraded (< 60) -> max becomes 8
  ctrl.UpdateHealth(gcn, 50.0);
  EXPECT_FALSE(ctrl.AcquireSlot(gcn));

  // Release down to below new max (7), then acquire back to max (8)
  for (int i = 0; i < 9; ++i) {
    ctrl.ReleaseSlot(gcn);
  }
  EXPECT_EQ(ctrl.InFlight(gcn), 7u);
  EXPECT_TRUE(ctrl.AcquireSlot(gcn));
  EXPECT_EQ(ctrl.InFlight(gcn), 8u);
  EXPECT_FALSE(ctrl.AcquireSlot(gcn));  // at degraded limit

  // Health drops to severely degraded (< 30) -> max becomes 4
  ctrl.UpdateHealth(gcn, 20.0);
  EXPECT_FALSE(ctrl.AcquireSlot(gcn));

  for (int i = 0; i < 4; ++i) {
    ctrl.ReleaseSlot(gcn);
  }
  EXPECT_EQ(ctrl.InFlight(gcn), 4u);
  EXPECT_FALSE(ctrl.AcquireSlot(gcn));  // at severe limit

  // Release one more, then acquire back to 4
  ctrl.ReleaseSlot(gcn);
  EXPECT_EQ(ctrl.InFlight(gcn), 3u);
  EXPECT_TRUE(ctrl.AcquireSlot(gcn));
  EXPECT_EQ(ctrl.InFlight(gcn), 4u);

  // Health recovers -> max becomes 16
  ctrl.UpdateHealth(gcn, 90.0);
  EXPECT_TRUE(ctrl.AcquireSlot(gcn));
  EXPECT_EQ(ctrl.InFlight(gcn), 5u);
}

TEST(BackpressureControllerTest, HealthyNodeAllowsFullConcurrency) {
  BackpressureController ctrl;
  const std::string gcn = "gcn-1";

  ctrl.UpdateHealth(gcn, 100.0);
  for (int i = 0; i < 16; ++i) {
    EXPECT_TRUE(ctrl.AcquireSlot(gcn));
  }
  EXPECT_EQ(ctrl.InFlight(gcn), 16u);
  EXPECT_FALSE(ctrl.AcquireSlot(gcn));
}
