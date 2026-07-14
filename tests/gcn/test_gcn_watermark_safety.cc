#include <gtest/gtest.h>

#include <limits>

#include "cedar/gcn/watermark_gc.h"

namespace cedar::gcn {
namespace {

TEST(GcnWatermarkSafetyTest, NeverPassesActiveQueryOrAppliedVersion) {
  WatermarkInputs inputs{.minimum_applied_version = 120,
                         .minimum_active_query_version = 90,
                         .retention_floor_version = 100};

  EXPECT_EQ(ComputeSafeWatermark(inputs), 90u);
}

TEST(GcnWatermarkSafetyTest, TreatsMissingActiveQueryAsUnbounded) {
  WatermarkInputs inputs{
      .minimum_applied_version = 120,
      .minimum_active_query_version = std::numeric_limits<uint64_t>::max(),
      .retention_floor_version = 100};

  EXPECT_EQ(ComputeSafeWatermark(inputs), 100u);
}

TEST(GcnWatermarkSafetyTest, EmptyAppliedStateKeepsGcDisabled) {
  WatermarkInputs inputs{.minimum_applied_version = 0,
                         .minimum_active_query_version = 90,
                         .retention_floor_version = 100};

  EXPECT_EQ(ComputeSafeWatermark(inputs), 0u);
}

}  // namespace
}  // namespace cedar::gcn
