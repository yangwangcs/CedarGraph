#include <gtest/gtest.h>

#include <limits>

#include "cedar/gcn/event_applier.h"
#include "cedar/gcn/tmv_engine.h"

using namespace cedar::gcn;

TEST(EventApplierTest, ApplyOrderedEvents) {
  TMVEngine engine(16);
  EventApplier applier(&engine);

  GraphCDCEvent e1{1, 100, 200, 1000, std::numeric_limits<uint32_t>::max(), 1,
                   CDCEventOp::kCreate};
  GraphCDCEvent e2{2, 100, 300, 2000, std::numeric_limits<uint32_t>::max(), 1,
                   CDCEventOp::kCreate};
  GraphCDCEvent e3{3, 100, 400, 3000, std::numeric_limits<uint32_t>::max(), 1,
                   CDCEventOp::kCreate};

  applier.ApplyOrdered(e1);
  applier.ApplyOrdered(e2);
  applier.ApplyOrdered(e3);

  EXPECT_EQ(applier.applied_version(), 3u);

  auto edges = engine.ScanAtTime(100, Direction::kOut, 3500);
  EXPECT_EQ(edges.size(), 3);
  EXPECT_EQ(edges[0].target_id, 200u);
  EXPECT_EQ(edges[1].target_id, 300u);
  EXPECT_EQ(edges[2].target_id, 400u);
}

TEST(EventApplierTest, ReorderUnorderedEvents) {
  TMVEngine engine(16);
  EventApplier applier(&engine);

  GraphCDCEvent e1{1, 100, 200, 1000, std::numeric_limits<uint32_t>::max(), 1,
                   CDCEventOp::kCreate};
  GraphCDCEvent e2{2, 100, 300, 2000, std::numeric_limits<uint32_t>::max(), 1,
                   CDCEventOp::kCreate};
  GraphCDCEvent e3{3, 100, 400, 3000, std::numeric_limits<uint32_t>::max(), 1,
                   CDCEventOp::kCreate};

  // Apply out of order: 3, 1, 2
  applier.ApplyUnordered(e3);
  EXPECT_EQ(applier.applied_version(), 0u);  // 3 is buffered

  applier.ApplyUnordered(e1);
  EXPECT_EQ(applier.applied_version(), 1u);  // 1 applied immediately

  applier.ApplyUnordered(e2);
  // Now 2 is next, apply it, then drain 3 from buffer
  EXPECT_EQ(applier.applied_version(), 3u);

  auto edges = engine.ScanAtTime(100, Direction::kOut, 3500);
  EXPECT_EQ(edges.size(), 3);
  EXPECT_EQ(edges[0].target_id, 200u);
  EXPECT_EQ(edges[1].target_id, 300u);
  EXPECT_EQ(edges[2].target_id, 400u);
}
