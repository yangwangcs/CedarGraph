// Copyright 2026 CedarGraph Authors
//
// Licensed under the Apache License, Version 2.0

#include <gtest/gtest.h>

#include <chrono>
#include <vector>

#include "cedar/dtx/storage_service_impl.h"

namespace cedar {
namespace dtx {
namespace {

TEST(MetaServiceNodeClientShutdownTest, StopWakesHeartbeatThreadPromptly) {
  MetaServiceNodeClient client;

  client.StartHeartbeatLoop([] { return std::vector<PartitionID>{}; });

  auto start = std::chrono::steady_clock::now();
  client.StopHeartbeatLoop();
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
            1000);
}

}  // namespace
}  // namespace dtx
}  // namespace cedar
