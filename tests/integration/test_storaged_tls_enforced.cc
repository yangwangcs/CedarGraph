#include <gtest/gtest.h>
#include "cedar/dtx/raft/grpc_tls.h"

TEST(StorageDTls, DefaultTlsEnabled) {
  // Verify that default TlsConfig has TLS enabled
  cedar::dtx::raft::TlsConfig tls;
  EXPECT_TRUE(tls.enabled);
}
