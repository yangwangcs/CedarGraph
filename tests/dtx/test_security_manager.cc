#include <gtest/gtest.h>
#include "cedar/dtx/security.h"

TEST(SecurityManagerTest, AuditLoggerShortPathDoesNotCrash) {
  cedar::dtx::security::AuditLogger logger;
  cedar::dtx::security::AuditLogger::Config config;
  config.log_file = "/x";  // shorter than allowed_log_prefix
  config.allowed_log_prefix = "/var/log/cedar";
  auto s = logger.Initialize(config);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsInvalidArgument());
}
