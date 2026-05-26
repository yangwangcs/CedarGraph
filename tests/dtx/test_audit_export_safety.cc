#include <gtest/gtest.h>
#include "cedar/dtx/security.h"

TEST(AuditExport, RejectsDirectoryTraversal) {
  cedar::dtx::security::AuditLogger logger;
  cedar::dtx::security::AuditLogger::Config config;
  config.log_file = "/tmp/audit_test.log";
  config.max_entries = 1000;
  ASSERT_TRUE(logger.Initialize(config).ok());
  
  auto status = logger.ExportToFile("../../../etc/passwd", {}, {});
  EXPECT_FALSE(status.ok());
  
  logger.Shutdown();
}

TEST(AuditExport, AllowsSafeFilename) {
  cedar::dtx::security::AuditLogger logger;
  cedar::dtx::security::AuditLogger::Config config;
  config.log_file = "/tmp/audit_test2.log";
  config.max_entries = 1000;
  ASSERT_TRUE(logger.Initialize(config).ok());
  
  auto status = logger.ExportToFile("export_2024.json", {}, {});
  // Should succeed even if there are no entries to export
  EXPECT_TRUE(status.ok());
  
  logger.Shutdown();
}
