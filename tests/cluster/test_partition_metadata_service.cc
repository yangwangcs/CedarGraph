#include <gtest/gtest.h>
#include "cedar/dtx/meta_service.h"

using namespace cedar::dtx;

TEST(PartitionMetadataServiceStub, CompileAndLink) {
  MetadataService service;
  EXPECT_TRUE(service.ListSpaces().empty() || !service.ListSpaces().empty());
}
