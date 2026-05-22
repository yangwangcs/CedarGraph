#include <gtest/gtest.h>
#include <limits>
#include "cedar/core/env.h"

TEST(EnvPosixTest, MmapReadRejectsWraparoundOffset) {
  cedar::Env* env = cedar::Env::Default();
  std::string tmpfile = "/tmp/cedar_mmap_test_" + std::to_string(getpid());
  {
    cedar::WritableFile* file;
    ASSERT_TRUE(env->NewWritableFile(tmpfile, &file).ok());
    ASSERT_TRUE(file->Append("hello").ok());
    ASSERT_TRUE(file->Close().ok());
    delete file;
  }
  cedar::RandomAccessFile* raf;
  ASSERT_TRUE(env->NewRandomAccessFile(tmpfile, &raf).ok());
  cedar::Slice result;
  char scratch[10];
  auto s = raf->Read(std::numeric_limits<uint64_t>::max() - 1, 5, &result, scratch);
  EXPECT_FALSE(s.ok());
  delete raf;
  env->DeleteFile(tmpfile);
}
