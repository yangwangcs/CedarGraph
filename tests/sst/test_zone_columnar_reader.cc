#include <gtest/gtest.h>

#include <algorithm>
#include <cstdio>
#include <memory>
#include <vector>

#include "cedar/core/env.h"
#include "cedar/sst/sst_builder_factory.h"
#include "cedar/sst/zone_columnar_reader.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

namespace cedar {

TEST(ZoneColumnarReader, RoundTrip) {
  // Use a temporary file path.
  std::string file_path = "/tmp/test_zone_columnar_reader.sst";
  std::remove(file_path.c_str());

  auto env = Env::Default();
  WritableFile* file = nullptr;
  Status s = env->NewWritableFile(file_path, &file);
  ASSERT_TRUE(s.ok()) << s.ToString();

  // Small blocks to force multiple blocks.
  SstBuilderOptions options;
  options.target_block_size = 256;
  options.block_row_limit = 4;
  auto builder = SstBuilderFactory::Create(file, "", options);

  // Build a set of keys that span multiple blocks.
  std::vector<std::pair<CedarKey, Descriptor>> entries;
  for (uint64_t e = 1; e <= 3; ++e) {
    for (uint16_t col = 1; col <= 2; ++col) {
      for (uint64_t ts = 10; ts >= 1; --ts) {
        CedarKey key(e, EntityType::Vertex, col, Timestamp(ts), 0, 0, 0, 0);
        Descriptor desc = Descriptor::InlineInt(col, static_cast<int32_t>(ts));
        entries.emplace_back(key, desc);
      }
    }
  }

  std::sort(entries.begin(), entries.end(),
            [](const auto& a, const auto& b) {
              return a.first.LessForSorting(b.first);
            });

  for (const auto& [key, desc] : entries) {
    builder->Add(key, desc, Timestamp(0));
  }

  s = builder->Finish();
  ASSERT_TRUE(s.ok()) << s.ToString();

  // The builder wrapper owns the ZoneColumnarSstBuilderV2 which may reference
  // the WritableFile, but the factory interface doesn't expose closing it.
  // Sync and close explicitly.
  s = file->Sync();
  ASSERT_TRUE(s.ok()) << s.ToString();
  delete file;
  file = nullptr;

  // Read back via the V2 reader.
  ZoneColumnarSstReader reader(file_path);
  s = reader.Open();
  ASSERT_TRUE(s.ok()) << s.ToString();

  EXPECT_EQ(reader.NumEntries(), entries.size());

  for (size_t i = 0; i < entries.size(); ++i) {
    CedarKey reconstructed = reader.ReconstructKey(static_cast<uint32_t>(i));
    EXPECT_EQ(reconstructed, entries[i].first)
        << "Key mismatch at row " << i;

    auto opt_desc = reader.GetValueByRow(static_cast<uint32_t>(i));
    ASSERT_TRUE(opt_desc.has_value()) << "Missing value at row " << i;
    EXPECT_EQ(opt_desc->AsRaw(), entries[i].second.AsRaw())
        << "Value mismatch at row " << i;
  }

  // Also verify iterator round-trip.
  auto it = std::unique_ptr<ZoneColumnarSstReader::Iterator>(reader.NewIterator());
  it->SeekToFirst();
  size_t iter_idx = 0;
  while (it->Valid()) {
    EXPECT_EQ(it->Key(), entries[iter_idx].first)
        << "Iterator key mismatch at row " << iter_idx;
    EXPECT_EQ(it->Value().AsRaw(), entries[iter_idx].second.AsRaw())
        << "Iterator value mismatch at row " << iter_idx;
    it->Next();
    ++iter_idx;
  }
  EXPECT_EQ(iter_idx, entries.size());

  std::remove(file_path.c_str());
}

}  // namespace cedar
