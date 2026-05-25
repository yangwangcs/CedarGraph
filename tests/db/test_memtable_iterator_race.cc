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
#include <thread>
#include <atomic>
#include <chrono>

#include "cedar/storage/cedar_memtable.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

using namespace cedar;

TEST(MemTableIteratorRaceTest, ConcurrentWritersAndReaders) {
  CedarMemTable memtable;
  std::atomic<bool> stop{false};
  std::atomic<uint64_t> write_count{0};
  std::atomic<uint64_t> read_count{0};

  // Writer thread: continuous Put
  auto writer = [&]() {
    uint64_t id = 1;
    while (!stop.load()) {
      CedarKey key(id, EntityType::Vertex, 0, Timestamp(id * 1000), 0, 0, 0, 0);
      Descriptor desc = Descriptor::InlineInt(0, static_cast<int64_t>(id));
      Status s = memtable.Put(key, desc, Timestamp(id * 1000));
      (void)s;  // Ignore status in this stress test
      ++write_count;
      id = (id % 100) + 1;  // cycle through 1..100
    }
  };

  // Reader thread: continuous Iterator creation and traversal
  auto reader = [&]() {
    while (!stop.load()) {
      auto* iter = memtable.NewIterator();
      if (iter) {
        iter->SeekToFirst();
        while (iter->Valid()) {
          iter->Next();
        }
        delete iter;
      }
      ++read_count;
    }
  };

  std::thread t1(writer);
  std::thread t2(writer);
  std::thread t3(reader);
  std::thread t4(reader);

  // Run for 1 second
  std::this_thread::sleep_for(std::chrono::seconds(1));
  stop.store(true);

  t1.join();
  t2.join();
  t3.join();
  t4.join();

  EXPECT_GT(write_count.load(), 0u);
  EXPECT_GT(read_count.load(), 0u);
}
