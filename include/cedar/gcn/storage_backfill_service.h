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

#ifndef CEDAR_GCN_STORAGE_BACKFILL_SERVICE_H_
#define CEDAR_GCN_STORAGE_BACKFILL_SERVICE_H_

#include <cstdint>
#include <vector>

namespace cedar {

class CedarGraphStorage;

namespace gcn {

class TMVEngine;

class StorageBackfillService {
 public:
  StorageBackfillService(TMVEngine* tmv_engine, CedarGraphStorage* storage);

  // Backfill a single vertex from storage into TMV
  void BackfillVertex(uint64_t entity_id, uint16_t edge_type);

  // Batch backfill a range of vertices
  void BackfillRange(uint64_t start_id, uint64_t end_id, uint16_t edge_type = 0);

 private:
  TMVEngine* tmv_engine_;
  CedarGraphStorage* storage_;
};

}  // namespace gcn
}  // namespace cedar

#endif  // CEDAR_GCN_STORAGE_BACKFILL_SERVICE_H_
