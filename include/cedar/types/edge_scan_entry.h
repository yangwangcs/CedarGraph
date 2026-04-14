//===----------------------------------------------------------------------===//
// EdgeScanEntry - Shared type for edge scanning
// Used by both LsmEngine and CedarScan
//===----------------------------------------------------------------------===//

#ifndef CEDAR_EDGE_SCAN_ENTRY_H
#define CEDAR_EDGE_SCAN_ENTRY_H

#include <cstdint>
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

namespace cedar {

// Edge scan result entry
// Used by ScanEdgesWithFolding and EdgeIterator
struct EdgeScanEntry {
  uint64_t target_id;
  Timestamp timestamp;
  Descriptor descriptor;
  uint16_t edge_type;
  CedarKey key;
};

}  // namespace cedar

#endif  // CEDAR_EDGE_SCAN_ENTRY_H
