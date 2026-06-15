#ifndef CEDAR_QUERYD_STORAGE_EXECUTION_CONTEXT_H_
#define CEDAR_QUERYD_STORAGE_EXECUTION_CONTEXT_H_

#include "cedar/cypher/execution_plan.h"
#include "cedar/queryd/query_storage_client.h"
#include <memory>

namespace cedar {
namespace queryd {

/// Execution context backed by QueryStorageClient for partition-local scans.
class StorageBackedExecutionContext : public cypher::ExecutionContext {
 public:
  StorageBackedExecutionContext(
      QueryStorageClient* storage_client,
      uint32_t partition_id,
      const std::string& space_name = "default",
      const std::string& label = "");

 private:
  void SequentialEntityScan(uint64_t min_id, uint64_t max_id, uint64_t step,
                            std::vector<uint64_t>* results);

  QueryStorageClient* storage_client_;
  uint32_t partition_id_;
  std::string space_name_;
  std::string label_;
};

}  // namespace queryd
}  // namespace cedar

#endif
