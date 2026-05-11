#ifndef CEDAR_QUERYD_STORAGE_EXECUTION_CONTEXT_H_
#define CEDAR_QUERYD_STORAGE_EXECUTION_CONTEXT_H_

#include "cedar/cypher/execution_plan.h"
#include "cedar/queryd/query_storage_client.h"
#include <memory>

namespace cedar {
namespace queryd {

class StorageBackedExecutionContext : public cypher::ExecutionContext {
 public:
  explicit StorageBackedExecutionContext(
      QueryStorageClient* storage_client,
      uint32_t partition_id);

 private:
  QueryStorageClient* storage_client_;
  uint32_t partition_id_;
};

}  // namespace queryd
}  // namespace cedar

#endif
