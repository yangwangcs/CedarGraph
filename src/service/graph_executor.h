#ifndef CEDAR_SERVICE_GRAPH_EXECUTOR_H_
#define CEDAR_SERVICE_GRAPH_EXECUTOR_H_

#include <vector>
#include <future>
#include <chrono>
#include <unordered_map>
#include <string>
#include "query_service.pb.h"

namespace cedar {
namespace service {

class GraphServiceRouter;

class GraphExecutor {
 public:
  explicit GraphExecutor(GraphServiceRouter* router);
  ~GraphExecutor();

  cedar::query::ResultSet ExecuteParallel(
      const std::string& query,
      const std::vector<uint32_t>& partition_ids,
      const std::unordered_map<std::string, cedar::query::Value>& parameters);

  cedar::query::ResultSet ExecuteParallelWithTimeout(
      const std::string& query,
      const std::vector<uint32_t>& partition_ids,
      std::chrono::milliseconds timeout);

 private:
  GraphServiceRouter* router_;
  
  cedar::query::ResultSet ExecuteSinglePartition(
      const std::string& query,
      uint32_t partition_id,
      const std::unordered_map<std::string, cedar::query::Value>& parameters);
};

}  // namespace service
}  // namespace cedar

#endif
