#ifndef CEDAR_SERVICE_QUERY_RESULT_AGGREGATOR_H_
#define CEDAR_SERVICE_QUERY_RESULT_AGGREGATOR_H_

#include <vector>
#include <string>
#include <memory>
#include "query_service.pb.h"

namespace cedar {
namespace service {

class QueryResultAggregator {
 public:
  using ResultSet = cedar::query::ResultSet;
  using Row = cedar::query::Row;
  using Value = cedar::query::Value;

  static ResultSet MergeResults(const std::vector<ResultSet>& partial_results);
  static void SortResults(ResultSet* results, 
                          const std::vector<std::string>& order_by_columns,
                          bool ascending = true);
  static void ApplyPagination(ResultSet* results, uint32_t offset, uint32_t limit);
  static void Deduplicate(ResultSet* results);
  static Value CalculateAggregate(const std::string& function,
                                   const std::string& column,
                                   const ResultSet& results);

 private:
  static bool CompareRows(const Row& a, const Row& b, 
                          const std::vector<int>& column_indices,
                          bool ascending);
  static int FindColumnIndex(const ResultSet& results, const std::string& column);
};

}  // namespace service
}  // namespace cedar

#endif
