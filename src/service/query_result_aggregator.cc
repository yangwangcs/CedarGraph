#include "cedar/service/query_result_aggregator.h"

#include <algorithm>
#include <unordered_set>
#include <functional>
#include <sstream>

namespace cedar {
namespace service {

namespace {

using QueryValue = QueryResultAggregator::Value;

bool IsValueType(const QueryValue& value, QueryValue::ValueTypeCase type) {
  return value.value_type_case() == type;
}

std::string ValueTypeName(const QueryValue& value) {
  switch (value.value_type_case()) {
    case QueryValue::kBoolVal:
      return "bool";
    case QueryValue::kIntVal:
      return "int";
    case QueryValue::kFloatVal:
      return "float";
    case QueryValue::kStringVal:
      return "string";
    case QueryValue::kBytesVal:
      return "bytes";
    case QueryValue::kListVal:
      return "list";
    case QueryValue::kMapVal:
      return "map";
    case QueryValue::kNullVal:
      return "null";
    default:
      return "other";
  }
}

}  // namespace

// Helper to compare two values
static int CompareValues(const QueryResultAggregator::Value& a, 
                         const QueryResultAggregator::Value& b) {
  // Handle null values
  if (IsValueType(a, QueryValue::kNullVal) && IsValueType(b, QueryValue::kNullVal)) return 0;
  if (IsValueType(a, QueryValue::kNullVal)) return -1;
  if (IsValueType(b, QueryValue::kNullVal)) return 1;

  // Compare based on type
  if (IsValueType(a, QueryValue::kIntVal) && IsValueType(b, QueryValue::kIntVal)) {
    int64_t av = a.int_val();
    int64_t bv = b.int_val();
    if (av < bv) return -1;
    if (av > bv) return 1;
    return 0;
  }
  if (IsValueType(a, QueryValue::kFloatVal) && IsValueType(b, QueryValue::kFloatVal)) {
    double diff = a.float_val() - b.float_val();
    return diff < 0 ? -1 : (diff > 0 ? 1 : 0);
  }
  if (IsValueType(a, QueryValue::kStringVal) && IsValueType(b, QueryValue::kStringVal)) {
    return a.string_val().compare(b.string_val());
  }
  if (IsValueType(a, QueryValue::kBoolVal) && IsValueType(b, QueryValue::kBoolVal)) {
    return static_cast<int>(a.bool_val()) - static_cast<int>(b.bool_val());
  }
  
  // Different types - order by type name for consistency
  return ValueTypeName(a).compare(ValueTypeName(b));
}

// Generate a fingerprint for a row for deduplication
static std::string RowFingerprint(const QueryResultAggregator::Row& row) {
  std::stringstream ss;
  for (int i = 0; i < row.values_size(); ++i) {
    if (i > 0) ss << "|";
    const auto& val = row.values(i);
    if (IsValueType(val, QueryValue::kNullVal)) {
      ss << "null";
    } else if (IsValueType(val, QueryValue::kBoolVal)) {
      ss << (val.bool_val() ? "1" : "0");
    } else if (IsValueType(val, QueryValue::kIntVal)) {
      ss << val.int_val();
    } else if (IsValueType(val, QueryValue::kFloatVal)) {
      ss << val.float_val();
    } else if (IsValueType(val, QueryValue::kStringVal)) {
      ss << val.string_val();
    } else if (IsValueType(val, QueryValue::kBytesVal)) {
      ss << val.bytes_val();
    } else {
      ss << "?";
    }
  }
  return ss.str();
}

// Extract numeric value from a Value for aggregation
static bool ExtractNumeric(const QueryResultAggregator::Value& value, double* out) {
  if (IsValueType(value, QueryValue::kIntVal)) {
    *out = static_cast<double>(value.int_val());
    return true;
  }
  if (IsValueType(value, QueryValue::kFloatVal)) {
    *out = value.float_val();
    return true;
  }
  return false;
}

QueryResultAggregator::ResultSet 
QueryResultAggregator::MergeResults(const std::vector<ResultSet>& partial_results) {
  ResultSet merged;
  
  if (partial_results.empty()) {
    return merged;
  }

  // Use the column names from the first result set
  const auto& first = partial_results[0];
  for (int i = 0; i < first.columns_size(); ++i) {
    merged.add_columns(first.columns(i));
  }

  // Pre-calculate total rows to reserve space and avoid repeated reallocations
  uint64_t total_rows = 0;
  for (const auto& partial : partial_results) {
    total_rows += static_cast<uint64_t>(partial.rows_size());
  }
  merged.mutable_rows()->Reserve(static_cast<int>(total_rows));

  // Merge all rows from all partial results
  for (const auto& partial : partial_results) {
    for (int i = 0; i < partial.rows_size(); ++i) {
      auto* new_row = merged.add_rows();
      new_row->CopyFrom(partial.rows(i));
    }
  }

  merged.set_total_rows(total_rows);
  return merged;
}

void QueryResultAggregator::SortResults(ResultSet* results,
                                        const std::vector<std::string>& order_by_columns,
                                        bool ascending) {
  if (!results || order_by_columns.empty() || results->rows_size() == 0) {
    return;
  }

  // Find column indices
  std::vector<int> column_indices;
  for (const auto& col_name : order_by_columns) {
    int idx = FindColumnIndex(*results, col_name);
    if (idx >= 0) {
      column_indices.push_back(idx);
    }
  }

  if (column_indices.empty()) {
    return;
  }

  // Get mutable rows and sort
  auto* rows = results->mutable_rows();
  std::sort(rows->begin(), rows->end(),
            [&column_indices, ascending](const Row& a, const Row& b) {
              return CompareRows(a, b, column_indices, ascending);
            });
}

void QueryResultAggregator::ApplyPagination(ResultSet* results, 
                                            uint32_t offset, 
                                            uint32_t limit) {
  if (!results || results->rows_size() == 0) {
    return;
  }

  int total = results->rows_size();
  int start = static_cast<int>(offset);
  int end = (limit == 0) ? total : std::min(start + static_cast<int>(limit), total);

  if (start >= total) {
    // Offset is beyond the end, clear all rows
    results->clear_rows();
    return;
  }

  if (start == 0 && end >= total) {
    // No pagination needed
    return;
  }

  // Create a new row list with only the paginated rows
  google::protobuf::RepeatedPtrField<Row> new_rows;
  for (int i = start; i < end; ++i) {
    auto* new_row = new_rows.Add();
    new_row->CopyFrom(results->rows(i));
  }

  // Replace the old rows with the paginated ones
  results->clear_rows();
  for (int i = 0; i < new_rows.size(); ++i) {
    auto* row = results->add_rows();
    row->CopyFrom(new_rows[i]);
  }
}

void QueryResultAggregator::Deduplicate(ResultSet* results) {
  if (!results || results->rows_size() <= 1) {
    return;
  }

  std::unordered_set<std::string> seen;
  google::protobuf::RepeatedPtrField<Row> unique_rows;

  for (int i = 0; i < results->rows_size(); ++i) {
    const auto& row = results->rows(i);
    std::string fingerprint = RowFingerprint(row);
    
    if (seen.find(fingerprint) == seen.end()) {
      seen.insert(fingerprint);
      auto* new_row = unique_rows.Add();
      new_row->CopyFrom(row);
    }
  }

  // Replace rows with deduplicated ones
  results->clear_rows();
  for (int i = 0; i < unique_rows.size(); ++i) {
    auto* row = results->add_rows();
    row->CopyFrom(unique_rows[i]);
  }
}

QueryResultAggregator::Value 
QueryResultAggregator::CalculateAggregate(const std::string& function,
                                          const std::string& column,
                                          const ResultSet& results) {
  Value result;
  int col_idx = FindColumnIndex(results, column);
  
  if (col_idx < 0) {
    result.mutable_null_val();
    return result;
  }

  std::string func_lower = function;
  std::transform(func_lower.begin(), func_lower.end(), func_lower.begin(), ::tolower);

  if (func_lower == "count") {
    // Count all non-null values
    int64_t count = 0;
    for (int i = 0; i < results.rows_size(); ++i) {
      const auto& row = results.rows(i);
      if (col_idx < row.values_size() &&
          !IsValueType(row.values(col_idx), QueryValue::kNullVal)) {
        ++count;
      }
    }
    result.set_int_val(count);
  } 
  else if (func_lower == "sum") {
    double sum = 0.0;
    bool has_values = false;
    for (int i = 0; i < results.rows_size(); ++i) {
      const auto& row = results.rows(i);
      if (col_idx < row.values_size()) {
        double val;
        if (ExtractNumeric(row.values(col_idx), &val)) {
          sum += val;
          has_values = true;
        }
      }
    }
    if (has_values) {
      result.set_float_val(sum);
    } else {
      result.mutable_null_val();
    }
  } 
  else if (func_lower == "avg") {
    double sum = 0.0;
    int64_t count = 0;
    for (int i = 0; i < results.rows_size(); ++i) {
      const auto& row = results.rows(i);
      if (col_idx < row.values_size()) {
        double val;
        if (ExtractNumeric(row.values(col_idx), &val)) {
          sum += val;
          ++count;
        }
      }
    }
    if (count > 0) {
      result.set_float_val(sum / count);
    } else {
      result.mutable_null_val();
    }
  }
  else {
    // Unknown function, return null
    result.mutable_null_val();
  }

  return result;
}

bool QueryResultAggregator::CompareRows(const Row& a, 
                                        const Row& b,
                                        const std::vector<int>& column_indices,
                                        bool ascending) {
  for (int col_idx : column_indices) {
    if (col_idx >= a.values_size() || col_idx >= b.values_size()) {
      continue;
    }

    int cmp = CompareValues(a.values(col_idx), b.values(col_idx));
    if (cmp != 0) {
      return ascending ? (cmp < 0) : (cmp > 0);
    }
  }
  return false;  // Equal
}

int QueryResultAggregator::FindColumnIndex(const ResultSet& results, 
                                           const std::string& column) {
  for (int i = 0; i < results.columns_size(); ++i) {
    if (results.columns(i) == column) {
      return i;
    }
  }
  return -1;
}

}  // namespace service
}  // namespace cedar
