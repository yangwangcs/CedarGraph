// Copyright 2025 The Cedar Authors
//
// Async query execution with optimized thread pool

#ifndef CEDAR_CLIENT_ASYNC_QUERY_H_
#define CEDAR_CLIENT_ASYNC_QUERY_H_

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "cedar/client/types.h"

namespace cedar {
namespace client {

// Async query callback
using AsyncQueryCallback = std::function<void(const QueryResult&)>;

// Async query status
enum class AsyncQueryStatus {
  PENDING,    // Query is pending
  RUNNING,    // Query is running
  COMPLETED,  // Query completed successfully
  FAILED,     // Query failed
  CANCELLED   // Query was cancelled
};

// Async query result
struct AsyncQueryResult {
  AsyncQueryStatus status = AsyncQueryStatus::PENDING;
  QueryResult result;
  std::string error_message;
  int64_t execution_time_ms = 0;
};

// Async query handle
class AsyncQuery {
 public:
  AsyncQuery(const std::string& query, const std::string& space = "default");
  ~AsyncQuery();

  // Get query
  const std::string& GetQuery() const;
  const std::string& GetSpace() const;

  // Get status
  AsyncQueryStatus GetStatus() const;

  // Check if completed
  bool IsCompleted() const;
  bool IsFailed() const;
  bool IsCancelled() const;

  // Wait for completion
  AsyncQueryResult Wait();
  AsyncQueryResult WaitFor(int timeout_ms);

  // Cancel query
  void Cancel();

  // Set callback
  void SetCallback(AsyncQueryCallback callback);

  // Get future
  std::shared_future<AsyncQueryResult> GetFuture();

 private:
  std::string query_;
  std::string space_;
  std::atomic<AsyncQueryStatus> status_{AsyncQueryStatus::PENDING};
  std::promise<AsyncQueryResult> promise_;
  std::shared_future<AsyncQueryResult> future_;
  AsyncQueryCallback callback_;
  mutable std::mutex mutex_;

  // Set result
  void SetResult(const AsyncQueryResult& result);

  // Set error
  void SetError(const std::string& error);

  friend class AsyncQueryPool;
};

// Work item for thread pool
struct WorkItem {
  std::shared_ptr<AsyncQuery> query;
  std::function<QueryResult(const std::string&, const std::string&)> executor;
};

// Optimized thread pool for async queries
class AsyncQueryPool {
 public:
  AsyncQueryPool(int thread_count = 4);
  ~AsyncQueryPool();

  // Submit query
  std::shared_ptr<AsyncQuery> Submit(const std::string& query, 
                                       const std::string& space,
                                       std::function<QueryResult(const std::string&, const std::string&)> executor);

  // Get active query count
  int GetActiveQueryCount() const;

  // Get pending query count
  int GetPendingQueryCount() const;

  // Get total query count
  int GetTotalQueryCount() const;

  // Cancel all queries
  void CancelAll();

  // Wait for all pending and active queries to finish
  void WaitForAll();

  // Shutdown pool
  void Shutdown();

 private:
  int thread_count_;
  std::vector<std::thread> threads_;
  std::queue<WorkItem> work_queue_;
  mutable std::mutex mutex_;
  std::condition_variable condition_;
  std::condition_variable completion_cv_;
  std::atomic<bool> running_{true};
  std::atomic<int> active_queries_{0};
  std::atomic<int> total_queries_{0};

  // Worker thread function
  void WorkerThread();
};

}  // namespace client
}  // namespace cedar

#endif  // CEDAR_CLIENT_ASYNC_QUERY_H_
