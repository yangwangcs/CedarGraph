// Copyright 2025 The Cedar Authors
//
// Async query execution with optimized thread pool implementation

#include "cedar/client/async_query.h"

#include <chrono>
#include <iostream>

namespace cedar {
namespace client {

// ============================================================================
// AsyncQuery
// ============================================================================

AsyncQuery::AsyncQuery(const std::string& query, const std::string& space)
    : query_(query), space_(space), future_(promise_.get_future().share()) {}

AsyncQuery::~AsyncQuery() = default;

const std::string& AsyncQuery::GetQuery() const {
  return query_;
}

const std::string& AsyncQuery::GetSpace() const {
  return space_;
}

AsyncQueryStatus AsyncQuery::GetStatus() const {
  return status_;
}

bool AsyncQuery::IsCompleted() const {
  return status_ == AsyncQueryStatus::COMPLETED;
}

bool AsyncQuery::IsFailed() const {
  return status_ == AsyncQueryStatus::FAILED;
}

bool AsyncQuery::IsCancelled() const {
  return status_ == AsyncQueryStatus::CANCELLED;
}

AsyncQueryResult AsyncQuery::Wait() {
  return future_.get();
}

AsyncQueryResult AsyncQuery::WaitFor(int timeout_ms) {
  auto status = future_.wait_for(std::chrono::milliseconds(timeout_ms));
  if (status == std::future_status::timeout) {
    AsyncQueryResult result;
    result.status = AsyncQueryStatus::RUNNING;
    result.error_message = "Query timed out";
    result.execution_time_ms = 0;
    return result;
  }
  return future_.get();
}

void AsyncQuery::Cancel() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (status_ == AsyncQueryStatus::PENDING || status_ == AsyncQueryStatus::RUNNING) {
    status_ = AsyncQueryStatus::CANCELLED;
    
    // Only set promise if it hasn't been set yet
    if (future_.valid()) {
      try {
        AsyncQueryResult result;
        result.status = AsyncQueryStatus::CANCELLED;
        result.error_message = "Query cancelled";
        result.execution_time_ms = 0;
        promise_.set_value(result);
      } catch (const std::future_error&) {
        // Promise already set, ignore
      }
    }
  }
}

void AsyncQuery::SetCallback(AsyncQueryCallback callback) {
  std::lock_guard<std::mutex> lock(mutex_);
  callback_ = callback;
}

std::shared_future<AsyncQueryResult> AsyncQuery::GetFuture() {
  return future_;
}

void AsyncQuery::SetResult(const AsyncQueryResult& result) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  // Don't set result if query was cancelled
  if (status_ == AsyncQueryStatus::CANCELLED) {
    return;
  }
  
  status_ = result.status;
  
  try {
    promise_.set_value(result);
  } catch (const std::future_error&) {
    // Promise already set, ignore
  }
  
  if (callback_) {
    callback_(result.result);
  }
}

void AsyncQuery::SetError(const std::string& error) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  // Don't set error if query was cancelled
  if (status_ == AsyncQueryStatus::CANCELLED) {
    return;
  }
  
  status_ = AsyncQueryStatus::FAILED;
  
  try {
    AsyncQueryResult result;
    result.status = AsyncQueryStatus::FAILED;
    result.error_message = error;
    result.execution_time_ms = 0;
    promise_.set_value(result);
  } catch (const std::future_error&) {
    // Promise already set, ignore
  }
}

// ============================================================================
// AsyncQueryPool - Optimized Thread Pool
// ============================================================================

AsyncQueryPool::AsyncQueryPool(int thread_count)
    : thread_count_(thread_count) {
  // Start worker threads
  for (int i = 0; i < thread_count_; ++i) {
    threads_.emplace_back(&AsyncQueryPool::WorkerThread, this);
  }
}

AsyncQueryPool::~AsyncQueryPool() {
  Shutdown();
}

void AsyncQueryPool::Shutdown() {
  {
    std::unique_lock<std::mutex> lock(mutex_);
    running_ = false;
  }
  
  // Notify all threads to wake up and exit
  condition_.notify_all();
  
  // Join threads
  for (auto& thread : threads_) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}

std::shared_ptr<AsyncQuery> AsyncQueryPool::Submit(
    const std::string& query,
    const std::string& space,
    std::function<QueryResult(const std::string&, const std::string&)> executor) {
  
  auto async_query = std::make_shared<AsyncQuery>(query, space);
  
  // Create work item
  WorkItem item;
  item.query = async_query;
  item.executor = executor;
  
  // Add to work queue
  {
    std::unique_lock<std::mutex> lock(mutex_);
    work_queue_.push(std::move(item));
    total_queries_++;
  }
  
  // Notify one worker thread
  condition_.notify_one();
  
  return async_query;
}

int AsyncQueryPool::GetActiveQueryCount() const {
  return active_queries_;
}

int AsyncQueryPool::GetPendingQueryCount() const {
  std::unique_lock<std::mutex> lock(mutex_);
  return work_queue_.size();
}

int AsyncQueryPool::GetTotalQueryCount() const {
  return total_queries_;
}

void AsyncQueryPool::CancelAll() {
  // Cancel all pending queries in the queue
  std::unique_lock<std::mutex> lock(mutex_);
  while (!work_queue_.empty()) {
    auto& item = work_queue_.front();
    item.query->Cancel();
    work_queue_.pop();
  }
  completion_cv_.notify_all();
}

void AsyncQueryPool::WaitForAll() {
  std::unique_lock<std::mutex> lock(mutex_);
  completion_cv_.wait(lock, [this]() {
    return work_queue_.empty() && active_queries_.load() == 0;
  });
}

void AsyncQueryPool::WorkerThread() {
  while (true) {
    WorkItem item;
    
    // Wait for work or shutdown
    {
      std::unique_lock<std::mutex> lock(mutex_);
      condition_.wait(lock, [this] { 
        return !running_ || !work_queue_.empty(); 
      });
      
      // Check if we should exit
      if (!running_ && work_queue_.empty()) {
        return;
      }
      
      // Get work item from queue
      if (!work_queue_.empty()) {
        item = std::move(work_queue_.front());
        work_queue_.pop();
        active_queries_++;
      } else {
        continue;
      }
    }
    
    // Check if query was cancelled
    if (item.query->IsCancelled()) {
      active_queries_--;
      completion_cv_.notify_all();
      continue;
    }
    
    // Execute query
    item.query->status_ = AsyncQueryStatus::RUNNING;
    
    auto start = std::chrono::high_resolution_clock::now();
    
    try {
      auto result = item.executor(item.query->GetQuery(), item.query->GetSpace());
      
      auto end = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      
      AsyncQueryResult async_result;
      async_result.status = result.success ? AsyncQueryStatus::COMPLETED : AsyncQueryStatus::FAILED;
      async_result.result = result;
      async_result.execution_time_ms = duration;
      
      if (!result.success) {
        async_result.error_message = result.error_message;
      }
      
      item.query->SetResult(async_result);
    } catch (const std::exception& e) {
      item.query->SetError(e.what());
    }
    
    active_queries_--;
    completion_cv_.notify_all();
  }
}

}  // namespace client
}  // namespace cedar
