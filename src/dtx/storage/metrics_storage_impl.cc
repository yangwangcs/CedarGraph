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

#include "cedar/dtx/storage/metrics_collector.h"
#include "cedar/dtx/transaction_metrics.h"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/resource.h>
#if defined(__APPLE__)
#include <sys/mount.h>
#include <mach/mach.h>
#include <sys/sysctl.h>
#else
#include <sys/statfs.h>
#endif

namespace cedar {
namespace dtx {
namespace storage {

// =============================================================================
// Metric
// =============================================================================

Metric::Metric(const std::string& name, MetricType type,
               const std::string& description)
    : name_(name), type_(type), description_(description) {}

// =============================================================================
// Counter
// =============================================================================

Counter::Counter(const std::string& name, const std::string& description)
    : Metric(name, MetricType::kCounter, description) {}

void Counter::Increment(double value) {
  double old = value_.load(std::memory_order_relaxed);
  double desired;
  do {
    desired = old + value;
  } while (!value_.compare_exchange_weak(old, desired,
                                          std::memory_order_relaxed));
}

std::string Counter::ToPrometheusFormat() const {
  std::ostringstream oss;
  oss << "# HELP " << name_ << " " << description_ << "\n";
  oss << "# TYPE " << name_ << " counter\n";
  oss << name_ << " " << GetValue() << "\n";
  return oss.str();
}

// =============================================================================
// Gauge
// =============================================================================

Gauge::Gauge(const std::string& name, const std::string& description)
    : Metric(name, MetricType::kGauge, description) {}

void Gauge::Set(double value) {
  value_.store(value, std::memory_order_relaxed);
}

void Gauge::Increment(double value) {
  double old = value_.load(std::memory_order_relaxed);
  double desired;
  do {
    desired = old + value;
  } while (!value_.compare_exchange_weak(old, desired,
                                          std::memory_order_relaxed));
}

void Gauge::Decrement(double value) {
  Increment(-value);
}

std::string Gauge::ToPrometheusFormat() const {
  std::ostringstream oss;
  oss << "# HELP " << name_ << " " << description_ << "\n";
  oss << "# TYPE " << name_ << " gauge\n";
  oss << name_ << " " << GetValue() << "\n";
  return oss.str();
}

// =============================================================================
// Histogram
// =============================================================================

Histogram::Histogram(const std::string& name, const std::string& description,
                     std::vector<double> buckets)
    : Metric(name, MetricType::kHistogram, description),
      bucket_bounds_(std::move(buckets)) {
  std::sort(bucket_bounds_.begin(), bucket_bounds_.end());
  for (size_t i = 0; i <= bucket_bounds_.size(); ++i) {
    bucket_counts_.emplace_back(
        std::make_unique<std::atomic<uint64_t>>(0));
  }
}

void Histogram::Observe(double value) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  size_t idx = bucket_bounds_.size();
  for (size_t i = 0; i < bucket_bounds_.size(); ++i) {
    if (value <= bucket_bounds_[i]) {
      idx = i;
      break;
    }
  }
  (*bucket_counts_[idx]).fetch_add(1, std::memory_order_relaxed);
  total_count_.fetch_add(1, std::memory_order_relaxed);
  double old_sum = total_sum_.load(std::memory_order_relaxed);
  double desired_sum;
  do {
    desired_sum = old_sum + value;
  } while (!total_sum_.compare_exchange_weak(old_sum, desired_sum,
                                               std::memory_order_relaxed));
}

std::string Histogram::ToPrometheusFormat() const {
  std::ostringstream oss;
  oss << "# HELP " << name_ << " " << description_ << "\n";
  oss << "# TYPE " << name_ << " histogram\n";
  uint64_t cumulative = 0;
  std::shared_lock<std::shared_mutex> lock(mutex_);
  for (size_t i = 0; i < bucket_bounds_.size(); ++i) {
    cumulative += bucket_counts_[i]->load(std::memory_order_relaxed);
    oss << name_ << "_bucket{le=\"" << bucket_bounds_[i] << "\"} "
        << cumulative << "\n";
  }
  cumulative += bucket_counts_[bucket_bounds_.size()]->load(std::memory_order_relaxed);
  oss << name_ << "_bucket{le=\"+Inf\"} " << cumulative << "\n";
  oss << name_ << "_sum " << total_sum_.load(std::memory_order_relaxed) << "\n";
  oss << name_ << "_count " << total_count_.load(std::memory_order_relaxed) << "\n";
  return oss.str();
}

// =============================================================================
// MetricFamily
// =============================================================================

MetricFamily::MetricFamily(const std::string& name, MetricType type,
                           const std::string& description,
                           const std::vector<std::string>& label_names)
    : name_(name), type_(type), description_(description),
      label_names_(label_names) {}

std::string MetricFamily::ToPrometheusFormat() const {
  std::ostringstream oss;
  std::shared_lock<std::shared_mutex> lock(mutex_);
  for (const auto& [key, metric] : metrics_) {
    (void)key;
    oss << metric->ToPrometheusFormat();
  }
  return oss.str();
}

// =============================================================================
// StorageNodeMetrics
// =============================================================================

StorageNodeMetrics::StorageNodeMetrics() = default;

void StorageNodeMetrics::Initialize(const std::string& prefix) {
  put_ops_total = std::make_unique<Counter>(prefix + "_put_ops_total", "Total put operations");
  get_ops_total = std::make_unique<Counter>(prefix + "_get_ops_total", "Total get operations");
  scan_ops_total = std::make_unique<Counter>(prefix + "_scan_ops_total", "Total scan operations");
  delete_ops_total = std::make_unique<Counter>(prefix + "_delete_ops_total", "Total delete operations");
  batch_ops_total = std::make_unique<Counter>(prefix + "_batch_ops_total", "Total batch operations");

  put_latency_seconds = std::make_unique<Histogram>(
      prefix + "_put_latency_seconds", "Put latency in seconds",
      std::vector<double>{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0});
  get_latency_seconds = std::make_unique<Histogram>(
      prefix + "_get_latency_seconds", "Get latency in seconds",
      std::vector<double>{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0});
  scan_latency_seconds = std::make_unique<Histogram>(
      prefix + "_scan_latency_seconds", "Scan latency in seconds",
      std::vector<double>{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0});

  storage_size_bytes = std::make_unique<Gauge>(prefix + "_storage_size_bytes", "Storage size in bytes");
  storage_keys_total = std::make_unique<Gauge>(prefix + "_storage_keys_total", "Total number of keys");
  memtable_size_bytes = std::make_unique<Gauge>(prefix + "_memtable_size_bytes", "Memtable size in bytes");
  sst_file_count = std::make_unique<Gauge>(prefix + "_sst_file_count", "Number of SST files");

  raft_role = std::make_unique<Gauge>(prefix + "_raft_role", "Raft role");
  raft_leader_changes_total = std::make_unique<Counter>(prefix + "_raft_leader_changes_total", "Raft leader changes");
  raft_proposals_failed_total = std::make_unique<Counter>(prefix + "_raft_proposals_failed_total", "Failed raft proposals");
  raft_commit_index = std::make_unique<Gauge>(prefix + "_raft_commit_index", "Raft commit index");
  raft_applied_index = std::make_unique<Gauge>(prefix + "_raft_applied_index", "Raft applied index");

  replication_lag_updates_total = std::make_unique<Counter>(prefix + "_replication_lag_updates_total", "Replication lag updates");
  replication_lag_seconds = std::make_unique<Gauge>(prefix + "_replication_lag_seconds", "Replication lag in seconds");
}

// =============================================================================
// MetricsRegistry
// =============================================================================

MetricsRegistry& MetricsRegistry::Instance() {
  static MetricsRegistry instance;
  return instance;
}

void MetricsRegistry::Register(std::unique_ptr<Metric> metric) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  metrics_[metric->GetName()] = std::move(metric);
}

void MetricsRegistry::RegisterFamily(std::unique_ptr<MetricFamily> family) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  // Not stored in a named map currently; families manage their own metrics.
  (void)family;
}

Counter* MetricsRegistry::GetCounter(const std::string& name,
                                     const std::string& description) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto it = metrics_.find(name);
  if (it != metrics_.end()) {
    if (it->second->GetType() == MetricType::kCounter) {
      return static_cast<Counter*>(it->second.get());
    }
    // Type mismatch: overwrite with correct type
  }
  auto counter = std::make_unique<Counter>(name, description);
  Counter* ptr = counter.get();
  metrics_[name] = std::move(counter);
  return ptr;
}

Gauge* MetricsRegistry::GetGauge(const std::string& name,
                                 const std::string& description) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto it = metrics_.find(name);
  if (it != metrics_.end()) {
    if (it->second->GetType() == MetricType::kGauge) {
      return static_cast<Gauge*>(it->second.get());
    }
    // Type mismatch: overwrite with correct type
  }
  auto gauge = std::make_unique<Gauge>(name, description);
  Gauge* ptr = gauge.get();
  metrics_[name] = std::move(gauge);
  return ptr;
}

Histogram* MetricsRegistry::GetHistogram(const std::string& name,
                                         const std::string& description,
                                         std::vector<double> buckets) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto it = metrics_.find(name);
  if (it != metrics_.end()) {
    if (it->second->GetType() == MetricType::kHistogram) {
      return static_cast<Histogram*>(it->second.get());
    }
    // Type mismatch: overwrite with correct type
  }
  auto hist = std::make_unique<Histogram>(name, description, std::move(buckets));
  Histogram* ptr = hist.get();
  metrics_[name] = std::move(hist);
  return ptr;
}

std::string MetricsRegistry::ExportPrometheus() const {
  std::ostringstream oss;
  std::shared_lock<std::shared_mutex> lock(mutex_);
  for (const auto& [name, metric] : metrics_) {
    (void)name;
    oss << metric->ToPrometheusFormat();
  }
  return oss.str();
}

std::string MetricsRegistry::ExportJSON() const {
  std::ostringstream oss;
  oss << "{";
  bool first = true;
  std::shared_lock<std::shared_mutex> lock(mutex_);
  for (const auto& [name, metric] : metrics_) {
    (void)name;
    if (!first) oss << ",";
    first = false;
    oss << "\"" << metric->GetName() << "\":" << 0;
  }
  oss << "}";
  return oss.str();
}

void MetricsRegistry::Clear() {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  metrics_.clear();
  families_.clear();
}

// =============================================================================
// MetricsCollector
// =============================================================================

namespace {

void SimpleHttpServer(int port, std::atomic<bool>* running) {
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) return;
  int opt = 1;
  setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
  // Set accept timeout so the loop can check running flag periodically
  struct timeval tv;
  tv.tv_sec = 1;
  tv.tv_usec = 0;
  setsockopt(server_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(static_cast<uint16_t>(port));
  if (bind(server_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    close(server_fd);
    return;
  }
  if (listen(server_fd, 5) < 0) {
    close(server_fd);
    return;
  }
  while (running->load()) {
    int client_fd = accept(server_fd, nullptr, nullptr);
    if (client_fd < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
        continue;  // Timeout or interrupt, re-check running flag
      }
      break;
    }

    // Parse HTTP request path from the first line
    char buf[1024] = {0};
    recv(client_fd, buf, sizeof(buf) - 1, 0);
    std::string path = "/metrics";
    std::string req(buf);
    size_t space1 = req.find(' ');
    if (space1 != std::string::npos) {
      size_t space2 = req.find(' ', space1 + 1);
      if (space2 != std::string::npos) {
        path = req.substr(space1 + 1, space2 - space1 - 1);
      }
    }

    std::string body;
    std::string content_type = "text/plain; version=0.0.4";
    if (path == "/health") {
      body = "{\"status\":\"healthy\"}\n";
      content_type = "application/json";
    } else if (path == "/ready") {
      body = "{\"status\":\"ready\"}\n";
      content_type = "application/json";
    } else {
      body = MetricsRegistry::Instance().ExportPrometheus();
      if (auto* txn_metrics = cedar::dtx::GetGlobalTransactionMetrics()) {
        body += txn_metrics->ExportPrometheusFormat();
      }
    }

    std::ostringstream response;
    response << "HTTP/1.1 200 OK\r\n";
    response << "Content-Type: " << content_type << "\r\n";
    response << "Content-Length: " << body.size() << "\r\n";
    response << "\r\n";
    response << body;
    std::string resp_str = response.str();
#ifdef MSG_NOSIGNAL
    send(client_fd, resp_str.data(), resp_str.size(), MSG_NOSIGNAL);
#else
    send(client_fd, resp_str.data(), resp_str.size(), 0);
#endif
    close(client_fd);
  }
  close(server_fd);
}

}  // namespace

MetricsCollector::MetricsCollector() = default;

MetricsCollector::~MetricsCollector() {
  Shutdown();
}

Status MetricsCollector::Initialize(const Config& config) {
  if (running_.exchange(true)) {
    return Status::OK();
  }
  config_ = config;
  collection_thread_ = std::make_unique<std::thread>(
      &MetricsCollector::CollectionLoop, this);
  if (config_.enable_http_server) {
    auto status = StartHttpServer();
    if (!status.ok()) {
      // Non-fatal: keep running without HTTP endpoint
    }
  }
  return Status::OK();
}

void MetricsCollector::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }
  if (collection_thread_ && collection_thread_->joinable()) {
    collection_thread_->join();
  }
  if (http_thread_ && http_thread_->joinable()) {
    http_thread_->join();
  }
}

Status MetricsCollector::StartHttpServer() {
  int port = 9090;
  size_t pos = config_.endpoint.find(':');
  if (pos != std::string::npos) {
    try {
      port = std::stoi(config_.endpoint.substr(pos + 1));
    } catch (...) {
      std::cerr << "[MetricsStorage] Failed to parse endpoint port, using default 9090" << std::endl;
      port = 9090;
    }
  }
  http_thread_ = std::make_unique<std::thread>([port, this]() {
    SimpleHttpServer(port, &running_);
  });
  return Status::OK();
}

void MetricsCollector::Collect() {
  CollectSystemMetrics();
  CollectStorageMetrics();
}

void MetricsCollector::RegisterStorageNode(PartitionID pid,
                                           const std::string& node_addr) {
  (void)node_addr;
  std::unique_lock<std::shared_mutex> lock(nodes_mutex_);
  auto metrics = std::make_unique<StorageNodeMetrics>();
  metrics->Initialize("partition_" + std::to_string(pid));
  node_metrics_[pid] = std::move(metrics);
}

void MetricsCollector::UnregisterStorageNode(PartitionID pid) {
  std::unique_lock<std::shared_mutex> lock(nodes_mutex_);
  node_metrics_.erase(pid);
}

StorageNodeMetrics* MetricsCollector::GetNodeMetrics(PartitionID pid) {
  std::shared_lock<std::shared_mutex> lock(nodes_mutex_);
  auto it = node_metrics_.find(pid);
  if (it != node_metrics_.end()) {
    return it->second.get();
  }
  return nullptr;
}

void MetricsCollector::CollectionLoop() {
  while (running_.load()) {
    Collect();
    std::this_thread::sleep_for(config_.collection_interval);
  }
}

void MetricsCollector::CollectSystemMetrics() {
  auto* registry = &MetricsRegistry::Instance();

  // Process memory (RSS)
  size_t rss_bytes = 0;
#if defined(__APPLE__)
  struct task_basic_info info;
  mach_msg_type_number_t size = TASK_BASIC_INFO_COUNT;
  if (task_info(mach_task_self(), TASK_BASIC_INFO,
                reinterpret_cast<task_info_t>(&info), &size) == KERN_SUCCESS) {
    rss_bytes = info.resident_size;
  }
#else
  struct rusage usage;
  if (getrusage(RUSAGE_SELF, &usage) == 0) {
    rss_bytes = static_cast<size_t>(usage.ru_maxrss) * 1024;
  }
#endif

  auto* mem_gauge = registry->GetGauge("cedar_process_memory_bytes",
                                       "Process resident memory in bytes");
  mem_gauge->Set(static_cast<double>(rss_bytes));

  // CPU cores
  int ncpus = 1;
#if defined(__APPLE__)
  size_t len = sizeof(ncpus);
  sysctlbyname("hw.ncpu", &ncpus, &len, nullptr, 0);
#else
  ncpus = sysconf(_SC_NPROCESSORS_ONLN);
  if (ncpus < 1) ncpus = 1;
#endif
  auto* cpu_gauge = registry->GetGauge("cedar_cpu_cores_total",
                                         "Number of CPU cores available");
  cpu_gauge->Set(static_cast<double>(ncpus));

  // Disk usage (root filesystem)
  struct statfs buf;
  if (statfs(".", &buf) == 0) {
    double total = static_cast<double>(buf.f_blocks) * buf.f_bsize;
    double free = static_cast<double>(buf.f_bfree) * buf.f_bsize;
    auto* disk_total = registry->GetGauge("cedar_disk_total_bytes",
                                          "Total disk space in bytes");
    disk_total->Set(total);
    auto* disk_free = registry->GetGauge("cedar_disk_free_bytes",
                                         "Free disk space in bytes");
    disk_free->Set(free);
  }
}

void MetricsCollector::CollectStorageMetrics() {
  auto* registry = &MetricsRegistry::Instance();
  std::shared_lock<std::shared_mutex> lock(nodes_mutex_);

  for (const auto& [pid, metrics] : node_metrics_) {
    (void)pid;
    if (!metrics) continue;
    // Export per-node counters/gauges into the global registry for scraping
    if (metrics->put_ops_total) {
      registry->GetCounter("cedar_storage_put_ops_total", "Total put operations")
          ->Increment(metrics->put_ops_total->GetValue());
    }
    if (metrics->get_ops_total) {
      registry->GetCounter("cedar_storage_get_ops_total", "Total get operations")
          ->Increment(metrics->get_ops_total->GetValue());
    }
    if (metrics->storage_size_bytes) {
      registry->GetGauge("cedar_storage_size_bytes", "Storage size in bytes")
          ->Set(metrics->storage_size_bytes->GetValue());
    }
    if (metrics->storage_keys_total) {
      registry->GetGauge("cedar_storage_keys_total", "Total number of keys")
          ->Set(metrics->storage_keys_total->GetValue());
    }
  }
}

// =============================================================================
// Default Alert Rules
// =============================================================================

namespace alerts {

std::vector<AlertRule> GetDefaultAlertRules() {
  std::vector<AlertRule> rules;
  return rules;
}

}  // namespace alerts

}  // namespace storage
}  // namespace dtx
}  // namespace cedar
