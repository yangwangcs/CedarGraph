// tools/benchmark_metrics.cc
#include "benchmark_metrics.h"
#include <iostream>
#include <iomanip>
#include <fstream>
#include <filesystem>
#include <algorithm>

namespace cedar {
namespace benchmark {

void OperationMetrics::Record(uint64_t latency_us, uint64_t bytes, bool success) {
  count.fetch_add(1, std::memory_order_relaxed);
  total_latency_us.fetch_add(latency_us, std::memory_order_relaxed);
  bytes_processed.fetch_add(bytes, std::memory_order_relaxed);
  
  if (!success) {
    errors.fetch_add(1, std::memory_order_relaxed);
  }
  
  // Update min/max
  uint64_t current_min = min_latency_us.load();
  while (latency_us < current_min && 
         !min_latency_us.compare_exchange_weak(current_min, latency_us)) {}
  
  uint64_t current_max = max_latency_us.load();
  while (latency_us > current_max && 
         !max_latency_us.compare_exchange_weak(current_max, latency_us)) {}
}

double OperationMetrics::GetAvgLatency() const {
  uint64_t c = count.load();
  return c > 0 ? static_cast<double>(total_latency_us.load()) / c : 0.0;
}

double OperationMetrics::GetThroughput() const {
  double total_latency_sec = total_latency_us.load() / 1e6;
  return total_latency_sec > 0 ? count.load() / total_latency_sec : 0.0;
}

void DiskUsageMetrics::Measure(const std::string& data_dir) {
  namespace fs = std::filesystem;
  
  total_data_size_bytes = 0;
  sst_files_size_bytes = 0;
  wal_size_bytes = 0;
  blob_storage_size_bytes = 0;
  num_sst_files = 0;
  
  if (!fs::exists(data_dir)) return;
  
  for (const auto& entry : fs::recursive_directory_iterator(data_dir)) {
    if (!entry.is_regular_file()) continue;
    
    auto size = entry.file_size();
    total_data_size_bytes += size;
    
    auto filename = entry.path().filename().string();
    if (filename.size() >= 4 && filename.substr(filename.size() - 4) == ".sst") {
      sst_files_size_bytes += size;
      num_sst_files++;
    } else if ((filename.size() >= 4 && filename.substr(filename.size() - 4) == ".wal") ||
               (filename.size() >= 3 && filename.substr(0, 3) == "wal")) {
      wal_size_bytes += size;
    } else if (filename.find("blob") != std::string::npos) {
      blob_storage_size_bytes += size;
    }
  }
  
  // Calculate amplification
  if (sst_files_size_bytes > 0) {
    amplification_ratio = static_cast<double>(total_data_size_bytes) / 
                          (total_data_size_bytes - wal_size_bytes + 1);
  }
}

void DiskUsageMetrics::Print() const {
  std::cout << "\n📊 Disk Usage Metrics:\n";
  std::cout << "  Total Data Size:      " << std::setw(10) << (total_data_size_bytes / 1024 / 1024) << " MB\n";
  std::cout << "  SST Files:            " << std::setw(10) << (sst_files_size_bytes / 1024 / 1024) << " MB (" << num_sst_files << " files)\n";
  std::cout << "  WAL Files:            " << std::setw(10) << (wal_size_bytes / 1024 / 1024) << " MB\n";
  std::cout << "  Blob Storage:         " << std::setw(10) << (blob_storage_size_bytes / 1024 / 1024) << " MB\n";
  std::cout << "  Write Amplification:  " << std::setw(10) << std::fixed << std::setprecision(2) << amplification_ratio << "x\n";
}

void BenchmarkReport::PrintConsole() const {
  std::cout << "\n" << std::string(80, '=') << "\n";
  std::cout << "🚀 CedarGraph 3-Node Cluster Performance Report\n";
  std::cout << std::string(80, '=') << "\n";
  std::cout << "Cluster: " << cluster_config << "\n";
  std::cout << "Test:    " << benchmark_name << "\n\n";
  
  // Write Performance
  std::cout << "✏️  Write Performance:\n";
  std::cout << "   Operations:  " << write_metrics.count.load() << "\n";
  std::cout << "   Avg Latency: " << std::fixed << std::setprecision(2) 
            << write_metrics.GetAvgLatency() << " μs\n";
  std::cout << "   Throughput:  " << std::fixed << std::setprecision(1) 
            << write_metrics.GetThroughput() << " ops/sec\n";
  
  // Read Performance
  std::cout << "\n📖 Read Performance:\n";
  std::cout << "   Operations:  " << read_metrics.count.load() << "\n";
  std::cout << "   Avg Latency: " << std::fixed << std::setprecision(2) 
            << read_metrics.GetAvgLatency() << " μs\n";
  std::cout << "   Throughput:  " << std::fixed << std::setprecision(1) 
            << read_metrics.GetThroughput() << " ops/sec\n";
  
  // Latency Percentiles
  std::cout << "\n⏱️  Latency Distribution:\n";
  std::cout << "   P50:  " << p50_latency_ms << " ms\n";
  std::cout << "   P99:  " << p99_latency_ms << " ms\n";
  std::cout << "   P999: " << p999_latency_ms << " ms\n";
  
  // Disk Usage
  disk_metrics.Print();
  
  std::cout << "\n" << std::string(80, '=') << "\n";
}

void BenchmarkReport::SaveToJson(const std::string& filename) const {
  std::ofstream ofs(filename);
  if (!ofs.is_open()) return;
  
  ofs << "{\n";
  ofs << "  \"benchmark\": \"" << benchmark_name << "\",\n";
  ofs << "  \"cluster\": \"" << cluster_config << "\",\n";
  
  // Write metrics
  ofs << "  \"write\": {\n";
  ofs << "    \"count\": " << write_metrics.count.load() << ",\n";
  ofs << "    \"avg_latency_us\": " << write_metrics.GetAvgLatency() << ",\n";
  ofs << "    \"throughput_ops\": " << write_metrics.GetThroughput() << "\n";
  ofs << "  },\n";
  
  // Read metrics
  ofs << "  \"read\": {\n";
  ofs << "    \"count\": " << read_metrics.count.load() << ",\n";
  ofs << "    \"avg_latency_us\": " << read_metrics.GetAvgLatency() << ",\n";
  ofs << "    \"throughput_ops\": " << read_metrics.GetThroughput() << "\n";
  ofs << "  },\n";
  
  // Disk metrics
  ofs << "  \"disk\": {\n";
  ofs << "    \"total_mb\": " << (disk_metrics.total_data_size_bytes / 1024 / 1024) << ",\n";
  ofs << "    \"sst_mb\": " << (disk_metrics.sst_files_size_bytes / 1024 / 1024) << ",\n";
  ofs << "    \"sst_files\": " << disk_metrics.num_sst_files << ",\n";
  ofs << "    \"amplification\": " << disk_metrics.amplification_ratio << "\n";
  ofs << "  }\n";
  
  ofs << "}\n";
}

void BenchmarkReport::SaveToCsv(const std::string& filename) const {
  std::ofstream ofs(filename);
  if (!ofs.is_open()) return;
  
  // CSV Header
  ofs << "benchmark,cluster,write_count,write_avg_latency_us,write_throughput,"
      << "read_count,read_avg_latency_us,read_throughput,"
      << "disk_total_mb,disk_sst_mb,dist_sst_files,disk_amplification\n";
  
  // CSV Data
  ofs << benchmark_name << ","
      << cluster_config << ","
      << write_metrics.count.load() << ","
      << write_metrics.GetAvgLatency() << ","
      << write_metrics.GetThroughput() << ","
      << read_metrics.count.load() << ","
      << read_metrics.GetAvgLatency() << ","
      << read_metrics.GetThroughput() << ","
      << (disk_metrics.total_data_size_bytes / 1024 / 1024) << ","
      << (disk_metrics.sst_files_size_bytes / 1024 / 1024) << ","
      << disk_metrics.num_sst_files << ","
      << disk_metrics.amplification_ratio << "\n";
}

}  // namespace benchmark
}  // namespace cedar
