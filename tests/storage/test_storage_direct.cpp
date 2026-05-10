// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// 直接存储测试 - 测试数据分布和落盘

#include <iostream>
#include <random>
#include <chrono>
#include <thread>
#include <vector>
#include <sstream>
#include <iomanip>
#include <fstream>
#include <filesystem>

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

using namespace cedar;

class DirectStorageTest {
 public:
  DirectStorageTest(const std::string& data_dir)
      : data_dir_(data_dir),
        rng_(std::random_device{}()),
        dist_(1, 1000000) {
    std::filesystem::create_directories(data_dir);
  }

  ~DirectStorageTest() {
    if (storage_) {
      delete storage_;
      storage_ = nullptr;
    }
  }

  // 初始化存储
  bool Initialize() {
    std::cout << "=== 初始化存储 ===" << std::endl;
    std::cout << "数据目录: " << data_dir_ << std::endl;

    CedarOptions options;
    options.create_if_missing = true;
    options.error_if_exists = false;

    auto status = CedarGraphStorage::Open(options, data_dir_, &storage_);
    if (!status.ok()) {
      std::cerr << "打开存储失败: " << status.ToString() << std::endl;
      return false;
    }

    std::cout << "存储初始化成功" << std::endl;
    return true;
  }

  // 执行写入测试 - 测试数据分布
  bool RunWriteTest(int num_writes) {
    std::cout << "\n=== 分布式写入测试 ===" << std::endl;
    std::cout << "写入数量: " << num_writes << std::endl;

    int success_count = 0;
    int fail_count = 0;

    // 记录每个分区的写入数量（模拟分区）
    std::map<int, int> partition_distribution;
    const int num_partitions = 4;  // 模拟4个分区

    auto start = std::chrono::steady_clock::now();

    for (int i = 0; i < num_writes; i++) {
      uint64_t entity_id = dist_(rng_);
      
      // 计算分区（模拟分区逻辑）
      int partition = entity_id % num_partitions;
      
      if (WriteData(entity_id, i)) {
        success_count++;
        partition_distribution[partition]++;
        written_ids_.push_back(entity_id);  // 记录写入的ID
      } else {
        fail_count++;
      }

      if ((i + 1) % 100 == 0) {
        std::cout << "进度: " << (i + 1) << "/" << num_writes 
                  << " (成功: " << success_count << ", 失败: " << fail_count << ")" << std::endl;
      }
    }

    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "\n写入完成:" << std::endl;
    std::cout << "  总时间: " << duration.count() << " ms" << std::endl;
    std::cout << "  成功: " << success_count << std::endl;
    std::cout << "  失败: " << fail_count << std::endl;
    std::cout << "  QPS: " << (num_writes * 1000 / (duration.count() + 1)) << std::endl;

    // 显示分布情况
    std::cout << "\n数据分布情况:" << std::endl;
    for (const auto& [partition, count] : partition_distribution) {
      double percentage = (100.0 * count) / success_count;
      std::cout << "  分区 " << partition << ": " << count 
                << " (" << std::fixed << std::setprecision(1) << percentage << "%)" << std::endl;
    }

    // 检查分布均匀性
    double expected = static_cast<double>(success_count) / num_partitions;
    bool uniform = true;
    for (const auto& [partition, count] : partition_distribution) {
      double deviation = std::abs(count - expected) / expected;
      if (deviation > 0.3) {  // 允许30%的偏差
        uniform = false;
        std::cout << "  警告: 分区 " << partition << " 分布不均匀 (偏差: " 
                  << (deviation * 100) << "%)" << std::endl;
      }
    }
    if (uniform) {
      std::cout << "  ✓ 数据分布均匀" << std::endl;
    }

    return fail_count == 0;
  }

  // 检查数据落盘
  bool CheckDataPersistence() {
    std::cout << "\n=== 检查数据落盘 ===" << std::endl;

    // 检查数据目录中的所有文件（包括子目录）
    int file_count = 0;
    size_t total_size = 0;
    std::map<std::string, int> file_types;

    for (const auto& entry : std::filesystem::recursive_directory_iterator(data_dir_)) {
      if (entry.is_regular_file()) {
        file_count++;
        total_size += entry.file_size();
        
        // 统计文件类型
        std::string ext = entry.path().extension().string();
        if (ext.empty()) {
          ext = "(no ext)";
        }
        file_types[ext]++;
      }
    }

    std::cout << "数据文件数量: " << file_count << std::endl;
    std::cout << "总数据大小: " << (total_size / 1024) << " KB" << std::endl;
    
    std::cout << "文件类型分布:" << std::endl;
    for (const auto& [ext, count] : file_types) {
      std::cout << "  " << ext << ": " << count << std::endl;
    }

    if (file_count == 0) {
      std::cout << "  ✗ 未找到数据文件" << std::endl;
      return false;
    }

    std::cout << "  ✓ 数据已落盘" << std::endl;
    return true;
  }

  // 读取测试 - 验证数据完整性
  bool RunReadTest(int num_reads) {
    std::cout << "\n=== 读取测试 ===" << std::endl;
    std::cout << "读取数量: " << num_reads << std::endl;

    int found_count = 0;
    int not_found_count = 0;

    // 使用已写入的 ID 进行读取测试
    for (size_t i = 0; i < std::min(static_cast<size_t>(num_reads), written_ids_.size()); i++) {
      uint64_t entity_id = written_ids_[i];
      
      if (ReadData(entity_id)) {
        found_count++;
      } else {
        not_found_count++;
      }
    }
    
    // 如果没有写入任何 ID，使用随机读取
    if (written_ids_.empty()) {
      for (int i = 0; i < num_reads; i++) {
        uint64_t entity_id = dist_(rng_);
        if (ReadData(entity_id)) {
          found_count++;
        } else {
          not_found_count++;
        }
      }
    }

    std::cout << "读取结果:" << std::endl;
    std::cout << "  测试ID数: " << std::min(static_cast<size_t>(num_reads), written_ids_.size()) << std::endl;
    std::cout << "  找到: " << found_count << std::endl;
    std::cout << "  未找到: " << not_found_count << std::endl;
    if (found_count + not_found_count > 0) {
      std::cout << "  命中率: " << std::fixed << std::setprecision(1) 
                << (100.0 * found_count / (found_count + not_found_count)) << "%" << std::endl;
    }

    return found_count > 0;
  }

  // 检查存储统计
  bool CheckStorageStats() {
    std::cout << "\n=== 存储统计 ===" << std::endl;

    // 获取存储统计信息
    auto stats = storage_->GetStats();
    std::cout << "存储统计:" << std::endl;
    std::cout << "  MemTable大小: " << (stats.memtable_size / 1024) << " KB" << std::endl;
    std::cout << "  Immutable MemTable: " << (stats.imm_memtable_size / 1024) << " KB" << std::endl;
    std::cout << "  SST文件数: " << stats.sst_count << std::endl;
    std::cout << "  SST大小: " << (stats.sst_size / 1024) << " KB" << std::endl;
    std::cout << "  LSM层级: " << stats.num_levels << std::endl;

    return true;
  }

  // 强制刷盘并检查
  bool ForceFlushAndCheck() {
    std::cout << "\n=== 强制刷盘 ===" << std::endl;

    storage_->ForceFlush();
    std::cout << "  ✓ 强制刷盘成功" << std::endl;

    // 重新检查文件
    return CheckDataPersistence();
  }

 private:
  // 写入数据
  bool WriteData(uint64_t entity_id, int seq) {
    Timestamp now = Timestamp::Now();
    
    // 创建描述符 - 使用 InlineInt 类型存储序列号
    Descriptor desc(EntryKind::InlineInt, 1, seq, sizeof(seq));
    
    auto status = storage_->Put(entity_id, now.value(), desc, now);
    if (!status.ok()) {
      std::cerr << "Put failed: " << status.ToString() << std::endl;
    }
    return status.ok();
  }

  // 读取数据
  bool ReadData(uint64_t entity_id) {
    // 使用 Max 时间戳读取最新版本
    auto result = storage_->Get(entity_id, std::numeric_limits<uint64_t>::max());
    bool found = result.has_value();
    if (!found) {
      // 调试输出
      // std::cerr << "Not found: entity_id=" << entity_id << std::endl;
    }
    return found;
  }

  std::string data_dir_;
  CedarGraphStorage* storage_ = nullptr;
  std::mt19937 rng_;
  std::uniform_int_distribution<uint64_t> dist_;
  std::vector<uint64_t> written_ids_;  // 记录写入的ID
};

int main(int argc, char** argv) {
  std::string data_dir = "/tmp/cedar_storage_test";
  if (argc > 1) {
    data_dir = argv[1];
  }

  std::cout << "========================================" << std::endl;
  std::cout << "CedarGraph 直接存储测试" << std::endl;
  std::cout << "========================================" << std::endl;

  DirectStorageTest test(data_dir);

  // 初始化
  if (!test.Initialize()) {
    return 1;
  }

  // 运行写入测试
  bool write_ok = test.RunWriteTest(1000);

  // 检查数据落盘
  test.CheckDataPersistence();

  // 强制刷盘
  test.ForceFlushAndCheck();

  // 检查存储统计
  test.CheckStorageStats();

  // 运行读取测试
  test.RunReadTest(100);

  std::cout << "\n========================================" << std::endl;
  std::cout << "测试完成" << std::endl;
  std::cout << "========================================" << std::endl;

  return write_ok ? 0 : 1;
}
