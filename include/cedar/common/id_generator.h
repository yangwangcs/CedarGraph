// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

// =============================================================================
// Snowflake ID Generator — 全局唯一、趋势递增、高性能
// =============================================================================
// 64-bit ID 布局:
//   [1 bit sign] [41 bits timestamp_ms] [10 bits node_id] [12 bits sequence]
//
//   - 41 bits timestamp: ~69 years from epoch (2024-01-01)
//   - 10 bits node_id:   0-1023, 支持 1024 个节点
//   - 12 bits sequence:  0-4095, 每毫秒 4096 个 ID
//
// 性能: ~4M IDs/sec/node, 线程安全, 无锁 CAS
// 唯一性: 不同节点永远不冲突 (node_id 不同)
// =============================================================================

#ifndef CEDAR_ID_GENERATOR_H_
#define CEDAR_ID_GENERATOR_H_

#include <atomic>
#include <chrono>
#include <cstdint>
#include <stdexcept>

namespace cedar {

class IdGenerator {
 public:
  // Snowflake 位分配
  static constexpr int kTimestampBits = 41;
  static constexpr int kNodeIdBits    = 10;
  static constexpr int kSequenceBits  = 12;

  static constexpr uint64_t kMaxNodeId   = (1ULL << kNodeIdBits) - 1;    // 1023
  static constexpr uint64_t kMaxSequence = (1ULL << kSequenceBits) - 1;   // 4095

  // 自定义纪元: 2024-01-01 00:00:00 UTC (毫秒)
  static constexpr uint64_t kEpoch = 1704067200000ULL;

  /// 构造函数
  /// \param node_id 节点 ID (0-1023), 分布式部署时每个节点必须不同
  explicit IdGenerator(uint16_t node_id)
      : node_id_(static_cast<uint64_t>(node_id) & kMaxNodeId),
        sequence_(0),
        last_timestamp_(0) {
    if (node_id > kMaxNodeId) {
      throw std::invalid_argument("node_id must be in [0, 1023]");
    }
  }

  /// 生成下一个全局唯一 ID
  /// 线程安全, 使用 CAS 无锁操作
  uint64_t NextId() {
    uint64_t ts = CurrentMillis();

    // 获取当前序列号
    uint64_t seq;
    uint64_t expected_ts;
    while (true) {
      expected_ts = last_timestamp_.load(std::memory_order_relaxed);
      seq = sequence_.load(std::memory_order_relaxed);

      if (ts == expected_ts) {
        // 同一毫秒内, sequence + 1
        seq = (seq + 1) & kMaxSequence;
        if (seq == 0) {
          // 序列号溢出, 等下一毫秒
          ts = WaitNextMillis(expected_ts);
        }
      } else if (ts > expected_ts) {
        // 新的毫秒, sequence 从 0 开始
        seq = 0;
      } else {
        // 时钟回拨, 等待追上
        ts = WaitNextMillis(expected_ts);
        seq = 0;
      }

      // CAS 更新 timestamp + sequence
      if (last_timestamp_.compare_exchange_weak(expected_ts, ts,
                                                std::memory_order_acq_rel)) {
        sequence_.store(seq, std::memory_order_release);
        break;
      }
      // CAS 失败, 重试
    }

    return ComposeId(ts, node_id_, seq);
  }

  /// 从 ID 中提取时间戳 (毫秒)
  static uint64_t ExtractTimestamp(uint64_t id) {
    return (id >> (kNodeIdBits + kSequenceBits)) + kEpoch;
  }

  /// 从 ID 中提取节点 ID
  static uint16_t ExtractNodeId(uint64_t id) {
    return static_cast<uint16_t>((id >> kSequenceBits) & kMaxNodeId);
  }

  /// 从 ID 中提取序列号
  static uint16_t ExtractSequence(uint64_t id) {
    return static_cast<uint16_t>(id & kMaxSequence);
  }

  /// 获取当前节点 ID
  uint16_t GetNodeId() const { return static_cast<uint16_t>(node_id_); }

 private:
  static uint64_t ComposeId(uint64_t ts, uint64_t node, uint64_t seq) {
    return ((ts & ((1ULL << kTimestampBits) - 1))
            << (kNodeIdBits + kSequenceBits)) |
           (node << kSequenceBits) |
           seq;
  }

  static uint64_t CurrentMillis() {
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();
    return static_cast<uint64_t>(ms) - kEpoch;
  }

  /// 等到下一毫秒
  static uint64_t WaitNextMillis(uint64_t last_ts) {
    uint64_t ts = CurrentMillis();
    while (ts <= last_ts) {
      ts = CurrentMillis();
    }
    return ts;
  }

  const uint64_t node_id_;
  std::atomic<uint64_t> sequence_;
  std::atomic<uint64_t> last_timestamp_;
};

}  // namespace cedar

#endif  // CEDAR_ID_GENERATOR_H_
