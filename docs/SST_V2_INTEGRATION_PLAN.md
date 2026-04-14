# SST V2 集成实施计划

## 概述
将 Zone-Columnar SST V2（生产级设计）集成到 CedarGraph 存储引擎中。

## 关键改进

| 组件 | V1 | V2 | 收益 |
|------|-----|-----|------|
| Block 大小 | 64KB | 256KB | 压缩效率↑ |
| SST 大小 | ~350KB | 8-64MB | 文件数量↓ |
| 索引级别 | 行级 (12B/row) | Block级 (48B/block) | 内存节省 99% |
| 索引内存 | 12MB/百万行 | 9KB/百万行 | 可常驻内存 |

---

## 实施步骤

### Phase 1: Builder 替换

```cpp
// src/sst/sst_builder_factory.h
#pragma once

#include "cedar/sst/zone_columnar_builder.h"
#include "cedar/sst/zone_columnar_builder_v2.h"

namespace cedar {

// Builder 版本选择
enum class SSTVersion { V1, V2 };

class SstBuilderFactory {
 public:
  static std::unique_ptr<SstBuilderInterface> Create(
      SSTVersion version,
      const std::string& filepath,
      const ZoneColumnarSstBuilderV2::Options& options);
};

}  // namespace cedar
```

### Phase 2: LsmEngine 集成点

```cpp
// src/storage/lsm_engine.cc 修改点

// 1. FlushEntityGroup 使用 V2 Builder
Status LsmEngine::FlushEntityGroupV2(...) {
  ZoneColumnarSstBuilderV2::Options options;
  options.target_block_size = 256 * 1024;      // 256KB
  options.target_sst_size = 64 * 1024 * 1024;  // 64MB
  
  ZoneColumnarSstBuilderV2 builder(options, file);
  // ... 添加数据
  return builder.Finish();
}

// 2. 添加配置选项
CedarOptions::Builder& CedarOptions::Builder::UseSSTV2() {
  options_.sst_version = SSTVersion::V2;
  options_.size_tiered_config.l0_max_size = 64 * 1024 * 1024;      // 64MB
  options_.size_tiered_config.target_file_size = 64 * 1024 * 1024; // 64MB
  return *this;
}
```

### Phase 3: Reader 适配

```cpp
// src/sst/zone_columnar_reader_v2.h
// - 支持读取 V2 格式
// - Block Index 常驻内存
// - Block Cache 按需加载
```

### Phase 4: Compaction 调优

```cpp
// 大 SST 文件的 Compaction 策略
SizeTieredConfig config;
config.l0_max_size = 64 * 1024 * 1024;        // L0: 64MB
config.l0_max_files = 8;                       // L0 最多 8 个文件触发合并
config.size_ratio = 4;                         // 每层 4 倍
config.target_file_size = 64 * 1024 * 1024;    // 目标文件 64MB
config.max_merge_width = 4;                    // 单次合并最多 4 个文件
```

---

## 配置指南

### 方案 A：平滑迁移（推荐）
```cpp
// 新数据用 V2，旧数据保持 V1
CedarOptions options;
options.sst_version = SSTVersion::V2;  // 只影响新写入
// Reader 自动识别版本
```

### 方案 B：完全切换
```cpp
// 导出 → 导入 迁移
// 1. 导出旧数据
// 2. 清空目录
// 3. 使用 V2 重新导入
```

---

## 性能预期

| 指标 | V1 | V2 | 提升 |
|------|-----|-----|------|
| 索引内存 | 12MB/百万行 | 9KB/百万行 | 99.9%↓ |
| 文件数量 | 多小文件 | 少大文件 | 10x↓ |
| 查询 I/O | 随机读多文件 | 顺序读大文件 | 5x↑ |
| Compaction | 频繁小合并 | 少量大合并 | 吞吐↑ |

---

## 回滚策略

```cpp
// 如果 V2 有问题，快速回滚
CedarOptions options;
options.sst_version = SSTVersion::V1;  // 回退到 V1
```
