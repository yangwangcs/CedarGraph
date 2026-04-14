# CedarGraph-DTx 使用指南

## 快速开始

### 1. 基础概念

CedarGraph-DTx 通过五个创新技术实现高性能分布式事务：

| 技术 | 解决的问题 | 核心优势 |
|------|-----------|---------|
| **GLTR** | 事务路由 | 90%+ 事务无需跨分区协调 |
| **TW-CD** | 冲突检测 | 时序解耦减少 40-60% 假阳性冲突 |
| **LND-OCC** | 提交协议 | 三层策略，单层事务零开销 |
| **DVC-Val** | 版本验证 | O(1) 快速验证 |
| **BBCC** | 因果一致性 | 轻量级书签，-90% 空间开销 |

### 2. 基本使用模式

```cpp
#include "cedar/dtx/partition.h"
#include "cedar/dtx/lsm_native_occ.h"
#include "cedar/dtx/bookmark_manager.h"

using namespace cedar::dtx;

// 初始化组件
PartitionManager partition_mgr(256);  // 256 个分区
LndOccEngine lnd_engine;
BookmarkManager bookmark_mgr(shard_id);
```

## 详细用法

### GLTR - 分区路由

```cpp
// 创建带分区 ID 的 Key
CedarKey key1 = CreateKeyWithPartition(entity_id=100, column=1, partition=0);
CedarKey key2 = CreateKeyWithPartition(entity_id=200, column=1, partition=1);

// 检查事务是否需要协调
std::vector<CedarKey> keys = {key1, key2};
bool needs_coord = partition_mgr.NeedsCoordination(keys);
// returns: true (跨越多个分区)

// 检查本地事务
bool is_local = partition_mgr.IsLocalTransaction(0, {key1});
// returns: true (key1 在分区 0)
```

**关键优势**：单分区事务完全避免 RPC 开销

---

### TW-CD - 时序窗口冲突检测

```cpp
TwcdEngine engine;

// 事务 T1: 读取 2024 年数据
TemporalWindow window1(
    Timestamp(1704067200000000ULL),  // 2024-01-01
    Timestamp(1735689600000000ULL),  // 2024-12-31
    cedar::TemporalWindowType::SNAPSHOT
);

engine.RegisterTransaction(1, window1, read_set, {});

// 事务 T2: 写入 2025 年数据 (相同 key，不同时序)
TemporalWindow window2(
    Timestamp(1735689600000001ULL),  // 2025-01-01
    Timestamp(1767225600000000ULL)   // 2025-12-31
);

auto result = engine.CheckConflict(2, window2, {}, write_set);
// result.IsValid() == true (无冲突！时序不重叠)
```

**关键优势**：时序不重叠的事务即使操作相同 key 也不会冲突

---

### LND-OCC - 三层提交

```cpp
// 准备事务上下文
DistributedTxnContext ctx;
ctx.txn_id = 100;
ctx.read_set = {...};
ctx.write_set = {...};
ctx.temporal_range = {start_ts, end_ts};

// 自动分类
TxnType type = lnd_engine.ClassifyTransaction(&ctx);

switch (type) {
    case TxnType::kSinglePartition:
        // Layer 1: 单分区，零协调开销
        result = lnd_engine.SinglePartitionCommit(&ctx);
        break;
        
    case TxnType::kSameTemporalRange:
        // Layer 2: 同时序范围，轻量验证
        result = lnd_engine.SameTemporalRangeCommit(&ctx);
        break;
        
    case TxnType::kCrossTemporalRange:
        // Layer 3: 跨时序范围，完整 2PC
        result = lnd_engine.FullTwoPhaseCommit(&ctx);
        break;
}
```

**三层策略对比**：

| 层级 | 场景 | 协调开销 | 延迟 |
|------|------|---------|------|
| Layer 1 | 单分区 | 0 RPC | < 1ms |
| Layer 2 | 跨分区+同时序 | 1 RPC | 1-5ms |
| Layer 3 | 跨分区+跨时序 | 2PC | 5-20ms |

---

### DVC-Val - 版本链验证

```cpp
VersionChainIndex index(partition_id);

// 获取或创建版本链头
auto* head = index.GetOrCreateHead(key);

// O(1) 快速验证
bool valid = head->FastValidate(read_version, commit_ts);

// 批量验证
std::vector<std::pair<CedarKey, uint64_t>> read_set = {
    {key1, version1},
    {key2, version2},
};
auto results = index.BatchValidate(read_set, commit_ts);
```

**验证规则**：
- `read_version == latest_version` → 有效
- `commit_ts > latest_commit_ts` → 有效
- 否则 → 需要遍历版本链

---

### BBCC - 因果一致性

```cpp
BookmarkManager bookmark_mgr(shard_id);

// 获取当前 HLC
HybridLogicalClock hlc = bookmark_mgr.GetCurrentHLC();

// 更新 HLC (收到其他节点的时钟)
HybridLogicalClock remote_hlc = ReceiveFromRemote();
bookmark_mgr.UpdateHLC(remote_hlc);

// 创建书签
DistributedBookmark bookmark = bookmark_mgr.CreateBookmark();

// 因果检查
CausalConsistencyChecker checker;
bool ryw = checker.CheckReadYourWrites(prev_bookmark, current_bookmark);
bool monotonic = checker.CheckMonotonicReads(prev_bookmark, current_bookmark);

// 会话管理
bookmark_mgr.UpdateSessionBookmark(session_id, bookmark);
auto session_bookmark = bookmark_mgr.GetSessionBookmark(session_id);
```

**支持的一致性保证**：
- **RYW** (Read Your Writes): 读到自己写的数据
- **Monotonic Reads**: 单调读
- **Monotonic Writes**: 单调写
- **WFR** (Writes Follow Reads): 写跟随读

---

## 完整事务示例

```cpp
class DistributedTransactionExample {
public:
    void ExecuteTransaction(const std::vector<CedarKey>& reads,
                           const std::vector<CedarKey>& writes) {
        // 1. 创建事务上下文
        DistributedTxnContext ctx;
        ctx.txn_id = GenerateTxnID();
        ctx.read_set = reads;
        ctx.write_set = writes;
        ctx.start_time = Now();
        
        // 2. 检查是否需要协调 (GLTR)
        if (!partition_mgr_.NeedsCoordination(ctx.AllKeys())) {
            // 单分区事务 - 零开销！
            return ExecuteLocalTransaction(ctx);
        }
        
        // 3. TW-CD 冲突检测
        auto conflict = twcd_.CheckConflict(ctx.txn_id, window, reads, writes);
        if (!conflict.IsValid()) {
            return Abort(ctx, "TW-CD conflict");
        }
        
        // 4. LND-OCC 分层提交
        TxnType type = lnd_.ClassifyTransaction(&ctx);
        LndOccCommitResult result;
        
        switch (type) {
            case TxnType::kSinglePartition:
                result = lnd_.SinglePartitionCommit(&ctx);
                break;
            case TxnType::kSameTemporalRange:
                result = lnd_.SameTemporalRangeCommit(&ctx);
                break;
            case TxnType::kCrossTemporalRange:
                result = lnd_.FullTwoPhaseCommit(&ctx);
                break;
        }
        
        // 5. 创建书签 (BBCC)
        if (result.success) {
            auto bookmark = bookmark_mgr_.CreateBookmark();
            bookmark_mgr_.UpdateSessionBookmark(session_id_, bookmark);
        }
        
        return result;
    }

private:
    PartitionManager partition_mgr_{256};
    TwcdEngine twcd_;
    LndOccEngine lnd_;
    BookmarkManager bookmark_mgr_{0};
    std::string session_id_;
};
```

---

## 配置调优

```cpp
DTxConfig config;

// 超时配置
config.prepare_timeout = std::chrono::milliseconds(100);
config.commit_timeout = std::chrono::milliseconds(1000);

// TW-CD
config.enable_twcd = true;
config.default_temporal_window_us = 3600 * 1000000ULL;  // 1小时

// 版本链
config.max_version_chain_length = 100;
config.gc_interval_ms = 1000;

// 重试
config.max_retry_count = 3;
```

---

## 性能对比

### 与传统 2PC 对比

| 场景 | 传统 2PC | CedarGraph-DTx | 提升 |
|------|---------|----------------|------|
| 单分区事务 | 2 RTT | 0 RTT | **∞** |
| 同时序跨分区 | 4 RTT | 1 RTT | **4x** |
| 跨时序跨分区 | 4 RTT | 4 RTT | 持平 |
| 假阳性冲突 | 高 | 低 40-60% | - |

### 与向量时钟对比 (因果一致性)

| 系统 | 书签大小 | 传播开销 |
|------|---------|---------|
| Vector Clock | 500+ bytes | 高 |
| BBCC | ~50 bytes | **10x** 更低 |

---

## 故障排查

### 常见问题

1. **事务频繁回滚**
   - 检查 TW-CD 窗口设置是否过小
   - 增加 `default_temporal_window_us`

2. **验证失败率高**
   - 启用 DVC-Val 的详细日志
   - 检查 `max_version_chain_length` 是否足够

3. **HLC 回退警告**
   - 确保 NTP 同步
   - 调整时钟漂移容忍度

### 监控指标

```cpp
// 统计信息
BookmarkManager::Stats stats = bookmark_mgr.GetStats();
std::cout << "Sessions: " << stats.session_count << "\n";
std::cout << "HLC updates: " << stats.hlc_update_count << "\n";

// LND-OCC 统计
LndOccEngine::Stats lnd_stats = lnd_engine.GetStats();
std::cout << "Layer 1 commits: " << lnd_stats.layer1_count << "\n";
std::cout << "Layer 2 commits: " << lnd_stats.layer2_count << "\n";
std::cout << "Layer 3 commits: " << lnd_stats.layer3_count << "\n";
```

---

## 下一步

- [API 参考文档](DTx-API-Reference.md)
- [性能调优指南](DTx-Performance-Tuning.md)
- [架构设计文档](../CedarGraph-DTx-Architecture.md)
