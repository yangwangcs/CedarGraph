# 全局时钟同步方案选型

## 问题背景

CedarGraph DTx 需要跨节点事务的因果一致性：
- 事务 A 在 Node1 提交 (ts=100)
- 事务 B 在 Node2 读取，必须看到 A 的写入

如果仅用本地时钟，节点间时钟偏差可能导致：
- 后发生的事务有更小的时间戳 (违反因果序)
- 事务可见性判断错误

## 候选方案对比

| 特性 | HLC (混合逻辑时钟) | TSO (时间戳服务器) | NTP |
|-----|-------------------|-------------------|-----|
| **时钟类型** | 物理 + 逻辑混合 | 中心分配 | 物理时钟同步 |
| **精度** | ⭐⭐ 毫秒级 | ⭐⭐⭐ 微秒级 | ⭐ 毫秒~秒级 |
| **单点故障** | ⭐⭐⭐ 无 | ⭐ 有 (需 HA) | ⭐⭐⭐ 无 |
| **网络开销** | ⭐⭐⭐ 极低 (本地计算) | ⭐ 高 (每次事务 RPC) | ⭐⭐ 低 |
| **复杂度** | ⭐⭐ 中等 | ⭐⭐⭐ 高 | ⭐ 低 |
| **Causal Consistency** | ⭐⭐⭐ 天然支持 | ⭐⭐ 需额外元数据 | ⭐ 不支持 |
| **参考实现** | CockroachDB | TiDB / Spanner | - |

## 推荐方案：HLC (混合逻辑时钟)

### 理由

1. **无单点故障**: 不像 TSO 依赖中心服务器
2. **因果一致性**: 天然保证跨节点事务的因果序
3. **低延迟**: 无需网络 RTT 获取时间戳
4. **CockroachDB 验证**: 生产级分布式数据库采用

### HLC 原理

```
HLC = (物理时间, 逻辑计数器)

示例:
Node1: (1000, 0) --发送消息--> Node2
Node2: (999, 0) --接收--> 更新为 (1000, 1)

比较规则:
(a, b) < (c, d) 如果:
  a < c, 或
  a == c 且 b < d
```

### 实现设计

```cpp
// include/cedar/dtx/hybrid_logical_clock.h
class HybridLogicalClock {
 public:
  struct Timestamp {
    uint64_t physical;  // 物理时间 (微秒)
    uint64_t logical;   // 逻辑计数器
    
    bool operator<(const Timestamp& other) const {
      return physical < other.physical ||
             (physical == other.physical && logical < other.logical);
    }
  };
  
  // 获取当前 HLC 时间戳
  Timestamp Now() {
    uint64_t phys = GetPhysicalTime();
    if (phys > last_physical_) {
      last_physical_ = phys;
      last_logical_ = 0;
    } else {
      last_logical_++;
    }
    return {last_physical_, last_logical_};
  }
  
  // 接收远程时间戳后更新 (保证因果序)
  void Update(const Timestamp& remote) {
    uint64_t phys = GetPhysicalTime();
    last_physical_ = std::max({phys, last_physical_, remote.physical});
    if (remote.physical == last_physical_) {
      last_logical_ = std::max(last_logical_, remote.logical) + 1;
    }
  }
  
 private:
  uint64_t last_physical_ = 0;
  uint64_t last_logical_ = 0;
  std::mutex mutex_;
};

// 在每个 RPC 消息中携带 HLC
typedef std::pair<uint64_t, uint64_t> HlcTimestamp;

struct RpcMessage {
  HlcTimestamp sender_hlc;
  bytes payload;
  
  // 接收方更新本地 HLC
  void OnReceive(HybridLogicalClock& local_clock) {
    local_clock.Update({sender_hlc.first, sender_hlc.second});
  }
};
```

### 与事务集成

```cpp
class DistributedTxnContext {
  HybridLogicalClock hlc_;
  Timestamp start_ts_;
  Timestamp commit_ts_;
  
 public:
  void Begin() {
    start_ts_ = hlc_.Now();
  }
  
  // 跨节点通信时传播 HLC
  void SendPrepare(NodeID node) {
    PrepareRequest req;
    req.hlc = hlc_.Now();  // 携带当前 HLC
    rpc_client_->Prepare(node, req);
  }
  
  void OnReceiveResponse(const PrepareResponse& resp) {
    hlc_.Update(resp.hlc);  // 更新本地 HLC
  }
  
  Timestamp GetCommitTs() {
    commit_ts_ = hlc_.Now();
    return commit_ts_;
  }
};
```

### TSO 作为备选

如果 HLC 精度不满足需求（如需要严格的全序），可考虑：

```cpp
// TSO Client
class TsoClient {
  std::string tso_leader_addr_;
  
  Timestamp GetTimestamp() {
    // 批量预取时间戳 (减少 RPC)
    if (cached_timestamps_.empty()) {
      cached_timestamps_ = FetchBatch(100);  // 批量获取
    }
    return cached_timestamps_.pop();
  }
};
```

**缺点**:
- 每次事务需 RPC (或批量缓存)
- TSO Leader 故障时需切换
- 跨地域部署延迟高

---

## 实施计划

### Phase 1: HLC 实现 (3 天)

```cpp
// 新增文件
include/cedar/dtx/hybrid_logical_clock.h
src/dtx/utils/hybrid_logical_clock.cc
```

### Phase 2: 集成到 RPC (3 天)

- 所有 RPC 消息携带 HLC
- 接收时更新本地 HLC

### Phase 3: 事务集成 (2 天)

- 替换本地时钟
- 因果一致性验证测试
