# CedarGraph 分布式图数据库 - 完整使用指南

## 目录

1. [架构概述](#1-架构概述)
2. [快速开始](#2-快速开始)
3. [核心模块详解](#3-核心模块详解)
   - 3.1 [事务模块](./TRANSACTION_MODULE_COMPLETE_GUIDE.md)
4. [生产部署](#4-生产部署)
5. [故障排除](#5-故障排除)

---

## 1. 架构概述

### 1.1 系统架构图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        CedarGraph 分布式图数据库                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      事务层 (Transaction Layer)                       │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                  │   │
│  │  │ OCC         │  │ MVCC        │  │ Raft        │                  │   │
│  │  │ 乐观并发    │  │ 多版本      │  │ 分布式      │                  │   │
│  │  │ 控制        │  │ 并发控制    │  │ 一致性      │                  │   │
│  │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                  │   │
│  │         └─────────────────┴─────────────────┘                        │   │
│  │                           │                                          │   │
│  │                   ┌───────▼───────┐                                  │   │
│  │                   │ Transaction   │                                  │   │
│  │                   │ Manager       │                                  │   │
│  │                   └───────┬───────┘                                  │   │
│  └───────────────────────────┼──────────────────────────────────────────┘   │
│                              │                                              │
│  ┌───────────────────────────▼──────────────────────────────────────────┐   │
│  │                      存储层 (Storage Layer)                          │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                  │   │
│  │  │ LSM Engine  │  │ Multi-Raft  │  │ Partition   │                  │   │
│  │  │             │  │ Replication │  │ Manager     │                  │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘                  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 核心设计理念

```cpp
// 核心理念: "分布式事务 + Raft强一致"
// 数据按 entity_id 哈希分片
// 每个分区使用Raft保证强一致性
// 跨分区事务使用两阶段提交(2PC)
```

### 1.3 一致性模型

```
┌────────────────────────────────────────────────────────────┐
│                   一致性模型                                │
├────────────────────────────────────────────────────────────┤
│                                                            │
│  主表 (Main Table)                                         │
│  ┌─────────────────┐                                      │
│  │ Strong          │                                      │
│  │ Consistency     │                                      │
│  │                 │                                      │
│  │ Raft Consensus  │                                      │
│  │ ─────────────── │                                      │
│  │ Write → Leader  │                                      │
│  │ Read  → Leader  │                                      │
│  │                 │                                      │
│  │ Latency: ~10ms  │                                      │
│  └─────────────────┘                                      │
│                                                            │
│  使用场景:                                                  │
│  - 转账交易: 强一致                                         │
│  - 库存扣减: 强一致                                         │
│  - 订单创建: 强一致                                         │
└────────────────────────────────────────────────────────────┘
```

---

## 2. 快速开始

### 2.1 5分钟上手

```cpp
// ========== Step 1: 包含头文件 ==========
#include "cedar/graph/cedar_graph.h"
#include "cedar/driver/session.h"

using namespace cedar;
using namespace cedar::graph;
using namespace cedar::driver;

// ========== Step 2: 配置并初始化 ==========
int main() {
    // 连接到图数据库
    GraphDb db("localhost:7687");
    auto session = db.NewSession();
    
    // ========== Step 3: 执行事务 ==========
    {
        // 开始事务
        auto txn = session.BeginTransaction();
        
        // 读取数据
        auto alice = txn.GetVertex("Person", "name", "Alice");
        auto balance = alice.GetProperty<int64_t>("balance");
        
        // 修改数据
        if (balance >= 100) {
            alice.SetProperty("balance", balance - 100);
            
            auto bob = txn.GetVertex("Person", "name", "Bob");
            auto bob_balance = bob.GetProperty<int64_t>("balance");
            bob.SetProperty("balance", bob_balance + 100);
            
            // 提交事务
            txn.Commit();
            std::cout << "Transfer committed!" << std::endl;
        } else {
            txn.Rollback();
            std::cout << "Insufficient balance!" << std::endl;
        }
    }
    
    return 0;
}
```

### 2.2 编译运行

```bash
# 编译
cd /Users/wangyang/Desktop/CedarGraph-Core/build
cmake .. -DBUILD_EXAMPLES=ON
make cedar_graph_example

# 运行
./examples/cedar_graph_example
```

---

## 3. 核心模块详解

### 3.1 事务模块

详细文档: [TRANSACTION_MODULE_COMPLETE_GUIDE.md](./TRANSACTION_MODULE_COMPLETE_GUIDE.md)

**核心概念:**
- **OCC (乐观并发控制)**: 提交时验证冲突，适合读多写少
- **MVCC (多版本并发控制)**: 读写不阻塞，支持快照读
- **Raft**: 分区内部强一致
- **2PC**: 跨分区事务

**使用示例:**
```cpp
// 基础事务
auto txn = session.BeginTransaction();
auto vertex = txn.GetVertex("Person", "id", "123");
vertex.SetProperty("name", "Alice", txn);
txn.Commit();

// 跨分区事务
auto txn = TxnContext::Begin();
txn.AsyncRead("partition_A", "key1");
txn.AsyncRead("partition_B", "key2");
txn.Commit();  // 内部使用2PC
```

---

## 4. 生产部署

### 4.1 配置文件

```yaml
# cedar.yaml
cluster:
  node_count: 3
  node_addresses:
    - "192.168.1.1:7687"
    - "192.168.1.2:7687"
    - "192.168.1.3:7687"

transaction:
  occ:
    max_retries: 10
    retry_delay_ms: 10
  
  mvcc:
    snapshot_isolation: true
    version_ttl_hours: 168
  
  raft:
    heartbeat_interval_ms: 100
    election_timeout_ms: 1000

storage:
  data_dir: "/data/cedar"
  log_dir: "/var/log/cedar"
```

### 4.2 部署步骤

```bash
# 1. 准备数据目录
mkdir -p /data/cedar /var/log/cedar

# 2. 启动节点
./cedar_server --config cedar.yaml --node_id 0

# 3. 验证集群
./cedar_cli --host 192.168.1.1 --command "show cluster"
```

---

## 5. 故障排除

### 5.1 常见问题

| 问题 | 可能原因 | 解决方案 |
|------|----------|----------|
| 事务频繁冲突 | 热点数据 | 减小事务范围 |
| 事务超时 | 网络延迟 | 增加超时时间 |
| 节点无法加入 | 网络不通 | 检查防火墙 |
| 数据不一致 | Raft分裂 | 强制选主 |

### 5.2 监控指标

```cpp
// 关键指标
struct ClusterMetrics {
  uint64_t committed_transactions;
  uint64_t aborted_transactions;
  double conflict_rate;
  double avg_latency_ms;
  double p99_latency_ms;
  uint64_t raft_leader_changes;
};
```

---

**更多文档:**
- [事务模块完整指南](./TRANSACTION_MODULE_COMPLETE_GUIDE.md)
- [快速上手](./00_QUICKSTART.md)
