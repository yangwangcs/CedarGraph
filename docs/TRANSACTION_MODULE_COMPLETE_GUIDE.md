# CedarGraph 事务模块 - 傻瓜式完全指南

> **目标**: 零基础理解分布式事务，从"为什么需要事务"到"生产环境配置"

---

## 目录

1. [事务是什么? (5分钟理解)](#1-事务是什么-5分钟理解)
2. [单机事务 vs 分布式事务](#2-单机事务-vs-分布式事务)
3. [CedarGraph事务架构](#3-cedargraph事务架构)
4. [核心概念详解](#4-核心概念详解)
5. [完整使用教程](#5-完整使用教程)
6. [生产环境配置](#6-生产环境配置)
7. [故障排查](#7-故障排查)

---

## 1. 事务是什么? (5分钟理解)

### 1.1 用银行转账理解事务

```
场景: Alice 给 Bob 转账 100元

┌─────────────────────────────────────────────────────────────────┐
│                          转账操作                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Step 1: 检查 Alice 余额 >= 100元                               │
│           Alice: 500元 → OK                                      │
│                                                                  │
│  Step 2: Alice 账户扣款 100元                                   │
│           Alice: 500元 → 400元                                   │
│                                                                  │
│  Step 3: Bob 账户收款 100元                                     │
│           Bob: 200元 → 300元                                     │
│                                                                  │
│  Step 4: 记录转账日志                                            │
│           "Alice → Bob: 100元 at 2024-01-15 10:30:00"           │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

问题来了: 如果在 Step 2 完成后系统崩溃了怎么办?
┌─────────────────────────────────────────────────────────────────┐
│                          灾难场景                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  崩溃前: Alice 已扣款, Bob 未收款                                │
│                                                                  │
│  Alice: 400元 (已扣)                                             │
│  Bob:   200元 (未加)  ← 100元消失了!                            │
│                                                                  │
│  结果: 钱不见了! 这是不可接受的!                                  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 1.2 事务的ACID特性

```
ACID = 事务的四个核心特性

┌─────────────────────────────────────────────────────────────────┐
│ A - Atomicity (原子性)                                          │
│   含义: 要么全部成功, 要么全部失败                               │
│   例子: 转账要么完成(扣款+收款), 要么什么都没发生                 │
│   口诀: "不可分割"                                               │
├─────────────────────────────────────────────────────────────────┤
│ C - Consistency (一致性)                                        │
│   含义: 事务执行前后, 数据必须满足约束条件                        │
│   例子: 转账前后, 总余额不变 (Alice+Bob的和保持不变)              │
│   口诀: "数据正确"                                               │
├─────────────────────────────────────────────────────────────────┤
│ I - Isolation (隔离性)                                          │
│   含义: 多个事务同时执行, 互不干扰                               │
│   例子: Alice同时给Bob和Charlie转账, 两个转账互不干扰             │
│   口诀: "互不干扰"                                               │
├─────────────────────────────────────────────────────────────────┤
│ D - Durability (持久性)                                         │
│   含义: 一旦提交, 数据永久保存, 即使系统崩溃                      │
│   例子: 转账完成后断电, 重启后数据仍在                            │
│   口诀: "永不丢失"                                               │
└─────────────────────────────────────────────────────────────────┘
```

### 1.3 CedarGraph事务的核心设计

```
CedarGraph = 分布式时态图数据库

┌─────────────────────────────────────────────────────────────────┐
│              时态数据 + 分布式事务 = ?                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  场景: "Alice在2024-01-15 10:30:00时给Bob转账100元"            │
│                                                                  │
│  普通数据库:                                                      │
│    UPDATE accounts SET balance = balance - 100 WHERE name='Alice'│
│    UPDATE accounts SET balance = balance + 100 WHERE name='Bob'  │
│    问题: 不知道转账发生的时间点                                   │
│                                                                  │
│  CedarGraph:                                                    │
│    // 事务在指定时间戳执行                                        │
│    txn.SetTimestamp(1705315800000000ULL);  // 微秒时间戳         │
│    alice_vertex.Put("balance", 400, txn);  // 写入带版本的数据    │
│    bob_vertex.Put("balance", 300, txn);                            │
│    edge.Create(alice, bob, "transfer", 100, txn);  // 创建时态边  │
│                                                                  │
│  优势: 可以查询 "Alice在10:30时的余额是多少?"                    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. 单机事务 vs 分布式事务

### 2.1 单机事务 (简单理解)

```
┌─────────────────────────────────────────────────────────────────┐
│                      单机事务模型                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   ┌──────────────┐                                              │
│   │  Application │                                              │
│   └──────┬───────┘                                              │
│          │ BEGIN TRANSACTION                                     │
│          ▼                                                      │
│   ┌──────────────┐                                              │
│   │   Database   │  单机 = 一台机器, 一个进程                    │
│   │   (SQLite/   │  所有数据在一个地方                           │
│   │    MySQL)    │  用锁就能搞定并发                             │
│   └──────────────┘                                              │
│                                                                  │
│   特点:                                                          │
│   1. 用一把大锁就能保护数据                                      │
│   2. 事务开始 → 获取锁 → 执行操作 → 释放锁 → 提交               │
│   3. 崩溃恢复: 用WAL(预写日志)重放                                │
│                                                                  │
│   缺点: 单机性能有上限, 无法水平扩展                              │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 分布式事务 (CedarGraph场景)

```
┌─────────────────────────────────────────────────────────────────┐
│                     分布式事务模型                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │                    Application                         │   │
│   └─────────────────────┬───────────────────────────────────┘   │
│                         │                                       │
│        ┌────────────────┼────────────────┐                     │
│        │                │                │                     │
│        ▼                ▼                ▼                     │
│   ┌─────────┐     ┌─────────┐     ┌─────────┐                 │
│   │ Node A  │     │ Node B  │     │ Node C  │                 │
│   │ Alice   │     │ Bob     │     │ Charlie │                 │
│   │ 的数据  │     │ 的数据  │     │ 的数据  │                 │
│   │ 在A节点 │     │ 在B节点 │     │ 在C节点 │                 │
│   └─────────┘     └─────────┘     └─────────┘                 │
│                                                                  │
│   场景: Alice 给 Bob 转账                                        │
│   问题: Alice在A节点, Bob在B节点, 如何原子性更新两个节点?         │
│                                                                  │
│   困难:                                                          │
│   1. 网络可能断开                                                │
│   2. A节点成功但B节点失败怎么办?                                  │
│   3. 怎么保证所有节点同时看到一样的结果?                          │
│   4. 节点崩溃如何恢复?                                           │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 2.3 CedarGraph的解决方案

```
┌─────────────────────────────────────────────────────────────────┐
│              CedarGraph 分布式事务方案                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   核心技术:                                                      │
│                                                                  │
│   1. OCC (乐观并发控制)                                          │
│      ┌────────────────────────────────────────────────────┐     │
│      │ 不提前加锁, 提交时检查冲突                          │     │
│      │ 适合: 读多写少, 冲突少的场景                        │     │
│      └────────────────────────────────────────────────────┘     │
│                                                                  │
│   2. MVCC (多版本并发控制)                                       │
│      ┌────────────────────────────────────────────────────┐     │
│      │ 每个写操作创建新版本, 读操作读快照                  │     │
│      │ 读写不阻塞, 支持时态查询                            │     │
│      └────────────────────────────────────────────────────┘     │
│                                                                  │
│   3. Raft (分布式一致性)                                         │
│      ┌────────────────────────────────────────────────────┐     │
│      │ 每个分区一个Raft组, 保证分区内部强一致              │     │
│      │ 跨分区事务用两阶段提交(2PC)                         │     │
│      └────────────────────────────────────────────────────┘     │
│                                                                  │
│   4. TWCD (时间窗口冲突检测)                                     │
│      ┌────────────────────────────────────────────────────┐     │
│      │ CedarGraph特有: 检测时态数据的时间范围冲突          │     │
│      │ 例如: 同一房间同一时间不能有两个预订                │     │
│      └────────────────────────────────────────────────────┘     │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. CedarGraph事务架构

### 3.1 整体架构图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CedarGraph 分布式事务架构                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        应用层 (Application)                          │   │
│  │                                                                     │   │
│  │  // 开始事务                                                        │   │
│  │  auto txn = graph.BeginTransaction();                               │   │
│  │                                                                     │   │
│  │  // 读取 (创建读快照)                                               │   │
│  │  auto alice = graph.GetVertex("Alice", txn);                        │   │
│  │  auto balance = alice.GetProperty("balance", txn);                  │   │
│  │                                                                     │   │
│  │  // 写入 (带版本)                                                   │   │
│  │  alice.SetProperty("balance", balance - 100, txn);                  │   │
│  │                                                                     │   │
│  │  // 提交或回滚                                                      │   │
│  │  txn.Commit();  // 或 txn.Rollback();                              │   │
│  │                                                                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                     事务管理层 (Transaction Manager)                 │   │
│  │                                                                     │   │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐     │   │
│  │  │ Transaction     │  │ Version Chain   │  │ Lock Manager    │     │   │
│  │  │ Context         │  │ Manager         │  │ (OCC/MVCC)      │     │   │
│  │  │                 │  │                 │  │                 │     │   │
│  │  │ - txn_id        │  │ - version list  │  │ - read_set      │     │   │
│  │  │ - read_set      │  │ - timestamp     │  │ - write_set     │     │   │
│  │  │ - write_set     │  │ - visibility    │  │ - conflict check│     │   │
│  │  │ - start_ts      │  └─────────────────┘  └─────────────────┘     │   │
│  │  └─────────────────┘                                               │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                     存储引擎层 (Storage Engine)                      │   │
│  │                                                                     │   │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐     │   │
│  │  │ LSM Engine      │  │ Raft Consensus  │  │ WAL (Write-Ahead│     │   │
│  │  │                 │  │                 │  │ Log)            │     │   │
│  │  │ - MemTable      │  │ - Leader        │  │                 │     │   │
│  │  │ - SST Files     │  │ - Follower      │  │ - redo log      │     │   │
│  │  │ - Compaction    │  │ - Quorum        │  │ - crash recovery│     │   │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────┘     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 事务状态机

```
┌─────────────────────────────────────────────────────────────────┐
│                    事务生命周期状态机                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│    BEGIN                                                        │
│      │                                                          │
│      ▼                                                          │
│   ┌────────┐                                                    │
│   │ ACTIVE │◄──────────────────────┐                            │
│   │ 活跃   │                       │                            │
│   │        │  读写操作             │                            │
│   └───┬────┘                       │                            │
│       │                            │                            │
│       │ Commit()                   │ Retry                      │
│       ▼                            │                            │
│   ┌──────────────┐  Conflict       │                            │
│   │ PREPARING    │─────────────────┘                            │
│   │ 准备提交     │  冲突检测                                      │
│   │ (OCC check)  │  发现冲突                                      │
│   └──────┬───────┘                                               │
│          │                                                       │
│          │ No conflict                                           │
│          ▼                                                       │
│   ┌──────────────┐                                               │
│   │ COMMITTED    │                                               │
│   │ 已提交       │◄────────────────────────────────┐             │
│   │              │                                │             │
│   └──────────────┘                                │             │
│          ▲                                        │             │
│          │ Rollback()                             │             │
│          │                                        │             │
│   ┌──────┴───────┐                                │             │
│   │ ABORTED      │────────────────────────────────┘             │
│   │ 已回滚       │  崩溃恢复后                                   │
│   └──────────────┘  从WAL重放                                   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 4. 核心概念详解

### 4.1 OCC (乐观并发控制)

```
┌─────────────────────────────────────────────────────────────────┐
│              OCC (Optimistic Concurrency Control)               │
│                        乐观并发控制                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  核心思想: 假设冲突很少发生, 提交时再检查                          │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ 悲观并发控制 (传统锁)                                    │    │
│  │ 1. 开始事务 → 2. 加锁 → 3. 执行 → 4. 解锁 → 5. 提交    │    │
│  │ 问题: 锁竞争激烈, 并发度低                               │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ 乐观并发控制 (OCC)                                       │    │
│  │ 1. 开始事务 → 2. 执行(不加锁) → 3. 验证 → 4. 提交/回滚 │    │
│  │ 优势: 无锁, 高并发, 适合读多写少                         │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  具体实现:                                                       │
│                                                                  │
│  1. 读阶段 (Read Phase)                                         │
│     - 记录所有读过的数据到 read_set                             │
│     - 记录读时的版本号 (timestamp)                              │
│                                                                  │
│  2. 验证阶段 (Validation Phase)                                 │
│     - 检查 read_set 中的数据是否被其他事务修改                  │
│     - 如果版本号变了 → 冲突! 回滚                               │
│                                                                  │
│  3. 写阶段 (Write Phase)                                        │
│     - 如果没有冲突, 写入所有修改                                │
│     - 更新版本号                                                │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

OCC示例:
┌─────────────────────────────────────────────────────────────────┐
│  Transaction 1 (T1)          Transaction 2 (T2)                 │
│  ===============             ===============                    │
│                                                                  │
│  Read(A=100)                                                      │
│                              Read(A=100)                          │
│  A = A - 10 = 90                                                  │
│                              A = A + 20 = 120                     │
│  Validate: A还是100 ✓                                             │
│                              Validate: A还是100 ✓                 │
│  Write(A=90)                                                      │
│                              Write(A=120)                         │
│  Commit                                                           │
│                              Commit                               │
│                                                                  │
│  结果: 数据不一致! (应该是110, 但T2覆盖了T1)                     │
│                                                                  │
│  解决方案: 用时间戳排序                                           │
│  - T1 timestamp=100, T2 timestamp=200                            │
│  - T2验证时发现 A已被T1修改 (version=100 < 200)                  │
│  - T2回滚重试                                                     │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 MVCC (多版本并发控制)

```
┌─────────────────────────────────────────────────────────────────┐
│            MVCC (Multi-Version Concurrency Control)             │
│                      多版本并发控制                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  核心思想: 每次写操作创建新版本, 读操作读快照                     │
│                                                                  │
│  数据结构: 版本链 (Version Chain)                                │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  Key: "Alice.balance"                                    │    │
│  │                                                          │    │
│  │  Version 3 (ts=300) ──► value=400  ◄── 最新版本        │    │
│  │      │                                                   │    │
│  │      ▼ next                                              │    │
│  │  Version 2 (ts=200) ──► value=500  ◄── T2读到的值      │    │
│  │      │                                                   │    │
│  │      ▼ next                                              │    │
│  │  Version 1 (ts=100) ──► value=600  ◄── T1读到的值      │    │
│  │                                                          │    │
│  │  每个版本包含: value + timestamp + prev指针              │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  读操作:                                                         │
│  - 事务开始时获取 start_timestamp                               │
│  - 读取时找最大的 timestamp <= start_timestamp 的版本           │
│  - 这样读到的就是事务开始时的快照                               │
│                                                                  │
│  写操作:                                                         │
│  - 创建新版本, timestamp = commit_timestamp                     │
│  - 插入版本链头部 (最新)                                        │
│  - 旧版本保留 (用于其他事务读快照)                              │
│                                                                  │
│  垃圾回收:                                                       │
│  - 定期清理比所有活跃事务都老的版本                             │
│  - 或根据TTL清理                                                │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

MVCC示例:
┌─────────────────────────────────────────────────────────────────┐
│  时间 ─────────────────────────────────────────►                  │
│                                                                  │
│  T=100: T1开始 (start_ts=100)                                    │
│          Read(Alice) → 读到 Version1 (ts=100) = 600             │
│                                                                  │
│  T=150: T2开始 (start_ts=150)                                    │
│          Read(Alice) → 读到 Version1 (ts=100) = 600             │
│                                                                  │
│  T=200: T1写 Alice = 500                                        │
│          创建 Version2 (ts=200)                                 │
│          T1提交                                                  │
│                                                                  │
│  T=250: T2读 Alice → 还是读到 Version1 (ts=100) = 600           │
│          因为 T2.start_ts=150 < Version2.ts=200                 │
│          保证T2看到的是自己开始时的快照                         │
│                                                                  │
│  T=300: T2写 Alice = 400                                        │
│          创建 Version3 (ts=300)                                 │
│          T2提交                                                  │
│                                                                  │
│  T=400: T3开始 (start_ts=400)                                    │
│          Read(Alice) → 读到 Version3 (ts=300) = 400 (最新)      │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 4.3 TWCD (时间窗口冲突检测)

```
┌─────────────────────────────────────────────────────────────────┐
│    TWCD (Temporal Window Conflict Detection)                    │
│              时间窗口冲突检测                                     │
│                   (CedarGraph特有)                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  场景: 会议室预订系统                                            │
│                                                                  │
│  需求: 同一会议室同一时间不能被两个人预订                         │
│                                                                  │
│  传统方案:                                                       │
│  - 加锁: 预订前锁定整个会议室                                     │
│  - 问题: 并发低, 不同时间段也不能并行预订                         │
│                                                                  │
│  TWCD方案:                                                       │
│  - 检测时间范围是否重叠                                           │
│  - 只有真正冲突时才拒绝                                           │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  会议室: Room A                                          │    │
│  │                                                          │    │
│  │  时间轴: 9am      10am     11am     12pm                 │    │
│  │           │─────────│─────────│─────────│                │    │
│  │                                                          │    │
│  │  预订1:   [=========]         Bob 预订 9-10am            │    │
│  │  预订2:             [=========] Alice 预订 10-11am       │    │
│  │  预订3:   [==================] Charlie 预订 9-11am       │    │
│  │                                                          │    │
│  │  冲突检测:                                                │    │
│  │  - 预订1和预订2: 不重叠 (9-10 vs 10-11) ✓ 允许           │    │
│  │  - 预订1和预订3: 重叠 (9-10 vs 9-11) ✗ 拒绝Charlie       │    │
│  │                                                          │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  代码示例:                                                       │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  // 尝试预订会议室                                        │    │
│  │  auto room = graph.GetVertex("RoomA");                    │    │
│  │                                                           │    │
│  │  // 检查时间窗口冲突                                      │    │
│  │  auto conflict = graph.CheckTemporalConflict(             │    │
│  │      room,                                                │    │
│  │      "booking",                                           │    │
│  │      start_time,  // 9:00am                               │    │
│  │      end_time     // 10:00am                              │    │
│  │  );                                                       │    │
│  │                                                           │    │
│  │  if (conflict.ok()) {                                     │    │
│  │      // 有时间冲突!                                       │    │
│  │      std::cout << "时间已被预订: " << conflict->by_who;   │    │
│  │      return false;                                        │    │
│  │  }                                                        │    │
│  │                                                           │    │
│  │  // 没有冲突, 可以预订                                    │    │
│  │  room.AddTemporalEdge("booking", user, start, end);       │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 5. 完整使用教程

### 5.1 基础事务示例

```cpp
#include "cedar/graph/cedar_graph.h"
#include "cedar/driver/session.h"

using namespace cedar;
using namespace cedar::graph;
using namespace cedar::driver;

int main() {
    // 1. 连接到图数据库
    GraphDb db("localhost:7687");
    auto session = db.NewSession();
    
    // 2. 简单事务示例
    {
        // 开始事务
        auto txn = session.BeginTransaction();
        
        // 读取数据
        auto alice = txn.GetVertex("Person", "name", "Alice");
        auto balance = alice.GetProperty<int64_t>("balance");
        
        std::cout << "Alice balance: " << balance << std::endl;
        
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
            // 余额不足, 回滚
            txn.Rollback();
            std::cout << "Insufficient balance, rolled back!" << std::endl;
        }
    }
    
    // 3. 时态事务示例
    {
        auto txn = session.BeginTransaction();
        
        // 设置事务时间戳 (时态查询用)
        uint64_t query_time = ParseTimestamp("2024-01-15T10:30:00");
        txn.SetTimestamp(query_time);
        
        // 查询特定时间点的数据
        auto alice = txn.GetVertexAtTime("Person", "name", "Alice", query_time);
        std::cout << "Alice balance at " << query_time << ": " 
                  << alice.GetProperty<int64_t>("balance") << std::endl;
        
        txn.Rollback();  // 只读事务, 不需要提交
    }
    
    // 4. 批量操作事务
    {
        auto txn = session.BeginTransaction();
        
        // 批量插入
        for (int i = 0; i < 1000; ++i) {
            auto person = txn.CreateVertex("Person");
            person.SetProperty("name", "Person_" + std::to_string(i));
            person.SetProperty("age", 20 + (i % 50));
        }
        
        // 批量提交
        txn.Commit();
        std::cout << "Batch insert committed!" << std::endl;
    }
    
    return 0;
}
```

### 5.2 分布式事务示例

```cpp
#include "cedar/dtx/coordinator/txn_context.h"
#include "cedar/dtx/protocol/twcd_engine.h"

using namespace cedar::dtx;

// 跨分区转账 (Alice在分区A, Bob在分区B)
void CrossPartitionTransfer() {
    // 获取全局事务上下文
    auto txn = TxnContext::Begin();
    
    try {
        // 阶段1: 读取 (并行读取多个分区)
        auto future_alice = txn.AsyncRead("partition_A", "Alice");
        auto future_bob = txn.AsyncRead("partition_B", "Bob");
        
        auto alice_data = future_alice.get();
        auto bob_data = future_bob.get();
        
        // 执行业务逻辑
        int64_t alice_balance = ParseBalance(alice_data);
        if (alice_balance < 100) {
            throw InsufficientBalanceException();
        }
        
        // 阶段2: 写入 (准备)
        txn.PrepareWrite("partition_A", "Alice", alice_balance - 100);
        txn.PrepareWrite("partition_B", "Bob", ParseBalance(bob_data) + 100);
        
        // 阶段3: 提交 (两阶段提交)
        // 3a. Prepare阶段: 询问所有分区是否可以提交
        // 3b. Commit阶段: 所有分区确认后真正提交
        txn.Commit();
        
        std::cout << "Cross-partition transfer committed!" << std::endl;
        
    } catch (const ConflictException& e) {
        // OCC冲突, 重试
        txn.Rollback();
        std::cout << "Conflict detected, retrying..." << std::endl;
        CrossPartitionTransfer();  // 重试
        
    } catch (const Exception& e) {
        txn.Rollback();
        throw;
    }
}
```

### 5.3 TWCD时间窗口示例

```cpp
#include "cedar/graph/cedar_graph_temporal.h"

// 会议室预订系统
class RoomBookingSystem {
public:
    // 预订会议室
    bool BookRoom(const std::string& room_id, 
                  const std::string& user_id,
                  uint64_t start_time,
                  uint64_t end_time) {
        
        auto txn = graph_.BeginTransaction();
        
        // 1. 获取会议室节点
        auto room = txn.GetVertex("Room", "room_id", room_id);
        
        // 2. 检查时间冲突 (TWCD)
        auto bookings = room.GetTemporalEdges("BOOKING", start_time, end_time);
        
        if (!bookings.empty()) {
            std::cout << "Room already booked during this time!" << std::endl;
            for (const auto& booking : bookings) {
                std::cout << "  Conflicts with: " << booking.GetProperty("user_id")
                          << " at " << booking.GetStartTime() << "-" 
                          << booking.GetEndTime() << std::endl;
            }
            txn.Rollback();
            return false;
        }
        
        // 3. 创建预订边 (时态边)
        auto user = txn.GetVertex("User", "user_id", user_id);
        auto booking = txn.CreateTemporalEdge(room, user, "BOOKING", 
                                               start_time, end_time);
        booking.SetProperty("user_id", user_id);
        booking.SetProperty("booked_at", CurrentTimestamp());
        
        // 4. 提交
        txn.Commit();
        std::cout << "Room booked successfully!" << std::endl;
        return true;
    }
    
    // 查询某时间段可用会议室
    std::vector<Vertex> FindAvailableRooms(uint64_t start_time, 
                                           uint64_t end_time) {
        auto txn = graph_.BeginTransaction();
        
        // 获取所有会议室
        auto all_rooms = txn.GetVertices("Room");
        
        std::vector<Vertex> available;
        for (const auto& room : all_rooms) {
            // 检查该时间段是否有预订
            auto bookings = room.GetTemporalEdges("BOOKING", 
                                                   start_time, end_time);
            if (bookings.empty()) {
                available.push_back(room);
            }
        }
        
        txn.Rollback();
        return available;
    }
    
private:
    CedarGraph graph_;
};
```

---

## 6. 生产环境配置

### 6.1 事务配置

```yaml
# cedar_txn.yaml
transaction:
  # OCC配置
  occ:
    max_retries: 10              # 冲突最大重试次数
    retry_delay_ms: 10           # 重试间隔
    validation_timeout_ms: 5000  # 验证超时
  
  # MVCC配置
  mvcc:
    snapshot_isolation: true     # 快照隔离级别
    version_ttl_hours: 168       # 版本保留7天
    gc_interval_minutes: 60      # 垃圾回收间隔
  
  # 2PC配置
  twopc:
    prepare_timeout_ms: 10000    # prepare阶段超时
    commit_timeout_ms: 30000     # commit阶段超时
    coordinator_timeout_ms: 60000 # 协调器超时
  
  # TWCD配置
  twcd:
    enabled: true
    conflict_check_batch_size: 100
```

### 6.2 性能调优

```cpp
// 1. 调整OCC重试策略
OccConfig occ_config;
occ_config.max_retries = 20;
occ_config.backoff_strategy = BackoffStrategy::EXPONENTIAL;
occ_config.initial_delay_ms = 1;
occ_config.max_delay_ms = 100;

// 2. 配置MVCC版本清理
MvccConfig mvcc_config;
mvcc_config.version_retention_policy = RetentionPolicy::TIME_BASED;
mvcc_config.retention_duration_hours = 24;  // 保留24小时版本

// 3. 启用事务流水线
TransactionConfig txn_config;
txn_config.enable_pipelining = true;
txn_config.pipeline_batch_size = 100;

// 4. 分区策略
PartitionConfig partition_config;
partition_config.strategy = PartitionStrategy::HASH;
partition_config.num_partitions = 128;
```

---

## 7. 故障排查

### 7.1 常见问题

| 问题 | 可能原因 | 解决方案 |
|------|----------|----------|
| 事务频繁冲突 | 热点数据访问 | 减小事务范围, 使用乐观锁 |
| 事务超时 | 网络延迟或死锁 | 增加超时时间, 检查锁顺序 |
| 版本链过长 | MVCC未及时清理 | 调整GC频率, 缩短版本保留期 |
| 2PC挂起 | 协调器崩溃 | 启用超时回滚, 检查日志 |

### 7.2 监控指标

```cpp
// 事务监控
struct TransactionMetrics {
  uint64_t total_committed;      // 总提交数
  uint64_t total_aborted;        // 总回滚数
  uint64_t conflict_count;       // 冲突次数
  double avg_latency_ms;         // 平均延迟
  double p99_latency_ms;         // P99延迟
  double conflict_rate;          // 冲突率
};

// 定期检查
void MonitorTransactions() {
    auto metrics = txn_manager.GetMetrics();
    
    if (metrics.conflict_rate > 0.1) {  // 冲突率>10%
        LOG(WARNING) << "High conflict rate: " << metrics.conflict_rate;
        // 可能需要调整分区策略
    }
    
    if (metrics.p99_latency_ms > 100) {  // P99延迟>100ms
        LOG(WARNING) << "High transaction latency: " << metrics.p99_latency_ms;
        // 检查是否有长事务
    }
}
```

---

## 8. 总结

```
CedarGraph事务核心:

1. OCC (乐观并发控制)
   - 无锁高并发
   - 提交时验证冲突
   - 冲突时自动重试

2. MVCC (多版本并发控制)
   - 读写不阻塞
   - 支持时态查询
   - 版本自动清理

3. Raft (分布式一致性)
   - 分区内部强一致
   - 跨分区2PC
   - 自动故障恢复

4. TWCD (时间窗口冲突检测)
   - CedarGraph特有
   - 检测时间范围重叠
   - 适合预订/排班场景

使用口诀:
- 读多写少用OCC
- 要查历史开MVCC
- 跨节点用2PC
- 时间冲突用TWCD
```

