# CedarGraph 新旧事务系统集成方案

## 1. 架构对比

### 1.1 原有事务系统 (OCCTransaction)

```
┌─────────────────────────────────────────────────────────────────┐
│                     OCCTransaction                              │
│  - 基于乐观并发控制 (OCC)                                        │
│  - 直接操作 LsmEngine 的 memtable                               │
│  - 支持读集/写集跟踪                                             │
│  - 需要 TransactionManager、WalWriter                          │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    TransactionManager                           │
│  - 分配事务 ID                                                  │
│  - 管理时间戳                                                   │
│  - 协调并发                                                     │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                     LsmEngine (memtable)                        │
└─────────────────────────────────────────────────────────────────┘
```

**使用方式**:
```cpp
OCCTransaction* txn = storage->BeginTransaction();
txn->Put(entity_id, EntityType::Vertex, column, descriptor, timestamp);
txn->Commit();  // OCC 验证
```

### 1.2 新 Driver 事务系统 (ExplicitTransaction)

```
┌─────────────────────────────────────────────────────────────────┐
│                 ExplicitTransaction (L3 API)                    │
│  - 基于 CedarUpdate（批量更新）                                 │
│  - 使用 CedarScan（快照读取）                                   │
│  - 支持书签（因果一致性）                                        │
│  - 支持重试策略                                                  │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                      CedarUpdate                                │
│  - 收集更新操作                                                 │
│  - 批量 Apply()                                                │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    CedarGraphStorage                            │
│  - 底层存储（复用）                                             │
└─────────────────────────────────────────────────────────────────┘
```

**使用方式**:
```cpp
auto txn = session.BeginTransaction();
txn.PutVertex(1, 0, descriptor);
txn.PutEdge(1, 2, 0, descriptor);
auto result = txn.Commit();  // 返回 Bookmark
```

---

## 2. 协作能力分析

### 2.1 ✅ 可以协作的场景

| 场景 | 支持度 | 说明 |
|------|--------|------|
| **共享存储** | ✅ 完全支持 | 两者都操作同一个 CedarGraphStorage/LsmEngine |
| **数据可见性** | ✅ 完全支持 | 通过 CedarScan 可以看到 OCCTransaction 提交的数据 |
| **因果一致性** | ✅ 支持 | ExplicitTransaction 的书签基于 sequence number，与 OCC 兼容 |
| **混合使用** | ⚠️ 需谨慎 | 同一线程不要混用两种事务 |

### 2.2 ⚠️ 需要注意的问题

#### 问题 1: 事务隔离级别不同

```cpp
// OCCTransaction - 乐观并发控制
// 提交时进行 OCC 验证（检查读集是否被修改）
OCCTransaction* txn1 = storage->BeginTransaction();
txn1->Get(...);  // 记录读集
txn1->Put(...);
txn1->Commit();  // 如果读集被修改，返回 Conflict

// ExplicitTransaction - 批量更新
// 基于 CedarUpdate，提交时进行存在性检查
auto txn2 = session.BeginTransaction();
txn2.GetNode(...);  // 使用 CedarScan，不记录读集
txn2.PutVertex(...);
txn2.Commit();  // 根据 StrictLevel 进行检查
```

**解决方案**:
- 如果需要强 OCC 语义，使用 `OCCTransaction`
- 如果需要简洁 API 和书签，使用 `ExplicitTransaction`
- 不要在一个逻辑事务中混用两者

#### 问题 2: 写操作冲突

```cpp
// 场景：两个事务并发修改同一实体

// 线程 1: OCCTransaction
OCCTransaction* occ_txn = storage->BeginTransaction();
occ_txn->Put(1, EntityType::Vertex, 0, desc1, ts);

// 线程 2: ExplicitTransaction
auto driver_txn = session.BeginTransaction();
driver_txn.PutVertex(1, 0, desc2);

// 提交顺序：
// 1. 如果 driver_txn 先提交，数据直接写入存储
// 2. occ_txn 提交时，OCC 可能检测到冲突（如果读了该实体）
```

**解决方案**:
- 两者最终都通过 LsmEngine 写入，存储层保证一致性
- OCCTransaction 会在 Commit 时检测冲突
- ExplicitTransaction 依赖 CedarUpdate 的严格级别检查

#### 问题 3: 书签同步

```cpp
// OCCTransaction 提交后，如何获取书签用于 ExplicitTransaction？

// 当前方案：OCCTransaction 不直接支持书签
//  workaround：通过 sequence number 桥接

OCCTransaction* occ_txn = storage->BeginTransaction();
// ... 操作 ...
occ_txn->Commit();
uint64_t seq_num = engine->GetLatestSequenceNumber();
Bookmark bookmark(seq_num);

// 后续使用书签
auto driver_txn = session.BeginTransaction(bookmark);
```

---

## 3. 集成方案

### 3.1 方案 A: 分层使用（推荐）

```
应用层
  │
  ├─> 新应用代码 ──> ExplicitTransaction (L3 API)
  │                    ├─ 书签管理
  │                    ├─ 重试策略
  │                    └─ 简洁 API
  │
  └─> 遗留代码 ────> OCCTransaction
                       ├─ 细粒度 OCC 控制
                       └─ 复杂事务逻辑
```

**适用场景**:
- 渐进式迁移
- 新功能使用新 API，旧功能保持不动

### 3.2 方案 B: 统一封装

创建一个统一的事务门面：

```cpp
// UnifiedTransaction - 封装两种事务
class UnifiedTransaction {
 public:
  enum class Mode {
    kOCC,     // 使用 OCCTransaction
    kDriver,  // 使用 ExplicitTransaction
  };
  
  // 根据场景自动选择
  static UnifiedTransaction Begin(Mode mode, ...);
  
  // 统一 API
  void Put(...);
  void Get(...);
  Result<Bookmark> Commit();
  
 private:
  std::unique_ptr<OCCTransaction> occ_txn_;
  std::unique_ptr<ExplicitTransaction> driver_txn_;
  Mode mode_;
};
```

### 3.3 方案 C: 增强 OCCTransaction 支持书签

修改现有 OCCTransaction，添加书签支持：

```cpp
// 在 occ_transaction.h 中添加
class OCCTransaction {
 public:
  // ... 原有 API ...
  
  // 新增：获取书签（提交后）
  Bookmark GetBookmark() const;
  
  // 新增：使用书签初始化
  Status Begin(const Bookmark& bookmark);
};
```

---

## 4. 具体集成代码示例

### 4.1 从 OCCTransaction 迁移到 ExplicitTransaction

```cpp
// ========== 原有代码（OCCTransaction）==========
void OldCode(CedarGraphStorage* storage) {
  TransactionOptions options;
  options.isolation_level = IsolationLevel::kSnapshot;
  
  OCCTransaction* txn = storage->BeginTransaction(&options);
  
  Descriptor desc;
  txn->Put(1, EntityType::Vertex, 0, desc, Timestamp(1000));
  txn->Put(2, EntityType::Vertex, 0, desc, Timestamp(1000));
  
  Status s = txn->Commit();
  if (!s.ok()) {
    txn->Abort();
    delete txn;
    throw std::runtime_error(s.ToString());
  }
  
  delete txn;
}

// ========== 新代码（ExplicitTransaction）==========
void NewCode(Session& session) {
  auto txn = session.BeginTransaction(TransactionConfig{
    .timeout = std::chrono::seconds(30),
    .strict_level = StrictLevel::CHECK_EXISTS,
  });
  
  Descriptor desc;
  txn.PutVertex(1, 0, desc);
  txn.PutVertex(2, 0, desc);
  
  auto result = txn.Commit();
  if (!result.ok()) {
    throw std::runtime_error(result.error().ToString());
  }
  
  // 自动 RAII，无需手动 delete
}
```

### 4.2 混合使用（谨慎）

```cpp
void MixedUsage(CedarGraphStorage* storage, Session& session) {
  // 第一步：使用 OCCTransaction 进行复杂事务
  {
    OCCTransaction* occ_txn = storage->BeginTransaction();
    
    // 复杂操作，需要 OCC 验证
    occ_txn->Put(100, EntityType::Vertex, 0, desc, ts);
    // ... 更多操作 ...
    
    Status s = occ_txn->Commit();
    if (!s.ok()) {
      occ_txn->Abort();
      delete occ_txn;
      return;
    }
    delete occ_txn;
  }
  
  // 第二步：使用 ExplicitTransaction 进行简单查询
  // 注意：这里可以看到第一步提交的数据
  {
    auto driver_txn = session.BeginTransaction();
    
    auto node = driver_txn.GetNode(100);
    if (node) {
      std::cout << "Found node: " << node->id() << std::endl;
    }
    
    driver_txn.Commit();  // 只读事务也可以提交
  }
}
```

### 4.3 书签桥接

```cpp
// 桥接函数：从 OCCTransaction 获取书签
Bookmark GetBookmarkAfterOCC(CedarGraphStorage* storage, 
                              LsmEngine* engine) {
  // 执行 OCC 事务
  OCCTransaction* txn = storage->BeginTransaction();
  // ... 操作 ...
  Status s = txn->Commit();
  delete txn;
  
  if (!s.ok()) {
    return Bookmark();  // 空书签
  }
  
  // 获取当前 sequence number 作为书签
  uint64_t seq = engine->GetLatestSequenceNumber();
  return Bookmark(seq);
}

// 使用书签进行因果一致性读取
void ReadWithConsistency(Session& session, Bookmark bookmark) {
  auto txn = session.BeginTransaction(bookmark);
  
  // 这个读取保证能看到书签对应事务的写入
  auto node = txn.GetNode(1);
  
  txn.Commit();
}
```

---

## 5. 推荐的最佳实践

### 5.1 新项目/新模块

**推荐使用 ExplicitTransaction (L3 API)**

理由：
- API 更简洁（RAII，无需手动 delete）
- 支持书签（因果一致性）
- 支持重试策略
- 更符合 Neo4j 风格

```cpp
// 推荐写法
void RecommendedCode(Session& session) {
  auto result = Retry(RetryPolicies::Default(), [&]() {
    auto txn = session.BeginTransaction();
    
    // 业务逻辑
    txn.PutVertex(1, 0, desc);
    auto node = txn.GetNode(2);
    
    return txn.Commit();
  });
  
  if (result.ok()) {
    session.UpdateBookmark(result.value());
  }
}
```

### 5.2 遗留代码

**保持使用 OCCTransaction，逐步迁移**

```cpp
// 遗留代码保持不动
void LegacyCode(CedarGraphStorage* storage) {
  OCCTransaction* txn = storage->BeginTransaction();
  // ... 原有逻辑 ...
  txn->Commit();
  delete txn;
}
```

### 5.3 需要强 OCC 语义的场景

**继续使用 OCCTransaction**

```cpp
// 需要精确控制读集/写集的场景
void StrongOCCRequired(CedarGraphStorage* storage) {
  OCCTransaction* txn = storage->BeginTransaction();
  
  // 读取关键数据（记录到读集）
  Descriptor desc;
  txn->Get(1, EntityType::Vertex, 0, &desc, nullptr);
  
  // 业务逻辑...
  
  // 写入
  txn->Put(1, EntityType::Vertex, 0, new_desc, ts);
  
  // OCC 验证：如果读取期间数据被修改，Commit 会失败
  Status s = txn->Commit();
  if (s.IsConflict()) {
    // 需要重试
  }
  
  delete txn;
}
```

---

## 6. 性能对比

| 指标 | OCCTransaction | ExplicitTransaction |
|------|----------------|---------------------|
| **单次操作延迟** | 低（直接操作 memtable） | 稍高（通过 CedarUpdate 封装） |
| **批量写入** | 一般 | 优（CedarUpdate 批量优化） |
| **读取性能** | 优 | 优（都使用 CedarScan） |
| **并发冲突检测** | 强（OCC 验证） | 中等（依赖 StrictLevel） |
| **内存占用** | 高（读集/写集） | 低（只记录写操作） |
| **API 复杂度** | 高（需管理生命周期） | 低（RAII） |

---

## 7. 总结

### ✅ 可以协作

1. **数据层面**：两者操作同一个存储，数据完全互通
2. **读取层面**：CedarScan 可以看到 OCCTransaction 提交的数据
3. **书签层面**：可以通过 sequence number 桥接

### ⚠️ 注意事项

1. **不要混用**：同一线程/逻辑事务中不要混用两种 API
2. **隔离级别**：OCCTransaction 提供更强的 OCC 保证
3. **生命周期**：OCCTransaction 需手动 delete，ExplicitTransaction 自动管理

### 📋 建议

- **新项目**：直接使用 ExplicitTransaction（L3 API）
- **遗留代码**：保持 OCCTransaction，需要时迁移
- **书签功能**：ExplicitTransaction 原生支持，OCCTransaction 需桥接
