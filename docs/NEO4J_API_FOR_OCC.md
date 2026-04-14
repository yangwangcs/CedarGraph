# Neo4j 风格 API 封装现有 OCCTransaction

## 核心思想

```
┌─────────────────────────────────────────────────────────────────┐
│                     Neo4j 风格三层 API                          │
│  L1: driver.ExecuteQuery()       - 自动查询                      │
│  L2: session.ExecuteWrite()      - 托管事务（自动重试）           │
│  L3: session.BeginTransaction()  - 显式事务（完全控制）           │
└─────────────────────────┬───────────────────────────────────────┘
                          │ 封装层
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                     OCCTransaction（不变）                      │
│  - 乐观并发控制 (OCC)                                           │
│  - TransactionManager                                           │
│  - WalWriter                                                    │
│  - 读集/写集跟踪                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**原则**：底层 OCC 机制完全不变，只在上层封装简洁的 API

---

## 1. 三层 API 设计

### 1.1 L3 显式事务 - 最接近现有代码

```cpp
// 原有使用方式
OCCTransaction* txn = storage->BeginTransaction();
txn->Put(1, EntityType::Vertex, 0, desc, ts);
Status s = txn->Commit();
if (!s.ok()) txn->Abort();
delete txn;

// Neo4j 风格封装
auto txn = session.BeginTransaction();
txn.Put(1, EntityType::Vertex, 0, desc);  // 内部调用 txn_->Put()
auto result = txn.Commit();  // 返回 Bookmark，内部调用 txn_->Commit()
if (!result.ok()) txn.Rollback();  // 内部调用 txn_->Abort()
// 自动 delete txn_
```

### 1.2 L2 托管事务 - 自动重试

```cpp
// 自动处理 OCC Conflict 重试
session.ExecuteWrite([](ManagedTransaction& txn) {
    txn.Put(1, EntityType::Vertex, 0, desc);
    // 自动 Commit
    // 如果 Conflict 自动重试回调
});
```

### 1.3 L1 自动查询 - 单条操作

```cpp
// 单条操作，自动事务
Result result = driver.ExecuteQuery([&](Transaction& txn) {
    return txn.Get(1, EntityType::Vertex, 0, &desc);
});
```

---

## 2. 具体实现

### 2.1 Bookmark（基于现有 timestamp）

```cpp
// 使用 OCCTransaction 的 read_timestamp_ 或 commit_timestamp_
class Bookmark {
  uint64_t timestamp_;  // 直接使用 Timestamp::value()
  uint64_t txn_id_;     // TransactionManager 分配的事务 ID
};
```

### 2.2 Session 管理 OCCTransaction

```cpp
class Session {
 public:
  // L3: 显式事务
  ExplicitTransaction BeginTransaction(const TransactionConfig& config) {
    OCCTransaction* occ_txn = txn_manager_->BeginTransaction(config.options);
    return ExplicitTransaction(occ_txn, config);
  }
  
 private:
  TransactionManager* txn_manager_;
};
```

### 2.3 ExplicitTransaction 包装 OCCTransaction

```cpp
class ExplicitTransaction {
 public:
  // 写操作 - 转发到 OCCTransaction
  void Put(uint64_t entity_id, EntityType type, uint16_t col, 
           const Descriptor& desc) {
    txn_->Put(entity_id, type, col, desc, current_timestamp_);
  }
  
  // 读操作 - 转发到 OCCTransaction
  Status Get(uint64_t entity_id, EntityType type, uint16_t col,
             Descriptor* desc) {
    return txn_->Get(entity_id, type, col, desc, &version_ts_);
  }
  
  // 提交 - 调用 OCCTransaction::Commit()
  Result<Bookmark> Commit() {
    Status s = txn_->Commit();
    if (s.ok()) {
      return Bookmark(txn_->GetCommitTimestamp(), txn_->GetTransactionId());
    }
    return Result::Err(s);
  }
  
  // 回滚 - 调用 OCCTransaction::Abort()
  void Rollback() {
    txn_->Abort();
  }
  
  ~ExplicitTransaction() {
    if (txn_) {
      txn_->Abort();
      delete txn_;
    }
  }
  
 private:
  OCCTransaction* txn_;
  TransactionConfig config_;
};
```

---

## 3. 保持不变的内部机制

| 组件 | 状态 | 说明 |
|------|------|------|
| OCCTransaction | ✅ 不变 | 核心 OCC 实现 |
| TransactionManager | ✅ 不变 | 事务生命周期管理 |
| WalWriter | ✅ 不变 | WAL 日志 |
| 读集/写集 | ✅ 不变 | OCC 验证基础 |
| Timestamp 分配 | ✅ 不变 | 现有机制 |

---

## 4. 迁移示例

### 迁移前（直接使用 OCCTransaction）

```cpp
void OldCode(CedarGraphStorage* storage) {
  TransactionOptions options;
  options.isolation_level = IsolationLevel::kSnapshot;
  
  OCCTransaction* txn = storage->BeginTransaction(&options);
  
  Descriptor desc;
  desc.SetInt(42);
  
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
```

### 迁移后（Neo4j 风格 API，底层仍是 OCCTransaction）

```cpp
void NewCode(Session& session) {
  auto txn = session.BeginTransaction(TransactionConfig{
    .options = {.isolation_level = IsolationLevel::kSnapshot},
    .timeout = std::chrono::seconds(30),
  });
  
  Descriptor desc;
  desc.SetInt(42);
  
  txn.Put(1, EntityType::Vertex, 0, desc);
  txn.Put(2, EntityType::Vertex, 0, desc);
  
  auto result = txn.Commit();
  if (!result.ok()) {
    throw std::runtime_error(result.error().ToString());
  }
  
  // 自动 RAII，无需手动 delete
  // 还可以获取书签
  Bookmark bm = result.value();
}
```

---

## 5. 书签与因果一致性

```cpp
// 基于 OCCTransaction 的 commit timestamp 实现书签
class Bookmark {
 public:
  Bookmark(Timestamp ts, uint64_t txn_id) 
      : timestamp_(ts.value()), txn_id_(txn_id) {}
  
  // 使用书签创建事务（保证因果一致性）
  static OCCTransaction* BeginWithBookmark(
      TransactionManager* mgr, 
      const Bookmark& bookmark) {
    // 等待直到书签对应的时间戳被提交
    mgr->WaitForTimestamp(Timestamp(bookmark.timestamp_));
    
    // 创建新事务，确保读取能看到书签之前的写入
    return mgr->BeginTransaction();
  }
};
```

---

## 6. 重试策略集成 OCC Conflict

```cpp
class RetryPolicy {
 public:
  template<typename Func>
  auto Execute(Func&& func) {
    for (size_t i = 0; i < max_attempts_; ++i) {
      auto result = func();
      
      // OCC Conflict 是可重试错误
      if (result.status().IsConflict()) {
        if (i < max_attempts_ - 1) {
          Backoff(i);
          continue;
        }
      }
      
      return result;
    }
  }
};
```

---

## 7. 实现文件结构

```
include/cedar/driver/
├── driver.h              # Driver 类（入口）
├── session.h             # Session 类
├── explicit_transaction.h # L3 API（包装 OCCTransaction）
├── managed_transaction.h  # L2 API（托管，自动重试）
├── bookmark.h            # 书签（基于 OCCTimestamp）
└── retry_policy.h        # 重试策略（识别 OCC Conflict）

src/driver/
├── driver.cc
├── session.cc
├── explicit_transaction.cc  # 包装 OCCTransaction
└── ...
```

---

## 8. 关键设计决策

### 决策 1: 谁管理 OCCTransaction 生命周期？
**答**: ExplicitTransaction RAII 管理
```cpp
ExplicitTransaction(OCCTransaction* txn) : txn_(txn) {}
~ExplicitTransaction() {
  if (txn_) { txn_->Abort(); delete txn_; }
}
```

### 决策 2: Bookmark 基于什么？
**答**: OCCTransaction 的 commit_timestamp_
```cpp
Bookmark GetBookmark() const {
  return Bookmark(txn_->GetCommitTimestamp(), txn_->GetTransactionId());
}
```

### 决策 3: 如何处理 OCC Conflict？
**答**: RetryPolicy 识别 Conflict 状态码自动重试
```cpp
if (status.IsConflict()) {
  // 这是瞬态错误，可以重试
  Retry();
}
```

### 决策 4: L2 托管事务如何工作？
**答**: 自动创建/提交 OCCTransaction，Conflict 时重试整个回调
```cpp
ExecuteWrite([](ManagedTransaction& mtxn) {
  // 这里是一个完整的 OCCTransaction 生命周期
  // 如果 Commit 返回 Conflict，整个回调会重新执行
  mtxn.Put(...);
  mtxn.Put(...);
  // 自动 Commit
});
```
