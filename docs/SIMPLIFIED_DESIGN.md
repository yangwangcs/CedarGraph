# 简化设计：去掉 ExplicitTransaction 中间层

## 新架构

```
┌─────────────────────────────────────────────────────────────┐
│                       Session                                │
│  - 直接创建/管理 OCCTransaction                              │
│  - 提供 Neo4j 风格 API                                       │
│  - 书签管理                                                  │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                OCCTransaction（直接暴露）                    │
│  - Begin() / Commit() / Abort()                             │
│  - Put() / Get() / Delete()                                 │
│  - 完全保持现有实现                                          │
└─────────────────────────────────────────────────────────────┘
```

## 变化点

### 1. 去掉 ExplicitTransaction 类
- 不再有包装层
- Session 直接返回 `OCCTransaction*`
- 但 Session 负责生命周期管理（RAII）

### 2. Session 简化
```cpp
class Session {
 public:
  // L3: 返回原始 OCCTransaction，但 Session 管理生命周期
  std::unique_ptr<ManagedTxn> BeginTransaction();
  
  // L2: 托管事务（自动重试）
  template<typename Func>
  auto ExecuteWrite(Func&& func);
  
 private:
  TransactionManager* txn_manager_;
  Bookmark last_bookmark_;
};
```

### 3. ManagedTxn - 轻量级 RAII 包装
只负责自动清理，不隐藏 OCCTransaction：
```cpp
class ManagedTxn {
 public:
  OCCTransaction* operator->() { return txn_; }
  OCCTransaction& operator*() { return *txn_; }
  
  // 提交时自动提取书签
  Bookmark Commit();
  
  // RAII：未提交则 Abort
  ~ManagedTxn();
  
 private:
  std::unique_ptr<OCCTransaction> txn_;
  Session* session_;  // 用于更新书签
};
```

## 使用方式对比

### 原设计（有 ExplicitTransaction）
```cpp
auto txn = session.BeginTransaction();
txn.Put(1, EntityType::Vertex, 0, desc);  // 封装层方法
auto result = txn.Commit();  // 返回 Bookmark
```

### 新设计（直接 OCCTransaction）
```cpp
auto txn = session.BeginTransaction();
txn->Put(1, EntityType::Vertex, 0, desc, Timestamp::Now());  // 原生方法
Bookmark bm = txn.Commit();  // ManagedTxn 的 Commit，自动提取书签
// 或
// txn->Commit();  // 原生 Commit
// txn->Abort();
```

## 优点

1. **更简洁**：少了一层抽象
2. **无隐藏**：用户直接使用 OCCTransaction 的完整能力
3. **零开销**：ManagedTxn 只是 RAII 包装，无额外逻辑
4. **易维护**：不需要同步两套 API

## 实现步骤

1. 删除 `explicit_transaction.h/cc`
2. 简化 `session.h/cc`：直接返回 `ManagedTxn`
3. `ManagedTxn` 只提供：
   - `operator->` 访问 OCCTransaction
   - `Commit()` 提取书签并提交
   - RAII 析构
4. 保留 `Bookmark` 和 `RetryPolicy`
