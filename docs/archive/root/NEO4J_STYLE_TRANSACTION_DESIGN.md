# CedarGraph Neo4j Bolt 风格事务接口设计方案

## 1. 现状分析

### 1.1 当前架构
```
CedarGraph 当前事务层:
- OCCTransaction: 乐观并发控制事务实现
- TransactionManager: 事务生命周期管理
- WalWriter: WAL 日志写入
- CedarGraph: 图操作接口
- LsmEngine: 底层存储引擎

当前使用方式:
```cpp
auto txn = db->BeginTransaction(options);
txn->Put(vertex_id, EntityType::Vertex, column, descriptor);
txn->Commit();
```

### 1.2 与 Neo4j 的差距
| 能力 | Neo4j | CedarGraph 当前 |
|------|-------|----------------|
| 三级事务 API | ✅ | ❌ (仅显式) |
| 自动重试 | ✅ | ❌ |
| 因果一致性/书签 | ✅ | ❌ |
| Session 概念 | ✅ | ❌ |
| 托管事务 | ✅ | ❌ |
| 自动查询 | ✅ | ❌ |

---

## 2. 目标设计

### 2.1 三级 API 架构

```cpp
// L1: 自动查询 - 最简单，自动处理一切
Result result = driver.ExecuteQuery(
    "MATCH (n:User) WHERE n.id = $id RETURN n",
    Params({{"id", 123}})
);

// L2: 托管事务 - 自动重试，适合业务逻辑
auto result = session.ExecuteWrite([](Transaction& tx) {
    tx.Run("CREATE (n:User {name: $name})", Params({{"name", "Alice"}}));
    return tx.Run("MATCH (n:User) RETURN count(n)");
});

// L3: 显式事务 - 完全控制
auto txn = session.BeginTransaction();
try {
    txn.Run("CREATE (n:Order {id: $id})", Params({{"id", 123}}));
    txn.Commit();
} catch (...) {
    txn.Rollback();
}
```

---

## 3. 核心组件设计

### 3.1 Bookmark (因果一致性令牌)

```cpp
// include/cedar/driver/bookmark.h
namespace cedar {
namespace driver {

// 书签代表数据库的某个逻辑状态点
class Bookmark {
 public:
  Bookmark() = default;
  explicit Bookmark(std::string data);
  
  // 序列化/反序列化（用于跨服务传递）
  std::string ToString() const;
  static std::optional<Bookmark> FromString(const std::string& str);
  
  // 内部使用：获取存储引擎的序列号
  uint64_t GetSequenceNumber() const { return seq_num_; }
  
  // 合并多个书签（取最大值）
  static Bookmark Combine(const std::vector<Bookmark>& bookmarks);
  
  bool IsEmpty() const { return seq_num_ == 0; }
  
 private:
  uint64_t seq_num_ = 0;
  std::string server_id_;  // 用于分布式场景
  uint64_t timestamp_ = 0;
};

// 书签管理器 - 集中管理跨 Session 的书签
class BookmarkManager {
 public:
  // 更新最新书签
  void UpdateBookmark(const Bookmark& bookmark);
  
  // 获取当前书签（用于确保后续读取能看到之前的写入）
  Bookmark GetBookmark() const;
  
  // 等待直到指定书签被应用（因果一致性保证）
  Status AwaitBookmark(const Bookmark& bookmark, std::chrono::milliseconds timeout);
  
 private:
  mutable std::mutex mutex_;
  Bookmark current_bookmark_;
  std::condition_variable cv_;
};

}  // namespace driver
}  // namespace cedar
```

### 3.2 Driver (顶层入口)

```cpp
// include/cedar/driver/driver.h
namespace cedar {
namespace driver {

struct DriverConfig {
  // 连接配置
  std::string db_path;
  CedarOptions db_options;
  
  // 连接池配置
  size_t max_connections = 10;
  size_t min_connections = 1;
  
  // 重试配置
  size_t max_retry_count = 3;
  std::chrono::milliseconds retry_delay{100};
  
  // 超时配置
  std::chrono::milliseconds default_timeout{30000};
  
  // 日志/监控
  std::string application_name;
  std::unordered_map<std::string, std::string> default_metadata;
};

class Driver {
 public:
  // 工厂方法
  static std::shared_ptr<Driver> New(const DriverConfig& config);
  
  // 关闭驱动
  void Close();
  
  // ========== L1 API: 自动查询 ==========
  
  // 执行查询（自动处理事务、重试、书签）
  Result ExecuteQuery(
      const std::string& query,
      const Params& params = {},
      const QueryConfig& config = {});
  
  // 执行查询（带书签的因果一致性版本）
  Result ExecuteQuery(
      const std::string& query,
      const Params& params,
      const Bookmark& bookmark,
      const QueryConfig& config = {});
  
  // ========== L2/L3 API: Session 入口 ==========
  
  // 创建会话（自动管理书签）
  std::unique_ptr<Session> NewSession();
  
  // 创建会话（指定访问模式）
  std::unique_ptr<Session> NewSession(AccessMode mode);
  
  // 创建会话（完全自定义配置）
  std::unique_ptr<Session> NewSession(const SessionConfig& config);
  
  // ========== 书签管理 ==========
  
  std::shared_ptr<BookmarkManager> GetBookmarkManager();
  
  // 获取数据库统计信息
  DatabaseInfo GetDatabaseInfo() const;
  
 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace driver
}  // namespace cedar
```

### 3.3 Session (事务容器)

```cpp
// include/cedar/driver/session.h
namespace cedar {
namespace driver {

enum class AccessMode {
  kRead,   // 只读 - 可路由到副本（未来分布式）
  kWrite   // 读写 - 必须到主节点
};

struct SessionConfig {
  AccessMode default_access_mode = AccessMode::kWrite;
  std::chrono::milliseconds default_timeout{30000};
  std::unordered_map<std::string, std::string> default_metadata;
  std::shared_ptr<BookmarkManager> bookmark_manager;
  // 是否自动管理书签（默认 true）
  bool auto_bookmark_management = true;
};

class Session {
 public:
  Session(const SessionConfig& config, std::shared_ptr<Driver> driver);
  ~Session();
  
  // 禁止拷贝，允许移动
  Session(const Session&) = delete;
  Session& operator=(const Session&) = delete;
  Session(Session&&) noexcept;
  Session& operator=(Session&&) noexcept;
  
  // ========== L2 API: 托管事务（自动重试）==========
  
  // 执行写事务（自动重试 transient 错误）
  template<typename Func>
  auto ExecuteWrite(Func&& func, const TransactionConfig& config = {}) 
      -> decltype(func(std::declval<ManagedTransaction&>()));
  
  // 执行读事务（可路由到副本）
  template<typename Func>
  auto ExecuteRead(Func&& func, const TransactionConfig& config = {})
      -> decltype(func(std::declval<ManagedTransaction&>()));
  
  // 执行事务（自动选择读写模式）
  template<typename Func>
  auto ExecuteTransaction(AccessMode mode, Func&& func, const TransactionConfig& config = {})
      -> decltype(func(std::declval<ManagedTransaction&>()));
  
  // ========== L3 API: 显式事务 ==========
  
  // 开始显式事务
  ExplicitTransaction BeginTransaction(const TransactionConfig& config = {});
  
  // 开始显式事务（指定书签保证因果一致性）
  ExplicitTransaction BeginTransaction(const Bookmark& bookmark, 
                                        const TransactionConfig& config = {});
  
  // ========== L1 API: 自动提交查询 ==========
  
  // 单条查询（自动提交事务）
  Result Run(const std::string& query, const Params& params = {});
  
  // ========== 书签管理 ==========
  
  // 获取本会话的最新书签
  Bookmark GetLastBookmark() const;
  
  // 手动设置书签（用于跨会话一致性）
  void SetBookmark(const Bookmark& bookmark);
  
  // 等待直到指定书签被应用
  Status AwaitBookmark(const Bookmark& bookmark, 
                       std::chrono::milliseconds timeout = std::chrono::seconds(30));
  
  // ========== 状态查询 ==========
  
  bool IsOpen() const;
  void Close();
  
 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace driver
}  // namespace cedar
```

### 3.4 ManagedTransaction (托管事务)

```cpp
// include/cedar/driver/managed_transaction.h
namespace cedar {
namespace driver {

// 托管事务 - 在 ExecuteWrite/ExecuteRead 回调中使用
// 特点：
// 1. 自动处理重试（回调可能被多次执行）
// 2. 幂等性要求：回调函数必须是幂等的
// 3. 不能手动 commit/rollback（由驱动控制）
class ManagedTransaction {
 public:
  // 执行查询
  Result Run(const std::string& query, const Params& params = {});
  
  // 获取事务状态
  bool IsOpen() const;
  
  // 获取事务元数据
  uint64_t GetTransactionId() const;
  
 private:
  // 只有 Session 可以创建
  friend class Session;
  explicit ManagedTransaction(std::shared_ptr<OCCTransaction> txn);
  
  std::shared_ptr<OCCTransaction> txn_;
};

}  // namespace driver
}  // namespace cedar
```

### 3.5 ExplicitTransaction (显式事务)

```cpp
// include/cedar/driver/explicit_transaction.h
namespace cedar {
namespace driver {

struct TransactionConfig {
  std::chrono::milliseconds timeout{30000};
  std::unordered_map<std::string, std::string> metadata;
  AccessMode access_mode = AccessMode::kWrite;
};

// 显式事务 - 完全手动控制
class ExplicitTransaction {
 public:
  // 执行查询
  Result Run(const std::string& query, const Params& params = {});
  
  // 提交事务
  Status Commit();
  
  // 回滚事务
  Status Rollback();
  
  // 关闭事务（自动回滚未提交的事务）
  void Close();
  
  // 获取事务状态
  bool IsOpen() const;
  TransactionState GetState() const;
  
  // 获取书签（提交后才有）
  Bookmark GetBookmark() const;
  
  ~ExplicitTransaction();
  
  // 显式事务可以移动，不能拷贝
  ExplicitTransaction(ExplicitTransaction&&) noexcept;
  ExplicitTransaction& operator=(ExplicitTransaction&&) noexcept;
  
 private:
  friend class Session;
  ExplicitTransaction();
  
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace driver
}  // namespace cedar
```

### 3.6 查询结果和参数

```cpp
// include/cedar/driver/result.h
namespace cedar {
namespace driver {

// 查询参数（类型安全的键值对）
class Params {
 public:
  Params() = default;
  
  // 支持多种类型
  Params& Add(const std::string& key, int64_t value);
  Params& Add(const std::string& key, double value);
  Params& Add(const std::string& key, const std::string& value);
  Params& Add(const std::string& key, bool value);
  Params& Add(const std::string& key, const std::vector<uint8_t>& value);
  Params& Add(const std::string& key, const Timestamp& value);
  Params& Add(const std::string& key, const EntityId& value);
  
  // 初始化列表构造
  Params(std::initializer_list<std::pair<std::string, TypedValue>> init);
  
 private:
  std::unordered_map<std::string, TypedValue> params_;
};

// 查询结果
class Result {
 public:
  // 迭代结果
  class Iterator {
   public:
    bool HasNext() const;
    Record Next();
   private:
    class Impl;
    std::unique_ptr<Impl> impl_;
  };
  
  // 获取单条记录
  std::optional<Record> Single();
  
  // 获取所有记录
  std::vector<Record> ToVector();
  
  // 迭代结果
  Iterator Iterate();
  
  // 消费结果（遍历但不返回）
  void Consume();
  
  // 查询统计信息
  QueryStatistics GetStatistics() const;
  
  // 获取结果的书签（用于因果一致性）
  Bookmark GetBookmark() const;
  
 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

// 记录（一行结果）
class Record {
 public:
  // 按索引获取
  const TypedValue& operator[](size_t index) const;
  
  // 按名称获取
  const TypedValue& operator[](const std::string& key) const;
  
  // 获取所有键
  std::vector<std::string> Keys() const;
  
  // 获取所有值
  std::vector<TypedValue> Values() const;
  
  size_t Size() const;
};

}  // namespace driver
}  // namespace cedar
```

---

## 4. 使用示例

### 4.1 L1 API - 简单查询

```cpp
#include "cedar/driver/driver.h"

using namespace cedar::driver;

int main() {
  // 创建驱动
  DriverConfig config{
    .db_path = "/data/graph",
    .application_name = "payment-service"
  };
  auto driver = Driver::New(config);
  
  // 最简单的查询 - 自动处理事务、重试、书签
  auto result = driver->ExecuteQuery(
    "MATCH (u:User {id: $id}) RETURN u.name, u.email",
    Params({{"id", 12345}})
  );
  
  if (auto record = result.Single()) {
    std::cout << "Name: " << (*record)["u.name"].AsString() << std::endl;
  }
  
  driver->Close();
  return 0;
}
```

### 4.2 L2 API - 托管事务

```cpp
void TransferMoney(Driver& driver, int from, int to, double amount) {
  auto session = driver.NewSession();
  
  // 执行写事务 - 自动重试 transient 错误
  // 回调可能被多次执行，必须是幂等的
  session->ExecuteWrite([from, to, amount](ManagedTransaction& tx) {
    // 检查余额
    auto result = tx.Run(
      "MATCH (a:Account {id: $from}) RETURN a.balance",
      Params({{"from", from}})
    );
    
    auto record = result.Single().value();
    double balance = record["a.balance"].AsDouble();
    
    if (balance < amount) {
      throw InsufficientFundsException();
    }
    
    // 扣款
    tx.Run(
      "MATCH (a:Account {id: $from}) "
      "SET a.balance = a.balance - $amount",
      Params({{"from", from}, {"amount", amount}})
    );
    
    // 入账
    tx.Run(
      "MATCH (a:Account {id: $to}) "
      "SET a.balance = a.balance + $amount",
      Params({{"to", to}, {"amount", amount}})
    );
    
    return true;  // 返回任意值
  });
}
```

### 4.3 L3 API - 显式事务

```cpp
void ComplexWorkflow(Driver& driver) {
  auto session = driver.NewSession();
  
  // 开始显式事务
  auto txn = session->BeginTransaction(TransactionConfig{
    .timeout = std::chrono::seconds(60),
    .metadata = {{"workflow", "order-processing"}, {"order_id", "12345"}}
  });
  
  try {
    // 创建订单
    txn.Run(
      "CREATE (o:Order {id: $id, status: 'pending', created: timestamp()})",
      Params({{"id", 12345}})
    );
    
    // 扣减库存
    auto result = txn.Run(
      "MATCH (p:Product {id: $id}) "
      "WHERE p.stock >= $quantity "
      "SET p.stock = p.stock - $quantity "
      "RETURN p",
      Params({{"id", 100}, {"quantity", 2}})
    );
    
    if (!result.Single().has_value()) {
      throw OutOfStockException();
    }
    
    // 只有在需要时才提交
    if (ShouldCommit()) {
      txn.Commit();
      
      // 获取书签用于后续因果一致性
      auto bookmark = txn.GetBookmark();
      NotifyOtherService(bookmark);
    } else {
      txn.Rollback();
    }
  } catch (const std::exception& e) {
    txn.Rollback();
    throw;
  }
}
```

### 4.4 因果一致性（书签）

```cpp
void CrossSessionConsistency(Driver& driver) {
  // Session 1: 写入数据
  Bookmark bookmark;
  {
    auto session = driver.NewSession();
    session->ExecuteWrite([](ManagedTransaction& tx) {
      tx.Run("CREATE (u:User {id: 1, name: 'Alice'})");
    });
    bookmark = session->GetLastBookmark();
  }
  
  // Session 2: 读取（必须看到 Session 1 的写入）
  {
    auto session = driver.NewSession();
    // 方式1: 使用书签开始会话
    session->SetBookmark(bookmark);
    
    // 方式2: 在 BeginTransaction 时指定
    // auto txn = session->BeginTransaction(bookmark);
    
    auto result = session->Run("MATCH (u:User {id: 1}) RETURN u.name");
    // 保证能看到 Alice
  }
  
  // 方式3: ExecuteQuery 直接带书签
  auto result = driver->ExecuteQuery(
    "MATCH (u:User {id: 1}) RETURN u.name",
    Params(),
    bookmark  // 确保因果一致性
  );
}
```

---

## 5. 错误处理和重试策略

### 5.1 错误分类

```cpp
namespace cedar {
namespace driver {

enum class ErrorCategory {
  kClientError,       // 客户端错误（语法错误、参数错误）- 不重试
  kTransientError,    // 瞬态错误（锁冲突、超时）- 可重试
  kDatabaseError,     // 数据库错误（磁盘满、损坏）- 不重试
  kClusterError,      // 集群错误（leader 切换）- 可重试
  kNetworkError,      // 网络错误（连接断开）- 可重试
};

// 可重试异常基类
class TransientException : public std::runtime_error {
 public:
  TransientException(const std::string& msg, ErrorCategory category);
  ErrorCategory GetCategory() const { return category_; }
 private:
  ErrorCategory category_;
};

// 具体异常类型
class LockConflictException : public TransientException {
 public:
  LockConflictException() : TransientException("Lock conflict", ErrorCategory::kTransientError) {}
};

class LeaderNotAvailableException : public TransientException {
 public:
  LeaderNotAvailableException() : TransientException("Leader not available", ErrorCategory::kClusterError) {}
};

}  // namespace driver
}  // namespace cedar
```

### 5.2 重试策略配置

```cpp
struct RetryConfig {
  // 最大重试次数
  size_t max_attempts = 3;
  
  // 初始退避时间
  std::chrono::milliseconds initial_backoff{100};
  
  // 最大退避时间
  std::chrono::milliseconds max_backoff{5000};
  
  // 退避乘数（指数退避）
  double backoff_multiplier = 2.0;
  
  // 是否添加随机抖动
  bool jitter = true;
  
  // 自定义重试判断函数
  std::function<bool(const std::exception&)> retry_predicate;
};
```

---

## 6. 实现计划

### Phase 1: 基础组件
- [ ] Bookmark 和 BookmarkManager 实现
- [ ] TypedValue 和 Params 实现
- [ ] Result、Record、Iterator 实现

### Phase 2: 核心 API
- [ ] Driver 类实现
- [ ] Session 类实现
- [ ] ExplicitTransaction 实现
- [ ] ManagedTransaction 实现

### Phase 3: 高级特性
- [ ] 自动重试机制
- [ ] 连接池（复用现有 LsmEngine）
- [ ] 查询解析和路由

### Phase 4: Cypher 支持（可选）
- [ ] Cypher 查询解析器集成
- [ ] 查询计划生成
- [ ] 参数绑定

---

## 7. 与现有代码的集成点

```
新 Driver API                    现有 CedarGraph 实现
────────────────────────────────────────────────────────
Driver::New()         ───────▶   GraphDB::Open()
Session::Run()        ───────▶   CedarScan + OCCTransaction
Session::ExecuteWrite() ─────▶   OCCTransaction::Begin/Commit
Bookmark              ───────▶   GetLatestSequenceNumber()
Connection Pool       ───────▶   LsmEngine (复用)
WAL                   ───────▶   WalWriter
```

---

## 8. 性能考虑

1. **Session 复用**: Session 应该长连接，避免频繁创建/销毁
2. **书签传播**: 跨服务传递书签时只传最小必要信息（seq_num）
3. **重试代价**: 托管事务的回调必须是轻量级的幂等操作
4. **内存管理**: Result 使用流式迭代，避免大查询内存爆炸

---

## 9. 与 Neo4j 的差异

| 特性 | Neo4j | CedarGraph 方案 |
|------|-------|----------------|
| 协议 | Bolt | 原生 C++ API |
| 网络 | TCP Socket | 本地方案（未来可扩展 gRPC）|
| 序列化 | PackStream | 直接使用 Cedar 类型 |
| 查询语言 | Cypher | 当前用 Cedar API，未来可加 Cypher |
| 集群 | 原生支持 | 当前单机，书签为未来分布式做准备 |
