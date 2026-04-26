# CedarGraph L3 显式事务实现方案

## 1. 当前 API 确认

### 写操作 - CedarUpdate
```cpp
CedarUpdate update = CedarUpdate::Create(StrictLevel::STRICT);
update.At(Timestamp(1000))
      .PutVertex(1, 0, descriptor)
      .PutEdge(1, 2, 0, descriptor)
      .Property(1, EntityType::Vertex, 1, value);

CedarStatus status = update.Apply(storage);  // 提交
```

### 读操作 - CedarScan
```cpp
CedarScan scan = CedarScan::At(Timestamp(1000), engine);
auto node = scan.GetNode(1);
auto edges = scan.OutEdges(1, 0);
```

---

## 2. L3 显式事务 API 设计

### 2.1 核心接口

```cpp
// include/cedar/driver/explicit_transaction.h
namespace cedar {
namespace driver {

// 事务配置
struct TransactionConfig {
  // 超时配置
  std::chrono::milliseconds timeout{30000};
  
  // 元数据（用于监控/日志）
  std::unordered_map<std::string, std::string> metadata;
  
  // 严格级别（控制校验强度）
  StrictLevel strict_level = StrictLevel::STRICT;
  
  // 是否启用锚点优化
  bool enable_anchor_optimization = true;
  
  // 是否记录读集（用于 OCC 验证）
  bool track_read_set = true;
  
  // 书签（因果一致性）
  std::optional<Bookmark> bookmark;
};

// 显式事务 - L3 API
class ExplicitTransaction {
 public:
  // 禁止拷贝
  ExplicitTransaction(const ExplicitTransaction&) = delete;
  ExplicitTransaction& operator=(const ExplicitTransaction&) = delete;
  
  // 允许移动
  ExplicitTransaction(ExplicitTransaction&&) noexcept;
  ExplicitTransaction& operator=(ExplicitTransaction&&) noexcept;
  
  ~ExplicitTransaction();
  
  // ========== 写操作 ==========
  
  // 创建/更新节点
  ExplicitTransaction& PutVertex(uint64_t vertex_id,
                                  uint16_t label_id,
                                  const Descriptor& descriptor);
  
  // 删除节点
  ExplicitTransaction& DeleteVertex(uint64_t vertex_id);
  
  // 创建/更新边
  ExplicitTransaction& PutEdge(uint64_t src_id,
                                uint64_t dst_id,
                                uint16_t edge_type,
                                const Descriptor& descriptor);
  
  // 删除边
  ExplicitTransaction& DeleteEdge(uint64_t src_id,
                                   uint64_t dst_id,
                                   uint16_t edge_type);
  
  // 设置属性
  ExplicitTransaction& PutProperty(uint64_t entity_id,
                                    EntityType type,
                                    uint16_t prop_id,
                                    const Descriptor& value);
  
  // ========== 读操作 ==========
  
  // 获取节点（自动处理版本折叠）
  std::optional<NodeView> GetNode(uint64_t vertex_id);
  
  // 扫描出边
  EdgeIterator OutEdges(uint64_t src_id, uint16_t edge_type = EdgeIterator::kAllLabels);
  
  // 扫描入边
  EdgeIterator InEdges(uint64_t dst_id, uint16_t edge_type = EdgeIterator::kAllLabels);
  
  // 获取属性
  std::optional<Descriptor> GetProperty(uint64_t entity_id,
                                         EntityType type,
                                         uint16_t prop_id);
  
  // 检查实体是否存在
  bool Exists(uint64_t entity_id, EntityType type);
  
  // ========== 事务控制 ==========
  
  // 提交事务
  // 返回：成功时包含书签，失败时包含冲突信息
  Result<Bookmark, ConflictInfo> Commit();
  
  // 回滚事务
  void Rollback();
  
  // 关闭事务（未提交则自动回滚）
  void Close();
  
  // ========== 状态查询 ==========
  
  bool IsOpen() const;
  TransactionState GetState() const;
  
  // 获取当前书签（提交前为空）
  std::optional<Bookmark> GetBookmark() const;
  
  // 获取事务开始时间
  Timestamp GetStartTime() const;
  
  // 获取读集统计
  size_t GetReadSetSize() const;
  size_t GetWriteSetSize() const;
  
 private:
  friend class Session;
  explicit ExplicitTransaction(std::shared_ptr<CedarGraphStorage> storage,
                                const TransactionConfig& config);
  
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace driver
}  // namespace cedar
```

---

## 3. 重试策略设计

### 3.1 错误分类

```cpp
// include/cedar/driver/retry_policy.h
namespace cedar {
namespace driver {

// 错误分类
enum class ErrorClass {
  // 客户端错误 - 不应重试
  kClientError,
  
  // 瞬态错误 - 可以重试
  // 锁冲突、OCC 验证失败、临时超时
  kTransientError,
  
  // 系统错误 - 不应重试
  // 磁盘满、数据损坏、配置错误
  kSystemError,
  
  // 可用性错误 - 可以重试（未来分布式）
  // 节点不可用、网络超时
  kAvailabilityError,
};

// 错误分类器
class ErrorClassifier {
 public:
  static ErrorClass Classify(const CedarStatus& status);
  static ErrorClass Classify(const std::exception& ex);
  
  // 是否可重试
  static bool IsRetryable(const CedarStatus& status) {
    auto cls = Classify(status);
    return cls == ErrorClass::kTransientError ||
           cls == ErrorClass::kAvailabilityError;
  }
};

// 具体错误到分类的映射
inline ErrorClass ErrorClassifier::Classify(const CedarStatus& status) {
  switch (status.code()) {
    // 瞬态错误 - 可重试
    case StatusCode::kConflict:           // OCC 冲突
    case StatusCode::kLockTimeout:        // 锁超时
    case StatusCode::kBusy:               // 系统繁忙
    case StatusCode::kTryAgain:           // 临时失败
      return ErrorClass::kTransientError;
    
    // 可用性错误 - 可重试（未来）
    case StatusCode::kNotLeader:          // 非 Leader 节点
    case StatusCode::kConnectionReset:    // 连接重置
    case StatusCode::kTimeout:            // 网络超时
      return ErrorClass::kAvailabilityError;
    
    // 系统错误 - 不可重试
    case StatusCode::kIOError:
    case StatusCode::kCorruption:
    case StatusCode::kNotSupported:
      return ErrorClass::kSystemError;
    
    // 客户端错误 - 不可重试
    case StatusCode::kInvalidArgument:
    case StatusCode::kNotFound:
    case StatusCode::kAlreadyExists:
    default:
      return ErrorClass::kClientError;
  }
}

}  // namespace driver
}  // namespace cedar
```

### 3.2 重试策略配置

```cpp
// include/cedar/driver/retry_policy.h
namespace cedar {
namespace driver {

// 退避策略类型
enum class BackoffStrategy {
  kFixed,      // 固定间隔
  kLinear,     // 线性增长
  kExponential, // 指数增长（推荐）
};

// 重试配置
struct RetryConfig {
  // 最大尝试次数（包括首次尝试）
  // 默认: 3（首次 + 2次重试）
  size_t max_attempts = 3;
  
  // 初始退避时间
  std::chrono::milliseconds initial_backoff{100};
  
  // 最大退避时间
  std::chrono::milliseconds max_backoff{5000};
  
  // 退避策略
  BackoffStrategy backoff_strategy = BackoffStrategy::kExponential;
  
  // 是否添加抖动（防止惊群）
  // 抖动范围: [0, backoff * jitter_factor]
  bool jitter = true;
  double jitter_factor = 0.1;  // 10% 抖动
  
  // 可重试错误判断函数（自定义扩展点）
  std::function<bool(const CedarStatus&)> retry_predicate;
  
  // 每次重试前的回调（用于日志/监控）
  std::function<void(size_t attempt, const CedarStatus& error, 
                     std::chrono::milliseconds delay)> on_retry;
};

// 重试策略执行器
class RetryPolicy {
 public:
  explicit RetryPolicy(RetryConfig config);
  
  // 执行带重试的操作
  template<typename Func>
  auto Execute(Func&& func) -> decltype(func()) {
    using ReturnType = decltype(func());
    
    std::chrono::milliseconds current_delay = config_.initial_backoff;
    
    for (size_t attempt = 1; attempt <= config_.max_attempts; ++attempt) {
      auto result = func();
      
      // 成功或最后一次尝试
      if (IsSuccess(result) || attempt == config_.max_attempts) {
        return result;
      }
      
      // 获取错误并判断是否可重试
      auto error = ExtractError(result);
      if (!ShouldRetry(error)) {
        return result;  // 不可重试，直接返回错误
      }
      
      // 计算下一次延迟
      if (attempt < config_.max_attempts) {
        auto delay = CalculateDelay(current_delay, attempt);
        
        if (config_.on_retry) {
          config_.on_retry(attempt, error, delay);
        }
        
        std::this_thread::sleep_for(delay);
        
        // 更新下一次的延迟
        current_delay = NextDelay(current_delay);
      }
    }
    
    unreachable();  // 逻辑上不会到达
  }
  
 private:
  RetryConfig config_;
  
  // 计算延迟（包含抖动）
  std::chrono::milliseconds CalculateDelay(
      std::chrono::milliseconds base_delay, 
      size_t attempt) const;
  
  // 计算下一次延迟（不含抖动，用于状态更新）
  std::chrono::milliseconds NextDelay(
      std::chrono::milliseconds current) const;
};

// 计算延迟实现
inline std::chrono::milliseconds RetryPolicy::CalculateDelay(
    std::chrono::milliseconds base_delay, 
    size_t attempt) const {
  
  std::chrono::milliseconds delay = base_delay;
  
  // 添加抖动
  if (config_.jitter) {
    static thread_local std::mt19937 rng(std::random_device{}());
    std::uniform_real_distribution<double> dist(0.0, config_.jitter_factor);
    double jitter_mult = 1.0 + dist(rng);
    delay = std::chrono::milliseconds(
        static_cast<int64_t>(delay.count() * jitter_mult));
  }
  
  return delay;
}

inline std::chrono::milliseconds RetryPolicy::NextDelay(
    std::chrono::milliseconds current) const {
  
  switch (config_.backoff_strategy) {
    case BackoffStrategy::kFixed:
      return config_.initial_backoff;
    
    case BackoffStrategy::kLinear:
      return std::min(
          current + config_.initial_backoff,
          config_.max_backoff);
    
    case BackoffStrategy::kExponential:
      return std::min(
          std::chrono::milliseconds(current.count() * 2),
          config_.max_backoff);
  }
  
  return config_.initial_backoff;
}

}  // namespace driver
}  // namespace cedar
```

### 3.3 预设重试策略

```cpp
// include/cedar/driver/retry_policy.h
namespace cedar {
namespace driver {

// 预设重试策略工厂
class RetryPolicies {
 public:
  // 默认策略 - 适合大多数场景
  // 3次尝试，指数退避 100ms -> 200ms -> 400ms
  static RetryConfig Default() {
    return RetryConfig{
      .max_attempts = 3,
      .initial_backoff = std::chrono::milliseconds(100),
      .max_backoff = std::chrono::milliseconds(1000),
      .backoff_strategy = BackoffStrategy::kExponential,
      .jitter = true,
    };
  }
  
  // 激进策略 - 适合高冲突场景
  // 5次尝试，更短的初始延迟
  static RetryConfig Aggressive() {
    return RetryConfig{
      .max_attempts = 5,
      .initial_backoff = std::chrono::milliseconds(10),
      .max_backoff = std::chrono::milliseconds(500),
      .backoff_strategy = BackoffStrategy::kExponential,
      .jitter = true,
    };
  }
  
  // 保守策略 - 适合低频关键操作
  // 3次尝试，较长延迟，避免系统过载
  static RetryConfig Conservative() {
    return RetryConfig{
      .max_attempts = 3,
      .initial_backoff = std::chrono::milliseconds(500),
      .max_backoff = std::chrono::milliseconds(5000),
      .backoff_strategy = BackoffStrategy::kLinear,
      .jitter = true,
    };
  }
  
  // 不重试 - 适合一次性操作
  static RetryConfig NoRetry() {
    return RetryConfig{
      .max_attempts = 1,
      .initial_backoff = std::chrono::milliseconds(0),
    };
  }
  
  // 仅瞬态错误重试
  static RetryConfig TransientOnly() {
    auto config = Default();
    config.retry_predicate = [](const CedarStatus& status) {
      return ErrorClassifier::Classify(status) == ErrorClass::kTransientError;
    };
    return config;
  }
};

}  // namespace driver
}  // namespace cedar
```

---

## 4. 使用示例

### 4.1 基础 L3 事务

```cpp
#include "cedar/driver/session.h"
#include "cedar/driver/explicit_transaction.h"

using namespace cedar::driver;

void BasicTransactionExample(Driver& driver) {
  auto session = driver.NewSession();
  
  // 开始显式事务
  auto txn = session->BeginTransaction(TransactionConfig{
    .timeout = std::chrono::seconds(30),
    .metadata = {{"operation", "transfer"}, {"request_id", "uuid-123"}},
    .strict_level = StrictLevel::STRICT,
  });
  
  try {
    // 1. 读取检查
    auto from_account = txn.GetNode(1001);
    if (!from_account) {
      throw std::runtime_error("Source account not found");
    }
    
    double balance = from_account->GetProperty("balance").AsDouble();
    if (balance < 1000.0) {
      throw std::runtime_error("Insufficient balance");
    }
    
    // 2. 写入操作
    txn.PutProperty(1001, EntityType::Vertex, 
                    PROPERTY_BALANCE, 
                    Descriptor::Double(balance - 1000.0));
    
    auto to_account = txn.GetNode(1002);
    if (to_account) {
      double to_balance = to_account->GetProperty("balance").AsDouble();
      txn.PutProperty(1002, EntityType::Vertex,
                      PROPERTY_BALANCE,
                      Descriptor::Double(to_balance + 1000.0));
    } else {
      // 创建新账户
      txn.PutVertex(1002, LABEL_ACCOUNT, 
                    Descriptor::Object({{"balance", 1000.0}}));
    }
    
    // 3. 记录交易
    txn.PutEdge(1001, 1002, EDGE_TRANSFER,
                Descriptor::Object({
                  {"amount", 1000.0},
                  {"timestamp", std::time(nullptr)}
                }));
    
    // 4. 提交
    auto result = txn.Commit();
    if (!result.ok()) {
      // 获取冲突详情
      auto conflict = result.error();
      std::cerr << "Conflict: " << conflict.ToString() << std::endl;
    } else {
      // 获取书签用于因果一致性
      Bookmark bookmark = result.value();
      std::cout << "Transaction committed, bookmark: " << bookmark.ToString() << std::endl;
    }
    
  } catch (const std::exception& e) {
    txn.Rollback();
    throw;
  }
}
```

### 4.2 带重试的显式事务

```cpp
void TransactionWithRetry(Driver& driver) {
  auto session = driver.NewSession();
  
  // 使用激进重试策略
  RetryPolicy retry_policy(RetryPolicies::Aggressive());
  
  // 配置重试回调（用于监控）
  auto config = RetryPolicies::Aggressive();
  config.on_retry = [](size_t attempt, const CedarStatus& error, 
                       std::chrono::milliseconds delay) {
    std::cout << "Retry attempt " << attempt 
              << " after " << delay.count() << "ms"
              << " due to " << error.ToString() << std::endl;
  };
  
  // 执行带重试的事务
  auto result = retry_policy.Execute([&]() -> Result<Bookmark, CedarStatus> {
    auto txn = session->BeginTransaction();
    
    try {
      // 业务逻辑
      txn.PutVertex(1, LABEL_USER, 
                    Descriptor::Object({{"name", "Alice"}}));
      
      auto result = txn.Commit();
      if (!result.ok()) {
        txn.Rollback();
      }
      return result;
      
    } catch (...) {
      txn.Rollback();
      throw;
    }
  });
  
  if (!result.ok()) {
    std::cerr << "Transaction failed after retries: " 
              << result.error().ToString() << std::endl;
  }
}
```

### 4.3 因果一致性（书签）

```cpp
void CausalConsistencyExample(Driver& driver) {
  // 写入操作
  Bookmark bookmark;
  {
    auto session = driver.NewSession();
    auto txn = session->BeginTransaction();
    
    txn.PutVertex(1, LABEL_USER, 
                  Descriptor::Object({{"name", "Alice"}}));
    
    auto result = txn.Commit();
    if (result.ok()) {
      bookmark = result.value();
    }
  }
  
  // 另一个会话读取（保证看到写入）
  {
    auto session = driver.NewSession();
    
    // 方式1: 在 BeginTransaction 时指定书签
    auto txn = session->BeginTransaction(TransactionConfig{
      .bookmark = bookmark
    });
    
    // 这个读取保证能看到上面的写入
    auto node = txn.GetNode(1);
    assert(node.has_value());  // 一定存在
    
    txn.Commit();
  }
  
  // 方式2: 使用 L1 API 直接带书签
  auto result = driver.ExecuteQuery(
    "MATCH (n:User {id: 1}) RETURN n.name",
    Params(),
    bookmark  // 确保因果一致性
  );
}
```

### 4.4 复杂事务模式

```cpp
void ComplexTransactionPattern(Driver& driver) {
  auto session = driver.NewSession();
  
  // 长事务示例（包含多次读取和条件写入）
  auto txn = session->BeginTransaction(TransactionConfig{
    .timeout = std::chrono::minutes(5),  // 长超时
    .track_read_set = true,               // 启用 OCC
  });
  
  try {
    // 阶段1: 批量读取
    std::vector<uint64_t> user_ids = {1, 2, 3, 4, 5};
    std::vector<NodeView> users;
    
    for (auto id : user_ids) {
      if (auto user = txn.GetNode(id)) {
        users.push_back(*user);
      }
    }
    
    // 阶段2: 业务逻辑处理
    auto active_users = FilterActiveUsers(users);
    auto groups = GroupByRegion(active_users);
    
    // 阶段3: 条件写入
    for (auto& [region, members] : groups) {
      if (members.size() > 10) {
        // 创建区域组
        uint64_t group_id = GenerateId();
        txn.PutVertex(group_id, LABEL_GROUP,
                      Descriptor::Object({{
                        {"region", region},
                        {"member_count", members.size()}
                      }}));
        
        // 建立成员关系
        for (auto& member : members) {
          txn.PutEdge(member.id(), group_id, EDGE_BELONGS_TO, {});
        }
      }
    }
    
    // 阶段4: 验证和提交
    // OCC 会自动验证读集未被修改
    auto result = txn.Commit();
    
    if (!result.ok()) {
      // OCC 冲突 - 可以获取详细信息
      auto conflict = result.error();
      
      switch (conflict.type()) {
        case ConflictType::kReadWrite:
          std::cerr << "Read set modified by concurrent transaction" << std::endl;
          break;
        case ConflictType::kWriteWrite:
          std::cerr << "Write conflict on entity " << conflict.entity_id() << std::endl;
          break;
        case ConflictType::kConstraintViolation:
          std::cerr << "Constraint violated: " << conflict.message() << std::endl;
          break;
      }
    }
    
  } catch (const std::exception& e) {
    txn.Rollback();
    throw;
  }
}
```

---

## 5. 与现有代码集成

```
新 ExplicitTransaction        现有 CedarGraph 组件
────────────────────────────────────────────────────────
PutVertex()           ───▶   CedarUpdate::PutVertex()
PutEdge()             ───▶   CedarUpdate::PutEdge()
PutProperty()         ───▶   CedarUpdate::Property()
GetNode()             ───▶   CedarScan::GetNode()
OutEdges()            ───▶   CedarScan::OutEdges()
InEdges()             ───▶   CedarScan::InEdges()
Commit()              ───▶   CedarUpdate::Apply() + OCC验证
Rollback()            ───▶   清理 CedarUpdate
Bookmark              ───▶   GetLatestSequenceNumber()
```

---

## 6. 实现优先级

### Phase 1: 核心 L3 API
- [ ] ExplicitTransaction 类实现
- [ ] 集成 CedarUpdate（写操作）
- [ ] 集成 CedarScan（读操作）
- [ ] Commit/Rollback 实现

### Phase 2: 书签支持
- [ ] Bookmark 类（基于 sequence number）
- [ ] 因果一致性检查

### Phase 3: 重试策略
- [ ] ErrorClassifier
- [ ] RetryPolicy
- [ ] 预设策略工厂

### Phase 4: Session 和 Driver
- [ ] Session 类
- [ ] Driver 类
- [ ] L1/L2 API（后续）
