# CedarGraph 锁顺序文档

## 概述

本文档定义了 CedarGraph 中所有锁的获取顺序，以防止死锁。所有开发者必须遵守此顺序。

## 锁顺序规则

**核心原则：锁必须按照固定的全局顺序获取，不得违反。**

```
全局锁顺序（从高到低）：
1. LsmEngine::mutex_ (读写锁)
2. LsmEngine::index_mutex_ (读写锁)
3. LsmEngine::accumulated_mutex_ (读写锁)
4. LsmEngine::column_map_mutex_ (读写锁)
5. LsmEngine::flush_completion_mutex_ (互斥锁)
6. CedarGraphStorage::rep_->mutex_ (读写锁)
7. PartitionStorage::txn_mutex_ (读写锁)
8. MetricsRegistry::mutex_ (互斥锁)
```

## 详细锁说明

### 1. LsmEngine::mutex_
- **类型**: `std::shared_mutex`
- **用途**: 保护 MemTable 和 Immutable MemTable
- **获取顺序**: 最高优先级
- **使用场景**: 
  - 读操作: `std::shared_lock`
  - 写操作: `std::unique_lock`

### 2. LsmEngine::index_mutex_
- **类型**: `std::shared_mutex`
- **用途**: 保护标签索引和属性索引
- **获取顺序**: 第二优先级
- **使用场景**:
  - 查询索引: `std::shared_lock`
  - 更新索引: `std::unique_lock`

### 3. LsmEngine::accumulated_mutex_
- **类型**: `std::shared_mutex`
- **用途**: 保护累积缓冲区
- **获取顺序**: 第三优先级
- **使用场景**:
  - 查询累积数据: `std::shared_lock`
  - 更新累积数据: `std::unique_lock`

### 4. LsmEngine::column_map_mutex_
- **类型**: `std::shared_mutex`
- **用途**: 保护列映射（entity_column_map_）
- **获取顺序**: 第四优先级
- **使用场景**:
  - 查询列映射: `std::shared_lock`
  - 更新列映射: `std::unique_lock`

### 5. LsmEngine::flush_completion_mutex_
- **类型**: `std::mutex`
- **用途**: 保护 Flush 完成状态
- **获取顺序**: 第五优先级
- **使用场景**: 等待 Flush 完成

### 6. CedarGraphStorage::rep_->mutex_
- **类型**: `std::shared_mutex`
- **用途**: 保护存储引擎访问
- **获取顺序**: 第六优先级
- **使用场景**:
  - 读操作: `std::shared_lock`
  - 写操作: `std::unique_lock`

### 7. PartitionStorage::txn_mutex_
- **类型**: `std::shared_mutex`
- **用途**: 保护事务状态
- **获取顺序**: 第七优先级
- **使用场景**:
  - 查询事务状态: `std::shared_lock`
  - 更新事务状态: `std::unique_lock`

### 8. MetricsRegistry::mutex_
- **类型**: `std::mutex`
- **用途**: 保护指标注册表
- **获取顺序**: 最低优先级
- **使用场景**: 注册和查询指标

## 锁顺序示例

### 正确的锁获取顺序
```cpp
// 正确：按照全局顺序获取锁
std::shared_lock<std::shared_mutex> lock1(mutex_);  // LsmEngine::mutex_
std::shared_lock<std::shared_mutex> lock2(index_mutex_);  // LsmEngine::index_mutex_
```

### 错误的锁获取顺序
```cpp
// 错误：违反全局顺序
std::shared_lock<std::shared_mutex> lock1(index_mutex_);  // LsmEngine::index_mutex_
std::shared_lock<std::shared_mutex> lock2(mutex_);  // LsmEngine::mutex_ - 应该先获取
```

## 特殊情况

### 1. 事务锁
事务锁（`commit_lock_stripes_`）不遵循全局顺序，因为：
- 它们是按实体 ID 分条的
- 获取顺序由实体 ID 决定
- 使用 `std::lock_guard` 自动管理

### 2. 读写锁升级
- 不支持锁升级（从 `shared_lock` 升级到 `unique_lock`）
- 如果需要写操作，必须释放 `shared_lock` 后重新获取 `unique_lock`

### 3. 锁超时
- 所有锁获取都不设置超时
- 如果需要超时，使用 `std::unique_lock` + `std::try_to_lock`

## 死锁预防

### 1. 锁顺序检查
- 所有开发者必须遵守锁顺序
- 代码审查时检查锁顺序
- 使用静态分析工具检测锁顺序违反

### 2. 锁粒度
- 尽量使用细粒度锁
- 避免长时间持有锁
- 使用 RAII 管理锁生命周期

### 3. 锁超时
- 避免无限等待
- 使用 `std::try_to_lock` 进行非阻塞尝试
- 实现锁超时机制

## 最佳实践

### 1. 锁获取
```cpp
// 好的做法：使用 RAII
{
  std::shared_lock<std::shared_mutex> lock(mutex_);
  // 临界区
}  // 自动释放锁

// 坏的做法：手动管理锁
mutex_.lock();
// 临界区
mutex_.unlock();  // 可能忘记释放
```

### 2. 锁粒度
```cpp
// 好的做法：细粒度锁
{
  std::shared_lock<std::shared_mutex> lock(mutex_);
  // 只保护必要的代码
}

// 坏的做法：粗粒度锁
{
  std::shared_lock<std::shared_mutex> lock(mutex_);
  // 保护过多代码
}
```

### 3. 锁顺序
```cpp
// 好的做法：按照全局顺序获取锁
std::shared_lock<std::shared_mutex> lock1(mutex_);
std::shared_lock<std::shared_mutex> lock2(index_mutex_);

// 坏的做法：随机顺序
std::shared_lock<std::shared_mutex> lock1(index_mutex_);
std::shared_lock<std::shared_mutex> lock2(mutex_);
```

## 工具支持

### 1. 静态分析
- 使用 `clang-tidy` 检测锁顺序违反
- 使用 `ThreadSanitizer` 检测竞态条件
- 使用 `AddressSanitizer` 检测内存问题

### 2. 运行时检测
- 使用 `helgrind` 检测死锁
- 使用 `drd` 检测竞态条件
- 使用 `valgrind` 检测内存问题

### 3. 代码审查
- 检查锁顺序是否符合规范
- 检查锁粒度是否合适
- 检查锁生命周期是否正确

## 更新日志

- **2026-06-21**: 初始版本，定义全局锁顺序
