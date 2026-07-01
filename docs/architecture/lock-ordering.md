# 锁顺序规范

本文档记录 CedarGraph-Core 中跨模块锁获取顺序。新增代码若需要同时持有多个锁，应遵守从上到下的顺序，避免死锁。

## 全局顺序

| 顺序 | 锁 | 作用 |
|---:|---|---|
| 1 | `LsmEngine::mutex_` | MemTable 和 Immutable MemTable |
| 2 | `LsmEngine::index_mutex_` | 标签索引和属性索引 |
| 3 | `LsmEngine::accumulated_mutex_` | 累积缓冲区 |
| 4 | `LsmEngine::column_map_mutex_` | entity-column 映射 |
| 5 | `LsmEngine::flush_completion_mutex_` | flush 完成状态 |
| 6 | `CedarGraphStorage::rep_->mutex_` | 存储入口状态 |
| 7 | `PartitionStorage::txn_mutex_` | 分区事务状态 |
| 8 | `MetricsRegistry::mutex_` | 指标注册表 |

## 规则

1. 不得在持有低顺序锁时再获取高顺序锁。
2. I/O、RPC、长时间等待和 `future.wait()` 不应发生在大范围互斥锁内。
3. 读锁升级为写锁时应释放读锁后重新获取，避免升级死锁。
4. 新增锁必须在本文档登记顺序和保护范围。
5. 测试应覆盖 flush、compaction、指标采集、关闭流程和事务恢复中的并发路径。

## 审查清单

- 是否存在锁内 RPC 或磁盘 I/O。
- 是否存在多锁获取但未按本文顺序。
- 是否存在析构/关闭流程与后台线程竞争。
- 是否存在指标回调反向调用业务对象。
- 是否存在条件变量等待时仍持有不必要锁。

