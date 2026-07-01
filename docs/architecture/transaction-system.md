# 事务系统说明

## 1. 目标

事务系统负责在时态图写入中提供原子性、一致性、隔离性和可恢复性。系统采用本地 OCC、TW-CD 和跨分区 2PC 的组合，而不是对所有事务使用同一条重路径。

## 2. 事务上下文

事务上下文记录：

| 字段 | 说明 |
|---|---|
| transaction id | 全局或分区内事务标识 |
| read/commit timestamp | 快照读取和提交可见性 |
| read/write set | 冲突检测输入 |
| temporal window | 时态冲突检测范围 |
| participants | 参与分区和节点 |
| state | active、prepared、committing、committed、aborted |

## 3. 提交路径

| 路径 | 条件 | 行为 |
|---|---|---|
| 本地 OCC | 单分区 | 本地验证、WAL、MemTable、提交 |
| 轻量协调 | 多分区同时间范围 | prepare/commit，但缩小验证范围 |
| 完整 2PC | 多分区跨时间范围 | 参与者 prepare，协调者持久化决策，再提交或中止 |

## 4. TW-CD

TW-CD 在验证阶段先按时间窗口筛选候选事务，再检查读写集。事务结束时必须清理窗口和 key 索引，避免残留活跃事务造成误判或内存增长。

## 5. 恢复与超时

跨分区事务需要决策日志、参与者状态和超时管理。恢复时系统根据事务状态决定重放 commit、abort 或向协调者查询最终决策。

## 6. 正确性要求

- 写入不能绕过 WAL。
- 已 prepare 的事务必须可以恢复最终决策。
- abort 后不得留下可见版本。
- 超时不能破坏已经提交的事务。
- 认证失败、TLS 配置错误和未知 coordinator 必须 fail closed。

