# 存储引擎说明

## 1. 目标

存储引擎负责持久化时态图数据，并支持高写入吞吐、快照读取、范围扫描、故障恢复和后台整理。核心设计是 LSM-Tree 加多版本可见性。

## 2. 主要组件

| 组件 | 职责 |
|---|---|
| WAL | 崩溃恢复和提交持久性 |
| MemTable | 接收最新写入和短期读取 |
| Immutable MemTable | flush 前的只读缓冲 |
| SST | 有序持久文件 |
| Bloom/Temporal Filter | 减少无效读取 |
| Block Cache | 缓存热点块 |
| Blob Manager | 管理大值和回收 |
| Compaction | 合并层级、清理墓碑、控制读放大 |

## 3. CedarKey

`CedarKey` 以固定 32 字节组织实体、时间和分区信息。顶点、出边和入边共享同一底层排序模型。时间字段面向快照读取排序，降低时间旅行查询寻找可见版本的成本。

## 4. 写入流程

1. 校验写入和事务状态。
2. 写 WAL。
3. 更新 MemTable 和索引。
4. 达到阈值后切换 Immutable MemTable。
5. 后台 flush 生成 SST。
6. compaction 合并文件并回收过期版本。

## 5. 读取流程

读取按从新到旧的顺序查找 MemTable、Immutable MemTable 和 SST。时态读取需要根据快照时间、墓碑、schema 和事务可见性返回正确版本。边读取通过出边/入边 key 范围扫描并折叠多版本。

## 6. 恢复

节点重启后加载 manifest、SST 元数据和 WAL，重建 MemTable、索引和 compaction 状态。恢复路径必须避免把未提交事务当作已提交结果暴露。

## 7. 工程约束

- flush、compaction 和读路径不能长时间持有全局写锁。
- tombstone 和 TTL 回收必须受 watermark 或保留策略约束。
- 大值和 blob GC 必须与 SST 生命周期协调。
- 新增存储格式需提供向后兼容或明确迁移路径。

