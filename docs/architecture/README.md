# CedarGraph-Core 架构说明

## 1. 总体架构

CedarGraph-Core 由元数据层、存储层、查询事务层、计算层和运维客户端层组成。

| 层次 | 模块 | 职责 |
|---|---|---|
| 元数据层 | MetaD | schema、space、partition、服务发现、Raft 元数据复制 |
| 存储层 | StorageD | LSM、WAL、MVCC、SST、Raft 状态机、备份恢复 |
| 查询事务层 | GraphD | Cypher、路由、事务协调、结果聚合 |
| 计算层 | GCN | TMV 多版本视图、CDC 应用、遍历、Scatter-Gather |
| 运维客户端 | Client/Scripts | 连接池、健康检查、部署、预检和监控 |

服务间通信以 gRPC/Protobuf 为主，一致性组件使用 vendored brpc/braft。GraphD 是主要入口；MetaD 提供位置和 schema；StorageD 管理分区化数据；GCN 服务高频图计算。

## 2. 数据模型

CedarGraph 将图数据编码为顶点、出边和入边三类实体。边采用正反向存储，使出邻居和入邻居查询都能走有序范围扫描。

核心 key 是 `CedarKey`，长度固定为 32 字节，包含实体 ID、时间戳、目标 ID、列 ID、序列号、实体类型、标记和分区 ID。时间戳排序面向快照查询优化，使同一实体的较新版本更容易被定位。

## 3. 存储路径

写入路径：

1. GraphD 或内部调用方生成写入。
2. StorageD 先写 WAL。
3. 数据进入 MemTable。
4. 后台 flush 生成 SST。
5. compaction 合并层级、清理墓碑和整理历史版本。

读取路径：

1. 查询层确定实体、方向、列和快照时间。
2. 存储层查 MemTable、Immutable MemTable 和 SST。
3. CedarScan 折叠多版本，只返回快照下可见状态。
4. 边查询按 EdgeOut/EdgeIn 范围返回邻接结果。

## 4. 事务路径

LND-OCC 按事务参与范围选择路径：

| 情况 | 路径 |
|---|---|
| 单分区 | 本地 OCC |
| 多分区同时间范围 | 轻量协调 |
| 多分区跨时间范围 | 完整 2PC |
| 批量确定性写入 | 预留批处理路径 |

TW-CD 在验证阶段先过滤时间窗口，再检查 key 级冲突。这样可以降低时态负载下的保守 abort。

## 5. 查询和计算

Cypher 查询由解析、规划、执行和结果聚合阶段组成。GraphD 可将读写请求路由到 StorageD，也可调用分布式执行和 GCN/TMV 计算路径。GCN 维护多版本图视图，适合频繁遍历和局部计算。

## 6. 故障恢复

系统恢复依赖四类机制：

| 机制 | 目的 |
|---|---|
| Raft | 元数据和存储分区复制 |
| WAL | 单节点崩溃后重放 |
| 2PC 决策日志 | 跨分区事务恢复 |
| 健康检查/注册 | 服务发现、下线和故障转移 |

## 7. 架构边界

- 架构文档描述设计和当前主要实现，不等于所有路径都已生产验证。
- 性能数字必须由当前 commit、固定 workload 和固定环境重新生成。
- `third_party` 依赖只作为构建和一致性组件，不应污染 CedarGraph 的对外文档语义。

