# 查询引擎说明

## 1. 职责

查询引擎负责将 Cypher 输入转换为可执行计划，并在 GraphD、StorageD 和 GCN 之间组织执行。它的目标是支持普通图查询、时态快照查询、基本写语句和分布式结果聚合。

## 2. 流程

```text
Client
  -> GraphD
  -> Parser
  -> Validator
  -> Planner / Cost Optimizer
  -> Executor
  -> StorageD / GCN
  -> Result Aggregator
```

## 3. 支持语义

| 类别 | 示例 |
|---|---|
| 匹配 | `MATCH (n) RETURN n` |
| 过滤 | `MATCH (n) WHERE n.age > 30 RETURN n` |
| 遍历 | `MATCH (a)-[:KNOWS]->(b) RETURN a,b` |
| 写入 | `CREATE`, `SET`, `DELETE`, `MERGE` 的已实现子集 |
| 时间旅行 | `FOR SYSTEM_TIME AS OF ...` |
| 分页排序 | `ORDER BY`, `LIMIT`, `SKIP` |

## 4. 时态查询

时间旅行查询会生成快照时间或时间范围。执行器将该时间传给 CedarScan 或相关存储接口，由存储层完成多版本折叠。查询层不应自行扫描所有历史版本再过滤。

## 5. 分布式执行

当查询涉及多个分区时，GraphD 使用元数据和路由表拆分请求，向多个 StorageD 或 GCN 发起子查询，再由结果聚合器合并。聚合必须处理 partial failure、超时、重复结果和顺序要求。

## 6. 开发边界

- 新增 Cypher 语法必须同时更新 parser、validator、planner、executor 和测试。
- 写语句必须进入事务路径，不能绕过冲突检测和 WAL。
- 时间旅行语义必须由存储层可见性规则支撑。
- 计划缓存中的对象必须可安全 clone，不能共享可变执行状态。

