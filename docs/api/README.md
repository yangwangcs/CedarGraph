# CedarGraph-Core API 说明

本文档说明当前仓库中面向调用方的主要接口边界。API 仍处于内核演进阶段，使用时应以头文件、proto 和测试为准。

## 1. 接口层次

| 层次 | 位置 | 说明 |
|---|---|---|
| C++ Client | `include/cedar/client/*` | 连接池、查询、集群管理、TLS/JWT 配置 |
| Driver | `include/cedar/driver/*` | session、bookmark、retry policy、事务类型 |
| Graph Service | `proto/cedar_graph.proto`, `src/service/*` | GraphD 查询和路由服务 |
| Storage Service | `proto/storage_service.proto`, `src/dtx/storage*` | StorageD 读写、认证、Raft 状态机辅助 |
| Meta Service | `proto/meta_service.proto`, `src/dtx/meta/*` | schema、space、partition、服务发现 |
| GCN Service | `proto/gcn_service.proto`, `src/gcn/*` | 计算节点、TMV、遍历和事件应用 |

## 2. C++ Client 基本用法

典型调用流程：

```cpp
#include "cedar/client/cedar_client.h"

cedar::client::CedarClientConfig config;
config.endpoints = {"127.0.0.1:9669"};
config.enable_tls = false;

cedar::client::CedarClient client(config);
auto status = client.Connect();
if (!status.ok()) {
  return status;
}

auto result = client.ExecuteQuery("MATCH (n) RETURN n LIMIT 10");
```

生产环境应配置 TLS、mTLS 或 JWT 参数，并启用连接池、超时和重试策略。

## 3. 查询接口

GraphD 负责接收 Cypher 查询并路由到存储或分布式执行层。当前查询能力以测试覆盖为边界，包括基本 `MATCH`、过滤、投影、部分写语句、时间旅行语义和分布式结果聚合。

示例：

```cypher
MATCH (n:Person) WHERE n.age > 30 RETURN n.name LIMIT 20
MATCH (n) FOR SYSTEM_TIME AS OF timestamp('2026-01-01T00:00:00Z') RETURN n
```

## 4. 事务接口

事务层内部支持：

| 能力 | 说明 |
|---|---|
| 单分区 OCC | 本地验证和提交 |
| 跨分区 2PC | prepare、decision、commit/abort 和恢复 |
| TW-CD | 时间窗口冲突检测 |
| 超时与恢复 | timeout manager、recovery manager、状态持久化 |

外部用户应优先通过 GraphD/Client 进入事务路径，而不是直接构造内部 DTX 对象。

## 5. 当前明确边界

以下接口若尚未完整实现，应返回失败或未实现状态，不应被解释为成功：

| 接口/能力 | 当前含义 |
|---|---|
| 部分 schema DDL client helper | 以服务端和 CLI 已验证路径为准 |
| 集群级远程备份下载 | 元数据注册和远程一致性仍需补齐 |
| 客户端备份加密 | 未启用时不得报告为加密成功 |
| 安全扫描/CI 结果 | CI 中部分扫描为非阻断，发版前需单独审计 |

## 6. 开发者建议

- 新增 API 时同步更新 proto、头文件、服务实现和测试。
- 对外接口必须 fail closed；不能用空结果或 `OK` 掩盖未实现能力。
- 涉及认证、TLS、备份、删除空间和恢复的接口必须有负向测试。

