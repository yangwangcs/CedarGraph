# CedarGraph-Core

CedarGraph-Core 是一个面向分布式时态图的数据库内核实验与工程实现。系统以 C++17、gRPC/Protobuf、LSM-Tree、MVCC、Raft 复制和分布式事务为基础，目标是在图数据持续变化、历史版本需要查询、写入可能跨分区、服务可能故障的条件下，提供可验证的事务、存储、查询和部署能力。

本仓库当前更适合作为分布式时态图系统内核、研究原型和生产化候选工程，而不是开箱即用的托管数据库产品。文档会尽量区分“已经由代码和测试支撑的能力”和“仍需目标环境验证的生产承诺”。

## 核心问题

分布式时态图系统通常同时面对五类问题：

| 问题 | 说明 |
|---|---|
| 过度协调 | 单分区或同时间窗口事务不应总是走完整 2PC |
| 时态冲突误判 | 同一实体在不重叠时间窗口上的访问不一定冲突 |
| 历史版本扫描 | 时间旅行查询容易退化为大量无效版本查找 |
| 分区质量 | 图局部性、时间局部性和负载均衡需要同时考虑 |
| 计算视图同步 | 近实时遍历需要缓存视图，但不能牺牲一致性边界 |

## 方法概览

CedarGraph-Core 的主要方法由四部分组成：

| 方法 | 目标 | 主要位置 |
|---|---|---|
| LND-OCC | 按参与分区和时间范围选择本地提交、轻量协调或完整 2PC | `src/dtx/protocol/lsm_native_occ.cc` |
| TW-CD | 将时间窗口引入 OCC 冲突检测，降低误判 abort | `src/dtx/protocol/twcd_engine.cc` |
| CedarKey + LSM | 用固定长度时态 key、WAL、MemTable、SST 和版本折叠支持快照读取 | `include/cedar/types/cedar_key.h`, `src/storage/*` |
| GCN/TMV | 用多版本计算视图和 Scatter-Gather 路由服务低延迟遍历 | `src/gcn/*` |

## 系统组成

| 服务/模块 | 作用 |
|---|---|
| MetaD | 元数据、schema、分区映射、服务发现和 Raft 元数据复制 |
| StorageD | 分区化 LSM 存储、WAL、MVCC、Raft 状态机和恢复 |
| GraphD | 查询入口、Cypher 执行、事务协调、路由和结果聚合 |
| GCN | 图计算节点、TMV 多版本视图、CDC 应用和远程遍历 |
| Client/Tools | 连接池、配置、部署脚本、健康检查和运维辅助 |

## 快速构建

```bash
mkdir -p build
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=ON
cmake --build build -j4
```

常用验证入口：

```bash
ctest --test-dir build --output-on-failure
./scripts/preflight_release_gate.sh
```

`preflight_release_gate.sh` 是本地发布门禁入口，会串行执行构建、静态清单检查、关键测试、烟测、短时 soak、故障注入和 TLS/mTLS 相关检查。它能证明当前机器和当前配置下的发布候选质量，但不能替代真实生产集群验证。

## 文档导航

| 文档 | 内容 |
|---|---|
| [论文式系统说明](docs/ACADEMIC_SYSTEM_DESCRIPTION_CN.md) | 问题、方法、运行机制和预期实验 |
| [架构说明](docs/architecture/README.md) | 服务、数据模型、事务、存储和计算层 |
| [用户说明书](docs/user-manual/README.md) | 构建、运行、测试、排障和边界说明 |
| [部署指南](docs/deployment/README.md) | 单机、Docker Compose、Kubernetes 和上线门禁 |
| [生产部署指南](docs/PRODUCTION_DEPLOYMENT_GUIDE.md) | 生产参数、TLS、Secret、监控和回滚检查 |
| [API 说明](docs/api/README.md) | C++ 客户端、服务接口和当前未实现边界 |

## 当前边界

- 本仓库仍包含研究型与生产化候选代码，部分接口会明确返回未实现，而不是伪装成功。
- `third_party` 中的 brpc/braft 文档属于上游依赖，不代表 CedarGraph 自身用户文档。
- CI 中的部分扫描与容器安全检查被配置为非阻断，以避免外部服务、runner 或权限问题阻塞主分支；真实发版仍应单独审计扫描结果。
- 生产上线前必须在目标环境验证真实证书链、Secret 注入、持久卷、网络策略、回滚、备份恢复、监控告警和长时间压力。

