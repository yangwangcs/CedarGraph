# CedarGraph-Core：面向分布式时态图的事务、存储与计算系统

## 摘要

CedarGraph-Core 是一个用 C++17、gRPC、Protocol Buffers 与 Raft 组件构建的分布式时态图数据库内核。系统面向带有历史版本、时间旅行查询、跨分区更新和近实时图计算的工作负载，目标是在保持事务正确性和故障恢复能力的同时，降低分布式时态图系统中常见的协调开销、热点分区和历史版本扫描成本。

本文档采用论文式说明方式，先描述当前分布式时态图系统的问题，再说明 CedarGraph-Core 提出的核心方法，随后解释系统如何运行、各模块如何配合，以及预期应通过哪些实验结果证明其有效性。本文档以当前代码为准；历史性能数字、生产审计和路线图会被明确标注，避免把尚未在当前工作树验证的结果写成事实。

更完整的中文论文式系统说明见 `docs/ACADEMIC_SYSTEM_DESCRIPTION_CN.md`；工程操作说明见 `docs/user-manual/README.md`；生产部署步骤见 `docs/PRODUCTION_DEPLOYMENT_GUIDE.md`。

## 1. 问题背景

现代图数据库正在从静态拓扑查询扩展到时态图分析：同一顶点或边会在不同时间点拥有多个版本，查询不仅要回答“现在是什么”，还要回答“某一时间点是什么”“一段时间内如何变化”。当时态图进入分布式环境后，系统通常面临以下问题。

第一，传统分布式事务协议会把许多本可局部完成的写入推入跨节点协调。图数据天然存在邻接关系和跨分区边，若每次更新都走完整两阶段提交，延迟会受网络往返、参与者数量和故障恢复日志影响。时态图进一步增加了冲突检测维度：同一个实体在不同时间窗口上的读写未必真正冲突，但传统 key 级 OCC 往往会过度保守。

第二，时态版本链和 LSM 存储结构之间存在张力。LSM-Tree 适合高写入吞吐，但时态图读取常常需要在多个版本中折叠出某一快照下的可见状态。若查询层必须大量扫描历史版本或反复排序，就会削弱 LSM 的写入优势。

第三，分区策略需要同时考虑负载均衡、图局部性和时间局部性。单纯哈希分区容易打散邻接访问，流式图分区若只看拓扑又可能忽略时间窗口热点。对于时态图，热点不仅来自高度数顶点，也来自某些时间段内集中写入的实体或边。

第四，近实时图计算需要与存储层的版本更新保持同步。若每次分析都从存储层完整回扫，会导致高延迟；若计算层缓存不能正确失效或补全，又会破坏查询一致性。

## 2. 方法概述

CedarGraph-Core 的设计可以概括为四个互补方法。

### 2.1 LND-OCC：LSM 原生分层乐观并发控制

代码位置：`include/cedar/dtx/lsm_native_occ.h`、`src/dtx/protocol/lsm_native_occ.cc`。

LND-OCC 将事务按参与范围分层：

| 层级 | 事务类型 | 处理策略 | 目标 |
|---|---|---|---|
| Layer 1 | 单分区事务 | 本地 OCC 提交，不走网络 2PC | 消除不必要协调 |
| Layer 2 | 同时序范围跨分区事务 | 轻量 prepare/commit 协调 | 保持原子性并缩小验证范围 |
| Layer 3 | 跨时序范围跨分区事务 | 完整 2PC | 覆盖复杂事务 |
| Layer 4 | 确定性批处理事务 | 预留 Calvin 风格批处理接口 | 面向未来批量优化 |

该方法利用 LSM/MemTable 写入路径的顺序性和不可变 SST 特征，使单分区事务主要在本地完成验证、WAL 写入和 MemTable 写入；只有当事务确实涉及多分区或跨时序范围时，才升级为分布式协调。

### 2.2 TW-CD：时序窗口冲突检测

代码位置：`include/cedar/dtx/twcd_engine.h`、`src/dtx/protocol/twcd_engine.cc`。

TW-CD 不是只按 key 判断冲突，而是先按事务声明的时间窗口过滤候选冲突事务，再进行 key 级读写冲突检测。其核心数据结构包括：

| 组件 | 作用 |
|---|---|
| `TemporalWindow` | 表示事务读写关注的时间范围 |
| `TemporalWindowIntervalTree` | 快速查询与当前窗口重叠的活跃事务 |
| `key_to_txns_` | 维护活跃写集到事务的索引 |
| `CheckConflict()` | 先查时间窗口重叠，再查读写/写写冲突 |

该方法的直觉是：同一个实体在不重叠时间窗口上的访问不应被误判为冲突。例如，查询 `t=100` 的历史状态与写入 `t=200` 的新版本在许多场景下可以并发执行。

### 2.3 MTH 流式分区：融合时间局部性、拓扑局部性和负载约束

代码位置：`include/cedar/partition/mth/*`、`src/partition/mth/*`、`src/partition/strategies/*`。

MTH 分区器在事件流进入系统时分配分区。它结合实体历史归属、时间 sketch、分区负载和 fast path 阈值，为新事件选择分区。该策略试图在三类目标之间折中：

| 目标 | 含义 |
|---|---|
| 时间局部性 | 同一实体或相近时间窗口内的事件尽量落在稳定位置 |
| 图局部性 | 减少邻接访问和边遍历的跨分区比例 |
| 负载均衡 | 防止 fast path 将热点长期压到单个分区 |

当前实现保留静态哈希和 MTH 流式策略，便于在实验中比较“简单均匀分布”和“时态/拓扑感知分布”的差异。

### 2.4 GCN/TMV：面向近实时遍历的计算节点

代码位置：`include/cedar/gcn/*`、`src/gcn/*`、`proto/gcn_service.proto`。

GCN（Graph Compute Node）维护 TMV（Temporal Multi-Version）计算视图，接收 CDC 事件、执行本地遍历，并在本地 miss 时通过 Scatter-Gather 路由到其他计算节点。其目标不是替代持久化存储，而是为频繁图遍历和子查询提供低延迟计算层。

## 3. 系统架构

CedarGraph-Core 采用四类主要服务。

| 服务 | 默认端口 | 角色 |
|---|---:|---|
| MetaD | 9559 Raft / 10559 gRPC | 元数据、schema、分区映射、服务发现 |
| StorageD | 9779 | 分区化 LSM 存储、WAL、Raft、MVCC 版本 |
| GraphD | 9669 | 无状态查询入口、Cypher 执行、事务协调 |
| GCN | 配置化 | 图计算节点、TMV 视图、Scatter-Gather 查询 |

典型写入流程如下。

1. GraphD 解析写请求，构建读写集、参与分区和时序窗口。
2. 分区策略决定 key 所属分区；MTH 策略可根据时间/拓扑信息调整分配。
3. LND-OCC 根据参与者数量和窗口关系选择提交层级。
4. TW-CD 对活跃事务执行窗口和 key 级冲突检测。
5. 写入 WAL 和 MemTable；后台 flush/compaction 生成 SST。
6. 变更事件进入 GCN 或缓存失效路径，使计算视图与存储层收敛。

典型时间旅行查询流程如下。

1. 查询层根据 `FOR SYSTEM_TIME AS OF` 或范围语义生成快照时间。
2. CedarScan 通过实体状态锚点和区间锚点做快速存在性判断。
3. 存储层按 `CedarKey` 的降序时间编码定位可见版本。
4. 边查询使用出边/入边双向 key 和版本折叠返回快照下的边集合。
5. 若 GCN 可服务该遍历，则优先走 TMV；否则回退到存储或分布式 scatter-gather。

## 4. 数据模型与存储布局

CedarGraph 使用 32 字节固定长度 `CedarKey` 作为核心索引键。该 key 将实体、时间、方向、列、序列号和分区信息编码到固定内存布局中，使 LSM 排序、时态查询和边遍历可以共享同一底层结构。

| 字段 | 含义 |
|---|---|
| `entity_id` | 顶点 ID、边源点或路由实体 |
| `timestamp_be` | 降序编码时间戳，最新版本自然靠前 |
| `target_id` | 边目标点或内联值/扩展值 |
| `column_id` | 顶点属性列或边类型 |
| `sequence` | 同一微秒内的顺序号 |
| `entity_type` | Vertex、EdgeOut、EdgeIn |
| `flags` | create/update/delete、压缩、锁、墓碑等标记 |
| `part_id` | 分区 ID |

存储层围绕 LSM-Tree 组织，包括 MemTable、WAL、SST、块缓存、Blob 文件、压缩和 compaction。SST 子系统提供 zone-columnar 格式、Bloom Filter、列编码和压缩组件，用于降低历史版本和属性列扫描成本。

## 5. 当前实现状态

| 能力 | 当前状态 | 代码证据 |
|---|---|---|
| LND-OCC 单分区提交 | 已实现并有单元测试 | `test_lnd_occ` |
| TW-CD 窗口冲突检测 | 已实现并有单元测试 | `test_twcd_engine` |
| LND-OCC 默认启用 TW-CD | 已修复并补测试 | `LndOccEngineTest.DefaultInitializationEnablesTwcdConflictDetection` |
| TW-CD 注销清理 key 索引 | 已修复并补测试 | `TwcdEngineTest.UnregisterWindowAlsoClearsWriteSetIndex` |
| 完整 2PC 路径 | 已实现，仍需端到端压力验证 | `FullTwoPhaseCommit()`、DTX tests |
| MTH 流式分区 | 已实现并有测试 | `tests/partition/test_mth_partitioner.cc` |
| CedarScan 快照读取 | 已实现并有存储测试 | `tests/storage/test_cedar_scan*.cc` |
| GCN/TMV 计算层 | 已实现核心服务与测试 | `tests/gcn/*` |
| 生产部署文档 | 已有历史审计与部署说明 | `docs/PRODUCTION_READINESS_AUDIT_*.md` |

## 6. 预期实验结果

CedarGraph-Core 的实验评价应围绕“协调减少、冲突误判减少、时态查询加速、分区质量提升和故障恢复正确性”展开。推荐实验如下。

| 实验 | 指标 | 预期结果 |
|---|---|---|
| 单分区事务 vs 传统 2PC | 提交延迟、网络 RPC 次数 | LND-OCC 单分区路径应显著降低协调延迟 |
| TW-CD vs key-only OCC | abort 率、冲突检测延迟 | 时间不重叠场景下 abort 率应下降 |
| StaticHash vs MTH | 跨分区边比例、热点分区负载 | MTH 应降低时态/拓扑热点导致的跨分区代价 |
| CedarScan 快照查询 | P50/P95/P99、SST 访问次数 | 锚点和降序时间 key 应减少无效历史扫描 |
| GCN/TMV 遍历 | 遍历延迟、local hit ratio | 本地 TMV 命中时延迟应低于回源存储 |
| 故障恢复 | prepared/committing 恢复成功率 | 2PC 决策日志和状态管理应保证可恢复 |

历史 README 中曾记录 macOS ARM64 test mode 下的吞吐数字，例如写入 230K ops/sec、读取 102K ops/sec。这些数字应被视为历史基准记录；在论文、报告或发布材料中引用前，需要用当前 commit、当前构建参数和固定 workload 重新生成。

## 7. 构建与验证

### 7.1 依赖

建议环境：

- C++17 编译器
- CMake 3.14+
- gRPC / Protobuf
- OpenSSL
- yaml-cpp
- libcurl
- LZ4，可选 Zstd
- vendored brpc/braft，运行 `./scripts/setup_deps.sh` 获取

### 7.2 构建

```bash
mkdir -p build
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j4
```

### 7.3 运行关键单元测试

```bash
cmake --build build --target test_twcd_engine test_lnd_occ -j4
./build/tests/test_twcd_engine
./build/tests/test_lnd_occ
```

本次文档重写和底层检查过程中，已验证：

| 命令 | 结果 |
|---|---|
| `./build/tests/test_twcd_engine` | 18/18 通过 |
| `./build/tests/test_lnd_occ` | 15/15 通过 |

当前更大范围验证结果为：`ctest` 注册测试 1358 个，其中 1356 个实际运行测试全部通过，2 个性能基准测试未运行：`CedarUpdateValidationTest.ValidationPerformance` 与 `CedarUpdateE2ETest.WritePerformance`。功能性 CedarUpdate 严格存在性校验、同批依赖、时态 DELETE 历史版本返回等测试已恢复为可运行断言，不再作为 disabled 功能缺口保留。

上述关键目标和全量分批测试的当前构建日志已清理 brpc/braft 第三方头警告、`LOG/CHECK` 宏重定义警告和重复 `pthread` 链接警告。CMake 现在强制 braft 使用 vendored brpc，并将 brpc/braft 作为第三方目标静音处理。

此外，当前代码已新增本地上线前烟测脚本：

```bash
./scripts/preflight_release_gate.sh
./scripts/preflight_local_smoke.sh
./scripts/preflight_distributed_smoke.sh
./scripts/preflight_soak.sh
./scripts/preflight_failover_smoke.sh
./scripts/preflight_graphd_failover_smoke.sh
./scripts/preflight_tls_smoke.sh
```

`preflight_release_gate.sh` 是上线前本地总入口，会串行执行构建、构建日志扫描、单机烟测、分布式烟测、non-test-mode Raft 烟测、关键 CTest 守卫、短时 soak、StorageD 故障注入、GraphD 故障注入、TLS/mTLS 烟测和 `git diff --check`。关键 CTest 守卫包括 disabled 测试 `BLOCKED:` 文档约束和 metrics shutdown 回归测试。其余脚本可用于单项定位。

2026-06-29 已额外验证短版本地 release gate：

```bash
CEDAR_RELEASE_SOAK_SECONDS=9 CEDAR_RELEASE_SOAK_POLL_SECONDS=3 ./scripts/preflight_release_gate.sh
```

结果为通过；该次运行包含 Docker/Compose 静态门禁、Helm 静态门禁、Kubernetes 静态门禁和构建警告扫描。构建日志扫描未发现 brpc/braft 在 macOS 下的弃用 API 警告、`LOG/CHECK` 宏重定义、重复链接库警告或误用 `/opt/homebrew/include/butil` 的情况。

默认 gate 已覆盖 TLS/mTLS、non-test-mode Raft 和故障注入；正式发版前建议额外启用全量 CTest 和更长 soak：

```bash
CEDAR_RELEASE_FULL_CTEST=1 CEDAR_RELEASE_SOAK_SECONDS=300 CEDAR_RELEASE_SOAK_POLL_SECONDS=5 ./scripts/preflight_release_gate.sh
```

其中单机烟测在隔离的 `/tmp/cedar/preflight-*` 目录中启动 1 个 MetaD、1 个 StorageD 和 1 个 GraphD；分布式烟测在 `/tmp/cedar/distributed-preflight-*` 中启动 3 个 MetaD、3 个 StorageD 和 1 个 GraphD；短时 soak 在 `/tmp/cedar/soak-preflight-*` 中启动同样的分布式拓扑并持续轮询；StorageD 故障注入烟测在 `/tmp/cedar/failover-preflight-*` 中受控终止一个 StorageD，并验证 MetaD 心跳超时离线检测、剩余节点可观测和温和收尾；GraphD 故障注入烟测在 `/tmp/cedar/graphd-failover-preflight-*` 中启动 3 个 GraphD，杀掉一个查询入口，并验证 MetaD GraphD 清理线程移除失联节点；TLS/mTLS 烟测在非 test-mode 下验证 gRPC 凭据、GraphD 认证初始化和安全链路启动。上述脚本都会验证监听端口、pid 存活、health/metrics HTTP 端点、运行日志严重诊断，并要求非故障注入目标进程在 SIGTERM 后无需 SIGKILL 即可退出。脚本的 start/log scan/detection 输出会写入各自隔离集群目录，避免重复或并发运行时互相污染。

## 8. 运行方式

单机开发环境可使用：

```bash
./scripts/start_standalone.sh start
```

分布式开发环境可使用：

```bash
./scripts/start_distributed.sh start
```

也可以分别启动：

```bash
./build/cedar-metad
./build/cedar-storaged
./build/cedar-graphd
```

具体参数、查询示例、运维步骤见 `docs/user-manual/README.md`。

## 9. 局限与下一步

当前代码已经包含完整系统骨架和大量测试，但作为论文式系统说明，仍需承认以下局限。

1. 历史性能表需要用当前代码重新跑完整基准，给出机器配置、编译类型、数据规模、Raft 是否启用、是否 test mode。
2. Layer 2 “同时序范围跨分区事务”的轻量协调仍需要更系统的原子性和恢复压力测试。
3. MTH 分区的优势应通过公开 workload 或可复现实验脚本量化，而不是只凭策略描述。
4. GCN/TMV 与 StorageD 的一致性边界需要在文档中继续细化，例如缓存失效、backfill 和 watermark 的组合语义。
5. CedarUpdate 的严格存在性校验、同批依赖校验和时态 DELETE 历史版本返回已经恢复为功能性断言；当前仅保留 2 个性能基准测试未默认运行。若这些性能数字进入论文或上线 SLO，需要在固定硬件、固定数据规模和固定 Release 配置下重新跑基准。
6. 继续做真实部署级验证；当前代码级证据已经覆盖全量可运行测试、本地多进程烟测、non-test-mode Raft 烟测、TLS/mTLS 烟测、短时 soak、单 StorageD 故障注入和单 GraphD 故障注入，但生产上线仍需要目标环境中的真实证书链、真实部署/回滚、更长时间 soak、更多故障组合和监控告警验证。

## License

Apache 2.0 ("The Cedar Authors")
