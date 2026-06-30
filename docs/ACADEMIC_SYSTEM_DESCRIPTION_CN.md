# CedarGraph-Core 论文式系统说明：面向分布式时态图的事务、存储与计算内核

## 摘要

CedarGraph-Core 面向分布式时态图数据管理场景，试图解决传统分布式图数据库在多版本历史查询、跨分区事务协调、时态冲突检测、动态图分区和近实时遍历计算中面临的系统性开销。本文以当前代码实现为依据，采用论文式结构说明系统问题、方法设计、运行机制、预期实验结果和上线前验证边界。

本文提出的系统方法由四个部分组成：第一，LND-OCC 将事务按参与分区和时序范围分层，使单分区事务尽量走本地提交路径，只有必要时才进入分布式协调；第二，TW-CD 将时间窗口引入冲突检测，避免将同一 key 上时间不重叠的访问误判为冲突；第三，MTH 流式分区策略在时间局部性、拓扑局部性和负载均衡之间折中，以减少时态热点和跨分区遍历开销；第四，GCN/TMV 计算层通过多版本计算视图、事件应用和 Scatter-Gather 路由支持低延迟图遍历。

截至 2026-06-30 的本地验证结果显示，当前工作树已通过构建、构建日志警告扫描、Docker/Compose 静态检查、Helm 静态检查、Kubernetes 静态检查、部署清单静态检查、本地多进程 smoke、分布式 smoke、non-test-mode Raft smoke、CTest 守卫、短时 soak、StorageD 故障注入、GraphD 故障注入、TLS/mTLS smoke 和空白差异检查。Docker daemon、Helm、Go 与本机 Kubernetes API server 已确认可用；正式 Linux 镜像 `cedargraph/cedar:k8s-fix-20260630` 已构建成功，镜像 ID 为 `sha256:b1115b2528830b1e6a78e917e234ee018634df393024da9fd8839d2e03dc0769`，三进制运行时缺库检查无 `not found`。本机 Kubernetes 隔离恢复演练已达到 MetaD 3/3、StorageD 3/3、GraphD 1/1 Ready，并通过 Raft evidence、恢复计划、production gate 和 upgrade guard 验证。该证据说明代码和本地发布门禁已达到发布候选状态，但并不等同于真实生产环境 100% 就绪；真实上线仍需完成生产规格集群、真实证书链、长时间压力、证书轮换、回滚和监控告警验证。

## 1. 引言

图数据库通常以顶点、边和属性为核心抽象，适合表达社交网络、交易网络、知识图谱、设备拓扑和因果关系。随着业务系统从静态图查询走向历史追踪、审计回放、因果分析和实时推荐，图数据库需要同时回答当前状态查询和历史状态查询。这样的数据可称为时态图：同一顶点或边不再只有一个当前版本，而是在不同时间点拥有不同状态。

当时态图进入分布式系统后，问题会显著复杂化。首先，图数据天然存在邻接关系，边的两个端点可能落在不同分区。其次，时态版本让一个实体在不同时间窗口上存在多个可见状态。再次，分布式事务需要在正确性、延迟和可恢复性之间做权衡。若系统把所有更新都当作普通跨 key 事务处理，则会产生过多协调；若系统只追求低延迟而忽视故障恢复，则无法支撑生产部署。

CedarGraph-Core 的基本命题是：分布式时态图系统不应把所有图更新都退化为全局协调，而应利用时间窗口、分区局部性、LSM 写入结构和多版本计算视图，将大部分操作限制在更小的验证和提交范围内。

## 2. 问题定义

### 2.1 过度协调问题

传统两阶段提交能够为跨分区事务提供原子性，但它的代价来自多轮 RPC、参与者等待、决策日志和故障恢复。对于只涉及单个分区的事务，完整 2PC 是不必要的；对于涉及多个分区但时间窗口一致的事务，完整协调也可能过重。

在图数据库中，写入往往包含顶点属性更新、出边写入、入边写入和索引维护。若系统无法区分局部写入和跨分区写入，就会把大量本地可完成的操作推入分布式协议。

### 2.2 时态冲突误判问题

普通 OCC 通常基于 key 判断读写冲突。如果事务 A 读取实体 `v` 在时间 `t=100` 的版本，而事务 B 写入实体 `v` 在时间 `t=200` 的版本，二者是否冲突取决于查询语义和版本可见性，而不能只看 key 是否相同。key-only 冲突检测在时态图中会产生保守 abort，降低并发度。

### 2.3 历史版本扫描问题

LSM-Tree 适合高吞吐写入，但时态查询可能需要在多个版本中找到某一快照时间可见的版本。如果 key 布局、状态锚点和版本折叠设计不当，系统会在历史版本链上做大量无效扫描。

### 2.4 时态热点与分区质量问题

简单哈希分区可以带来基本负载均衡，但会破坏图邻接局部性。拓扑感知分区可以降低跨分区遍历，但可能把热点顶点长期压到少数分区。时态图还存在时间窗口热点，即某段时间内某些实体集中更新。分区器需要同时考虑时间、拓扑和负载。

### 2.5 计算视图一致性问题

近实时遍历不宜每次都回源存储层扫描历史版本。计算节点可以缓存多版本视图，但必须正确处理 CDC 事件、缓存失效、backfill 和 watermark 回收，否则会出现过期结果或漏读。

## 3. 方法设计

### 3.1 LND-OCC：分层乐观并发控制

LND-OCC 的目标是减少不必要的分布式协调。系统根据事务参与分区和时序范围选择提交路径。

| 层级 | 事务类型 | 提交路径 | 系统目的 |
|---|---|---|---|
| Layer 1 | 单分区事务 | 本地 Begin/Validate/Commit | 避免 2PC |
| Layer 2 | 同时序范围跨分区事务 | 轻量 prepare/commit | 缩小协调范围 |
| Layer 3 | 跨时序范围跨分区事务 | 完整 2PC | 保证复杂事务原子性 |
| Layer 4 | 确定性批处理事务 | 预留批处理接口 | 面向批量优化 |

当前代码中的主要入口包括 `LndOccEngine::SinglePartitionCommit()`、`LndOccEngine::SameTemporalRangeCommit()` 和 `LndOccEngine::FullTwoPhaseCommit()`。单分区路径会在本地分配事务 ID、开始时间戳、参与分区和 TW-CD 窗口，随后完成验证、WAL 写入、MemTable 写入和清理。

### 3.2 TW-CD：时序窗口冲突检测

TW-CD 的基本思想是把冲突检测分成两步。第一步按 `TemporalWindow` 查找与当前事务时间窗口重叠的活跃事务；第二步只在候选集合中做 key 级读写和写写检测。

该方法的运行流程如下：

1. 事务开始时注册自己的时间窗口。
2. 写事务将写集登记到 `key_to_txns_`。
3. 验证时先查询窗口重叠事务集合。
4. 若没有窗口重叠，则直接判定无冲突。
5. 若窗口重叠，再检查读写集和写写集交集。
6. 事务提交或中止后注销窗口并清理写集索引。

本次底层检查已确认两个关键不变量：`LndOccEngine` 在启用 TW-CD 时必须实际创建并持有 `TwcdEngine`；`UnregisterWindow()` 必须清理 key 写集索引，避免长时间运行后残留活跃事务映射。

### 3.3 MTH：时态/拓扑感知流式分区

MTH 分区器在事件进入系统时根据实体归属、时间 sketch、分区容量和负载约束选择分区。若 sketch 给出高置信分区且负载允许，则走 fast path；否则回退到通用分区逻辑。

该设计预期带来三个效果：减少相邻访问跨分区比例，稳定时间窗口内实体归属，避免热点长期集中在单个分区。

### 3.4 CedarKey 与 LSM 时态布局

CedarGraph 使用 32 字节固定长度 `CedarKey`，将实体 ID、降序时间戳、目标 ID、列 ID、序列号、实体类型、标记和分区 ID 编码到统一 key 中。降序时间戳使快照读可以优先遇到较新版本；出边和入边双写使正向和反向遍历都可通过有序范围扫描完成。

存储层围绕 WAL、MemTable、Immutable MemTable、SST、Block Cache、Blob Manager 和 Compaction 组织。时态查询通过 `CedarScan`、状态锚点、区间锚点和版本折叠降低历史扫描成本。

### 3.5 GCN/TMV：近实时计算层

GCN 维护 TMV 计算视图，支持本地遍历、子查询派发、事件应用、缓存失效和 backfill。它与持久化存储层之间形成分层关系：StorageD 是权威持久化状态，GCN 是面向低延迟遍历的可恢复计算视图。

## 4. 系统运行机制

### 4.1 服务拓扑

| 服务 | 默认端口 | 说明 |
|---|---:|---|
| MetaD Raft | 9559 | 元数据一致性复制 |
| MetaD gRPC | 10559 | GraphD/StorageD 元数据客户端接口 |
| StorageD | 9779 | 分区存储和存储 RPC |
| GraphD query | 9669 | 查询入口 |
| GraphD health | 9668 | 健康检查 |
| GraphD metrics | 9667 | Prometheus 指标 |
| StorageD health | 7000 | 健康检查 |
| StorageD metrics | 7001 | Prometheus 指标 |

当前部署约束是：StorageD、GraphD、GCN 以及 C++ SDK 等业务客户端必须连接 MetaD gRPC 端口 `10559`，不应把业务元数据客户端指向 MetaD Raft 端口 `9559`。该约束已经写入部署静态门禁、Docker 静态门禁、Helm 静态门禁和 Kubernetes 静态门禁；本轮底层检查还修复了 `CedarClientConfig`、`gcn_coordinator` 与 Go CLI 启动配置，使默认客户端路径与部署端口模型一致。Go CLI 中 `metad.port` 表示 Raft/listen 端口，`metad.grpc_port` 表示 gRPC 客户端 API 端口。

### 4.2 写入流程

1. GraphD 接收查询或写入请求，解析 Cypher 或内部请求结构。
2. 查询执行层构建事务上下文、读写集、参与分区和时间窗口。
3. 分区策略确定每个 key 的分区归属。
4. LND-OCC 根据参与分区和时序范围选择本地提交、轻量协调或完整 2PC。
5. TW-CD 对窗口重叠和 key 交集执行冲突检测。
6. StorageD 写 WAL，并把最新版本写入 MemTable。
7. 后台 flush/compaction 生成 SST 并回收过期或被墓碑覆盖的数据。
8. 变更事件驱动 GCN/TMV 更新或失效。

### 4.3 时间旅行查询流程

1. 查询层解析 `FOR SYSTEM_TIME AS OF` 或时间范围语义。
2. CedarScan 根据快照时间构建读取视图。
3. 状态锚点和区间锚点用于快速判断实体在目标时间是否存在。
4. `CedarKey` 降序时间编码帮助定位不晚于快照时间的最新可见版本。
5. 边查询通过 EdgeOut/EdgeIn 双向 key 和版本折叠返回快照下的邻接关系。
6. 若 GCN/TMV 命中，则可由计算层直接响应遍历；否则回源 StorageD。

### 4.4 故障恢复流程

系统通过 WAL、Raft、2PC 状态、事务恢复管理器、超时管理器和服务发现维持故障恢复能力。对于分布式事务，commit 决策持久化后，即使部分参与者失败，也必须保留可恢复状态。对于服务注册，MetaD 通过心跳和清理线程剔除失联 StorageD 或 GraphD。

## 5. 预期实验结果

论文或技术报告中的实验应避免只给系统描述，需要围绕可复现实验验证方法有效性。

| 研究问题 | 对比对象 | 指标 | 预期结果 |
|---|---|---|---|
| LND-OCC 是否降低协调开销 | 单分区本地提交 vs 强制 2PC | 提交延迟、RPC 次数、吞吐 | 单分区路径延迟更低、RPC 更少 |
| TW-CD 是否降低误 abort | TW-CD vs key-only OCC | abort 率、验证耗时 | 时间不重叠负载下 abort 率下降 |
| MTH 是否提升分区质量 | MTH vs StaticHash | 跨分区边比例、负载方差、tail latency | 在时态热点和邻接遍历中降低跨分区代价 |
| CedarKey/CedarScan 是否提升时态读 | 锚点扫描 vs 普通版本扫描 | SST 访问数、P95/P99 延迟 | 快照查询减少无效历史扫描 |
| GCN/TMV 是否降低遍历延迟 | TMV 命中 vs 回源 StorageD | 遍历延迟、local hit ratio | 高命中场景延迟下降 |
| 故障恢复是否可靠 | 节点故障注入 | 恢复成功率、服务清理耗时 | 单节点故障后剩余节点可观测，MetaD 清理失联注册 |

正式报告性能数字时必须记录 commit、构建类型、机器配置、数据规模、Raft 模式、是否 test mode、查询分布、热点分布和所有环境变量。

## 6. 当前验证证据

### 6.1 已通过的本地门禁

2026-06-29 已在当前工作树运行短版发布门禁：

```bash
CEDAR_RELEASE_SOAK_SECONDS=9 CEDAR_RELEASE_SOAK_POLL_SECONDS=3 ./scripts/preflight_release_gate.sh
```

结果：通过。

该门禁覆盖：

| 检查项 | 结果 |
|---|---|
| CMake build | 通过 |
| 构建日志扫描 | 通过，无 `warning:`、`ld: warning`、`macro redefined`、`deprecated`、`duplicate libraries`、`/opt/homebrew/include/butil`、`error:` |
| C++ SDK 默认 MetaD 端口 | 通过，`CedarClientConfig::metad_port` 默认使用 `10559` |
| GCN 默认 coordinator 端口 | 通过，`gcn_coordinator` 默认使用 `127.0.0.1:10559` |
| Go CLI MetaD 启动参数 | 通过，MetaD 使用 `--listen host:9559 --grpc_port 10559`，StorageD/GraphD 使用分离的 `--bind host --port port` |
| Local smoke | 通过 |
| Deployment manifest static guard | 通过 |
| Docker static guard | 通过 |
| Helm static guard | 通过 |
| Kubernetes static guard | 通过 |
| Distributed smoke | 通过 |
| Non-test-mode Raft smoke | 通过 |
| CTest guard tests | 3/3 通过 |
| Short soak | 9 秒，通过 |
| StorageD failover smoke | 通过 |
| GraphD failover smoke | 通过 |
| TLS smoke | 通过 |
| mTLS smoke | 通过 |
| `git diff --check` | 通过 |

2026-06-30 又完成了一次正式 Docker/Linux 与 Kubernetes 隔离演练验证：

| 检查项 | 结果 |
|---|---|
| Docker/Linux Release image | 通过，`cedargraph/cedar:k8s-fix-20260630`，`sha256:b1115b2528830b1e6a78e917e234ee018634df393024da9fd8839d2e03dc0769` |
| Runtime dependency check | 通过，`cedar-metad`、`cedar-storaged`、`cedar-graphd` 无 `ldd not found` |
| brpc/braft Linux build noise | 通过，未出现 macOS 弃用 API、宏重定义或重复链接库警告 |
| Kubernetes recovery drill | 通过，证据目录 `/tmp/cedargraph-recovery-drill-evidence/20260629T210446Z` |
| MetaD DNS bootstrap race | 已修复，Helm 启动脚本在 braft init 前等待 peer DNS 可解析 |
| StorageD MetaD leader hint | 已修复，StorageD 将 MetaD leader hint 的 host 映射回配置中的 gRPC `10559` endpoint，不再把 Raft `9559` 当作注册目标 |
| Recovery drill OnDelete wait | 已修复，脚本使用 Pod Ready 条件等待 OnDelete StatefulSet |

本轮还单独验证：

```bash
cmake --build build --target metad storaged graphd -j2
./scripts/preflight_local_smoke.sh
./scripts/preflight_docker_static.sh
./scripts/preflight_deployment_static.sh
./scripts/preflight_helm_static.sh
./scripts/preflight_k8s_static.sh
pgrep -fl 'cedar-(metad|storaged|graphd)' || true
```

结果均通过，且最终没有服务进程残留。

### 6.2 已消除的第三方构建噪音

此前 macOS 下 brpc/braft 集成可能出现弃用 API、宏重定义、重复链接库或误用 `/opt/homebrew/include/butil` 的构建噪音。当前 release gate 会扫描构建日志并在发现这些模式时失败。最新通过结果表明这些噪音未出现在当前构建输出中。

### 6.3 仍未验证的上线边界

以下项目是当前仍需在目标环境继续证明的上线边界：

| 项目 | 当前状态 | 原因 |
|---|---|---|
| Docker image 实构建 | 已验证 | 本机 Docker daemon 可用，镜像构建成功，构建日志无 brpc/braft 弃用 API、宏重定义、重复链接库噪音 |
| Helm 真实渲染和 lint | 已验证 | Helm 可用，默认 TLS 与 TLS=false 渲染均已检查 |
| Kubernetes API 与 minikube 部署 | 已验证本机路径 | 本机 API server 可用；默认 TLS Helm 安装达到 7/7 Ready、0 restart，日志无 TLS/SSL 握手失败 |
| MetaD Raft PVC/Pod IP 恢复 | 已有门禁和隔离演练，未根治 | 新增 `preflight_k8s_raft_identity.sh` 可比对当前 Pod IP 与 PVC 中持久化 Raft peer IP；`--upgrade-guard` 会阻止把持久化 Pod IP 的 MetaD 集群当作普通滚动升级对象；`preflight_k8s_recovery_drill.sh` 可在隔离 namespace 自动完成 Helm 安装、证据包采集、离线校验、恢复计划生成、production gate 和 upgrade guard 预期失败验证；Helm 默认把 MetaD StatefulSet 更新策略设为 `OnDelete`，且误设 `RollingUpdate` 会 fail-fast，除非显式打开危险开关；Helm 默认启用 MetaD/StorageD/GraphD PDB；NetworkPolicy preflight 可验证策略对象和端口，但 CNI 执行效果需目标平台证明；底层 braft `PeerId` 仍会持久化解析后的 Pod IP，生产升级仍需维护窗口内的恢复/迁移方案 |
| 生产证书链和证书轮换 | 未验证 | 需要目标环境证书体系 |
| 长时间 soak/压力/混合故障 | 未验证 | 需要更长运行窗口和生产式 workload |
| 监控告警闭环 | 未验证 | 需要真实 Prometheus/Grafana/告警通道 |

因此，当前结论应表述为：本地代码、门禁脚本和部署清单静态一致性已达到发布候选状态；真实生产上线仍需目标环境验证。

## 7. 讨论

CedarGraph-Core 的核心设计不是单一算法，而是多个层次的组合：事务层减少不必要协调，冲突检测层利用时间窗口降低误判，存储层以 LSM 和降序时间 key 支持多版本读取，分区层利用时间和拓扑信息提高局部性，计算层通过 TMV 缓存降低遍历延迟。

该设计的主要风险也来自组合复杂性。事务正确性依赖 TW-CD、WAL、2PC 状态和恢复管理器共同保持不变量；查询正确性依赖 CedarKey、墓碑、compaction 和快照时间共同保持版本可见性；部署正确性依赖 MetaD Raft 端口和 gRPC 端口严格分离。因此，系统文档不能只说明方法，还必须把可验证的不变量和门禁脚本固定下来。

## 8. 上线前结论

当前工作树已经比普通“能编译”状态更接近发布候选：它通过了多进程本地 smoke、分布式 smoke、非测试模式 Raft、TLS/mTLS、故障注入、短 soak 和多类部署静态门禁。尤其是 Docker/Compose、Helm、Kubernetes 与脚本配置已统一到 MetaD Raft `9559` / MetaD gRPC `10559` / GraphD query `9669` / health `9668` / metrics `9667` 的端口模型。

但是，生产上线不是只由本地门禁证明。上线前仍必须在目标环境完成真实部署、真实负载、真实证书、真实监控和真实回滚验证。只有当这些目标环境证据也通过后，才能把结论从“发布候选”推进为“可上线”。
