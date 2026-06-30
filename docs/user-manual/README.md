# CedarGraph-Core 中文说明书

## 1. 说明书定位

本文档面向希望构建、运行、测试和理解 CedarGraph-Core 的开发者。它与根目录 `README.md` 的区别是：根文档偏论文式问题和方法说明，本文档偏工程操作说明。所有命令默认在仓库根目录执行。

当前仓库路径示例：

```bash
<repo-root>
```

## 2. 系统组成

CedarGraph-Core 主要由以下服务组成。

| 服务 | 可执行文件 | 默认职责 |
|---|---|---|
| MetaD | `build/cedar-metad` | 元数据、schema、分区信息、服务注册 |
| StorageD | `build/cedar-storaged` | 分区存储、WAL、LSM、Raft、MVCC |
| GraphD | `build/cedar-graphd` | 查询入口、Cypher 执行、事务协调 |
| GCN | `build/graphcomputenode` | 图计算节点、TMV 缓存、CDC 事件 |

核心库模块：

| 模块 | 路径 | 作用 |
|---|---|---|
| DTX | `src/dtx` | LND-OCC、TW-CD、2PC、恢复、超时、死锁检测 |
| Storage | `src/storage` | LSM、MemTable、WAL、compaction、缓存 |
| SST | `src/sst` | zone-columnar SST、Bloom Filter、压缩 |
| Partition | `src/partition` | StaticHash、MTH 流式分区 |
| Query/Cypher | `src/query`、`src/cypher` | 快照扫描、Cypher 解析与执行 |
| GCN | `src/gcn` | TMV、事件应用、分布式遍历 |

### 2.1 SDK/API 当前边界

当前 C++ `CedarClient` 已覆盖连接池、TLS 参数传递、JWT fail-closed、部分 MetaD DDL 和查询入口辅助能力。以下接口仍是明确边界，不应作为生产上线承诺：

| 接口 | 当前行为 | 上线含义 |
|---|---|---|
| `CreateTag` | 返回 `success=false` 和未实现错误 | 不会假成功；schema 标签创建应走已验证的服务端/CLI 路径 |
| `CreateEdge` | 返回 `success=false` 和未实现错误 | 不会假成功；边类型创建应走已验证的服务端/CLI 路径 |
| `DropSpace` | 返回 `false` | 不会假成功；生产删除空间必须走受控运维流程 |
| `ListEdges` | 当前 MetaD proto 无对应接口，返回空结果并记录 warning | 空结果不能解释为“没有边类型” |
| `ClusterBackup::CreateBackup/RestoreBackup` | 对已知组件返回 `FAILED`/`false` 和未实现错误 | 当前客户端集群级备份恢复不 shell 调用不存在的 `cedar-backup/cedar-restore`；生产使用 `scripts/deploy.sh backup/restore` 或底层 `BackupManager` |
| `ClusterBackup::DownloadFromS3` | 返回 `false` | 远程备份元数据注册尚未实现，不会把无法验证/管理的下载结果报告为可用备份 |
| `BackupConfig.encrypt=true` | `ClusterBackup::Initialize()` 直接失败 | 客户端备份加密暂不支持，不会把未加密备份报告为加密成功 |

## 3. 安装依赖

建议环境：

- macOS 或 Linux
- C++17 编译器
- CMake 3.14+
- gRPC / Protobuf
- OpenSSL
- libcurl
- yaml-cpp
- LZ4
- 可选 Zstd

首次拉取或依赖缺失时运行：

```bash
./scripts/setup_deps.sh
```

该脚本用于准备 vendored brpc/braft。若 CMake 报错提示 `third_party/brpc` 或 `third_party/braft` 不存在，优先执行此脚本。

## 4. 构建

Debug 构建：

```bash
mkdir -p build
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j4
```

Release 构建：

```bash
mkdir -p build
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j4
```

构建指定测试目标：

```bash
cmake --build build --target test_twcd_engine test_lnd_occ -j4
```

当前关键测试目标已清理 brpc/braft 第三方头警告、`LOG/CHECK` 宏重定义警告和重复 `pthread` 链接警告。若新增目标再次出现类似噪音，优先检查是否误用了系统 brpc/butil 头、是否混用了 Cedar logging shim 与 butil logging，以及是否重复显式链接 `pthread`。

## 5. 快速启动

### 5.1 单机脚本

```bash
./scripts/start_standalone.sh start
```

停止：

```bash
./scripts/start_standalone.sh stop
```

### 5.2 分布式脚本

```bash
./scripts/start_distributed.sh start
```

停止：

```bash
./scripts/start_distributed.sh stop
```

### 5.3 手动启动

如需调试单个服务，可以分别启动：

```bash
./build/cedar-metad
./build/cedar-storaged
./build/cedar-graphd
```

不同工具和脚本支持的参数可能不同。若手动运行失败，优先查看对应 `tools/*.cc` 中的 gflags 定义，以及 `scripts/*.conf` 示例配置。

## 6. 基础查询示例

GraphD 是主要查询入口。典型 Cypher 写法如下。

创建顶点：

```cypher
CREATE (n:Person {name: 'Alice', age: 30, city: 'Beijing'})
```

创建边：

```cypher
CREATE (a:Person)-[:KNOWS {since: 2020}]->(b:Person)
```

查询顶点：

```cypher
MATCH (n:Person) RETURN n
```

属性过滤：

```cypher
MATCH (n:Person)
WHERE n.age > 25 AND n.city = 'Beijing'
RETURN n
```

邻接查询：

```cypher
MATCH (a:Person)-[:KNOWS]->(b:Person)
RETURN a.name, b.name
```

更新：

```cypher
MATCH (n:Person {name: 'Alice'})
SET n.age = 31
```

删除：

```cypher
MATCH (n:Person {name: 'Alice'})
DELETE n
```

## 7. 时态查询

CedarGraph-Core 的数据模型保留多版本信息，查询层支持时态语义扩展。示例：

指定时间点查询：

```cypher
MATCH (n:Person)
FOR SYSTEM_TIME AS OF timestamp('2024-01-01')
RETURN n
```

时间范围查询：

```cypher
MATCH (n:Person)
FOR SYSTEM_TIME BETWEEN timestamp('2024-01-01') AND timestamp('2024-12-31')
RETURN n
```

底层读取由 `CedarScan`、`CedarKey` 降序时间戳和 LSM 版本查询支持。对于论文或报告实验，建议同时记录快照时间、数据规模、版本数和 compaction 状态。

## 8. 事务与冲突检测使用说明

### 8.1 LND-OCC

LND-OCC 将事务分成单分区、同时序范围跨分区和跨时序范围跨分区三类。对用户来说，最重要的是：如果写入集中只涉及一个分区，系统应走本地提交路径，避免不必要的 2PC。

核心入口：

- `LndOccEngine::SinglePartitionCommit()`
- `LndOccEngine::SameTemporalRangeCommit()`
- `LndOccEngine::FullTwoPhaseCommit()`

### 8.2 TW-CD

TW-CD 使用时序窗口降低冲突误判。开发者构建事务上下文时，应尽量给读写集传入准确窗口：

```cpp
DistributedTxnContext ctx;
ctx.AddParticipant(1);
ctx.SetTemporalWindow(TemporalWindow(100, 200));
ctx.AddToWriteSet(key, TemporalWindow(100, 200));
```

若不提供窗口，默认 `TemporalWindow()` 是空窗口。业务层应根据查询语义设置窗口，否则冲突检测可能不能代表真实时间范围。

### 8.3 已修复的底层问题

本次检查修复了两处事务底层问题：

1. `LndOccEngine` 默认没有创建 `TwcdEngine`，导致初始化后的本地 coordinator 实际跳过 TW-CD 验证。
2. TW-CD 注销窗口时先删除活跃事务记录，之后无法根据记录清理写集索引，可能留下 key 索引残留。

新增或通过的关键测试：

```bash
./build/tests/test_twcd_engine
./build/tests/test_lnd_occ
```

当前结果：

| 测试 | 结果 |
|---|---|
| `test_twcd_engine` | 18/18 通过 |
| `test_lnd_occ` | 15/15 通过 |

## 9. 分区策略说明

系统提供 StaticHash 和 MTH Stream 两类策略。

StaticHash 适合做 baseline，对 key 做简单哈希分配。MTH Stream 适合时态事件流，它利用 temporal sketch、实体归属、fast path 阈值和负载约束决定事件分区。

建议实验：

```bash
cmake --build build --target test_mth_partitioner -j4
./build/tests/test_mth_partitioner
```

对比指标：

- 跨分区边比例
- 每分区事件数方差
- fast path ratio
- 时间窗口热点下的 tail latency

## 10. GCN 使用说明

GCN 是图计算节点，用于维护 TMV 计算视图并提供遍历服务。相关接口包括：

- `Traverse`
- `SubQuery`
- `OnCacheInvalidate`
- `OnEventStream`

如果启用认证，调用方需要提供 authorization metadata。GCN 本地 miss 时可通过 Scatter-Gather 路由到其他节点，或通过 backfill 服务回源存储补全。

GCN 的 coordinator 是 MetaD gRPC 服务，默认地址为 `127.0.0.1:10559`。不要把 GCN 或 SDK 客户端指向 MetaD Raft 端口 `9559`；`9559` 只用于 MetaD Raft/listen，业务元数据 API 使用 `10559`。

建议测试：

```bash
ctest --test-dir build -R gcn --output-on-failure
```

## 11. 常用测试命令

运行所有 CTest：

```bash
ctest --test-dir build --output-on-failure
```

当前完整门禁验证结果：

| 范围 | 结果 |
|---|---|
| `ctest --test-dir build --output-on-failure` | 1356/1356 个实际运行测试通过 |
| Disabled 测试 | 2 个性能基准未运行 |

合计：1358 个注册测试中，1356 个实际运行测试通过，0 failed。当前 disabled 测试只保留 CedarUpdate 性能基准，不作为 CI 正确性测试：

| 测试 | 当前状态 |
|---|---|
| `CedarUpdateValidationTest.ValidationPerformance` | 性能基准，不作为 CI 正确性测试 |
| `CedarUpdateE2ETest.WritePerformance` | 性能基准，不作为 CI 正确性测试 |

新增 disabled 测试必须在源码相邻位置写明 `BLOCKED:` 原因，并说明它是功能缺口、外部环境依赖还是性能基准。

本地进程级烟测：

```bash
./scripts/preflight_release_gate.sh
./scripts/preflight_local_smoke.sh
./scripts/preflight_distributed_smoke.sh
./scripts/preflight_soak.sh
./scripts/preflight_failover_smoke.sh
./scripts/preflight_graphd_failover_smoke.sh
./scripts/preflight_tls_smoke.sh
```

`preflight_release_gate.sh` 是上线前本地总入口，会串行执行构建、构建日志扫描、单机烟测、分布式烟测、non-test-mode Raft 烟测、关键 CTest 守卫、短时 soak、StorageD 故障注入、GraphD 故障注入、TLS/mTLS 烟测和 `git diff --check`。关键 CTest 守卫包括 disabled 测试 `BLOCKED:` 文档约束和 metrics shutdown 回归测试。单项脚本用于定位某一类问题。

2026-06-29 当前工作树已通过短版 release gate：

```bash
CEDAR_RELEASE_SOAK_SECONDS=9 CEDAR_RELEASE_SOAK_POLL_SECONDS=3 ./scripts/preflight_release_gate.sh
```

该次运行覆盖构建、构建日志警告扫描、Docker/Compose 静态检查、Helm 静态检查、Kubernetes 静态检查、部署清单静态检查、本地 smoke、分布式 smoke、non-test-mode Raft、CTest 守卫、短时 soak、StorageD 故障注入、GraphD 故障注入、TLS/mTLS 和空白差异检查。构建日志未再出现 macOS 下 brpc/braft 的弃用 API、宏重定义或重复链接库警告。

本轮额外修复并验证了两个默认端口一致性问题：C++ `CedarClientConfig` 默认 MetaD 端口已从 Raft `9559` 改为 gRPC `10559`；GCN `gcn_coordinator` 默认地址也已改为 `127.0.0.1:10559`。相关客户端/GCN 测试和部署静态门禁均已覆盖。

Go CLI 的本地集群配置也使用同一端口语义：`tools/cedargraph/config/cedar.yaml` 中 `metad.port` 是 MetaD Raft/listen 端口 `9559`，`metad.grpc_port` 是客户端 gRPC API 端口 `10559`。CLI 启动 MetaD 时使用 `--listen host:9559 --grpc_port 10559`；启动 StorageD/GraphD 时使用分离的 `--bind host --port port`，避免把 `host:port` 误传给只接受地址的 `--bind` 参数。

CLI 默认配置不再设置 `config_file: config/cedar.yaml`。原因是当前服务端会先解析命令行参数，再加载 `--config` 文件；若本地 CLI 同时传入 `--config` 和端口/数据目录 flags，根目录配置会覆盖 CLI 中的本地端口和 `/tmp/cedar` 数据目录，导致本地启动落到生产式路径或 TLS 配置上。

默认 gate 已覆盖 TLS/mTLS、non-test-mode Raft 和故障注入；正式发版前建议额外启用全量 CTest 和更长 soak：

```bash
CEDAR_RELEASE_FULL_CTEST=1 CEDAR_RELEASE_SOAK_SECONDS=300 CEDAR_RELEASE_SOAK_POLL_SECONDS=5 ./scripts/preflight_release_gate.sh
```

单机烟测会在 `/tmp/cedar/preflight-*` 中启动 1 个 MetaD、1 个 StorageD 和 1 个 GraphD；分布式烟测会在 `/tmp/cedar/distributed-preflight-*` 中启动 3 个 MetaD、3 个 StorageD 和 1 个 GraphD；短时 soak 会在 `/tmp/cedar/soak-preflight-*` 中启动分布式拓扑并持续轮询，默认 30 秒，可用 `CEDAR_SOAK_SECONDS` 调整；StorageD 故障注入烟测会在 `/tmp/cedar/failover-preflight-*` 中受控终止一个 StorageD，等待 MetaD 心跳超时离线检测，并验证剩余节点仍可观测；GraphD 故障注入烟测会在 `/tmp/cedar/graphd-failover-preflight-*` 中启动 3 个 GraphD，杀掉一个查询入口，并等待 MetaD GraphD 清理线程移除失联节点；TLS/mTLS 烟测会在非 test-mode 下验证 gRPC 凭据、GraphD 认证初始化和安全链路启动。上述脚本都会检查端口监听、pid 存活、health/metrics HTTP 端点、运行日志严重诊断以及 SIGTERM 温和退出。若非故障注入目标服务需要 SIGKILL，脚本会失败。脚本的 start/log scan/detection 输出写入各自隔离集群目录，避免重复或并发运行时互相污染。

2026-06-30 已额外用本机 minikube API server 验证 Helm chart lint、`helm template`、Kustomize 渲染以及 Kubernetes API server-side dry-run；`scripts/preflight_k8s_server_dry_run.sh` 已纳入 release gate，在不创建资源的前提下验证 Kustomize 与 Helm 渲染结果能被 API server 接受。

同日已验证本机固定 Docker 镜像 `cedargraph/cedar:k8s-fix-20260630` 存在，镜像 ID 为 `sha256:b1115b2528830b1e6a78e917e234ee018634df393024da9fd8839d2e03dc0769`，且容器内 `/usr/local/bin/cedar-metad`、`/usr/local/bin/cedar-storaged`、`/usr/local/bin/cedar-graphd` 可执行、`--help` 可运行、`ldd` 无缺失共享库。该检查已固化为 `scripts/preflight_docker_image_runtime.sh` 并纳入 release gate；它不会启动 CedarGraph 服务，也不会挂载或修改宿主机数据目录。

同日已用 `scripts/preflight_compose_smoke.sh` 验证 production Compose 核心拓扑在隔离临时目录中可以启动到 7/7 healthy：3 个 MetaD、3 个 StorageD 和 1 个 GraphD 均使用固定镜像 `cedargraph/cedar:k8s-fix-20260630`，数据、日志和证书目录位于 `/tmp/cedar/compose-smoke-*`，脚本结束时会执行 `docker compose down --remove-orphans`。默认 smoke 使用显式 `CEDAR_GRPC_ALLOW_INSECURE=1` 的开发模式验证编排、二进制和健康检查路径；`CEDAR_COMPOSE_SMOKE_TLS=1 scripts/preflight_compose_smoke.sh` 会生成临时自签证书并验证生产 Compose 的 MetaD、StorageD、GraphD TLS 环境变量、TLS 挂载和健康检查路径可以达到 7/7 healthy。

当前尚未由本机证明的部分包括正式 CA/生产证书链下的 Docker Compose `up`、Kubernetes API server-side apply 后的真实 Pod 运行态、生产证书链轮换、长时间压力、回滚演练和监控告警闭环。这些需要在目标环境补齐。

按模块过滤：

```bash
ctest --test-dir build -R dtx --output-on-failure
ctest --test-dir build -R storage --output-on-failure
ctest --test-dir build -R cypher --output-on-failure
ctest --test-dir build -R gcn --output-on-failure
ctest --test-dir build -R partition --output-on-failure
```

只跑本次修复相关测试：

```bash
./build/tests/test_twcd_engine
./build/tests/test_lnd_occ
```

## 12. 性能实验建议

为了让实验结果可复现，建议每次报告都记录：

| 项目 | 示例 |
|---|---|
| commit | `git rev-parse HEAD` |
| 构建类型 | Debug / Release |
| 机器 | CPU、内存、磁盘、OS |
| Raft | 启用 / 关闭 |
| 数据规模 | 顶点、边、版本数 |
| workload | 写入比例、读写窗口分布、热点分布 |
| 指标 | throughput、P50/P95/P99、abort rate、RPC 次数 |

推荐对比：

1. LND-OCC 单分区提交 vs 完整 2PC。
2. TW-CD vs key-only OCC。
3. StaticHash vs MTH Stream。
4. CedarScan with anchors vs 普通版本扫描。
5. GCN/TMV local hit vs 回源 StorageD。

历史 README 中的高吞吐数字只能作为旧基准线，正式引用前必须用当前代码重新跑。

## 13. 运维与监控

常见健康检查：

```bash
curl http://localhost:9668/health
curl http://localhost:9667/metrics
curl http://localhost:7000/health
curl http://localhost:7001/metrics
```

如果脚本提供封装，也可以使用：

```bash
./scripts/cedar_health_check.sh
./scripts/cedar_health_monitor.sh
```

建议关注：

- GraphD 查询延迟和慢查询日志
- StorageD WAL sync、flush、compaction 延迟
- Raft leader 变更次数
- TW-CD conflict_detected / total_checks
- LND-OCC coordination_ratio
- GCN local hit ratio 和 backfill 次数

## 14. 排错

### 14.1 CMake 找不到 brpc/braft

现象：

```text
third_party/brpc or third_party/braft not found
```

处理：

```bash
./scripts/setup_deps.sh
cmake -S . -B build
```

### 14.2 gRPC 或 Protobuf 找不到

检查本机安装，并确认 CMake 能找到对应 config。macOS Homebrew 环境通常需要 `/opt/homebrew` 在默认搜索路径内。

### 14.3 服务端口冲突

默认端口：

| 服务 | 端口 |
|---|---:|
| GraphD | 9669 |
| GraphD health | 9668 |
| GraphD metrics | 9667 |
| StorageD | 9779 |
| StorageD health | 7000 |
| StorageD metrics | 7001 |
| MetaD Raft | 9559 |
| MetaD gRPC | 10559 |

使用 `lsof -i :9669` 等命令查找占用进程。分布式开发脚本支持通过 `CEDAR_STORAGED_HEALTH_BASE_PORT`、`CEDAR_STORAGED_METRICS_BASE_PORT`、`CEDAR_GRAPHD_HEALTH_PORT` 和 `CEDAR_GRAPHD_METRICS_PORT` 覆盖辅助端口。

### 14.4 事务测试失败

优先运行：

```bash
./build/tests/test_twcd_engine
./build/tests/test_lnd_occ
```

若 TW-CD 相关测试失败，重点检查：

- 事务是否调用 `BeginTransaction()`
- `TemporalWindow` 是否为空窗口
- 提交/中止后是否清理写集和窗口
- `LndOccEngine` 是否在 `enable_twcd=true` 时持有 `TwcdEngine`

### 14.5 查询结果缺少历史版本

重点检查：

- 写入 key 的 timestamp 是否符合预期
- 查询使用的 snapshot time 是否早于写入时间
- tombstone 是否在快照前生效
- compaction 是否错误回收了仍需保留的版本
- `GetEntityColumnIds()` 是否能发现对应 column

## 15. 推荐阅读顺序

1. 根目录 `README.md`：理解论文式问题、方法和预期结果。
2. `docs/ACADEMIC_SYSTEM_DESCRIPTION_CN.md`：阅读完整中文论文式系统说明。
3. `docs/architecture/README.md`：理解模块和协议细节。
4. `docs/api/README.md`：查看 API 草案。
5. `tests/dtx/unit/test_twcd_engine.cc`：理解 TW-CD 正确性。
6. `tests/dtx/unit/test_lnd_occ.cc`：理解 LND-OCC 提交流程。
7. `tests/storage/test_cedar_scan*.cc`：理解快照查询。
8. `tests/gcn/*`：理解计算节点行为。
