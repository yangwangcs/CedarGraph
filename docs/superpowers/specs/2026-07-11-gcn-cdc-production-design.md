# CedarGraph GCN/CDC 生产化设计

日期：2026-07-11

状态：已确认设计，待实施

## 1. 背景与目标

CedarGraph 当前已经包含 TMV、EventApplier、GcnService、ScatterGatherRouter、StorageBackfillService、CoordinatorClient 和 graphcomputenode 等组件，但尚未形成可部署、可恢复、可验证的生产链路。

当前主要缺口如下。

1. StorageD 的提交路径没有生产 CDC 记录，也没有可重放的变更源。
2. `EnqueueEvent()` 和 `OnEventStream()` 只有测试调用者，方向也不适合作为 StorageD 到 GCN 的生产链路。
3. `graphcomputenode` 不注入 StorageD，默认 TMV 为空；现有回填只支持同进程 `CedarGraphStorage*`。
4. GraphD 只注册静态 GCN 地址，不验证可达性，无法依据所需版本判断 TMV 是否安全。
5. CMake 安装、Docker 镜像、Compose、Kubernetes、Helm 和预检没有完整包含 GCN 工作负载。
6. 现有测试以组件级或同进程 loopback 为主，没有真实 `GraphD -> StorageD -> CDC -> GCN -> GraphD` 多进程闭环。

本设计的目标是使 GCN 成为可选但真正可用的独立计算层：StorageD 产生持久化、可重放的分区 CDC；GCN 主动消费并维护 TMV；MetaD 管理 GCN 注册与分区租约；GraphD 只在版本满足时使用 GCN，并在故障或落后时正确回退 StorageD。

## 2. 非目标

本次不包含以下范围。

- 不以网络层“恰好一次”作为目标，而采用至少一次投递和幂等应用。
- 不删除旧 `OnEventStream` RPC；它暂时保留兼容，但不作为生产 CDC 主路径。
- 不把 GCN 变成持久化事实来源；StorageD 仍是权威存储。
- 不以手工 `BootstrapVertex`、测试内 `EnqueueEvent` 或单进程注入作为生产闭环证据。
- 不承诺在本次工作中完成所有图算法；重点是时态视图的数据正确性、可恢复性和部署可用性。

## 3. 总体架构

```text
                         +----------------------+
Clients ----------------> GraphD               |
                         | Query / Txn / Route  |
                         +----+------------+----+
                              |            |
                  metadata    |            | traversal
                              v            v
                     +--------+---+    +---+----------------+
                     | MetaD      |    | GCN replicas       |
                     | partitions |    | TMV + checkpoints  |
                     | nodes/GCN  |    +---+----------------+
                     +-----+------+        |
                           ^               | pull CDC / backfill
                           | heartbeat     v
                     +-----+-------------------------------+
                     | StorageD replicas                   |
                     | partition data + Raft + CDC logs    |
                     +-------------------------------------+
```

### 3.1 StorageD

StorageD 是图数据和 CDC 的权威来源。

- 只在写入正式提交并可见后产生 CDC。
- 每个分区维护单调 offset 的持久化 CDC 日志。
- CDC 记录与数据提交进入同一分区 Raft/WAL 决策。
- 提供批量拉取、高水位查询和一致性快照回填 RPC。
- 不感知具体 GCN 实例，也不保存消费者 checkpoint。

### 3.2 GCN

GCN 是可重建的时态计算视图。

- 从 MetaD 获取分区 leader、epoch 和 StorageD 地址。
- 为租约分配到本节点的分区拉取 CDC。
- 幂等应用 CDC 到 TMV，然后持久化 checkpoint。
- checkpoint 落后于 CDC 保留窗口时执行快照回填。
- 持久化 checkpoint、TMV snapshot 和节点元数据。
- 上报每分区 applied offset、applied version、回填状态和健康信息。

### 3.3 MetaD

MetaD 继续管理 StorageD 和分区拓扑，并扩展 GCN 控制面。

- 保存 GCN 注册信息、健康租约和服务地址。
- 为 GCN 分配分区租约，并附带单调 epoch。
- 在 GCN 退出或超时后重新分配租约。
- 为 GraphD 返回满足版本和健康条件的 GCN 候选。
- 不保存 CDC 内容和 GCN checkpoint。

### 3.4 GraphD

GraphD 负责选择查询路径，不负责同步 TMV。

- 从 MetaD 动态发现健康 GCN，不依赖静态 localhost 地址。
- 将查询的 `required_version` 传给 GCN。
- 仅在 GCN 对目标分区的 applied version 满足要求时使用 TMV。
- 对不可达、版本不足、租约失效、回填中或 cache miss 的请求回退 StorageD。
- 区分并监控版本不足回退、RPC 回退和缓存 miss 回退。

## 4. CDC 数据模型与协议

### 4.1 ChangeRecord

每条 CDC 记录至少包含以下字段。

| 字段 | 语义 |
|---|---|
| `partition_id` | 记录所属分区 |
| `partition_epoch` | 分区拓扑 epoch，用于拒绝旧 leader 响应 |
| `offset` | 分区内严格递增、连续的 CDC 位点 |
| `commit_version` | 写入可见的事务版本 |
| `txn_id` | 事务或批次标识 |
| `batch_index` / `batch_size` | 事务内记录顺序和完整性 |
| `entity_id` | 源实体 |
| `target_id` | 边目标；顶点事件可为空 |
| `entity_type` | 顶点、出边、入边或属性 |
| `edge_type` / `column_id` | 图语义标识 |
| `op` | create、update 或 delete |
| `valid_from` / `valid_to` | 业务时间范围 |
| `payload` | TMV 或回填所需的编码内容 |
| `checksum` | 记录完整性校验 |

offset 只在事务完成提交决策时成为可见高水位。已中止事务不得占用对消费者可见的 offset 空洞。

### 4.2 RPC

StorageService 增加向后兼容的 RPC。

- `GetChangeLogState(partition_id)`：返回 epoch、earliest offset、high watermark 和 committed version。
- `FetchChanges(partition_id, after_offset, limit, expected_epoch)`：返回有界批次、下一位点和当前高水位。
- `GetComputeSnapshot(partition_id, snapshot_version)`：流式返回一致性快照、snapshot version、resume offset 和 checksum。

FetchChanges 必须限制单次记录数和字节数，支持 deadline、取消和服务端背压。epoch 不匹配时返回明确的 stale-epoch 状态，引导 GCN 刷新 MetaD 路由。

## 5. 提交原子性

CDC 必须从已提交状态派生，不能从尚未提交的客户端请求或临时写集直接发布。

1. GraphD 或内部调用方发起写入。
2. StorageD 参与正常 OCC/2PC 和分区 Raft 决策。
3. 提交命令同时包含数据变更及对应 ChangeRecord 内容。
4. 分区状态机以确定性顺序应用数据变更，并把 CDC 追加到分区日志。
5. fsync/持久化成功后推进 committed version 和 CDC high watermark。
6. 失败或中止路径不产生可见 ChangeRecord。

对多记录事务，GCN 只有在完整批次到达且 batch metadata 一致时才应用；不完整批次保留在有界缓冲中并重新拉取。StorageD 重放同一 Raft entry 必须通过 `(partition_id, offset)` 保持幂等。

## 6. CDC 持久化与保留

每个分区使用独立的 append-only segment 集合，目录位于 StorageD 分区数据目录下。manifest 保存 segment 范围、校验和、earliest offset、high watermark、committed version 和 partition epoch。

- segment 创建、封口和 manifest 更新使用原子 rename 与 fsync。
- 启动恢复扫描 segment，验证 header、记录 checksum、offset 连续性和 manifest 一致性。
- 尾部不完整记录可以按明确规则截断；中间损坏或 offset 跳跃必须导致该分区启动失败，不能静默跳过。
- 保留策略同时受最短时间、最大字节数和管理水位约束。
- 删除 segment 前必须保留足够窗口供正常 GCN 停机恢复；过期消费者通过 snapshot 回填恢复。
- CDC 清理不能依赖单个 GCN 的 checkpoint，否则离线实例会无限阻止回收。

## 7. GCN 消费与 checkpoint

GCN 对每个租约分区运行独立、可取消的消费任务。

1. 从 MetaD 获取 leader、epoch 和 lease token。
2. 读取本地 checkpoint。
3. 调用 GetChangeLogState 校验 epoch 和保留窗口。
4. 若 checkpoint 有效，则从 `after_offset` 批量拉取。
5. 验证 checksum、offset 连续性、批次完整性和 lease token。
6. 以 `(partition_id, offset)` 幂等应用到 EventApplier/TMV。
7. 原子持久化 checkpoint。
8. 更新内存 applied version，并在心跳中上报。

checkpoint 至少包含 partition id、partition epoch、applied offset、applied version、TMV snapshot id 和 checksum。写入采用临时文件、fsync、atomic rename 和目录 fsync。

checkpoint 写入前崩溃会导致重复消费，这是允许的；EventApplier 必须拒绝已经应用的 offset。checkpoint 损坏时不得猜测位点，而应回退到有效 TMV snapshot 或执行完整回填。

## 8. 快照回填

当以下任一条件成立时进入回填状态。

- 本地没有 checkpoint。
- checkpoint 小于 StorageD earliest offset。
- TMV snapshot 缺失、损坏或与 checkpoint 不一致。
- 管理操作请求重建分区视图。

回填流程如下。

1. GCN 获取目标分区在 `snapshot_version` 下的一致性快照。
2. StorageD 返回 snapshot version 和与快照对应的 resume offset。
3. GCN 在临时 TMV 分片中构建数据并校验 checksum。
4. GCN 原子切换到新 TMV 分片并保存 snapshot/checkpoint。
5. GCN 从 resume offset 继续拉取 CDC。
6. 追到目标高水位后 readiness 才允许该分区承接查询。

回填期间 GraphD 必须走 StorageD，不得读取部分构建的 TMV。

## 9. 一致性与查询语义

CDC 使用至少一次投递和幂等应用，不承诺网络层恰好一次。

- 强版本查询：GraphD 提供 required version；GCN 只有在目标分区 applied version 不低于该版本时才返回结果。
- 显式最终一致性查询：调用方可允许旧视图，但响应必须包含实际 served version。
- 默认行为不能静默返回不满足版本要求的数据。
- GCN 响应必须包含 partition epoch、served version 和 cache hit/miss 状态。
- GraphD 对 stale epoch、版本不足、回填中、租约失效、RPC 错误和 miss 分别记录原因并回退。

TMV 的 watermark 不再仅依赖墙上时间启发式，而由已应用 CDC、活跃查询最小快照和保留策略共同计算。GC 不能删除仍可能被合法快照读取的版本。

## 10. 分区租约与迁移

MetaD 为每个分区分配一个有效 GCN owner 和单调 lease epoch。租约需要周期续约，超时后失效。

- GCN 只服务仍持有有效租约的分区。
- 旧 owner 在 lease 失效后停止消费并拒绝查询。
- 新 owner 从共享权威 StorageD 的 checkpoint/快照语义恢复，不依赖旧 GCN 在线。
- StorageD leader 迁移在切换点产生 barrier，并保持同一分区 offset 序列连续。
- GCN 对旧 partition epoch 的响应和 CDC 批次全部拒绝，刷新 MetaD 后从 checkpoint 重连。

首个生产版本可采用每分区单 owner，后续再扩展只读副本；不能用多个 GCN 无租约地重复消费全部分区作为生产负载均衡方案。

## 11. GraphD 路由与降级

GraphD 启动时不再把 `127.0.0.1:9780` 当作存在的默认服务。GCN 关闭或尚未注册时，GraphD 直接使用 StorageD。

MetaD 返回的 GCN 路由信息至少包含 endpoint、lease epoch、负责分区、applied version、状态和过期时间。GraphD 缓存该信息，并通过 watch 或有界刷新更新。

请求路径：

1. 计算目标分区和 required version。
2. 选择持有有效租约且 applied version 满足要求的 GCN。
3. 以短于 StorageD 总预算的 deadline 调用 GCN。
4. GCN 成功且响应 epoch/version 合法时返回结果。
5. 否则立即回退 StorageD，并保留完整遍历语义，包括方向、深度、边类型、过滤和路径内容。

GraphD 的 StorageD 回退必须有独立 E2E 验证，不能继续只返回 items count 和空 Path。

## 12. 部署设计

### 12.1 构建和安装

- 将 `graphcomputenode` 加入 CMake install target。
- Docker 构建阶段编译并在运行镜像复制 GCN 二进制。
- entrypoint 支持 `NODE_ROLE=gcn`。
- systemd 安装器创建 GCN 服务、数据目录、配置和健康检查。

### 12.2 Docker Compose

- 开发和生产 Compose 默认包含一个 GCN 服务。
- GCN 挂载持久化数据卷，连接 MetaD gRPC endpoint。
- GraphD 不硬编码 GCN 容器地址，而通过 MetaD 发现。
- 支持使用 Compose scale 增加 GCN 实例，不依赖固定 container name。

### 12.3 Kubernetes 与 Helm

- GCN 使用 StatefulSet，提供稳定身份和 PVC。
- 提供 headless Service 和客户端 gRPC Service。
- 配置 readiness、liveness、startup probe、PDB、资源 requests/limits 和安全上下文。
- NetworkPolicy 允许 GCN 访问 MetaD/StorageD，允许 GraphD 访问 GCN，拒绝不必要入口。
- values 支持 enabled、replica count、resources、persistence、CDC batch/retention/lag 参数。
- 删除把 9780 错列为 GraphD container port 的配置。

### 12.4 Readiness

进程存活不等于可承接查询。GCN readiness 必须满足：

- 已连接并注册到 MetaD。
- 本节点持有的所有必需租约有效。
- 每个服务分区已完成基础回填，或 applied version 达到配置的最大允许 lag。
- checkpoint 目录可写，后台消费任务健康。

## 13. 故障处理

| 故障 | 预期行为 |
|---|---|
| GCN 未部署或启动失败 | GraphD 直接走 StorageD，不等待无效 GCN 超时 |
| GCN 消费落后 | 版本门控拒绝 TMV，GraphD 回退，暴露 lag 告警 |
| GCN 重启 | 加载 snapshot/checkpoint 并继续消费；无有效快照则回填 |
| GCN checkpoint 损坏 | 拒绝猜测 offset，回退有效 snapshot 或完整回填 |
| StorageD leader 切换 | GCN 根据 MetaD epoch 重连并从 checkpoint 续接 |
| CDC 日志已过期 | GCN 获取一致性快照，再从 resume offset 续接 |
| CDC segment 中间损坏 | 对应 StorageD 分区启动失败并报告损坏，不静默跳过 |
| 单个 GCN 退出 | 租约到期，分区分配给其他健康 GCN |
| MetaD 暂时不可用 | 已有租约在有效期内继续；不可续约后停止服务以防旧 owner |

## 14. 监控与告警

### 14.1 StorageD 指标

- 每分区 earliest offset、high watermark 和 committed version。
- CDC segment 数量、字节数、追加/拉取延迟和校验失败数。
- FetchChanges 请求量、批次大小、stale epoch 和截断计数。

### 14.2 GCN 指标

- 每分区 checkpoint、applied version、high watermark 和 lag。
- 消费吞吐、重复记录数、批次重试、回填进度和失败数。
- TMV 命中率、miss、内存/磁盘占用、snapshot 年龄。
- 租约状态、MetaD 心跳和 StorageD 重连次数。

### 14.3 GraphD 指标

- GCN 查询量和成功率。
- version lag、RPC failure、lease invalid、backfill 和 cache miss 回退计数。
- GCN 与 StorageD 路径的延迟分布。

告警至少覆盖消费停滞、日志窗口即将越过 checkpoint、持续回填失败、无健康 GCN、CDC 校验失败和 lease 抖动。

## 15. 安全与资源边界

- CDC、snapshot 和 GCN RPC 使用现有 TLS/mTLS 配置，禁止生产环境静默降级明文。
- RPC 继承现有鉴权机制，并区分读取 CDC、管理租约和查询 TMV 权限。
- 所有批次、payload、分区数、并发消费任务和重试队列都有明确上限。
- checkpoint、segment 和 snapshot 路径必须防止目录穿越和符号链接替换。
- 消费循环使用 deadline、指数退避和可取消等待；关闭时必须有界退出。

## 16. 测试与验收

### 16.1 单元测试

- ChangeRecord 编解码、checksum 和版本兼容。
- segment rollover、恢复、尾部截断、中间损坏拒绝和 offset 连续性。
- checkpoint 原子写、损坏检测和幂等应用。
- lease epoch、stale response 和版本门控。

### 16.2 组件测试

- StorageD 提交后 FetchChanges 返回真实记录。
- 未提交、abort 和重复 Raft apply 不产生错误事件。
- GCN consumer 从 StorageD RPC 拉取、应用并保存 checkpoint。
- snapshot/backfill 与后续 CDC 无缝衔接。
- GCN 重启后不丢失、不重复影响最终 TMV 状态。

### 16.3 多进程 E2E

必须启动真实 MetaD、StorageD、GCN 和 GraphD 进程，验证：

1. 通过 GraphD 写入图数据。
2. StorageD 提交并产生 CDC。
3. GCN 自动消费并追到写入版本。
4. GraphD 遍历命中 GCN，返回正确路径和 served version。
5. kill/restart GCN 后从 checkpoint 继续。
6. GCN 落后或不可达时 GraphD 回退 StorageD，并保持结果语义一致。
7. 触发日志过期后，GCN 通过 snapshot 回填恢复。
8. StorageD leader 切换后 offset 连续，GCN 能继续消费。

### 16.4 部署验证

- Docker 镜像包含并能启动四个生产二进制。
- Compose smoke 证明四服务闭环。
- Kustomize build 和 server dry-run 生成有效 GCN StatefulSet/Service/PVC/NetworkPolicy。
- Helm lint/template 和部署 smoke 证明 values 与模板完整。
- 预检脚本必须要求 GCN workload、Service、端口、PVC、探针和网络连通性，而不只是检查 9780 字符存在。

## 17. 完成标准

只有以下证据全部成立，才能声称“GCN 已部署到系统中并且可用”。

- 真实 StorageD 提交可产生持久化、可重放的 CDC。
- GCN 可从独立 StorageD 消费，自动填充 TMV 并持久化 checkpoint。
- GCN 重启、重复事件、日志过期和 StorageD leader 切换均有通过的恢复测试。
- GraphD 能动态发现 GCN，执行版本门控，并在失败时语义正确地回退 StorageD。
- MetaD 的 GCN 注册、健康租约和分区归属有测试支撑。
- CMake/install、Docker、Compose、Kubernetes 和 Helm 都包含真实 GCN 工作负载。
- 多进程 E2E 不依赖手工 BootstrapVertex 或测试内 EnqueueEvent。
- 监控、告警、安全和资源上限均有配置和验证。
- 所有相关测试和部署门禁为阻断式通过，而不是 continue-on-error 或未覆盖的静态字符串检查。

## 18. 实施顺序

实施按依赖顺序拆分，但完成声明仍以第 17 节整体标准为准。

1. 定义 ChangeRecord、CDC RPC、错误码和兼容策略。
2. 实现 StorageD 分区 CDC log、提交原子性和恢复。
3. 实现 GCN StorageD RPC client、consumer、checkpoint 和 snapshot/backfill。
4. 扩展 MetaD GCN 注册、租约和路由发现。
5. 更新 GraphD 版本门控、动态发现和完整 StorageD 回退。
6. 补齐构建、安装、Docker、Compose、Kubernetes、Helm 和预检。
7. 运行组件、多进程、故障恢复和部署验证。
