# CedarGraph 生产环境部署指南

> **版本**: 1.1.0
> **日期**: 2026-06-29
> **状态**: 本地发布门禁已覆盖；真实生产上线前仍需目标环境验证

本文档描述 CedarGraph-Core 的生产部署前置条件、Docker Compose 部署、Kubernetes 部署、运行时安全参数和运维检查。当前代码已经通过本地 Release 门禁、全量可运行测试、non-test-mode Raft 烟测、TLS/mTLS 烟测、短时 soak 与单节点故障注入；这些证据说明代码和部署脚本已经具备上线前候选状态，但不等同于在任意生产环境中 100% 就绪。正式上线仍必须完成真实证书链、真实 Secret、真实负载、真实回滚和监控告警验证。

---

## 目录

1. [部署架构](#1-部署架构)
2. [环境要求](#2-环境要求)
3. [Docker Compose 部署](#3-docker-compose-部署)
4. [Kubernetes 部署](#4-kubernetes-部署)
5. [配置说明](#5-配置说明)
6. [监控告警](#6-监控告警)
7. [备份恢复](#7-备份恢复)
8. [常见问题](#8-常见问题)
9. [上线前门禁](#9-上线前门禁)

---

## 1. 部署架构

### 1.1 组件说明

| 组件 | 数量 | 用途 | 资源需求 |
|------|------|------|----------|
| MetaD | 3 | Raft 元数据管理 | 2 CPU, 4GB RAM |
| StorageD | 3 | LSM 存储引擎 | 4 CPU, 8GB RAM |
| GraphD | 2 | 查询路由与分布式查询分发；当前 QueryD 逻辑已合并入 GraphD | 2 CPU, 4GB RAM |
| Prometheus | 1 | 指标收集 | 1 CPU, 2GB RAM |
| Grafana | 1 | 监控可视化 | 1 CPU, 1GB RAM |

生产部署必须区分 MetaD 的两个端口：`9559` 是 MetaD Raft/listen 端口，`10559` 是业务元数据 gRPC API。StorageD、GraphD、GCN、C++ SDK 和其它业务客户端都应连接 `10559`；只有 MetaD 节点之间的 Raft peer 配置使用 `9559`。

### 1.2 网络架构

```
                    ┌─────────────────┐
                    │   Load Balancer │
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
         ┌────┴────┐   ┌────┴────┐   ┌────┴────┐
         │ GraphD-1│   │ GraphD-2│   │ GraphD-N│
         └────┬────┘   └────┬────┘   └────┬────┘
              │              │              │
              └──────────────┼──────────────┘
                             │
         ┌───────────────────┼───────────────────┐
         │                   │                   │
    ┌────┴────┐         ┌────┴────┐         ┌────┴────┐
    │StorageD1│         │StorageD2│         │StorageD3│
    └────┬────┘         └────┬────┘         └────┬────┘
         │                   │                   │
         └───────────────────┼───────────────────┘
                             │
         ┌───────────────────┼───────────────────┐
         │                   │                   │
    ┌────┴────┐         ┌────┴────┐         ┌────┴────┐
    │ MetaD-1 │         │ MetaD-2 │         │ MetaD-3 │
    └─────────┘         └─────────┘         └─────────┘
```

---

## 2. 环境要求

### 2.1 硬件要求

| 环境 | CPU | 内存 | 磁盘 | 网络 |
|------|-----|------|------|------|
| 开发环境 | 4核 | 8GB | 50GB SSD | 1Gbps |
| 测试环境 | 8核 | 16GB | 100GB SSD | 1Gbps |
| 生产环境 | 16核+ | 32GB+ | 500GB+ SSD | 10Gbps |

### 2.2 软件要求

- **操作系统**: Ubuntu 20.04+ / CentOS 7+ / macOS 12+
- **Docker**: 20.10+
- **Docker Compose**: 2.0+
- **Kubernetes**: 1.24+ (可选)
- **kubectl**: 1.24+ (可选)

### 2.3 上线前本地门禁

每次发布候选版本至少执行一次完整门禁：

```bash
CEDAR_RELEASE_FULL_CTEST=1 CEDAR_RELEASE_SOAK_SECONDS=300 CEDAR_RELEASE_SOAK_POLL_SECONDS=5 ./scripts/preflight_release_gate.sh
```

该门禁默认包含构建、构建日志扫描、单机烟测、Docker/Compose 运行时检查、Helm/Kubernetes 静态检查、Kubernetes API server-side dry-run、一次性 namespace Kubernetes 恢复演练、分布式烟测、non-test-mode Raft 烟测、关键 CTest 守卫、soak、StorageD 故障注入、GraphD 故障注入、TLS/mTLS 烟测和 `git diff --check`。当前基线为 `ctest` 注册测试 1358 个，其中 1356 个实际运行测试通过；仅 `CedarUpdateValidationTest.ValidationPerformance` 与 `CedarUpdateE2ETest.WritePerformance` 两个性能基准测试未默认运行。

完整门禁默认会执行 `preflight_k8s_recovery_drill.sh`，在隔离 namespace 中安装临时 Helm release、验证 production gate、生成 Raft evidence 和恢复计划，并在成功后清理演练 namespace。只有在没有真实 Kubernetes API server 或发布记录已经单独附上等价演练证据时，才允许显式设置 `CEDAR_RELEASE_K8S_RECOVERY_DRILL=0`；若连 API server dry-run 也无法执行，必须设置 `CEDAR_RELEASE_SKIP_K8S_API=1` 并记录该发布不是生产上线证明。

### 2.4 GraphD 认证与 TLS 要求

生产模式不得依赖测试账号。GraphD 在非测试模式下要求认证配置可初始化，否则会 fail fast，避免无认证服务暴露。

| 变量 | 要求 | 说明 |
|------|------|------|
| `CEDAR_GRAPHD_AUTH_JWT_SECRET` | 必填，建议至少 32 字节 | JWT 签名密钥；必须来自 Secret 管理系统 |
| `CEDAR_GRAPHD_AUTH_USER` | 必填 | 初始管理员账号 |
| `CEDAR_GRAPHD_AUTH_PASSWORD` | 必填 | 初始管理员密码 |
| `CEDAR_GRAPHD_AUTH_ROLE` | 可选，默认 `admin` | 初始账号角色 |
| `CEDAR_GRPC_TLS_ENABLED` | 生产建议 `1` | 启用 gRPC TLS |
| `CEDAR_GRPC_SERVER_CERT` | TLS 必填 | 服务端证书路径 |
| `CEDAR_GRPC_SERVER_KEY` | TLS 必填 | 服务端私钥路径 |
| `CEDAR_GRPC_CA_CERT` | TLS/mTLS 必填 | CA 证书路径 |
| `CEDAR_GRPC_MTLS_ENABLED` | mTLS 时设为 `1` | 要求客户端证书 |
| `CEDAR_GRPC_CLIENT_CERT` | mTLS 客户端必填 | 客户端证书路径 |
| `CEDAR_GRPC_CLIENT_KEY` | mTLS 客户端必填 | 客户端私钥路径 |
| `GRAFANA_PASSWORD` | 必填，建议至少 12 字符 | Grafana 管理员密码；不得使用默认弱口令 |

`CEDAR_GRPC_ALLOW_INSECURE=1` 仅允许用于开发、临时诊断或受控测试环境，不得作为生产默认值。

---

## 3. Docker Compose 部署

### 3.1 快速启动

```bash
# 克隆仓库
git clone https://github.com/cedargraph/cedar-docker-compose.git
cd cedar-docker-compose

# 启动集群
docker-compose up -d

# 查看状态
docker-compose ps

# 查看日志
docker-compose logs -f

# 停止集群
docker-compose down
```

### 3.2 自定义配置

编辑 `docker-compose.yml`:

```yaml
# 修改数据目录
volumes:
  - /data/cedar/meta0:/data/meta
  - /data/cedar/storage0:/data/storage

# 修改端口映射
ports:
  - "9559:9559"    # MetaD Raft
  - "10559:10559"  # MetaD gRPC
  - "9779:9779"  # StorageD
  - "9669:9669"  # GraphD

# 修改资源限制
deploy:
  resources:
    limits:
      cpus: '4'
      memory: 8G
    reservations:
      cpus: '2'
      memory: 4G
```

### 3.3 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `CEDAR_VERSION` | k8s-fix-20260630 | 镜像版本；生产必须使用固定标签，禁止使用 `latest` |
| `META_GRPC_PORT` | 10559 | 宿主机暴露的 MetaD gRPC 端口 |
| `DATA_DIR` | /data | 数据目录 |
| `LOG_LEVEL` | INFO | 日志级别 |
| `CEDAR_GRAPHD_AUTH_JWT_SECRET` | 无 | GraphD 生产认证必填 |
| `CEDAR_GRAPHD_AUTH_USER` | 无 | GraphD 初始管理员账号 |
| `CEDAR_GRAPHD_AUTH_PASSWORD` | 无 | GraphD 初始管理员密码 |
| `CEDAR_GRPC_TLS_ENABLED` | 0 | 生产建议开启 |
| `CEDAR_GRPC_SERVER_CERT` | 无 | TLS 服务端证书路径 |
| `CEDAR_GRPC_SERVER_KEY` | 无 | TLS 服务端私钥路径 |
| `CEDAR_GRPC_CA_CERT` | 无 | CA 证书路径 |
| `GRAFANA_PASSWORD` | 无 | Grafana 管理员密码，生产必填 |

---

## 4. Kubernetes 部署

### 4.0 GraphD Secret 准备

GraphD 生产模式需要认证 Secret 和 TLS Secret。应用 `k8s/graphd.yaml` 前必须先创建：

```bash
kubectl create namespace cedargraph --dry-run=client -o yaml | kubectl apply -f -

kubectl create secret generic cedargraph-graphd-auth -n cedargraph \
  --from-literal=jwt-secret='replace-with-at-least-32-bytes-secret' \
  --from-literal=user='admin' \
  --from-literal=password='replace-with-strong-password' \
  --from-literal=role='admin' \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl create secret generic cedargraph-graphd-tls -n cedargraph \
  --from-file=tls.crt=/path/to/tls.crt \
  --from-file=tls.key=/path/to/tls.key \
  --from-file=ca.crt=/path/to/ca.crt \
  --from-file=client.crt=/path/to/client.crt \
  --from-file=client.key=/path/to/client.key \
  --dry-run=client -o yaml | kubectl apply -f -
```

临时 preflight、恢复演练或内部测试环境可以用脚本生成覆盖 CedarGraph K8s DNS 的 Secret YAML：

```bash
CEDAR_K8S_NAMESPACE=cedargraph \
CEDAR_HELM_RELEASE=cedargraph \
CEDAR_TLS_SECRET_NAME=cedargraph-graphd-tls \
./scripts/generate_k8s_tls_secret.sh > /tmp/cedargraph-graphd-tls.yaml
```

如需直接应用到测试 namespace，可加 `--apply`。生产环境应使用同一 SAN 集合由正式 CA 或证书管理系统签发，不应直接使用自签测试证书。

也可以复制 `k8s/graphd-secrets.example.yaml` 为 `k8s/graphd-secrets.yaml` 后替换占位值，并在本地部署目录中引用该文件。真实 Secret 不应提交到仓库。

### 4.1 快速部署

```bash
# 创建命名空间
kubectl apply -f k8s/namespace.yaml

# 部署 MetaD
kubectl apply -f k8s/metad.yaml

# 部署 StorageD
kubectl apply -f k8s/storaged.yaml

# 部署 GraphD
kubectl apply -f k8s/graphd.yaml

# 查看状态
kubectl get pods -n cedargraph
```

### 4.2 使用 Kustomize

```bash
# 部署所有组件
kubectl apply -k k8s/

# 查看状态
kubectl get all -n cedargraph
```

### 4.3 扩缩容

```bash
# 扩展 StorageD
kubectl scale statefulset storaged --replicas=5 -n cedargraph

# 扩展 GraphD
kubectl scale deployment graphd --replicas=4 -n cedargraph
```

---

## 5. 配置说明

### 5.1 配置文件位置

| 组件 | 配置文件 | 说明 |
|------|----------|------|
| MetaD | `config/metad.conf` | 元数据服务配置 |
| StorageD | `config/storaged.conf` | 存储服务配置 |
| GraphD | `config/graphd.conf` | 查询服务配置 |

### 5.2 关键配置项

#### MetaD 配置

```ini
# 集群配置
cluster_id=1
election_timeout=1000
heartbeat_interval=100

# 存储配置
data_dir=/data/meta
snapshot_interval=3600
```

#### StorageD 配置

```ini
# 存储配置
data_dir=/data/storage
memtable_size=64MB
max_sst_files=1000

# TTL 配置
ttl_enabled=true
ttl_retention_days=30

# 性能配置
io_threads=4
worker_threads=8
```

#### GraphD 配置

```ini
# 查询配置
query_timeout=30000
max_connections=1000

# 缓存配置
plan_cache_size=1000
query_cache_size=10000
```

---

## 6. 监控告警

### 6.1 Prometheus 配置

```yaml
# prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'cedar-storaged'
    static_configs:
      - targets: ['storaged0:7001', 'storaged1:7001', 'storaged2:7001']

  - job_name: 'cedar-graphd'
    static_configs:
      - targets: ['graphd:9669']
```

### 6.2 Grafana Dashboard

Compose 部署会通过 `config/grafana/dashboards/cedargraph-overview.yml` 自动加载预置 Dashboard。如需手工导入，可使用同目录下的 JSON 文件：

```bash
# 导入 CedarGraph Dashboard
curl -u "admin:${GRAFANA_PASSWORD}" \
  -X POST http://localhost:3000/api/dashboards/import \
  -H "Content-Type: application/json" \
  -d @config/grafana/dashboards/cedargraph-overview.json
```

### 6.3 告警规则

```yaml
# cedar_alerts.yml
groups:
  - name: cedar_alerts
    rules:
      - alert: HighCPUUsage
        expr: cedar_cpu_usage > 80
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High CPU usage detected"

      - alert: HighMemoryUsage
        expr: cedar_memory_usage > 90
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "High memory usage detected"

      - alert: NodeDown
        expr: up == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Node is down"
```

---

## 7. 备份恢复

### 7.1 自动备份

```bash
# 创建 tar 归档备份；默认写入 ./backups
./scripts/deploy.sh backup

# 查看备份列表
ls -lh ./backups/

# 从指定归档恢复数据目录和日志目录
./scripts/deploy.sh restore ./backups/cedargraph-backup-20260614_020000.tar.gz
```

### 7.2 定时备份

```bash
# 添加 crontab；部署脚本采用 tar 归档当前 DATA_DIR 与 LOG_DIR
0 2 * * * cd /opt/cedargraph && ./scripts/deploy.sh backup >> /var/log/cedargraph-backup.log 2>&1
```

### 7.3 远程备份

```bash
# 选择最新本地备份归档
LATEST_BACKUP=$(ls -t ./backups/cedargraph-backup-*.tar.gz | head -n 1)

# 上传到 S3
aws s3 cp "$LATEST_BACKUP" s3://cedar-backups/

# 从 S3 下载
aws s3 cp s3://cedar-backups/cedargraph-backup-20260614_020000.tar.gz ./backups/

# 恢复下载后的归档
./scripts/deploy.sh restore ./backups/cedargraph-backup-20260614_020000.tar.gz
```

---

## 8. 常见问题

### 8.1 启动失败

**问题**: MetaD 启动失败

**解决**:
```bash
# 检查日志
docker logs cedar-metad-0

# 检查数据目录权限
ls -la /data/meta

# 清理数据重新启动
docker-compose down -v
docker-compose up -d
```

### 8.2 连接超时

**问题**: GraphD 连接 StorageD 超时

**解决**:
```bash
# 检查网络连通性
docker exec cedar-graphd ping storaged0

# 检查端口
docker exec cedar-graphd nc -zv storaged0 9779

# 检查防火墙
sudo ufw status
```

### 8.3 性能问题

**问题**: 查询延迟高

**解决**:
1. 检查索引: `SHOW INDEXES IN myspace`
2. 检查热点: `SHOW HOTSPOTS`
3. 检查资源: `docker stats`
4. 优化查询: 使用 `EXPLAIN` 分析执行计划

---

## 附录

### A. 端口列表

| 端口 | 服务 | 说明 |
|------|------|------|
| 9559 | MetaD Raft | 元数据共识复制 |
| 10559 | MetaD gRPC | 元数据客户端 API |
| 9779 | StorageD | 存储服务 |
| 9669 | GraphD | 查询服务 |
| 9090 | Prometheus | 监控指标 |
| 3000 | Grafana | 监控可视化 |

### B. 命令参考

```bash
# 集群管理
docker-compose up -d          # 启动集群
docker-compose down           # 停止集群
docker-compose ps             # 查看状态
docker-compose logs -f        # 查看日志

# 状态查看
cedar-cli -e "SHOW HOSTS"     # 列出存储节点

# 当前 cedar-cli 脚本不实现 CREATE SPACE、SHOW SPACES、CREATE TAG/EDGE、SHOW EDGES、INSERT、MATCH 等图操作。
# 这些操作应通过已验证的 GraphD/服务端接口或完整客户端路径执行。

# 备份恢复
./scripts/deploy.sh backup    # 创建 tar 归档备份
./scripts/deploy.sh restore ./backups/cedargraph-backup-YYYYMMDD_HHMMSS.tar.gz
```

### C. 相关链接

- GitHub: https://github.com/cedargraph/cedar-core
- 文档: https://docs.cedargraph.com
- 社区: https://community.cedargraph.com

## 9. 上线前门禁

上线前需要把本地门禁结果与目标环境检查放在同一个发布记录中。建议记录：

1. `preflight_release_gate.sh` 完整输出路径、退出码和测试数量。
2. Docker Compose、Kubernetes 或 Helm 渲染产物中的 GraphD auth/TLS Secret 名称。
3. 真实 CA 链、证书过期时间、证书轮换流程和回滚流程。
4. 至少一次目标环境部署、停止、回滚、重启和单节点故障注入结果。
5. Prometheus/Grafana/AlertManager 指标与告警是否能够覆盖进程存活、Raft 健康、请求错误率、延迟、磁盘、内存和 TLS 失败。
6. MetaD Raft PVC 与 Raft conf 恢复记录。当前 braft `PeerId` 持久化解析后的 Pod IP；如果 StatefulSet Pod 重建后复用旧 PVC，可能出现 `can't do pre_vote as it is not in ...`，导致 MetaD 无法选主、StorageD 注册持续失败。生产升级必须先完成固定 Raft 身份、Raft conf 迁移或有备份的维护窗口重建方案；禁止在生产中把直接删除 MetaD PVC 当作普通滚动升级手段。

Helm Chart 默认启用 MetaD/StorageD/GraphD PodDisruptionBudget，要求 3 副本 MetaD/StorageD 集群维护期至少保留 2 个可用副本，并防止维护期主动驱逐唯一的 GraphD 副本。生产环境不要关闭 `pdb.enabled`，除非维护方案已经明确说明驱逐窗口、人工检查和回滚步骤。

对于开发或临时环境中的 1 副本安装，Helm 模板会把 PDB `minAvailable` 自动限制到实际副本数，避免出现 `minAvailable` 大于 workload 副本数的无效预算。生产环境仍建议保持 MetaD 3 副本、StorageD 至少 3 副本。

Helm Chart 默认启用 `networkPolicy.enabled=true`。生产环境不要关闭该项；如由平台侧网络策略替代，必须在上线记录中写明豁免原因、替代策略和执行证据。该检查只能证明 NetworkPolicy 对象和 CedarGraph 所需端口存在；是否真正隔离流量取决于目标集群 CNI/云厂商网络策略实现，生产记录必须保存平台侧执行证据。目标集群检查命令：

```bash
CEDAR_K8S_NAMESPACE=cedargraph \
CEDAR_HELM_RELEASE=cedargraph \
./scripts/preflight_k8s_networkpolicy.sh
```

仅开发或临时测试环境允许用 `CEDAR_ALLOW_MISSING_NETWORKPOLICY=1` 跳过该检查。

建议生产最终上线前使用组合门禁汇总目标集群检查：

```bash
CEDAR_K8S_NAMESPACE=cedargraph \
CEDAR_HELM_RELEASE=cedargraph \
./scripts/preflight_k8s_production_gate.sh
```

该组合门禁默认要求每个容器 restart 计数为 0，适合新部署验证。已有集群若存在已解释的历史重启，可设置 `CEDAR_MAX_POD_RESTARTS=<n>`，但上线记录必须说明重启原因、发生时间和已完成的恢复验证。

组合门禁默认要求每个 Pod 至少运行 300 秒后才通过，避免刚启动完成但仍处在选主、注册或证书投射收敛阶段时过早放行。若维护复核需要调整，可设置 `CEDAR_MIN_POD_AGE_SECONDS=<n>`；生产上线记录必须说明调低或调高阈值的原因。

组合门禁还会检查 MetaD/StorageD PVC 数量与 Pod 数量一致、PVC 已 `Bound`、使用 `ReadWriteOnce` 且具备 StorageClass 与容量信息；同时检查 GraphD Deployment 实际引用的认证 Secret 和 TLS Secret 是否存在必需 key。生产上线前不应只确认 Pod Running，还必须保存该门禁输出作为持久化与密钥配置证据。

认证与证书检查默认要求 JWT secret 解码后至少 32 字节，TLS 服务端证书至少剩余 30 天有效期；`ca.crt` 可以是过渡期 CA bundle，但其中必须至少有一张证书满足该有效期阈值，短期旧 CA 只允许作为轮换过渡信任并会产生 warning。若临时 preflight 环境使用短期测试证书，可显式设置 `CEDAR_MIN_TLS_DAYS=<n>` 或 `CEDAR_MIN_JWT_SECRET_BYTES=<n>` 降低阈值，但生产上线记录不得用该豁免代替真实证书轮换计划和 Secret 强度证明。

组合门禁要求 GraphD、MetaD 和 StorageD 均启用 `CEDAR_GRPC_TLS_ENABLED=1`，并引用同一个非 optional TLS Secret。生产环境不得只给 GraphD 入口配置证书而让内部 MetaD/StorageD gRPC 链路退回明文。

TLS 服务端证书的 SAN 必须覆盖 GraphD Service、MetaD/StorageD Service，以及每个 MetaD/StorageD StatefulSet Pod 的 headless DNS 名称，例如 `cedargraph-metad-0.cedargraph-metad`、`cedargraph-metad-0.cedargraph-metad.<namespace>` 和 `cedargraph-metad-0.cedargraph-metad.<namespace>.svc`。只给普通 Service 名签证书会导致 GraphD 或 StorageD 连接 MetaD 时出现 `Peer name ... is not in peer certificate`。

组合门禁默认扫描最近 10 分钟关键日志。证书轮换或维护操作后，如果已经确认最新日志窗口不再出现 `Handshake`、`SSL`、`FATAL`、`panic` 等错误，可以用 `CEDAR_CRITICAL_LOG_SINCE=<n>s|<n>m|<n>h` 缩短复核窗口；生产发布记录必须说明缩短原因和完整维护时间线。

Helm 部署默认还会要求 release 状态为 `deployed`。仅当目标环境使用纯 Kubernetes 清单或 Kustomize 而不是 Helm 管理时，才允许设置 `CEDAR_REQUIRE_HELM_STATUS=0` 并在上线记录中说明部署来源。

该组合门禁面向生产 HA 基线：默认要求 MetaD 为 3 个 Pod、StorageD 为 3 个 Pod、GraphD 至少 1 个 Pod。若目标生产规格不同，必须显式设置 `CEDAR_EXPECTED_METAD_PODS`、`CEDAR_EXPECTED_STORAGED_PODS` 或 `CEDAR_MIN_GRAPHD_PODS`，并在上线记录中说明容量规划和高可用性依据。开发或临时单副本环境可以运行 Helm/K8s 静态门禁和组件专项 preflight，但不应把组合门禁结果等同于生产上线证明。

上线前建议先在隔离 namespace 运行一次可重复的 Kubernetes 恢复演练。该演练会创建独立 namespace、生成强 GraphD auth Secret、生成覆盖 Service 与 StatefulSet headless Pod DNS 的 TLS Secret、安装 Helm release、采集 MetaD Raft evidence、离线校验证据包、生成 `RECOVERY_PLAN.txt`、运行 production gate，并确认 Raft upgrade guard 会按预期阻止普通 MetaD 滚动升级：

```bash
CEDAR_DRILL_NAMESPACE=cedargraph-recovery-drill \
CEDAR_DRILL_RELEASE=recovery-drill \
CEDAR_DRILL_IMAGE_REPOSITORY=cedargraph/cedar \
CEDAR_DRILL_IMAGE_TAG=k8s-fix-20260630 \
./scripts/preflight_k8s_recovery_drill.sh
```

该脚本只允许用于隔离 namespace，会拒绝常见生产 namespace 名称；默认成功后清理演练 namespace，但 evidence 会保留在 `/tmp/cedargraph-recovery-drill-evidence/<timestamp>`。如需保留演练集群排查，可设置 `CEDAR_DRILL_CLEANUP=0`。该演练证明门禁、证据包和恢复计划链路可重复执行，不代表底层 braft `PeerId` 持久化 Pod IP 的设计问题已经根治；已有生产集群升级仍必须保留维护窗口、备份和回滚方案。

2026-06-30 的本机隔离演练已使用正式 Linux 镜像 `cedargraph/cedar:k8s-fix-20260630` 通过，镜像 ID 为 `sha256:b1115b2528830b1e6a78e917e234ee018634df393024da9fd8839d2e03dc0769`。证据目录为 `/tmp/cedargraph-recovery-drill-evidence/20260629T210446Z`；该次演练验证了 Helm 安装 Ready、MetaD Raft evidence、离线校验、恢复计划、production gate 和 upgrade guard 预期阻断。

若本次动作是已有集群升级演练，还应额外打开 Raft upgrade guard：

```bash
CEDAR_K8S_NAMESPACE=cedargraph \
CEDAR_HELM_RELEASE=cedargraph \
CEDAR_RUN_RAFT_UPGRADE_GUARD=1 \
./scripts/preflight_k8s_production_gate.sh
```

Helm Chart 默认将 MetaD StatefulSet 的 `metad.updateStrategy.type` 设置为 `OnDelete`，使 `helm upgrade` 不会自动滚动重启 MetaD Pod。生产环境不要改成 `RollingUpdate`，除非发布方案已经证明 Raft 身份、PVC 和恢复流程安全。

Chart 会阻止误配置：若设置 `metad.updateStrategy.type=RollingUpdate` 但未同时设置 `metad.allowUnsafeRollingUpdate=true`，Helm 渲染会 fail-fast。该危险开关只能在完成 Raft 身份迁移、备份恢复和回滚演练后使用。

如果已有集群的 MetaD StatefulSet 仍是 `RollingUpdate`，在升级 Chart 前先执行一次只修改策略的 patch，避免 Kubernetes server-side apply 因旧的 `rollingUpdate.partition` 字段残留而拒绝变更：

```bash
CEDAR_K8S_NAMESPACE=cedargraph \
CEDAR_HELM_RELEASE=cedargraph \
./scripts/preflight_k8s_raft_identity.sh --patch-ondelete
```

该 patch 不会删除 Pod，也不会触发 MetaD 自动重启；它只是把后续 Helm 升级从自动滚动改为人工删除 Pod 后才更新。

Kubernetes/Helm 生产升级前应先采集只读 Raft/PVC 证据包，作为维护窗口审批、恢复方案和回滚记录的输入：

```bash
CEDAR_K8S_NAMESPACE=cedargraph \
CEDAR_HELM_RELEASE=cedargraph \
./scripts/preflight_k8s_raft_identity.sh \
  --collect-evidence ./release-evidence/metad-raft-$(date -u +%Y%m%dT%H%M%SZ)
```

采集后必须离线校验证据包完整性：

```bash
./scripts/preflight_k8s_raft_identity.sh \
  --verify-evidence ./release-evidence/metad-raft-<timestamp>
```

校验通过后生成维护窗口恢复计划草案：

```bash
./scripts/preflight_k8s_raft_identity.sh \
  --plan-recovery ./release-evidence/metad-raft-<timestamp>
```

证据包包含 MetaD Pod IP、StatefulSet、PVC、每个 MetaD PVC 中扫描到的持久化 Raft peer、MetaD 日志尾部和身份检查摘要；脚本不会读取 Kubernetes Secret，也不会删除 PVC 或重启 Pod。生产记录应保存该目录，并在恢复演练中用它确认当前 Raft conf、Pod IP 与 PVC 归属关系。若 upgrade guard 失败，证据包仍会写出 `SUMMARY.txt`，离线校验也应通过，以便事故复盘和维护窗口审批使用。

`--plan-recovery` 只读取 evidence 包并生成 `RECOVERY_PLAN.txt`，不会连接集群或修改任何对象。该计划会列出当前身份状态、硬性停止条件、维护窗口检查项和事后验证步骤。它是审批和演练输入，不是自动恢复工具；生产恢复仍必须由负责人按已批准的备份、迁移或重建方案执行。

采集证据后，还必须在目标 namespace 执行 Raft 身份门禁：

```bash
CEDAR_K8S_NAMESPACE=cedargraph \
CEDAR_HELM_RELEASE=cedargraph \
./scripts/preflight_k8s_raft_identity.sh --upgrade-guard
```

该脚本会读取当前 MetaD Pod IP，扫描每个 MetaD PVC 中持久化的 Raft peer IP，并检查近期 MetaD 日志中的 Raft 身份错误。普通模式用于判断当前集群是否已经错配；`--upgrade-guard` 用于生产升级前，只要发现当前 Raft conf 仍持久化 Pod IP，就会阻止普通滚动升级。若检查失败，应停止发布并执行 MetaD Raft PVC/conf 恢复或迁移流程。
