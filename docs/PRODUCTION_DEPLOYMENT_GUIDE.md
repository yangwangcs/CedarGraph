# CedarGraph-Core 生产部署指南

## 1. 定位

本文档描述 CedarGraph-Core 从发布候选到生产部署需要满足的工程条件。它不是“执行一次脚本即可上线”的承诺；真实生产上线必须在目标集群完成证书、Secret、网络、存储、回滚、监控和压力验证。

## 2. 推荐拓扑

| 组件 | 建议副本 | 说明 |
|---|---:|---|
| MetaD | 3 | 元数据 Raft quorum |
| StorageD | 3+ | 分区数据和 Raft 状态机 |
| GraphD | 2+ | 无状态查询入口，可水平扩展 |
| GCN | 按负载 | 图计算和多版本视图 |
| Prometheus/Grafana | 1+ | 指标采集和展示 |

端口约定：

| 服务 | 端口 | 用途 |
|---|---:|---|
| MetaD | 9559 | Raft/listen |
| MetaD gRPC | 10559 | 元数据 API |
| StorageD | 9779 | 存储服务 |
| GraphD | 9669 | 查询入口 |

客户端和业务服务应连接 MetaD gRPC 端口 `10559`，不要把 Raft 端口当作业务 API。

## 3. 必备配置

生产环境必须配置：

- 强密码或 JWT secret。
- TLS，跨信任边界时启用 mTLS。
- Kubernetes Secret 或等价 Secret 管理系统。
- 持久卷和备份目录。
- NetworkPolicy 或防火墙规则。
- 资源 requests/limits。
- 监控、日志和告警。

示例：

```bash
export CEDAR_GRAPHD_AUTH_JWT_SECRET='replace-with-at-least-32-bytes-secret'
export CEDAR_GRAPHD_AUTH_USER='admin'
export CEDAR_GRAPHD_AUTH_PASSWORD='replace-with-strong-password'
export CEDAR_GRPC_TLS_ENABLED=1
export CEDAR_GRPC_SERVER_CERT=/etc/cedar/tls/tls.crt
export CEDAR_GRPC_SERVER_KEY=/etc/cedar/tls/tls.key
export CEDAR_GRPC_CA_CERT=/etc/cedar/tls/ca.crt
export CEDAR_GRPC_MTLS_ENABLED=1
```

## 4. Docker Compose 候选部署

```bash
docker compose -f docker-compose.production.yml config
docker compose -f docker-compose.production.yml up -d
docker compose -f docker-compose.production.yml ps
./scripts/cedar_health_check.sh
```

Compose 适合开发、演示和候选验证。生产建议使用 Kubernetes 或等价编排系统。

## 5. Kubernetes 部署

部署前执行：

```bash
./scripts/preflight_manifest_syntax.sh
./scripts/preflight_k8s_static.sh
./scripts/preflight_helm_static.sh
```

部署后验证：

```bash
kubectl get pods -n cedargraph
kubectl get svc -n cedargraph
kubectl logs -n cedargraph -l app=cedargraph --tail=200
```

## 6. 发布门禁

最低门禁：

```bash
./scripts/preflight_release_gate.sh
```

推荐门禁：

```bash
CEDAR_RELEASE_FULL_CTEST=1 \
CEDAR_RELEASE_SOAK_SECONDS=300 \
CEDAR_RELEASE_SOAK_POLL_SECONDS=5 \
./scripts/preflight_release_gate.sh
```

## 7. 上线检查表

- [ ] 当前 commit 已记录。
- [ ] 镜像 digest 已记录。
- [ ] 所有 Secret 来自 Secret 管理系统。
- [ ] TLS/mTLS 证书链验证通过。
- [ ] 持久卷重启后数据仍可读。
- [ ] MetaD quorum 故障演练通过。
- [ ] StorageD leader 切换演练通过。
- [ ] GraphD 滚动升级和回滚通过。
- [ ] 备份恢复演练通过。
- [ ] Prometheus/Grafana/Alertmanager 可用。
- [ ] 长时间压力和 soak 完成。

## 8. 回滚

回滚必须预先验证：

1. 保留上一版本镜像和配置。
2. 保留 schema 和数据格式兼容说明。
3. 有可执行的 Helm/manifest 回滚命令。
4. 有备份恢复方案。
5. 回滚后执行读写和健康检查。

