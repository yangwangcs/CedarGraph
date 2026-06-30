# CedarGraph Helm Chart

使用 Helm 在 Kubernetes 上快速部署 CedarGraph 集群。

## 快速开始

```bash
# 添加 Helm 仓库
helm repo add cedargraph https://charts.cedargraph.io
helm repo update

# 创建目标 namespace；Secret 必须先写入同一 namespace
kubectl create namespace cedargraph --dry-run=client -o yaml | kubectl apply -f -

# 创建 GraphD 认证 Secret；生产环境请使用 Secret 管理系统注入
kubectl create secret generic cedargraph-graphd-auth -n cedargraph \
  --from-literal=jwt-secret='replace-with-at-least-32-bytes-secret' \
  --from-literal=user='admin' \
  --from-literal=password='replace-with-strong-password' \
  --from-literal=role='admin' \
  --dry-run=client -o yaml | kubectl apply -f -

# 创建 TLS Secret；生产环境请使用真实 CA 签发证书
kubectl create secret generic cedargraph-graphd-tls -n cedargraph \
  --from-file=tls.crt=/path/to/tls.crt \
  --from-file=tls.key=/path/to/tls.key \
  --from-file=ca.crt=/path/to/ca.crt \
  --from-file=client.crt=/path/to/client.crt \
  --from-file=client.key=/path/to/client.key \
  --dry-run=client -o yaml | kubectl apply -f -

# 临时 preflight 或恢复演练可生成覆盖 CedarGraph K8s DNS 的 Secret YAML
CEDAR_K8S_NAMESPACE=<namespace> \
CEDAR_HELM_RELEASE=<release-name> \
CEDAR_TLS_SECRET_NAME=cedargraph-graphd-tls \
./scripts/generate_k8s_tls_secret.sh > /tmp/cedargraph-graphd-tls.yaml

# 安装 CedarGraph
helm install my-cedar cedargraph/cedargraph \
  --namespace cedargraph \
  --create-namespace

# 查看状态
kubectl get pods -n cedargraph -l app.kubernetes.io/name=cedargraph

# 连接集群
kubectl exec -n cedargraph -it deployment/my-cedar-cedargraph-graphd -- cedar-cli -e "SHOW HOSTS"
```

## 安装配置

默认示例使用 `cedargraph` namespace。Helm Chart 默认 NetworkPolicy 会按 `kubernetes.io/metadata.name=cedargraph` 限制入站来源；如果安装到其他 namespace，请同步设置：

```bash
--set networkPolicy.ingress[0].from[0].namespaceSelector.matchLabels.kubernetes\\.io/metadata\\.name=<namespace>
```

### 开发环境 (最小配置)

```bash
helm install cedar-dev cedargraph/cedargraph \
  --namespace cedargraph \
  --create-namespace \
  --set metad.replicas=1 \
  --set storaged.replicas=1 \
  --set metad.persistence.size=5Gi \
  --set storaged.persistence.size=10Gi
```

### 生产环境 (推荐配置)

```bash
helm install cedar-prod cedargraph/cedargraph \
  --namespace cedargraph \
  --create-namespace \
  --set metad.replicas=3 \
  --set storaged.replicas=5 \
  --set metad.persistence.size=50Gi \
  --set storaged.persistence.size=500Gi \
  --set global.studio.enabled=true \
  --set affinity.enabled=true
```

生产安装前必须提前创建 `cedargraph-graphd-auth` 和 `cedargraph-graphd-tls`，或通过以下参数指定已有 Secret：

```bash
helm install cedar-prod cedargraph/cedargraph \
  --namespace cedargraph \
  --create-namespace \
  --set graphd.auth.existingSecret=my-graphd-auth \
  --set graphd.tls.existingSecret=my-graphd-tls
```

### 启用 Web Studio

```bash
helm install cedar cedargraph/cedargraph \
  --namespace cedargraph \
  --create-namespace \
  --set global.studio.enabled=true

# 端口转发访问 Studio
kubectl port-forward -n cedargraph svc/my-cedar-cedargraph-studio 7001:7001
# 访问 http://localhost:7001
```

## 配置参数

| 参数 | 描述 | 默认值 |
|------|------|--------|
| `image.repository` | 镜像仓库 | `cedargraph/cedar` |
| `image.tag` | 镜像标签；默认使用已通过本机 K8s 恢复演练的固定标签，生产禁止使用 `latest` | `k8s-fix-20260630` |
| `metad.replicas` | MetaD 节点数 | `3` |
| `metad.updateStrategy.type` | MetaD StatefulSet 更新策略；默认 `OnDelete`，防止 Helm 自动滚动持久化 Raft Pod IP 的 MetaD Pod | `OnDelete` |
| `metad.allowUnsafeRollingUpdate` | 是否允许显式渲染 MetaD `RollingUpdate`；生产默认必须保持 `false` | `false` |
| `storaged.replicas` | StorageD 节点数 | `3` |
| `graphd.replicas` | GraphD 节点数 | `1` |
| `pdb.enabled` | 启用 MetaD/StorageD/GraphD PodDisruptionBudget，防止维护期一次驱逐过多副本 | `true` |
| `networkPolicy.enabled` | 渲染 CedarGraph NetworkPolicy；生产默认开启，或由平台侧策略替代并记录豁免 | `true` |
| `graphd.auth.existingSecret` | GraphD 认证 Secret 名称 | `cedargraph-graphd-auth` |
| `graphd.tls.enabled` | 启用 GraphD TLS | `true` |
| `graphd.tls.mtlsEnabled` | 启用 GraphD mTLS | `false` |
| `graphd.tls.existingSecret` | GraphD TLS Secret 名称 | `cedargraph-graphd-tls` |
| `metad.persistence.size` | MetaD 存储大小 | `10Gi` |
| `storaged.persistence.size` | StorageD 存储大小 | `100Gi` |
| `global.studio.enabled` | 启用 Studio | `false` |

## 升级

```bash
# 升级到新版本
helm upgrade my-cedar cedargraph/cedargraph \
  --namespace cedargraph \
  --version 0.2.0

# 修改配置
helm upgrade my-cedar cedargraph/cedargraph \
  --namespace cedargraph \
  --set storaged.replicas=5
```

### MetaD Raft PVC 升级约束

当前 MetaD 使用 braft 作为 Raft 后端；braft `PeerId` 会把解析后的 Pod IP 写入本地 Raft meta/log/snapshot。Kubernetes StatefulSet Pod 重建后可能获得新的 Pod IP，如果继续复用旧的 MetaD Raft PVC，节点会因为“自身 IP 不在旧 conf 中”而无法重新选主，StorageD 注册会持续返回 `Not a leader`。

因此 Helm Chart 默认将 MetaD StatefulSet 的 `updateStrategy.type` 设为 `OnDelete`，避免 `helm upgrade` 自动滚动重启 MetaD Pod。生产环境不要把该值改为 `RollingUpdate`，除非已经完成固定 Raft 身份、Raft conf 迁移或受控恢复演练。

Chart 还会 fail-fast 阻止误配置：如果设置 `metad.updateStrategy.type=RollingUpdate`，必须同时显式设置 `metad.allowUnsafeRollingUpdate=true`。该开关只应在已有正式迁移方案和回滚方案时使用。

已有 release 如果仍使用 `RollingUpdate`，升级 Chart 前先清理 StatefulSet 更新策略：

```bash
CEDAR_K8S_NAMESPACE=<namespace> \
CEDAR_HELM_RELEASE=<release-name> \
./scripts/preflight_k8s_raft_identity.sh --patch-ondelete
```

该 patch 只改变更新策略，不会自动重启 MetaD Pod。

因此，生产升级前必须满足以下条件之一：

1. 使用不会随 Pod 重建变化的 MetaD Raft 网络身份，并完成对应的 Raft conf 迁移验证。
2. 在维护窗口内按发布方案迁移或重建 MetaD Raft PVC，并确认元数据备份可恢复。
3. 仅在一次性 preflight、开发或空集群环境中删除 MetaD PVC 后重新引导；不要把删除生产 PVC 当作常规升级策略。

上线记录必须保存 `kubectl logs` 中的 Raft conf、Pod IP、PVC 名称和恢复步骤。若日志出现 `can't do pre_vote as it is not in ...`，说明当前 Pod 身份与持久化 Raft conf 不一致，应停止上线并执行 MetaD Raft 恢复流程。

升级前建议在目标 namespace 执行：

```bash
CEDAR_K8S_NAMESPACE=<namespace> \
CEDAR_HELM_RELEASE=<release-name> \
./scripts/preflight_k8s_raft_identity.sh \
  --collect-evidence ./release-evidence/metad-raft-$(date -u +%Y%m%dT%H%M%SZ)

./scripts/preflight_k8s_raft_identity.sh \
  --verify-evidence ./release-evidence/metad-raft-<timestamp>

./scripts/preflight_k8s_raft_identity.sh \
  --plan-recovery ./release-evidence/metad-raft-<timestamp>

CEDAR_K8S_NAMESPACE=<namespace> \
CEDAR_HELM_RELEASE=<release-name> \
./scripts/preflight_k8s_raft_identity.sh --upgrade-guard
```

该检查只读集群状态；`--collect-evidence` 会保存 MetaD Pod、StatefulSet、PVC、持久化 Raft peer 和日志尾部，不读取 Secret、不删除 PVC、不重启 Pod。`--verify-evidence` 可在离线环境中校验证据包是否完整，即使 upgrade guard 失败也应能验证失败证据包。`--plan-recovery` 只读取 evidence 包并生成 `RECOVERY_PLAN.txt`，用于维护窗口审批和演练准备，不会连接或修改集群。普通模式会比对当前 MetaD Pod IP 与 PVC 中持久化的 Raft peer IP，并在发现旧 PVC/新 Pod IP 错配时失败。`--upgrade-guard` 是生产升级模式：只要发现当前 Raft conf 仍持久化 Pod IP，就会阻止普通滚动升级，要求先执行维护窗口内的迁移、重建或恢复方案。

上线前可以先在隔离 namespace 中跑一键恢复演练：

```bash
CEDAR_DRILL_NAMESPACE=cedargraph-recovery-drill \
CEDAR_DRILL_RELEASE=recovery-drill \
CEDAR_DRILL_IMAGE_REPOSITORY=cedargraph/cedar \
CEDAR_DRILL_IMAGE_TAG=k8s-fix-20260630 \
./scripts/preflight_k8s_recovery_drill.sh
```

该脚本会创建演练 namespace、生成 GraphD auth/TLS Secret、安装 Helm release、采集和校验 MetaD Raft evidence、生成 `RECOVERY_PLAN.txt`、运行 production gate，并确认 upgrade guard 会阻止普通 MetaD 滚动升级。它会拒绝常见生产 namespace 名称；默认成功后清理演练 namespace，但保留 evidence 目录。该演练用于证明门禁链路可重复执行，不替代生产维护窗口内的备份、迁移或恢复方案。

PDB 默认按 3 副本生产部署保留 2 个可用副本；如果开发环境把 MetaD 或 StorageD 缩到 1 副本，模板会自动把对应 `minAvailable` 限制到 1，避免渲染出大于副本数的预算。

生产环境默认启用 NetworkPolicy，并将入站来源限制到带有目标 namespace 标签的流量。默认 values 假定 namespace 标签 `kubernetes.io/metadata.name=cedargraph`；如果使用其他 namespace，请同步调整 `networkPolicy.ingress[].from[].namespaceSelector.matchLabels` 或在平台侧记录等价策略。该 preflight 会验证策略对象和端口规则存在；CNI 是否实际执行策略，需要在生产 runbook 中由平台侧证据证明：

```bash
CEDAR_K8S_NAMESPACE=<namespace> \
CEDAR_HELM_RELEASE=<release-name> \
./scripts/preflight_k8s_networkpolicy.sh
```

目标集群最终上线前可使用组合门禁：

```bash
CEDAR_K8S_NAMESPACE=<namespace> \
CEDAR_HELM_RELEASE=<release-name> \
./scripts/preflight_k8s_production_gate.sh
```

新部署默认要求 Pod restart 为 0；已有集群如需允许历史重启，可显式设置 `CEDAR_MAX_POD_RESTARTS=<n>` 并在发布记录中说明原因。

组合门禁默认要求 Pod 至少运行 300 秒后才通过。临时演练或维护复核可以设置 `CEDAR_MIN_POD_AGE_SECONDS=<n>`，生产记录必须说明原因。

Helm 部署默认要求 release 状态为 `deployed`；非 Helm 部署才应设置 `CEDAR_REQUIRE_HELM_STATUS=0`。

组合门禁默认按 MetaD=3、StorageD=3、GraphD>=1 的生产 HA 基线检查。不同规格必须显式设置 `CEDAR_EXPECTED_METAD_PODS`、`CEDAR_EXPECTED_STORAGED_PODS` 或 `CEDAR_MIN_GRAPHD_PODS`。

组合门禁还会验证 MetaD/StorageD PVC 已绑定并具备 StorageClass、容量和 `ReadWriteOnce` 访问模式，同时检查 GraphD Deployment 实际引用的认证 Secret 与 TLS Secret 是否包含必需 key。生产发布记录应保存该门禁输出。

默认阈值要求 JWT secret 解码后至少 32 字节，TLS 服务端证书至少剩余 30 天有效期；`ca.crt` 可以包含轮换过渡期的多张证书，但其中至少一张必须满足有效期阈值。临时测试环境可以通过 `CEDAR_MIN_TLS_DAYS=<n>` 或 `CEDAR_MIN_JWT_SECRET_BYTES=<n>` 调低阈值；生产环境不应降低这些阈值。

组合门禁要求 GraphD、MetaD 和 StorageD 都启用 `CEDAR_GRPC_TLS_ENABLED=1`，并引用同一个非 optional TLS Secret，避免内部 gRPC 链路意外退回明文。

TLS 证书 SAN 必须覆盖 GraphD Service、MetaD/StorageD Service，以及每个 MetaD/StorageD StatefulSet Pod 的 headless DNS 名称，例如 `<release>-cedargraph-metad-0.<release>-cedargraph-metad`、带 namespace 的短名和 `.svc` 名称。只包含 Service 名会导致内部 gRPC 证书校验失败。

组合门禁默认扫描最近 10 分钟关键日志。维护后复核可以通过 `CEDAR_CRITICAL_LOG_SINCE=<window>` 缩短窗口，但生产记录必须说明原因。

## 卸载

```bash
# 卸载集群
helm uninstall my-cedar
```

默认卸载不会删除 PVC，这是为了保留 MetaD 和 StorageD 的持久化数据。生产环境不要直接复制通配式 PVC 删除命令；只有在已经完成备份、确认不再需要恢复、并核对 namespace 与 release 标签后，才可以按变更单手动清理 PVC。

建议先审计将被影响的 PVC：

```bash
kubectl get pvc -n <namespace> \
  -l app.kubernetes.io/name=cedargraph,app.kubernetes.io/instance=<release-name>
```

若确实需要销毁数据，应在发布记录中保存备份位置、审批记录、上述审计输出和最终清理命令。

## 故障排查

```bash
# 查看 Pod 状态
kubectl get pods -l app.kubernetes.io/name=cedargraph

# 查看日志
kubectl logs -f my-cedar-cedargraph-storaged-0

# 进入容器调试
kubectl exec -it my-cedar-cedargraph-storaged-0 -- /bin/sh
```
