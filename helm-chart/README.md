# CedarGraph Helm Chart

使用 Helm 在 Kubernetes 上快速部署 CedarGraph 集群。

## 快速开始

```bash
# 添加 Helm 仓库
helm repo add cedargraph https://charts.cedargraph.io
helm repo update

# 安装 CedarGraph
helm install my-cedar cedargraph/cedargraph

# 查看状态
kubectl get pods -l app.kubernetes.io/name=cedargraph

# 连接集群
kubectl exec -it deployment/my-cedar-cedargraph-graphd -- cedar-cli -e "SHOW HOSTS"
```

## 安装配置

### 开发环境 (最小配置)

```bash
helm install cedar-dev cedargraph/cedargraph \
  --set metad.replicas=1 \
  --set storaged.replicas=1 \
  --set metad.persistence.size=5Gi \
  --set storaged.persistence.size=10Gi
```

### 生产环境 (推荐配置)

```bash
helm install cedar-prod cedargraph/cedargraph \
  --set metad.replicas=3 \
  --set storaged.replicas=5 \
  --set metad.persistence.size=50Gi \
  --set storaged.persistence.size=500Gi \
  --set global.studio.enabled=true \
  --set affinity.enabled=true
```

### 启用 Web Studio

```bash
helm install cedar cedargraph/cedargraph \
  --set global.studio.enabled=true

# 端口转发访问 Studio
kubectl port-forward svc/my-cedar-cedargraph-studio 7001:7001
# 访问 http://localhost:7001
```

## 配置参数

| 参数 | 描述 | 默认值 |
|------|------|--------|
| `image.repository` | 镜像仓库 | `cedargraph/cedar` |
| `image.tag` | 镜像标签 | `latest` |
| `metad.replicas` | MetaD 节点数 | `3` |
| `storaged.replicas` | StorageD 节点数 | `3` |
| `graphd.replicas` | GraphD 节点数 | `1` |
| `metad.persistence.size` | MetaD 存储大小 | `10Gi` |
| `storaged.persistence.size` | StorageD 存储大小 | `100Gi` |
| `global.studio.enabled` | 启用 Studio | `false` |

## 升级

```bash
# 升级到新版本
helm upgrade my-cedar cedargraph/cedargraph --version 0.2.0

# 修改配置
helm upgrade my-cedar cedargraph/cedargraph --set storaged.replicas=5
```

## 卸载

```bash
# 卸载集群
helm uninstall my-cedar

# 清理数据 (PVC)
kubectl delete pvc -l app.kubernetes.io/name=cedargraph
```

## 故障排查

```bash
# 查看 Pod 状态
kubectl get pods -l app.kubernetes.io/name=cedargraph

# 查看日志
kubectl logs -f my-cedar-cedargraph-storaged-0

# 进入容器调试
kubectl exec -it my-cedar-cedargraph-storaged-0 -- /bin/sh
```
