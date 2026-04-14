# CedarGraph Kubernetes 部署指南

## 概述

CedarGraph 支持 Kubernetes 部署，提供 StatefulSet 管理的有状态服务。

## 前置条件

- Kubernetes 1.20+
- kubectl 配置完成
- StorageClass 已配置

## 快速部署

```bash
# 使用 kustomize 部署
kubectl apply -k k8s/

# 或使用普通方式
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/metad.yaml
kubectl apply -f k8s/storaged.yaml
```

## 验证部署

```bash
# 查看 Pod 状态
kubectl get pods -n cedargraph

# 查看 StatefulSet
kubectl get statefulset -n cedargraph

# 查看 PVC
kubectl get pvc -n cedargraph

# 查看 Service
kubectl get svc -n cedargraph
```

## 扩缩容

```bash
# 扩容 StorageD 到 5 节点
kubectl scale statefulset storaged --replicas=5 -n cedargraph

# 缩容
kubectl scale statefulset storaged --replicas=3 -n cedargraph
```

## 监控集成

```bash
# 安装 Prometheus Operator
kubectl apply -f https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/master/bundle.yaml

# 创建 ServiceMonitor
kubectl apply -f - <<EOF
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: cedargraph-metrics
  namespace: cedargraph
spec:
  selector:
    matchLabels:
      app: storaged
  endpoints:
  - port: metrics
    interval: 15s
EOF
```

## 备份与恢复

```bash
# 创建备份 Job
kubectl apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: cedargraph-backup
  namespace: cedargraph
spec:
  template:
    spec:
      containers:
      - name: backup
        image: cedargraph:latest
        command:
        - /bin/sh
        - -c
        - |
          tar czf /backup/cedar-backup-$(date +%Y%m%d).tar.gz /var/lib/cedar/storage
        volumeMounts:
        - name: data
          mountPath: /var/lib/cedar/storage
        - name: backup
          mountPath: /backup
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: data-storaged-0
      - name: backup
        emptyDir: {}
      restartPolicy: Never
EOF
```

## 故障排查

```bash
# 查看 Pod 日志
kubectl logs -f storaged-0 -n cedargraph

# 进入 Pod 调试
kubectl exec -it storaged-0 -n cedargraph -- /bin/bash

# 查看事件
kubectl get events -n cedargraph --sort-by='.lastTimestamp'

# 检查资源使用
kubectl top pods -n cedargraph
```
