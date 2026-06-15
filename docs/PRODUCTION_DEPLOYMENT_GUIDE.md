# CedarGraph 生产环境部署指南

> **版本**: 1.0.0  
> **日期**: 2026-06-14  
> **状态**: Production-Ready

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

---

## 1. 部署架构

### 1.1 组件说明

| 组件 | 数量 | 用途 | 资源需求 |
|------|------|------|----------|
| MetaD | 3 | Raft 元数据管理 | 2 CPU, 4GB RAM |
| StorageD | 3 | LSM 存储引擎 | 4 CPU, 8GB RAM |
| GraphD | 2 | 查询路由 | 2 CPU, 4GB RAM |
| QueryD | 1 | 查询分发 | 2 CPU, 4GB RAM |
| Prometheus | 1 | 指标收集 | 1 CPU, 2GB RAM |
| Grafana | 1 | 监控可视化 | 1 CPU, 1GB RAM |

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
  - "9559:9559"  # MetaD
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
| `CEDAR_VERSION` | latest | 镜像版本 |
| `META_PEERS` | metad0:9559,metad1:9559,metad2:9559 | MetaD 节点列表 |
| `DATA_DIR` | /data | 数据目录 |
| `LOG_LEVEL` | INFO | 日志级别 |

---

## 4. Kubernetes 部署

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
  - job_name: 'cedar-metad'
    static_configs:
      - targets: ['metad0:9559', 'metad1:9559', 'metad2:9559']

  - job_name: 'cedar-storaged'
    static_configs:
      - targets: ['storaged0:9779', 'storaged1:9779', 'storaged2:9779']

  - job_name: 'cedar-graphd'
    static_configs:
      - targets: ['graphd:9669']
```

### 6.2 Grafana Dashboard

导入预定义 Dashboard:

```bash
# 导入 CedarGraph Dashboard
curl -X POST http://admin:admin@localhost:3000/api/dashboards/import \
  -H "Content-Type: application/json" \
  -d @grafana/dashboards/cedargraph.json
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
# 创建备份
docker exec cedar-metad-0 cedar-backup --type=full --output=/backups/backup_$(date +%Y%m%d)

# 查看备份列表
docker exec cedar-metad-0 cedar-backup --list

# 恢复备份
docker exec cedar-metad-0 cedar-restore --input=/backups/backup_20260614
```

### 7.2 定时备份

```bash
# 添加 crontab
0 2 * * * docker exec cedar-metad-0 cedar-backup --type=incremental --output=/backups/backup_$(date +\%Y\%m\%d)
```

### 7.3 远程备份

```bash
# 上传到 S3
aws s3 cp /backups/backup_20260614 s3://cedar-backups/backup_20260614

# 从 S3 下载
aws s3 cp s3://cedar-backups/backup_20260614 /backups/backup_20260614
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
| 9559 | MetaD | 元数据服务 |
| 9779 | StorageD | 存储服务 |
| 9669 | GraphD | 查询服务 |
| 9889 | QueryD | 查询分发 |
| 9090 | Prometheus | 监控指标 |
| 3000 | Grafana | 监控可视化 |

### B. 命令参考

```bash
# 集群管理
docker-compose up -d          # 启动集群
docker-compose down           # 停止集群
docker-compose ps             # 查看状态
docker-compose logs -f        # 查看日志

# 数据操作
cedar-cli -e "SHOW SPACES"    # 列出空间
cedar-cli -e "USE myspace"    # 切换空间
cedar-cli -e "SHOW TAGS"      # 列出标签
cedar-cli -e "SHOW EDGES"     # 列出边

# 备份恢复
cedar-backup --type=full      # 全量备份
cedar-backup --type=incremental # 增量备份
cedar-restore --input=backup  # 恢复备份
```

### C. 相关链接

- GitHub: https://github.com/cedargraph/cedar-core
- 文档: https://docs.cedargraph.com
- 社区: https://community.cedargraph.com
