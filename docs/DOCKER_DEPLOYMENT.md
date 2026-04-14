# CedarGraph Docker 部署指南

## 概述

CedarGraph 支持通过 Docker 进行快速部署和测试。我们提供两种部署模式：

- **单节点模式**: 快速测试和开发
- **三节点集群模式**: 生产-like 的高可用测试

## 系统要求

- Docker 20.10+
- Docker Compose 1.29+
- 4GB+ 可用内存
- 10GB+ 可用磁盘空间

## 快速开始

### 1. 构建镜像

```bash
docker build -t cedargraph:latest .
```

### 2. 单节点快速测试

```bash
# 启动单节点集群
docker-compose -f docker-compose.single.yml up -d

# 查看状态
docker-compose -f docker-compose.single.yml ps

# 查看日志
docker-compose -f docker-compose.single.yml logs -f storaged
```

### 3. 使用测试脚本

```bash
# 一键测试
./test_docker.sh

# 包含混沌测试
./test_docker.sh --chaos

# 查看状态
./test_docker.sh --status

# 清理环境
./test_docker.sh --cleanup
```

## 三节点集群部署

```bash
# 启动 3 节点集群
docker-compose up -d

# 等待服务就绪
sleep 30

# 检查集群状态
docker-compose ps
```

## 服务访问

| 服务 | 地址 | 说明 |
|------|------|------|
| MetaD | http://localhost:6000-6002 | 元数据服务 (3节点) |
| StorageD | http://localhost:7000-7002 | 存储服务 (3节点) |
| QueryD | http://localhost:8080 | 查询服务 |
| Prometheus | http://localhost:9090 | 指标采集 |
| Grafana | http://localhost:3000 | 监控面板 (admin/cedargraph) |

## 配置说明

### 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `NODE_TYPE` | storaged | 节点类型 (storaged/metad/queryd) |
| `NODE_ID` | 1 | 节点 ID |
| `BIND_ADDRESS` | 0.0.0.0:7000 | 绑定地址 |
| `DATA_DIR` | /var/lib/cedar/storage | 数据目录 |
| `AUTO_RECOVERY` | true | 启用自动恢复 |
| `HEALTH_CHECK_INTERVAL` | 30 | 健康检查间隔(秒) |

### 数据持久化

数据存储在 Docker volumes 中：

```bash
# 查看 volumes
docker volume ls | grep cedar

# 备份数据
docker run --rm -v cedar_storaged1_data:/data -v $(pwd):/backup alpine tar czf /backup/storaged1_backup.tar.gz -C /data .

# 恢复数据
docker run --rm -v cedar_storaged1_data:/data -v $(pwd):/backup alpine tar xzf /backup/storaged1_backup.tar.gz -C /data
```

## 常见问题

### 1. 端口冲突

如果端口被占用，修改 `docker-compose.yml` 中的端口映射：

```yaml
ports:
  - "7001:7000"  # 将主机的 7001 映射到容器的 7000
```

### 2. 内存不足

减少工作线程数：

```yaml
environment:
  - IO_THREADS=2
  - WORKER_THREADS=4
```

### 3. 查看详细日志

```bash
# 特定服务日志
docker-compose logs -f storaged1

# 所有日志
docker-compose logs -f
```

### 4. 重启服务

```bash
# 重启单个服务
docker-compose restart storaged1

# 重建并重启
docker-compose up -d --build storaged1
```

## 生产部署注意事项

1. **数据安全**: 生产环境建议使用外部存储卷
2. **网络隔离**: 使用 Docker overlay 网络或 Kubernetes
3. **资源限制**: 设置 CPU 和内存限制
4. **日志收集**: 配置集中式日志收集 (ELK/Fluentd)
5. **监控告警**: 配置 Prometheus Alertmanager

## 故障排查

### 服务无法启动

```bash
# 检查日志
docker-compose logs storaged1

# 检查资源使用
docker stats

# 进入容器调试
docker-compose exec storaged1 /bin/bash
```

### 网络问题

```bash
# 检查网络
docker network inspect cedar-cluster

# 测试连通性
docker-compose exec storaged1 ping metad1
```

### 数据损坏

```bash
# 停止服务
docker-compose stop storaged1

# 删除数据卷
docker volume rm cedar_storaged1_data

# 重新启动
docker-compose up -d storaged1
```
