# Deployment Guide

## Overview

This guide covers deploying CedarGraph in various environments: single-node, Docker, and Kubernetes.

生产部署前应先通过本仓库发布门禁，再把认证、TLS/mTLS、Secret、证书轮换、回滚和监控告警在目标环境中验证完成。当前代码的本地基线是：`ctest` 注册测试 1358 个，1356 个实际运行测试通过，仅 2 个 CedarUpdate 性能基准测试未默认运行；发布门禁默认覆盖 non-test-mode Raft、TLS/mTLS、短时 soak 和单节点故障注入。

## Prerequisites

### System Requirements

**Minimum:**
- CPU: 4 cores
- RAM: 8GB
- Disk: 100GB SSD
- OS: Linux (Ubuntu 22.04+), macOS (12+)

**Recommended:**
- CPU: 16+ cores
- RAM: 64GB+
- Disk: 1TB+ NVMe SSD
- OS: Linux (Ubuntu 22.04)

### Software Dependencies

**Build Dependencies:**
```bash
# Ubuntu 22.04
sudo apt-get update
sudo apt-get install -y \
  build-essential cmake git wget curl \
  libcurl4-openssl-dev liblz4-dev libgrpc++-dev \
  protobuf-compiler-grpc libprotobuf-dev protobuf-compiler libprotoc-dev \
  libssl-dev libyaml-cpp-dev libgflags-dev libgoogle-glog-dev \
  nlohmann-json3-dev libgtest-dev libleveldb-dev libsnappy-dev pkg-config
```

**Runtime Dependencies:**
```bash
sudo apt-get install -y \
  liblz4-1 libgrpc++1 libprotobuf23 \
  curl netcat-openbsd dnsutils iputils-ping
```

## Pre-Deployment Gate

每个发布候选版本至少执行：

```bash
CEDAR_RELEASE_FULL_CTEST=1 CEDAR_RELEASE_SOAK_SECONDS=300 CEDAR_RELEASE_SOAK_POLL_SECONDS=5 ./scripts/preflight_release_gate.sh
```

该命令通过后，只能说明本地代码和本地门禁达到发布候选状态；真实生产上线仍需要在目标环境验证证书链、Secret 注入、部署/回滚、监控告警和长时间负载。

GraphD 非测试模式必须提供认证参数：

```bash
export CEDAR_GRAPHD_AUTH_JWT_SECRET='replace-with-at-least-32-bytes-secret'
export CEDAR_GRAPHD_AUTH_USER='admin'
export CEDAR_GRAPHD_AUTH_PASSWORD='replace-with-strong-password'
export CEDAR_GRAPHD_AUTH_ROLE='admin'
```

生产建议启用 TLS；启用 mTLS 时还必须配置客户端证书：

```bash
export CEDAR_GRPC_TLS_ENABLED=1
export CEDAR_GRPC_SERVER_CERT=/etc/cedar/tls/tls.crt
export CEDAR_GRPC_SERVER_KEY=/etc/cedar/tls/tls.key
export CEDAR_GRPC_CA_CERT=/etc/cedar/tls/ca.crt
export CEDAR_GRPC_MTLS_ENABLED=1
export CEDAR_GRPC_CLIENT_CERT=/etc/cedar/tls/client.crt
export CEDAR_GRPC_CLIENT_KEY=/etc/cedar/tls/client.key
```

`CEDAR_GRPC_ALLOW_INSECURE=1` 仅用于开发或受控测试，不应出现在生产默认部署中。

## Single-Node Deployment

### Quick Start

```bash
# Clone repository
git clone https://github.com/cedar-graph/CedarGraph-Core.git
cd CedarGraph-Core

# Build
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=ON
make -j$(nproc)

# Start cluster
cd ..
bash scripts/start_standalone.sh start
```

### Configuration

Edit `config/standalone.yaml`:

```yaml
# MetaD configuration
metad:
  listen_address: "0.0.0.0:9559"
  grpc_port: 10559
  data_dir: "/tmp/cedar/standalone/metad"
  
# StorageD configuration
storaged:
  bind_address: "0.0.0.0"
  port: 9779
  data_dir: "/tmp/cedar/standalone/storage"
  meta_server: "127.0.0.1:10559"
  health_port: 7000
  metrics_port: 7001
  
# GraphD configuration
graphd:
  bind_address: "0.0.0.0"
  port: 9669
  meta_server: "127.0.0.1:10559"
  health_port: 9668
  metrics_port: 9667
```

### Management Commands

```bash
# Start cluster
bash scripts/start_standalone.sh start

# Stop cluster
bash scripts/start_standalone.sh stop

# Check status
bash scripts/start_standalone.sh status

# View logs
tail -f /tmp/cedar/standalone/metad.log
tail -f /tmp/cedar/standalone/storaged.log
tail -f /tmp/cedar/standalone/graphd.log
```

### Using CLI Tool

```bash
# Build CLI tool
cd tools/cedargraph
go build -o cedargraph .

# Start cluster
./cedargraph start

# Check status
./cedargraph status

# Execute query
./cedargraph query "MATCH (n:Person) RETURN n LIMIT 10"

# Interactive shell
./cedargraph shell
```

## Docker Deployment

### Minimal Docker Compose

```yaml
# docker-compose.minimal.yml

services:
  metad:
    build:
      context: .
      dockerfile: cedar-docker-compose/Dockerfile
    command: cedar-metad --config /etc/cedar/metad.conf
    ports:
      - "9559:9559"
      - "10559:10559"
    volumes:
      - metad_data:/tmp/cedar/metad
    networks:
      - cedar-net

  storaged:
    build:
      context: .
      dockerfile: cedar-docker-compose/Dockerfile
    command: cedar-storaged --config /etc/cedar/storaged.conf
    ports:
      - "9779:9779"
    volumes:
      - storaged_data:/tmp/cedar/storage
    depends_on:
      - metad
    networks:
      - cedar-net

  graphd:
    build:
      context: .
      dockerfile: cedar-docker-compose/Dockerfile
    command: cedar-graphd --config /etc/cedar/graphd.conf
    environment:
      CEDAR_GRAPHD_AUTH_JWT_SECRET: ${CEDAR_GRAPHD_AUTH_JWT_SECRET:?required}
      CEDAR_GRAPHD_AUTH_USER: ${CEDAR_GRAPHD_AUTH_USER:?required}
      CEDAR_GRAPHD_AUTH_PASSWORD: ${CEDAR_GRAPHD_AUTH_PASSWORD:?required}
      CEDAR_GRAPHD_AUTH_ROLE: ${CEDAR_GRAPHD_AUTH_ROLE:-admin}
    ports:
      - "9669:9669"
    depends_on:
      - metad
      - storaged
    networks:
      - cedar-net

volumes:
  metad_data:
  storaged_data:

networks:
  cedar-net:
    driver: bridge
```

### Production Docker Compose

```yaml
# docker-compose.production.yml

services:
  metad1:
    image: cedargraph/cedar:k8s-fix-20260630
    environment:
      NODE_ROLE: metad
    command: ["--node_id", "1", "--listen", "0.0.0.0:9559", "--advertise", "metad1:9559", "--grpc_port", "10559", "--data_dir", "/tmp/cedar/metad", "--peer", "1:metad1:9559", "--peer", "2:metad2:9559", "--peer", "3:metad3:9559"]
    volumes:
      - metad1_data:/tmp/cedar/metad
    networks:
      - cedar-net

  metad2:
    image: cedargraph/cedar:k8s-fix-20260630
    environment:
      NODE_ROLE: metad
    command: ["--node_id", "2", "--listen", "0.0.0.0:9559", "--advertise", "metad2:9559", "--grpc_port", "10559", "--data_dir", "/tmp/cedar/metad", "--peer", "1:metad1:9559", "--peer", "2:metad2:9559", "--peer", "3:metad3:9559"]
    volumes:
      - metad2_data:/tmp/cedar/metad
    networks:
      - cedar-net

  metad3:
    image: cedargraph/cedar:k8s-fix-20260630
    environment:
      NODE_ROLE: metad
    command: ["--node_id", "3", "--listen", "0.0.0.0:9559", "--advertise", "metad3:9559", "--grpc_port", "10559", "--data_dir", "/tmp/cedar/metad", "--peer", "1:metad1:9559", "--peer", "2:metad2:9559", "--peer", "3:metad3:9559"]
    volumes:
      - metad3_data:/tmp/cedar/metad
    networks:
      - cedar-net

  storaged1:
    image: cedargraph/cedar:k8s-fix-20260630
    environment:
      NODE_ROLE: storaged
    command: ["--node_id", "1", "--bind", "0.0.0.0", "--data_dir", "/tmp/cedar/storage", "--meta", "metad1:10559,metad2:10559,metad3:10559"]
    volumes:
      - storaged1_data:/tmp/cedar/storage
    depends_on:
      - metad1
      - metad2
      - metad3
    networks:
      - cedar-net

  storaged2:
    image: cedargraph/cedar:k8s-fix-20260630
    environment:
      NODE_ROLE: storaged
    command: ["--node_id", "2", "--bind", "0.0.0.0", "--data_dir", "/tmp/cedar/storage", "--meta", "metad1:10559,metad2:10559,metad3:10559"]
    volumes:
      - storaged2_data:/tmp/cedar/storage
    depends_on:
      - metad1
      - metad2
      - metad3
    networks:
      - cedar-net

  storaged3:
    image: cedargraph/cedar:k8s-fix-20260630
    environment:
      NODE_ROLE: storaged
    command: ["--node_id", "3", "--bind", "0.0.0.0", "--data_dir", "/tmp/cedar/storage", "--meta", "metad1:10559,metad2:10559,metad3:10559"]
    volumes:
      - storaged3_data:/tmp/cedar/storage
    depends_on:
      - metad1
      - metad2
      - metad3
    networks:
      - cedar-net

  graphd:
    image: cedargraph/cedar:k8s-fix-20260630
    command: ["--bind", "0.0.0.0", "--meta", "metad1:10559,metad2:10559,metad3:10559", "--health_port", "9668", "--metrics_port", "9667"]
    environment:
      NODE_ROLE: graphd
      CEDAR_GRAPHD_AUTH_JWT_SECRET: ${CEDAR_GRAPHD_AUTH_JWT_SECRET:?required}
      CEDAR_GRAPHD_AUTH_USER: ${CEDAR_GRAPHD_AUTH_USER:?required}
      CEDAR_GRAPHD_AUTH_PASSWORD: ${CEDAR_GRAPHD_AUTH_PASSWORD:?required}
      CEDAR_GRAPHD_AUTH_ROLE: ${CEDAR_GRAPHD_AUTH_ROLE:-admin}
      CEDAR_GRPC_TLS_ENABLED: ${CEDAR_GRPC_TLS_ENABLED:-1}
      CEDAR_GRPC_SERVER_CERT: /etc/cedar/tls/tls.crt
      CEDAR_GRPC_SERVER_KEY: /etc/cedar/tls/tls.key
      CEDAR_GRPC_CA_CERT: /etc/cedar/tls/ca.crt
    ports:
      - "9669:9669"
    volumes:
      - ./certs:/etc/cedar/tls:ro
    depends_on:
      - metad1
      - metad2
      - metad3
      - storaged1
      - storaged2
      - storaged3
    networks:
      - cedar-net

volumes:
  metad1_data:
  metad2_data:
  metad3_data:
  storaged1_data:
  storaged2_data:
  storaged3_data:

networks:
  cedar-net:
    driver: bridge
```

### Building Docker Image

```bash
# Build image with an immutable local release-candidate tag
docker build -t cedargraph/cedar:k8s-fix-20260630 -f cedar-docker-compose/Dockerfile .

# Build with China mirrors
docker build -t cedargraph/cedar:k8s-fix-20260630 -f cedar-docker-compose/Dockerfile.cn .

# Run GraphD from the unified image
docker run --rm -p 9669:9669 \
  -e NODE_ROLE=graphd \
  -e CEDAR_GRAPHD_AUTH_JWT_SECRET='replace-with-at-least-32-bytes-secret' \
  -e CEDAR_GRAPHD_AUTH_USER='admin' \
  -e CEDAR_GRAPHD_AUTH_PASSWORD='replace-with-strong-password' \
  cedargraph/cedar:k8s-fix-20260630 \
  --bind 0.0.0.0 --meta 127.0.0.1:10559 --health_port 9668 --metrics_port 9667
```

## Kubernetes Deployment

### GraphD Secrets

Kubernetes 部署前必须先创建 GraphD 认证和 TLS Secret。可以复制 `k8s/graphd-secrets.example.yaml` 为 `k8s/graphd-secrets.yaml`，替换占位值后再应用；不要把真实 Secret 提交到仓库。

也可以用命令创建：

```bash
kubectl create namespace cedargraph --dry-run=client -o yaml | kubectl apply -f -

kubectl create secret generic graphd-auth -n cedargraph \
  --from-literal=jwt-secret='replace-with-at-least-32-bytes-secret' \
  --from-literal=user='admin' \
  --from-literal=password='replace-with-strong-password' \
  --from-literal=role='admin'

kubectl create secret generic graphd-tls -n cedargraph \
  --from-file=tls.crt=/path/to/tls.crt \
  --from-file=tls.key=/path/to/tls.key \
  --from-file=ca.crt=/path/to/ca.crt \
  --from-file=client.crt=/path/to/client.crt \
  --from-file=client.key=/path/to/client.key
```

### Namespace

```yaml
# k8s/namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: cedargraph
```

### ConfigMap

```yaml
# k8s/configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: cedargraph-config
  namespace: cedargraph
data:
  metad.conf: |
    listen_address: "0.0.0.0:9559"
    grpc_port: 10559
    data_dir: "/data/metad"
    
  storaged.conf: |
    bind_address: "0.0.0.0"
    port: 9779
    data_dir: "/data/storage"
    meta_server: "cedargraph-metad:10559"
    health_port: 7000
    metrics_port: 7001
    
  graphd.conf: |
    bind_address: "0.0.0.0"
    port: 9669
    meta_server: "cedargraph-metad:10559"
    health_port: 9668
    metrics_port: 9667
```

### MetaD StatefulSet

```yaml
# k8s/metad.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: cedargraph-metad
  namespace: cedargraph
spec:
  serviceName: cedargraph-metad
  replicas: 3
  selector:
    matchLabels:
      app: cedargraph-metad
  template:
    metadata:
      labels:
        app: cedargraph-metad
    spec:
      containers:
        - name: metad
          image: cedargraph/cedar:k8s-fix-20260630
          command: ["cedar-metad", "--config", "/etc/cedar/metad.conf"]
          ports:
            - containerPort: 9559
              name: raft
            - containerPort: 10559
              name: grpc
          volumeMounts:
            - name: config
              mountPath: /etc/cedar
            - name: data
              mountPath: /data/metad
          resources:
            requests:
              memory: "1Gi"
              cpu: "500m"
            limits:
              memory: "4Gi"
              cpu: "2"
      volumes:
        - name: config
          configMap:
            name: cedargraph-config
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 100Gi
---
apiVersion: v1
kind: Service
metadata:
  name: cedargraph-metad
  namespace: cedargraph
spec:
  selector:
    app: cedargraph-metad
  ports:
    - port: 9559
      name: raft
    - port: 10559
      name: grpc
  clusterIP: None
```

### StorageD StatefulSet

```yaml
# k8s/storaged.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: cedargraph-storaged
  namespace: cedargraph
spec:
  serviceName: cedargraph-storaged
  replicas: 3
  selector:
    matchLabels:
      app: cedargraph-storaged
  template:
    metadata:
      labels:
        app: cedargraph-storaged
    spec:
      containers:
        - name: storaged
          image: cedargraph/cedar:k8s-fix-20260630
          command: ["cedar-storaged", "--config", "/etc/cedar/storaged.conf"]
          ports:
            - containerPort: 9779
              name: grpc
          volumeMounts:
            - name: config
              mountPath: /etc/cedar
            - name: data
              mountPath: /data/storage
          resources:
            requests:
              memory: "4Gi"
              cpu: "1"
            limits:
              memory: "16Gi"
              cpu: "4"
      volumes:
        - name: config
          configMap:
            name: cedargraph-config
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 500Gi
        storageClassName: fast-ssd
---
apiVersion: v1
kind: Service
metadata:
  name: cedargraph-storaged
  namespace: cedargraph
spec:
  selector:
    app: cedargraph-storaged
  ports:
    - port: 9779
      name: grpc
  clusterIP: None
```

### GraphD Deployment

```yaml
# k8s/graphd.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cedargraph-graphd
  namespace: cedargraph
spec:
  replicas: 3
  selector:
    matchLabels:
      app: cedargraph-graphd
  template:
    metadata:
      labels:
        app: cedargraph-graphd
    spec:
      containers:
        - name: graphd
          image: cedargraph/cedar:k8s-fix-20260630
          command: ["cedar-graphd", "--config", "/etc/cedar/graphd.conf"]
          ports:
            - containerPort: 9669
              name: grpc
          volumeMounts:
            - name: config
              mountPath: /etc/cedar
          resources:
            requests:
              memory: "2Gi"
              cpu: "1"
            limits:
              memory: "8Gi"
              cpu: "4"
          livenessProbe:
            grpc:
              port: 9669
            initialDelaySeconds: 30
            periodSeconds: 10
          readinessProbe:
            grpc:
              port: 9669
            initialDelaySeconds: 5
            periodSeconds: 5
      volumes:
        - name: config
          configMap:
            name: cedargraph-config
---
apiVersion: v1
kind: Service
metadata:
  name: cedargraph-graphd
  namespace: cedargraph
spec:
  selector:
    app: cedargraph-graphd
  ports:
    - port: 9669
      name: grpc
  type: LoadBalancer
```

### StorageClass

```yaml
# k8s/storageclass.yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: fast-ssd
provisioner: kubernetes.io/aws-ebs
parameters:
  type: gp3
  iops: "3000"
  throughput: "125"
reclaimPolicy: Retain
volumeBindingMode: WaitForFirstConsumer
```

## Configuration Reference

### MetaD Configuration

```yaml
# MetaD configuration
metad:
  # Network
  listen_address: "0.0.0.0:9559"
  grpc_port: 10559
  
  # Storage
  data_dir: "/data/metad"
  
  # Raft
  election_timeout_ms: 1000
  heartbeat_interval_ms: 100
  heartbeat_timeout_sec: 10
  heartbeat_check_interval_sec: 5
```

### StorageD Configuration

```yaml
# StorageD configuration
storaged:
  # Network
  bind_address: "0.0.0.0"
  port: 9779
  
  # Storage
  data_dir: "/data/storage"
  meta_server: "127.0.0.1:10559"
  health_port: 7000
  metrics_port: 7001
```

### GraphD Configuration

```yaml
# GraphD configuration
graphd:
  # Network
  bind_address: "0.0.0.0"
  port: 9669
  
  # MetaD connection
  meta_server: "cedargraph-metad:10559"
  gcn_server: "127.0.0.1:9780"
  health_port: 9668
  metrics_port: 9667
  
  # Plan cache
  plan_cache_size: 1000
  
  # Connection pool
  max_connections: 100
  connection_timeout_ms: 5000
```

## Monitoring

### Health Checks

```bash
# MetaD gRPC connectivity
nc -z localhost 10559

# StorageD health
curl http://localhost:7000/health

# GraphD health
curl http://localhost:9668/health
```

### Metrics Endpoint

```yaml
# Add to configuration
monitoring:
  enable_metrics: true
  metrics_path: "/metrics"
  storaged_metrics_port: 7001
  graphd_metrics_port: 9667
```

### Prometheus Configuration

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'cedar-storaged'
    static_configs:
      - targets:
          - 'storaged:7001'

  - job_name: 'cedar-graphd'
    static_configs:
      - targets:
          - 'graphd:9667'
```

### Grafana Dashboard

Import the provided dashboard JSON:
- `config/grafana/dashboards/cedargraph-overview.json`

## Backup and Recovery

### Manual Backup

```bash
# Stop cluster
bash scripts/start_standalone.sh stop

# Backup data
tar -czf cedargraph-backup-$(date +%Y%m%d).tar.gz /tmp/cedar/standalone/

# Start cluster
bash scripts/start_standalone.sh start
```

### Automated Backup

```bash
# scripts/backup.sh
#!/bin/bash
BACKUP_DIR="/backups/cedargraph"
DATE=$(date +%Y%m%d_%H%M%S)

# Create backup
mkdir -p $BACKUP_DIR
tar -czf $BACKUP_DIR/cedargraph-backup-$DATE.tar.gz /tmp/cedar/standalone/

# Cleanup old backups (keep last 7 days)
find $BACKUP_DIR -name "*.tar.gz" -mtime +7 -delete
```

### Recovery

```bash
# Stop cluster
bash scripts/start_standalone.sh stop

# Restore backup
tar -xzf cedargraph-backup-20240101_120000.tar.gz -C /

# Start cluster
bash scripts/start_standalone.sh start
```

## Performance Tuning

### System Tuning

```bash
# Increase file descriptors
echo "* soft nofile 1000000" >> /etc/security/limits.conf
echo "* hard nofile 1000000" >> /etc/security/limits.conf

# Increase network buffers
echo "net.core.rmem_max = 16777216" >> /etc/sysctl.conf
echo "net.core.wmem_max = 16777216" >> /etc/sysctl.conf

# Apply changes
sysctl -p
```

### Storage Tuning

```bash
# Use NVMe SSD
# Mount with noatime
mount -o noatime /dev/nvme0n1 /data

# Use XFS filesystem
mkfs.xfs /dev/nvme0n1
```

### Application Tuning

当前 standalone `cedar-storaged --config` 只读取网络、数据目录、MetaD、心跳、health/metrics 和 TLS 字段。MemTable、block cache、row cache 和 compaction 线程等底层参数尚未作为 `cedar-storaged` YAML 配置项暴露；上线调优应先通过 NVMe/XFS/noatime、进程资源限制、Prometheus 指标和压力测试确认瓶颈，待这些参数进入当前 parser 后再写入部署配置。

## Troubleshooting

### Common Issues

**Cluster won't start:**
```bash
# Check logs
tail -f /tmp/cedar/standalone/*.log

# Check ports
netstat -tlnp | grep -E "9559|9779|9669"

# Check disk space
df -h /tmp/cedar/
```

**High latency:**
```bash
# Check compaction
curl http://localhost:7001/metrics | grep compaction

# Check cache hit rate
curl http://localhost:7001/metrics | grep cache_hit

# Check system resources
top -p $(pgrep cedar)
```

**Data loss:**
```bash
# Check WAL
ls -la /tmp/cedar/standalone/storage/wal/

# Check SST files
ls -la /tmp/cedar/standalone/storage/

# Check recovery logs
grep -i "recovery" /tmp/cedar/standalone/storaged.log
```

### Debug Mode

```bash
# Enable debug logging
export CEDAR_LOG_LEVEL=DEBUG

# Run with debug
cedar-storaged --config /etc/cedar/storaged.conf --log_level=DEBUG
```

### Core Dumps

```bash
# Enable core dumps
ulimit -c unlimited

# Run with gdb
gdb cedar-storaged core.<pid>

# Get backtrace
(gdb) bt
```
