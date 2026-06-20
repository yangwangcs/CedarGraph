# Deployment Guide

## Overview

This guide covers deploying CedarGraph in various environments: single-node, Docker, and Kubernetes.

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
  libgtest-dev libleveldb-dev libsnappy-dev pkg-config
```

**Runtime Dependencies:**
```bash
sudo apt-get install -y \
  liblz4-1 libgrpc++1 libprotobuf23 \
  curl netcat-openbsd dnsutils iputils-ping
```

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
  host: "0.0.0.0"
  port: 9559
  grpc_port: 10559
  data_dir: "/tmp/cedar/standalone/metad"
  
# StorageD configuration
storaged:
  host: "0.0.0.0"
  port: 9779
  data_dir: "/tmp/cedar/standalone/storage"
  storage_mode: "partitioned"
  max_open_partitions: 256
  
# GraphD configuration
graphd:
  host: "0.0.0.0"
  port: 9669
  metad_host: "localhost"
  metad_port: 10559
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
version: '3.8'

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
version: '3.8'

services:
  metad1:
    image: cedargraph/metad:latest
    command: cedar-metad --config /etc/cedar/metad.conf --node-id 1
    volumes:
      - metad1_data:/tmp/cedar/metad
    networks:
      - cedar-net

  metad2:
    image: cedargraph/metad:latest
    command: cedar-metad --config /etc/cedar/metad.conf --node-id 2
    volumes:
      - metad2_data:/tmp/cedar/metad
    networks:
      - cedar-net

  metad3:
    image: cedargraph/metad:latest
    command: cedar-metad --config /etc/cedar/metad.conf --node-id 3
    volumes:
      - metad3_data:/tmp/cedar/metad
    networks:
      - cedar-net

  storaged1:
    image: cedargraph/storaged:latest
    command: cedar-storaged --config /etc/cedar/storaged.conf --node-id 1
    volumes:
      - storaged1_data:/tmp/cedar/storage
    depends_on:
      - metad1
      - metad2
      - metad3
    networks:
      - cedar-net

  storaged2:
    image: cedargraph/storaged:latest
    command: cedar-storaged --config /etc/cedar/storaged.conf --node-id 2
    volumes:
      - storaged2_data:/tmp/cedar/storage
    depends_on:
      - metad1
      - metad2
      - metad3
    networks:
      - cedar-net

  storaged3:
    image: cedargraph/storaged:latest
    command: cedar-storaged --config /etc/cedar/storaged.conf --node-id 3
    volumes:
      - storaged3_data:/tmp/cedar/storage
    depends_on:
      - metad1
      - metad2
      - metad3
    networks:
      - cedar-net

  graphd:
    image: cedargraph/graphd:latest
    command: cedar-graphd --config /etc/cedar/graphd.conf
    ports:
      - "9669:9669"
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
# Build image
docker build -t cedargraph:latest -f cedar-docker-compose/Dockerfile .

# Build with China mirrors
docker build -t cedargraph:latest -f cedar-docker-compose/Dockerfile.cn .

# Run container
docker run -p 9669:9669 cedargraph:latest
```

## Kubernetes Deployment

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
    host: "0.0.0.0"
    port: 9559
    grpc_port: 10559
    data_dir: "/data/metad"
    
  storaged.conf: |
    host: "0.0.0.0"
    port: 9779
    data_dir: "/data/storage"
    storage_mode: "partitioned"
    max_open_partitions: 256
    
  graphd.conf: |
    host: "0.0.0.0"
    port: 9669
    metad_host: "cedargraph-metad"
    metad_port: 10559
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
          image: cedargraph/metad:latest
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
          image: cedargraph/storaged:latest
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
          image: cedargraph/graphd:latest
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
  host: "0.0.0.0"
  port: 9559          # Raft port
  grpc_port: 10559    # gRPC port
  
  # Storage
  data_dir: "/data/metad"
  
  # Raft
  election_timeout_ms: 1000
  heartbeat_interval_ms: 100
  snapshot_interval_sec: 3600
  
  # Performance
  max_concurrent_requests: 1000
  request_timeout_ms: 5000
```

### StorageD Configuration

```yaml
# StorageD configuration
storaged:
  # Network
  host: "0.0.0.0"
  port: 9779
  
  # Storage
  data_dir: "/data/storage"
  storage_mode: "partitioned"  # or "shared"
  max_open_partitions: 256
  
  # LSM
  memtable_size_mb: 64
  l0_max_files: 4
  max_bytes_for_level_base_mb: 256
  max_bytes_for_level_multiplier: 10
  
  # WAL
  enable_wal: true
  sync_on_write: false
  sync_interval_ms: 100
  
  # Compaction
  enable_auto_compaction: true
  compaction_threads: 1
  rate_limit_mb_per_sec: 100
  
  # Cache
  block_cache_mb: 256
  row_cache_mb: 64
```

### GraphD Configuration

```yaml
# GraphD configuration
graphd:
  # Network
  host: "0.0.0.0"
  port: 9669
  
  # MetaD connection
  metad_host: "cedargraph-metad"
  metad_port: 10559
  
  # Query
  max_query_timeout_ms: 30000
  max_result_size: 10000
  
  # Plan cache
  plan_cache_size: 1000
  
  # Connection pool
  max_connections: 100
  connection_timeout_ms: 5000
```

## Monitoring

### Health Checks

```bash
# MetaD health
curl http://localhost:10559/health

# StorageD health
curl http://localhost:9779/health

# GraphD health
curl http://localhost:9669/health
```

### Metrics Endpoint

```yaml
# Add to configuration
monitoring:
  enable_metrics: true
  metrics_port: 9090
  metrics_path: "/metrics"
```

### Prometheus Configuration

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'cedargraph'
    static_configs:
      - targets:
          - 'metad:9090'
          - 'storaged:9090'
          - 'graphd:9090'
```

### Grafana Dashboard

Import the provided dashboard JSON:
- `monitoring/grafana/cedargraph-dashboard.json`

## Backup and Recovery

### Manual Backup

```bash
# Stop cluster
bash scripts/start_standalone.sh stop

# Backup data
tar -czf cedar-backup-$(date +%Y%m%d).tar.gz /tmp/cedar/standalone/

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
tar -czf $BACKUP_DIR/cedar-backup-$DATE.tar.gz /tmp/cedar/standalone/

# Cleanup old backups (keep last 7 days)
find $BACKUP_DIR -name "*.tar.gz" -mtime +7 -delete
```

### Recovery

```bash
# Stop cluster
bash scripts/start_standalone.sh stop

# Restore backup
tar -xzf cedar-backup-20240101_120000.tar.gz -C /

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

```yaml
# Increase MemTable size for write-heavy workloads
storaged:
  memtable_size_mb: 128
  
# Increase cache for read-heavy workloads
storaged:
  block_cache_mb: 512
  row_cache_mb: 128
  
# Increase compaction threads for large datasets
storaged:
  compaction_threads: 4
```

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
curl http://localhost:9779/metrics | grep compaction

# Check cache hit rate
curl http://localhost:9779/metrics | grep cache_hit

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
