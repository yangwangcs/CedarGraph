# CedarGraph 系统架构与进程说明

## 📊 系统架构概览

CedarGraph 采用典型的分布式数据库三层架构：

```
┌─────────────────────────────────────────────────────────────────┐
│                        客户端层 (Client)                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │  Cypher CLI  │  │  Client SDK  │  │  Web Console │          │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘          │
└─────────┼─────────────────┼─────────────────┼──────────────────┘
          │                 │                 │
          └─────────────────┴─────────────────┘
                            │
                    gRPC / HTTP2
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│                    查询层 (Query Layer)                          │
│                                                                 │
│   ┌─────────────────────────────────────────────────────────┐  │
│   │                    cedar-queryd                          │  │
│   │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │  │
│   │  │ Cypher Parser│  │Query Planner │  │ gRPC Service │  │  │
│   │  └──────────────┘  └──────────────┘  └──────────────┘  │  │
│   │  ┌──────────────┐  ┌──────────────┐                   │  │
│   │  │Distributed   │  │  Plan Cache  │                   │  │
│   │  │ Executor     │  │              │                   │  │
│   │  └──────────────┘  └──────────────┘                   │  │
│   └─────────────────────────────────────────────────────────┘  │
│                              │                                  │
└──────────────────────────────┼──────────────────────────────────┘
                               │
                    内部通信 (RPC)
                               │
          ┌────────────────────┼────────────────────┐
          │                    │                    │
┌─────────▼─────────┐ ┌────────▼────────┐ ┌────────▼────────┐
│   元数据服务       │ │   存储服务 1     │ │   存储服务 N     │
│                   │ │                  │ │                  │
│  ┌─────────────┐  │ │  ┌────────────┐  │ │  ┌────────────┐  │
│  │  MetaD      │  │ │  │ StorageD   │  │ │  │ StorageD   │  │
│  │             │  │ │  │            │  │ │  │            │  │
│  │ • Cluster   │  │ │  │ • LSM-Tree │  │ │  │ • LSM-Tree │  │
│  │   Topology  │  │ │  │ • SST      │  │ │  │ • SST      │  │
│  │ • Partition │  │ │  │ • MVCC     │  │ │  │ • MVCC     │  │
│  │   Routing   │  │ │  │ • WAL      │  │ │  │ • WAL      │  │
│  │ • Schema    │  │ │  │ • Raft     │  │ │  │ • Raft     │  │
│  └─────────────┘  │ │  └────────────┘  │ │  └────────────┘  │
└───────────────────┘ └──────────────────┘ └──────────────────┘
```

---

## 🔧 核心进程（3个）

### 1. MetaD Server (元数据服务)

**可执行文件**: `metad_server` (814KB)

**功能**:
- 管理集群拓扑结构
- 分区路由表维护
- Schema 管理
- 节点健康监控
- 负载均衡决策

**端口**: 默认 9559

**启动命令**:
```bash
./metad_server --listen 0.0.0.0:9559
```

**关键特性**:
- 高可用（通过 Raft 共识）
- 动态分区调度
- 元数据缓存

---

### 2. StorageD (存储服务)

**可执行文件**: `storaged` (191KB)

**功能**:
- 数据持久化（LSM-Tree）
- MVCC 版本控制
- WAL 日志
- SST 文件管理
- 本地查询执行

**端口**: 默认 9779

**启动命令**:
```bash
./storaged --listen 0.0.0.0:9779 --meta 127.0.0.1:9559
```

**关键特性**:
- 基于 CedarKey 的存储引擎
- 双向边存储优化
- 时序数据支持
- Raft 复制（可选）

---

### 3. QueryD (查询服务)

**可执行文件**: `cedar-queryd` (225KB)

**功能**:
- Cypher 查询解析
- 查询计划生成与优化
- 分布式查询执行
- 结果聚合
- gRPC 服务接口

**端口**: 默认 9669

**启动命令**:
```bash
./cedar-queryd --listen 0.0.0.0:9669 --meta 127.0.0.1:9559
```

**关键特性**:
- 无状态设计（水平扩展）
- 查询计划缓存
- 并行执行
- 断路器保护

---

## 🚀 系统启动顺序

### 最小集群（单机开发环境）

```bash
# 1. 启动元数据服务（必须先启动）
./metad_server --listen 0.0.0.0:9559 &

# 2. 启动存储服务
./storaged --listen 0.0.0.0:9779 --meta 127.0.0.1:9559 &

# 3. 启动查询服务
./cedar-queryd --listen 0.0.0.0:9669 --meta 127.0.0.1:9559 &

# 检查进程
ps aux | grep -E "metad|storaged|cedar-queryd"
```

### 生产集群（多机部署）

```bash
# === 机器 1: MetaD Leader ===
./metad_server --listen 0.0.0.0:9559 --node_id 1

# === 机器 2: MetaD Follower ===
./metad_server --listen 0.0.0.0:9559 --node_id 2 --join 192.168.1.1:9559

# === 机器 3: MetaD Follower ===
./metad_server --listen 0.0.0.0:9559 --node_id 3 --join 192.168.1.1:9559

# === 机器 4-6: StorageD ===
./storaged --listen 0.0.0.0:9779 --meta 192.168.1.1:9559 --data_dir /data1

# === 机器 7-9: QueryD ===
./cedar-queryd --listen 0.0.0.0:9669 --meta 192.168.1.1:9559 --workers 32
```

---

## 📡 进程间通信

### 通信矩阵

| 源进程 | 目标进程 | 协议 | 用途 |
|--------|----------|------|------|
| Client | QueryD | gRPC | 查询请求 |
| QueryD | MetaD | gRPC | 获取元数据 |
| QueryD | StorageD | gRPC | 数据读写 |
| StorageD | MetaD | gRPC | 心跳/注册 |
| StorageD | StorageD | Raft | 数据复制 |
| MetaD | MetaD | Raft | 共识协议 |

### 端口分配

| 服务 | 默认端口 | 配置项 |
|------|----------|--------|
| MetaD | 9559 | `--listen` |
| StorageD | 9779 | `--listen` |
| QueryD | 9669 | `--listen` |
| StorageD Raft | 9780+ | `--raft_port` |

---

## 🔍 健康检查

### QueryD 健康检查

```bash
# 使用 grpcurl 或类似工具
grpcurl -plaintext localhost:9669 cedar.query.QueryService/Health

# 预期响应
{
  "healthy": true,
  "status": "healthy",
  "executor_healthy": true,
  "storage_client_healthy": true,
  "meta_client_healthy": true,
  "active_queries": 0
}
```

### MetaD 健康检查

```bash
# 检查端口
nc -zv localhost 9559

# 或使用 HTTP 接口（如果有）
curl http://localhost:9559/health
```

---

## 📊 资源需求

### 最小配置（开发环境）

| 进程 | CPU | 内存 | 磁盘 |
|------|-----|------|------|
| MetaD | 1核 | 512MB | 1GB |
| StorageD | 2核 | 2GB | 10GB |
| QueryD | 2核 | 1GB | - |
| **总计** | **5核** | **3.5GB** | **11GB** |

### 推荐配置（生产环境）

| 进程 | CPU | 内存 | 磁盘 |
|------|-----|------|------|
| MetaD | 4核 | 4GB | 10GB (SSD) |
| StorageD | 16核 | 32GB | 1TB (SSD) |
| QueryD | 8核 | 16GB | - |
| **总计** | **28核** | **52GB** | **1TB+** |

---

## 🛠️ 常用运维命令

### 查看进程状态

```bash
# 查看所有 CedarGraph 进程
ps aux | grep -E "metad|storaged|cedar-queryd" | grep -v grep

# 查看端口监听
netstat -tlnp | grep -E "9559|9669|9779"

# 查看资源使用
top -p $(pgrep -d',' -f "metad|storaged|cedar-queryd")
```

### 日志查看

```bash
# 查看 QueryD 日志
tail -f /var/log/cedar-queryd/cedar-queryd.INFO

# 查看错误日志
tail -f /var/log/cedar-queryd/cedar-queryd.ERROR
```

### 优雅关闭

```bash
# 发送 SIGTERM 信号
kill -TERM <pid>

# 或
killall cedar-queryd
killall storaged
killall metad_server
```

---

## 📝 总结

**CedarGraph 系统需要 3 个核心进程**:

1. **MetaD** (1个或多个) - 元数据服务，集群的大脑
2. **StorageD** (1个或多个) - 存储服务，数据的归宿
3. **QueryD** (1个或多个) - 查询服务，用户的入口

**最小运行配置**: 3个进程（单机）
**推荐生产配置**: 3+ MetaD + N StorageD + M QueryD

三个进程相互独立，通过 gRPC 通信，可以独立扩展。
