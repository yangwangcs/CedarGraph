# Embedded Raft - CedarGraph 自研 Raft 实现

## 概述

Embedded Raft 是一个**轻量级、自包含的 Raft 共识实现**，专为 CedarGraph 设计。它**不依赖 braft** 或其他外部 Raft 库，使用标准 C++17 实现。

## 架构

```
┌─────────────────────────────────────────────────────────────┐
│                      metad_server                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │   Metadata   │  │  Embedded    │  │   FileRaft   │       │
│  │   Service    │◄─┤  Raft Node   │◄─┤   Storage    │       │
│  └──────────────┘  └──────┬───────┘  └──────────────┘       │
│                           │                                  │
│                      ┌────┴────┐                            │
│                      │ Transport│  (gRPC - TODO)            │
│                      └─────────┘                            │
└─────────────────────────────────────────────────────────────┘
```

## 核心组件

### 1. EmbeddedRaftNode (`include/cedar/dtx/raft/embedded_raft.h`)

完整的 Raft 节点实现，包含：

- **Leader 选举**：基于随机超时
- **日志复制**：AppendEntries RPC
- **心跳机制**：定期发送
- **成员变更**：AddPeer/RemovePeer

```cpp
raft::EmbeddedRaftNode::Options options;
options.node_id = 1;
options.peers = {{2, "192.168.1.2:6000"}, {3, "192.168.1.3:6000"}};
options.data_dir = "/data/cedar/metad";

auto node = std::make_unique<raft::EmbeddedRaftNode>(
    options, transport, state_machine, storage);
node->Start();
```

### 2. FileRaftStorage (`src/dtx/raft/embedded_raft_storage.cc`)

基于文件的持久化存储：

- `raft.log` - 日志条目
- `raft.state` - 当前任期和投票信息
- `raft.snapshot` - 快照数据

### 3. StateMachineAdapter

桥接 `raft::StateMachine` (标准接口) 到 `EmbeddedStateMachine` (嵌入式实现)：

```cpp
auto inner = std::make_unique<MetadataStateMachine>();
auto adapter = std::make_unique<raft::StateMachineAdapter>(inner.get());
```

## 使用方式

### 单节点模式

```bash
# 创建配置文件
mkdir -p /tmp/cedar/metad
cat > /tmp/cedar/node.conf << 'EOF'
node_id = 1
bind_address = 0.0.0.0:6000
data_dir = /tmp/cedar/metad
peers = 1:127.0.0.1:6000
election_timeout_min_ms = 150
election_timeout_max_ms = 300
heartbeat_interval_ms = 50
EOF

# 启动 metad
./build/metad_server --config /tmp/cedar/node.conf
```

### 三节点集群

```bash
# 节点 1
peers = 1:192.168.1.1:6000,2:192.168.1.2:6000,3:192.168.1.3:6000

# 节点 2
peers = 1:192.168.1.1:6000,2:192.168.1.2:6000,3:192.168.1.3:6000

# 节点 3
peers = 1:192.168.1.1:6000,2:192.168.1.2:6000,3:192.168.1.3:6000
```

## 配置参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `node_id` | 节点唯一标识 | - |
| `bind_address` | 监听地址 | 0.0.0.0:6000 |
| `data_dir` | 数据目录 | /tmp/cedar/metad |
| `peers` | 集群节点列表 | - |
| `election_timeout_min_ms` | 最小选举超时 | 150ms |
| `election_timeout_max_ms` | 最大选举超时 | 300ms |
| `heartbeat_interval_ms` | 心跳间隔 | 50ms |

## 与 braft 对比

| 特性 | Embedded Raft | braft |
|------|--------------|-------|
| 外部依赖 | 无 | brpc, protobuf |
| 部署复杂度 | 低（单二进制） | 高（多库依赖） |
| 性能 | 适合中小集群 | 大规模生产级 |
| 功能 | 核心 Raft | 完整功能（预投票、流水线等）|
| 适用场景 | CedarGraph 元数据 | 通用场景 |

## 待办事项 (TODO)

- [ ] **gRPC Transport**: 实现跨节点 RPC
- [ ] **快照传输**: InstallSnapshot RPC
- [ ] **成员动态变更**: 运行时添加/删除节点
- [ ] **预投票 (Pre-vote)**: 减少无效选举
- [ ] **日志流水线**: 批量发送日志
- [ ] **只读查询优化**: Follower 读

## 测试

```bash
# 运行 HLC 测试
./build/tests/test_dtx_hybrid_logical_clock

# 运行元数据服务测试
./build/tests/test_dtx_meta_service

# 运行所有 DTx 测试
./build/tests/test_dtx_*
```

## 文件列表

```
include/cedar/dtx/raft/
├── embedded_raft.h          # 嵌入式 Raft 核心接口
├── raft_interface.h         # 标准 Raft 接口
├── raft_node_factory.h      # 工厂函数
└── braft_node.h             # braft 适配器（可选）

src/dtx/raft/
├── embedded_raft.cc         # 核心实现
├── embedded_raft_storage.cc # 文件存储实现
├── raft_node_factory.cc     # 工厂实现
├── memory_raft.cc           # 内存 Raft（测试用）
└── braft_node.cc            # braft 适配（可选）

src/dtx/metad/
└── metad_server.cc          # 元数据服务器
```
