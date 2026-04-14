# CedarGraph

[![CI/CD](https://github.com/YOUR_USERNAME/CedarGraph/actions/workflows/docker-release.yml/badge.svg)](https://github.com/YOUR_USERNAME/CedarGraph/actions)
[![Docker Pulls](https://img.shields.io/docker/pulls/cedargraph/cedar.svg)](https://hub.docker.com/r/cedargraph/cedar)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Release](https://img.shields.io/github/release/YOUR_USERNAME/CedarGraph.svg)](https://github.com/YOUR_USERNAME/CedarGraph/releases)

**CedarGraph** 🌲 是一个高性能分布式图数据库，支持万亿级图数据存储和毫秒级查询响应。

## 🚀 快速开始

### Docker Compose（推荐，2分钟启动）

```bash
git clone https://github.com/cedargraph/cedar-docker-compose.git
cd cedar-docker-compose
./scripts/quick-start.sh
```

然后连接集群：
```bash
./scripts/cedar-cli.sh -e "SHOW HOSTS"
```

### Kubernetes Helm

```bash
helm repo add cedargraph https://charts.cedargraph.io
helm install my-cedar cedargraph/cedargraph
```

### 从源码构建

```bash
git clone https://github.com/YOUR_USERNAME/CedarGraph.git
cd CedarGraph
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

## ✨ 核心特性

* **分布式架构**: 基于 Raft 共识协议，支持水平扩展到 100+ 节点
* **高性能写入**: LSM-tree 存储引擎，写入延迟 < 14 μs
* **时序图支持**: 原生支持 `AS OF` 点查询和 `BETWEEN` 范围扫描
* **自动服务发现**: GraphD 自动发现并注册 StorageD 节点
* **云原生**: Kubernetes Helm Chart 支持，内置监控和告警
* **统一事件编码**: 32 字节定长键，统一顶点/边/属性更新

## 📊 性能对比

与 Aion (基于 Neo4j 的时序图数据库) 在 6.74 亿时序记录上的对比：

| 指标 | CedarGraph | Aion | 提升 |
|------|------------|------|------|
| 存储空间 | 6.98 GB | 54.97 GB | **7.9x** |
| 写入延迟 | 14 μs | 200 μs | **14x** |
| AS OF 查询 | 0.8 ms | 6 ms | **7.5x** |
| BETWEEN 查询 | 56 ms | 65 ms | **1.2x** |
| 时序 BFS (深度 3) | 270 ms | 890 ms | **3.3x** |

## 🏗️ 架构设计

### 核心组件

```
┌─────────────────────────────────────────────────────────────┐
│                      CedarGraph Cluster                      │
├─────────────┬─────────────┬─────────────┬──────────────────┤
│   MetaD     │   MetaD     │   MetaD     │   GraphD         │
│  (Leader)   │  (Follower) │  (Follower) │  (Query Layer)   │
│   :9559     │   :9559     │   :9559     │   :9669          │
└─────────────┴─────────────┴─────────────┴──────────────────┘
         │              Raft Consensus              │
         └──────────────────────────────────────────┘
                              │
    ┌─────────────────────────┼─────────────────────────┐
    │                         │                         │
┌───▼────┐              ┌────▼────┐              ┌────▼────┐
│StorageD│              │ StorageD│              │ StorageD│
│  :9779 │              │  :9779  │              │  :9779  │
└────────┘              └─────────┘              └─────────┘
```

### 存储引擎

* **内存**: VCSL (Version Chain SkipList) - 无锁跳表 + 垂直版本链
* **磁盘**: Zone-Columnar SST - 5 个语义分区，ZLM (Zone-Level Merge) 压缩
* **一致性**: Raft 协议保证强一致性，支持线性一致性读

### CedarKey 设计

32 字节定长格式，统一顶点和边：

| 字段 | 大小 | 说明 |
|------|------|------|
| `entity_id` | 8B | 顶点 ID / 边源 ID |
| `timestamp` | 8B | 降序微秒时间戳 |
| `target_id` | 8B | 边目标 ID / 扩展数据 |
| `column_id` | 2B | 属性 ID / 边类型 ID |
| `sequence` | 2B | MVCC 版本序列号 |
| `type+flags` | 2B | 实体类型和标志 |
| `reserved` | 2B | 保留 |

## 📖 文档

- [快速开始指南](docs/00_QUICKSTART.md)
- [架构设计](CEDAR_ARCHITECTURE.md)
- [API 文档](docs/DTx-Usage-Guide.md)
- [部署指南](DEPLOYMENT_GUIDE.md)
- [性能调优](PERFORMANCE_OPTIMIZATION_REPORT.md)

## 💻 系统要求

### 最低配置
- CPU: 4 核
- 内存: 8 GB
- 磁盘: 50 GB SSD
- 网络: 1 Gbps

### 推荐配置
- CPU: 8 核+
- 内存: 16 GB+
- 磁盘: 200 GB+ NVMe SSD
- 网络: 10 Gbps

## 🛠️ 构建要求

* C++17 编译器 (GCC 7+, Clang 5+, MSVC 2017+)
* CMake 3.14+
* LZ4, Protocol Buffers
* gRPC (可选，用于 RPC 服务)

### macOS

```bash
brew install cmake pkg-config lz4 grpc googletest
```

### Ubuntu/Debian

```bash
sudo apt-get update
sudo apt-get install -y cmake pkg-config liblz4-dev libprotobuf-dev protobuf-compiler libgrpc++-dev
```

## 📁 项目结构

```
CedarGraph/
├── include/cedar/          # 公共头文件
│   ├── storage/            # 存储引擎接口
│   ├── dtx/                # 分布式事务
│   └── types/              # 核心类型定义
├── src/                    # 实现
│   ├── storage/            # VCSL, Zone-Columnar SST
│   ├── dtx/                # Raft, 2PC, 服务发现
│   └── dtx/service_discovery.*  # 自动服务发现 ⭐
├── cedar-docker-compose/   # Docker 部署仓库 ⭐
│   ├── docker-compose.yml
│   ├── scripts/quick-start.sh
│   └── helm-chart/
├── docs/                   # 文档
├── proto/                  # Protocol Buffer 定义
├── examples/               # 示例程序
├── tests/                  # 单元测试
└── CMakeLists.txt
```

## 🎯 使用示例

```cpp
#include "cedar/storage/cedar_graph_storage.h"
#include <iostream>

int main() {
    cedar::CedarOptions options;
    options.create_if_missing = true;

    cedar::CedarGraphStorage* storage = nullptr;
    cedar::Status s = cedar::CedarGraphStorage::Open(
        options, "/data/cedar", 
        &storage);

    // 写入时序数据: 顶点 123, 时间戳, 值
    cedar::Descriptor desc = cedar::Descriptor::InlineInt(1, 42);
    s = storage->Put(123ULL, 1700000000000000ULL, desc, 
                     cedar::Timestamp(1));

    // AS OF 查询: 读取特定时间点的状态
    auto result = storage->Get(123ULL, 1700000000000000ULL);
    if (result.has_value()) {
        if (auto val = result->AsInlineInt()) {
            std::cout << "Value: " << *val << std::endl;
        }
    }

    delete storage;
    return 0;
}
```

## 🤝 贡献

欢迎提交 Issue 和 PR！

- [GitHub Issues](https://github.com/YOUR_USERNAME/CedarGraph/issues)
- [贡献指南](CONTRIBUTING.md)

## 📄 许可证

Apache License 2.0. 详见 [LICENSE](LICENSE)。

---

**Made with ❤️ by CedarGraph Team**
