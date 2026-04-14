# CedarGraph Docker Compose

🌲 一键部署 CedarGraph 分布式图数据库

[![Docker Hub](https://img.shields.io/docker/pulls/cedargraph/cedar.svg)](https://hub.docker.com/r/cedargraph/cedar)
[![GitHub Release](https://img.shields.io/github/release/cedargraph/cedar-docker-compose.svg)](https://github.com/cedargraph/cedar-docker-compose/releases)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

---

## 🚀 快速开始

### 1. 克隆仓库

```bash
git clone https://github.com/cedargraph/cedar-docker-compose.git
cd cedar-docker-compose
```

### 2. 一键部署

```bash
./scripts/quick-start.sh
```

### 3. 连接集群

```bash
./scripts/cedar-cli.sh -e "SHOW HOSTS"
```

---

## 📋 系统要求

| 组件 | 最低要求 | 推荐配置 |
|------|---------|---------|
| Docker | 20.10+ | 24.0+ |
| Docker Compose | 2.0+ | 2.20+ |
| CPU | 4 核 | 8 核+ |
| 内存 | 8 GB | 16 GB+ |
| 磁盘 | 50 GB | 200 GB+ SSD |

---

## 🛠️ 部署选项

### 基础部署（3节点）

```bash
./scripts/quick-start.sh
```

### 带 Web Studio

```bash
./scripts/quick-start.sh --studio
```

### 指定版本

```bash
./scripts/quick-start.sh -v 0.1.0
```

### 清理并重新部署

```bash
./scripts/quick-start.sh --clean
```

---

## 📊 服务端口

| 服务 | 端口 | 说明 |
|------|------|------|
| GraphD | 9669 | 查询服务 |
| GraphD HTTP | 19669 | HTTP API |
| MetaD | 9559 | 元数据服务 |
| StorageD | 9779-9781 | 存储服务 |
| Studio | 7001 | Web UI (可选) |

---

## 🎯 CLI 命令

```bash
# 交互式模式
./scripts/cedar-cli.sh

# 查看存储节点
./scripts/cedar-cli.sh -e "SHOW HOSTS"

# 查看图空间
./scripts/cedar-cli.sh -e "SHOW SPACES"

# 指定服务器
./scripts/cedar-cli.sh -h graphd -P 9669
```

---

## ☸️ Kubernetes 部署

```bash
# 添加 Helm 仓库
helm repo add cedargraph https://charts.cedargraph.io
helm repo update

# 安装
helm install my-cedar cedargraph/cedargraph

# 带 Studio
helm install my-cedar cedargraph/cedargraph --set global.studio.enabled=true
```

详情见 [helm-chart/README.md](helm-chart/README.md)

---

## 📁 目录结构

```
cedar-docker-compose/
├── docker-compose.yml          # 主部署文件
├── scripts/
│   ├── quick-start.sh          # 一键部署脚本 ⭐
│   └── cedar-cli.sh            # CLI 客户端
├── helm-chart/                 # Kubernetes Helm Chart
├── data/                       # 数据目录 (自动创建)
├── logs/                       # 日志目录 (自动创建)
└── README.md
```

---

## 🔄 常用操作

```bash
# 查看状态
./scripts/quick-start.sh --status

# 停止集群
./scripts/quick-start.sh --stop

# 查看日志
docker-compose logs -f

# 进入容器
docker-compose exec graphd /bin/sh

# 扩容存储节点
docker-compose up -d storaged3 storaged4 storaged5
```

---

## 🐛 故障排查

### 检查服务状态

```bash
docker-compose ps
```

### 查看日志

```bash
# 所有服务
docker-compose logs -f

# 特定服务
docker-compose logs -f graphd
```

### 重置集群

```bash
./scripts/quick-start.sh --stop
./scripts/quick-start.sh --clean
./scripts/quick-start.sh
```

---

## 📚 文档

- [官方文档](https://docs.cedargraph.io)
- [架构设计](https://docs.cedargraph.io/architecture)
- [性能调优](https://docs.cedargraph.io/performance)
- [API 参考](https://docs.cedargraph.io/api)

---

## 🤝 贡献

欢迎提交 Issue 和 PR！

- [GitHub Issues](https://github.com/cedargraph/cedar-docker-compose/issues)
- [贡献指南](CONTRIBUTING.md)

---

## 📄 许可证

[Apache 2.0](LICENSE)

---

**Made with ❤️ by CedarGraph Team**
