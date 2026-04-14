# CedarGraph

🌲 高性能分布式图数据库，支持万亿级图数据存储和毫秒级查询响应。

## 快速开始

```bash
# 克隆部署仓库
git clone https://github.com/cedargraph/cedar-docker-compose.git
cd cedar-docker-compose

# 一键部署
./scripts/quick-start.sh

# 连接集群
./scripts/cedar-cli.sh -e "SHOW HOSTS"
```

## 特性

- **水平扩展**: 支持 3-100+ 节点集群
- **高性能**: 674K+ ops/sec 吞吐
- **一致性**: Raft 协议保证数据一致性
- **云原生**: Kubernetes Helm Chart 支持

## 支持的标签

- `latest` - 最新稳定版
- `v0.1.0`, `v0.1`, `v0` - 版本标签
- `dev` - 开发版

## 文档

- [官方文档](https://docs.cedargraph.io)
- [GitHub](https://github.com/cedargraph/cedar)
- [Helm Chart](https://github.com/cedargraph/cedar-docker-compose/tree/main/helm-chart)

## 许可证

Apache 2.0
