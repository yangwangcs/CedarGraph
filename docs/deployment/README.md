# 部署指南

本文档提供开发、测试和候选生产环境部署入口。更详细的生产参数见 [生产部署指南](../PRODUCTION_DEPLOYMENT_GUIDE.md)。

## 1. 环境要求

建议开发环境：

| 项目 | 建议 |
|---|---|
| OS | Ubuntu 22.04+ 或 macOS |
| CPU | 4 核以上 |
| 内存 | 8GB 以上 |
| 编译器 | 支持 C++17 |
| 工具 | CMake、gRPC、Protobuf、OpenSSL、yaml-cpp、curl、LZ4 |

Ubuntu 依赖示例：

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake git curl pkg-config \
  libcurl4-openssl-dev liblz4-dev libzstd-dev libleveldb-dev \
  libgrpc++-dev protobuf-compiler-grpc libprotobuf-dev protobuf-compiler \
  libssl-dev libyaml-cpp-dev libgflags-dev libgoogle-glog-dev \
  nlohmann-json3-dev libgtest-dev
```

## 2. 本地构建

```bash
mkdir -p build
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=ON
cmake --build build -j4
ctest --test-dir build --output-on-failure
```

## 3. 本地多进程

常用脚本：

```bash
./scripts/start_standalone.sh
./scripts/start_distributed.sh
./scripts/cedar_health_check.sh
```

如果端口冲突，请优先通过环境变量或脚本参数覆盖端口，不要直接修改生产默认配置。

## 4. Docker Compose

```bash
docker compose -f docker-compose.production.yml config
docker compose -f docker-compose.production.yml up -d
docker compose -f docker-compose.production.yml ps
```

生产环境不得使用默认弱密码。`.env` 中的示例值只用于开发或模板说明。

## 5. Kubernetes

静态检查：

```bash
./scripts/preflight_k8s_static.sh
./scripts/preflight_helm_static.sh
```

部署前需要准备：

- TLS/mTLS 证书和 Secret。
- 持久卷和 StorageClass。
- NetworkPolicy。
- PodDisruptionBudget。
- 监控和告警。
- 回滚和恢复演练。

## 6. 发布门禁

候选版本至少执行：

```bash
./scripts/preflight_release_gate.sh
```

正式发版建议启用更长测试：

```bash
CEDAR_RELEASE_FULL_CTEST=1 \
CEDAR_RELEASE_SOAK_SECONDS=300 \
CEDAR_RELEASE_SOAK_POLL_SECONDS=5 \
./scripts/preflight_release_gate.sh
```

## 7. 上线边界

本地门禁通过只说明当前机器和配置下达到候选质量。真实生产上线仍必须验证真实证书链、Secret 注入、持久卷、网络策略、备份恢复、滚动升级、回滚、监控告警和长时间压力。

