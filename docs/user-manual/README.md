# CedarGraph-Core 用户说明书

## 1. 适用对象

本文档面向需要构建、运行、测试和排障 CedarGraph-Core 的开发者与运维人员。系统仍处于内核和生产化候选阶段，使用时应优先验证目标场景。

## 2. 仓库结构

| 路径 | 内容 |
|---|---|
| `include/cedar` | 对外头文件和模块接口 |
| `src` | 核心实现 |
| `proto` | gRPC/Protobuf 定义 |
| `tools` | 服务可执行入口和测试工具 |
| `scripts` | 构建、部署、预检和运维脚本 |
| `docs` | 当前维护文档 |
| `k8s`, `helm-chart` | Kubernetes 和 Helm 部署资源 |
| `cedar-docker-compose` | Docker 镜像和 Compose 相关资源 |

## 3. 构建

```bash
mkdir -p build
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=ON
cmake --build build -j4
```

依赖缺失时先检查系统包和 `third_party` 目录。brpc/braft 由仓库 vendored 集成，构建时应避免误用 Homebrew 或系统中的不兼容头文件。

## 4. 测试

```bash
ctest --test-dir build --output-on-failure
```

常用定向测试：

```bash
./build/tests/test_twcd_engine
./build/tests/test_lnd_occ
./build/tests/test_storage_critical_batch
```

发布候选检查：

```bash
./scripts/preflight_release_gate.sh
```

## 5. 运行服务

主要服务：

| 服务 | 可执行文件 | 默认职责 |
|---|---|---|
| MetaD | `cedar-metad` | 元数据和服务发现 |
| StorageD | `cedar-storaged` | 分区存储和 Raft |
| GraphD | `cedar-graphd` | 查询入口和事务协调 |
| GCN | `graphcomputenode` | 图计算和 TMV 视图 |

本地脚本：

```bash
./scripts/start_standalone.sh
./scripts/start_distributed.sh
./scripts/cedar_health_check.sh
```

## 6. 安全配置

生产或准生产环境必须配置强认证和 TLS/mTLS。

```bash
export CEDAR_GRAPHD_AUTH_JWT_SECRET='replace-with-at-least-32-bytes-secret'
export CEDAR_GRAPHD_AUTH_USER='admin'
export CEDAR_GRAPHD_AUTH_PASSWORD='replace-with-strong-password'
export CEDAR_GRPC_TLS_ENABLED=1
export CEDAR_GRPC_SERVER_CERT=/etc/cedar/tls/tls.crt
export CEDAR_GRPC_SERVER_KEY=/etc/cedar/tls/tls.key
export CEDAR_GRPC_CA_CERT=/etc/cedar/tls/ca.crt
```

`CEDAR_GRPC_ALLOW_INSECURE=1` 只允许用于开发或受控测试。

## 7. 常见排障

| 现象 | 检查 |
|---|---|
| CMake 找不到 gRPC/protobuf | 检查系统包、`grpc_cpp_plugin` 和 CMake 输出 |
| brpc/braft 头文件混用 | 检查 include path 是否误指向 Homebrew brpc/butil |
| 端口冲突 | 检查 `.env`、脚本端口变量和本机占用 |
| GraphD 启动失败 | 检查认证参数、TLS 文件和 MetaD 地址 |
| K8s Pod 不 Ready | 检查 Secret、PVC、NetworkPolicy、探针和日志 |

## 8. 当前边界

- 部分客户端 helper 明确返回未实现或失败，不应被封装为成功。
- CI 中部分安全扫描为非阻断；发布前仍需人工审计扫描结果。
- 性能数字必须由当前 commit 和固定 workload 重新生成。

