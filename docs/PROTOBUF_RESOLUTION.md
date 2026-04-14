# Protobuf/gRPC 版本问题解决方案

## 问题诊断

```
生成代码错误: runtime_version.h not found
原因: 代码使用 protobuf 7.34.0 生成，但系统安装的是 3.21.0
```

## 解决方案

### 方案 1: 使用系统 protobuf 重新生成代码 (推荐)

```bash
# 1. 备份现有生成代码
mkdir -p generated_backup
mv *.pb.cc *.pb.h generated_backup/

# 2. 确认系统 protobuf 版本
protoc --version  # 当前: 3.21.0

# 3. 重新生成代码
protoc --cpp_out=. \
    --grpc_out=. \
    --plugin=protoc-gen-grpc=$(which grpc_cpp_plugin) \
    proto/*.proto

# 4. 验证生成
ls -la *.pb.cc *.pb.h
```

### 方案 2: 使用 Docker 统一编译环境

```dockerfile
# Dockerfile.build
FROM ubuntu:22.04

# 安装固定版本的 protobuf 和 gRPC
RUN apt-get update && apt-get install -y \
    protobuf-compiler=3.21.12-3 \
    libprotobuf-dev=3.21.12-3 \
    libgrpc++-dev=1.51.1-3 \
    grpc-proto=1.51.1-3

WORKDIR /build
COPY . .
RUN mkdir -p build && cd build && \
    cmake .. && make -j$(nproc)
```

### 方案 3: 静态链接 protobuf (完全避免版本问题)

```cmake
# CMakeLists.txt 修改
# 使用 FetchContent 下载并静态编译 protobuf
include(FetchContent)
FetchContent_Declare(
  protobuf
  GIT_REPOSITORY https://github.com/protocolbuffers/protobuf.git
  GIT_TAG        v3.21.12  # 固定版本
  GIT_SUBMODULES ""
)
FetchContent_MakeAvailable(protobuf)

# 使用项目内部的 protobuf 生成代码
add_custom_command(
  OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/storage_service.pb.cc
  COMMAND ${protobuf_BINARY_DIR}/protoc
  ARGS --cpp_out=${CMAKE_CURRENT_SOURCE_DIR}
       --grpc_out=${CMAKE_CURRENT_SOURCE_DIR}
       --plugin=protoc-gen-grpc=${grpc_BINARY_DIR}/grpc_cpp_plugin
       ${CMAKE_CURRENT_SOURCE_DIR}/proto/storage_service.proto
  DEPENDS proto/storage_service.proto
)
```

---

## 快速修复脚本

```bash
#!/bin/bash
# scripts/regenerate_protobuf.sh

set -e

PROTO_DIR="proto"
GENERATED_DIR="generated"

echo "=== Protobuf 代码重新生成 ==="

# 创建备份
mkdir -p ${GENERATED_DIR}/backup_$(date +%Y%m%d_%H%M%S)
cp *.pb.cc *.pb.h ${GENERATED_DIR}/backup_ 2>/dev/null || true

# 检查工具
echo "检查 protoc..."
protoc --version

echo "检查 grpc_cpp_plugin..."
which grpc_cpp_plugin

# 生成 C++ 代码
echo "生成 C++ 代码..."
for proto in ${PROTO_DIR}/*.proto; do
    echo "处理: $proto"
    protoc \
        --cpp_out=. \
        --grpc_out=. \
        --plugin=protoc-gen-grpc=$(which grpc_cpp_plugin) \
        -I${PROTO_DIR} \
        $proto
done

echo "=== 完成 ==="
echo "生成的文件:"
ls -la *.pb.cc *.pb.h
```

---

## 长期解决方案

### 1. 添加 protoc 到项目 (版本锁定)

```cmake
# cmake/FindProtobuf.cmake
# 优先使用项目本地的 protoc
find_program(PROTOC_EXECUTABLE 
    NAMES protoc
    PATHS ${CMAKE_SOURCE_DIR}/tools/protobuf/bin
    NO_DEFAULT_PATH
)

if(NOT PROTOC_EXECUTABLE)
    find_program(PROTOC_EXECUTABLE protoc)
endif()
```

### 2. CI/CD 中固定版本

```yaml
# .github/workflows/build.yml
jobs:
  build:
    runs-on: ubuntu-22.04
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Protobuf
        uses: arduino/setup-protoc@v2
        with:
          version: '21.12'  # 对应 3.21.12
          
      - name: Generate Code
        run: ./scripts/regenerate_protobuf.sh
        
      - name: Build
        run: |
          mkdir build && cd build
          cmake ..
          make -j4
```

---

## 推荐实施步骤

1. **立即**: 运行 `regenerate_protobuf.sh` 使用系统 protoc 重新生成代码
2. **本周**: 添加 CI 检查，确保生成的代码与系统 protoc 版本匹配
3. **下周**: 考虑使用 Docker 或 FetchContent 完全锁定依赖版本
