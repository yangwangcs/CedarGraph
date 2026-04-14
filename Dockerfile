# CedarGraph Storage Node Docker Image
# 构建命令: docker build -t cedargraph:latest .

FROM ubuntu:22.04 AS builder

# 安装依赖
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    wget \
    curl \
    libssl-dev \
    pkg-config \
    protobuf-compiler \
    libprotobuf-dev \
    grpc-proto \
    libgrpc++-dev \
    liblz4-dev \
    libsnappy-dev \
    libzstd-dev \
    && rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /build

# 复制源代码
COPY . /build/

# 构建项目
RUN mkdir -p build && cd build && \
    cmake .. -DCMAKE_BUILD_TYPE=Release && \
    cmake --build . -j$(nproc) --target storaged metad_server cedar-queryd

# 运行时镜像
FROM ubuntu:22.04

# 安装运行时依赖
RUN apt-get update && apt-get install -y \
    libssl3 \
    libprotobuf23 \
    libgrpc++1 \
    liblz4-1 \
    libsnappy1v5 \
    libzstd1 \
    curl \
    net-tools \
    iputils-ping \
    && rm -rf /var/lib/apt/lists/*

# 创建 cedar 用户
RUN groupadd -r cedar && useradd -r -g cedar -d /var/lib/cedar -s /bin/false cedar

# 从 builder 复制二进制文件
COPY --from=builder /build/build/storaged /usr/local/bin/
COPY --from=builder /build/build/metad_server /usr/local/bin/
COPY --from=builder /build/build/cedar-queryd /usr/local/bin/
COPY --from=builder /build/scripts/cedar_health_monitor.sh /usr/local/bin/

# 创建目录结构
RUN mkdir -p /var/lib/cedar/storage /var/lib/cedar/metad /var/log/cedar /etc/cedar && \
    chown -R cedar:cedar /var/lib/cedar /var/log/cedar

# 复制默认配置
COPY --from=builder /build/config/storaged_auto_recovery.conf /etc/cedar/storaged.conf.template
COPY --from=builder /build/config/metad.conf /etc/cedar/metad.conf.template

# 暴露端口
# 7000-7002: StorageD nodes
# 6000-6002: MetaD nodes
# 8080: QueryD
# 9090: Prometheus metrics
EXPOSE 7000 7001 7002 6000 6001 6002 8080 9090

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD /usr/local/bin/cedar_health_monitor.sh || exit 1

# 启动脚本
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["storaged"]
