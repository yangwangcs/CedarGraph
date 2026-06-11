# CedarGraph Docker Image
# 多阶段构建，生成轻量级生产镜像

# =============================================================================
# 构建阶段
# =============================================================================
FROM ubuntu:22.04 AS builder

# 安装构建依赖
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    wget \
    curl \
    liblz4-dev \
    libgrpc++-dev \
    protobuf-compiler-grpc \
    libprotobuf-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /build

# 复制源代码
COPY . /build/

# 构建
RUN mkdir -p build && cd build && \
    cmake .. -DCMAKE_BUILD_TYPE=Release && \
    make -j$(nproc)

# =============================================================================
# 运行阶段
# =============================================================================
FROM ubuntu:22.04

# 安装运行时依赖
RUN apt-get update && apt-get install -y \
    liblz4-1 \
    libgrpc++1 \
    libprotobuf23 \
    curl \
    netcat-openbsd \
    dnsutils \
    iputils-ping \
    && rm -rf /var/lib/apt/lists/*

# 创建用户
RUN groupadd -r cedar && useradd -r -g cedar cedar

# 创建目录
RUN mkdir -p /data /logs /etc/cedar /usr/local/bin && \
    chown -R cedar:cedar /data /logs

# 复制构建产物
COPY --from=builder /build/build/cedar-graphd /usr/local/bin/
COPY --from=builder /build/build/cedar-metad /usr/local/bin/
COPY --from=builder /build/build/cedar-storaged /usr/local/bin/
COPY --from=builder /build/build/cedar-queryd /usr/local/bin/

# 复制运维脚本
COPY scripts/cedar-cli.sh /usr/local/bin/cedar-cli
COPY scripts/cedar_health_monitor.sh /usr/local/bin/cedar_health_monitor.sh
COPY scripts/cedar_health_check.sh /usr/local/bin/cedar_health_check.sh
COPY cedar-docker-compose/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

RUN chmod +x /usr/local/bin/*.sh /usr/local/bin/cedar_health_monitor.sh /usr/local/bin/cedar_health_check.sh /usr/local/bin/docker-entrypoint.sh && \
    chmod +x /usr/local/bin/cedar-*

# 设置环境变量
ENV PATH="/usr/local/bin:${PATH}"
ENV NODE_ROLE=""
ENV DATA_DIR="/data"
ENV LOG_DIR="/logs"

# Health check: TCP probe based on NODE_ROLE port mapping.
# Ports: metad=9559, graphd=9669, storaged=9779, queryd=9889
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD bash -c 'case "${NODE_ROLE}" in metad) P=9559;; storaged) P=9779;; graphd) P=9669;; queryd) P=9889;; *) exit 1;; esac; nc -z localhost ${P} || exit 1'

# 暴露端口
EXPOSE 9559 9669 9779 9889 19669

# 切换到非 root 用户
USER cedar

# 数据卷
VOLUME ["/data", "/logs"]

# Default entrypoint uses the role-aware wrapper with exec for proper signal forwarding.
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD []
