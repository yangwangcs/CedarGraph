# CedarGraph Docker Image
# 多阶段构建，生成轻量级生产镜像

# =============================================================================
# 构建阶段
# =============================================================================
FROM ubuntu:22.04 AS builder

ARG APT_MIRROR=
ARG BUILD_JOBS=2

# 安装构建依赖
RUN if [ -n "${APT_MIRROR}" ]; then \
        sed -i "s|http://ports.ubuntu.com/ubuntu-ports|${APT_MIRROR}|g; s|http://archive.ubuntu.com/ubuntu|${APT_MIRROR}|g" /etc/apt/sources.list; \
    fi && \
    apt-get -o Acquire::Retries=5 update && \
    apt-get -o Acquire::Retries=5 install -y --no-install-recommends \
	    build-essential \
	    cmake \
	    ninja-build \
	    git \
    wget \
    curl \
    libcurl4-openssl-dev \
    liblz4-dev \
    libleveldb-dev \
    libgrpc++-dev \
    protobuf-compiler-grpc \
    libprotobuf-dev \
    protobuf-compiler \
    libprotoc-dev \
    libssl-dev \
    libyaml-cpp-dev \
    libgflags-dev \
    libgoogle-glog-dev \
    nlohmann-json3-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /build

# 复制源代码
COPY . /build/

# 构建
RUN cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release && \
    cmake --build build --target metad storaged graphd --parallel ${BUILD_JOBS}

# =============================================================================
# 运行阶段
# =============================================================================
FROM ubuntu:22.04

ARG APT_MIRROR=

# 安装运行时依赖
RUN if [ -n "${APT_MIRROR}" ]; then \
        sed -i "s|http://ports.ubuntu.com/ubuntu-ports|${APT_MIRROR}|g; s|http://archive.ubuntu.com/ubuntu|${APT_MIRROR}|g" /etc/apt/sources.list; \
    fi && \
    apt-get -o Acquire::Retries=5 update && \
	    apt-get -o Acquire::Retries=5 install -y --no-install-recommends \
	    liblz4-1 \
	    libleveldb1d \
	    libgrpc++1 \
    libprotobuf23 \
    libcurl4 \
    libssl3 \
    libyaml-cpp0.7 \
    libgflags2.2 \
    libgoogle-glog0v5 \
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

# 复制运维脚本
COPY scripts/cedar-cli.sh /usr/local/bin/cedar-cli
COPY cedar-docker-compose/scripts/cedar-admin.sh /usr/local/bin/cedar-admin
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
# Ports: metad gRPC=10559, graphd query=9669, storaged=9779
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD bash -c 'case "${NODE_ROLE}" in metad) P=10559;; storaged) P=9779;; graphd) P=9669;; *) exit 1;; esac; nc -z localhost ${P} || exit 1'

# 暴露端口
EXPOSE 9559 10559 9669 9668 9667 9779

# 切换到非 root 用户
USER cedar

# 数据卷
VOLUME ["/data", "/logs"]

# Default entrypoint uses the role-aware wrapper with exec for proper signal forwarding.
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD []
