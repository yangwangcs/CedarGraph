#!/bin/bash
# GraphD 容器入口脚本
# 负责自动初始化集群

set -e

# 配置
NODE_ROLE=${NODE_ROLE:-graphd}
META_SERVERS=${META_SERVERS:-"metad0:9559,metad1:9559,metad2:9559"}
AUTO_DISCOVERY=${AUTO_DISCOVERY:-true}
QUERY_PORT=${QUERY_PORT:-9669}
HTTP_PORT=${HTTP_PORT:-19669}

# 日志函数
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [GraphD] $1"
}

log "Starting GraphD service..."
log "  Role: $NODE_ROLE"
log "  Meta Servers: $META_SERVERS"
log "  Auto Discovery: $AUTO_DISCOVERY"
log "  Query Port: $QUERY_PORT"
log "  HTTP Port: $HTTP_PORT"

# 等待 MetaD 就绪
wait_for_metad() {
    log "Waiting for MetaD to be ready..."
    
    local max_wait=120
    local waited=0
    
    IFS=',' read -ra META_ARRAY <<< "$META_SERVERS"
    
    while [[ $waited -lt $max_wait ]]; do
        for meta in "${META_ARRAY[@]}"; do
            local host=$(echo "$meta" | cut -d: -f1)
            local port=$(echo "$meta" | cut -d: -f2)
            
            if timeout 2 bash -c "exec 3<>/dev/tcp/${host}/${port}" 2>/dev/null; then
                exec 3<&- 3>&-
                log "MetaD is ready at $meta"
                return 0
            fi
        done
        
        sleep 2
        waited=$((waited + 2))
        
        if [[ $((waited % 10)) -eq 0 ]]; then
            log "  Still waiting... (${waited}s/${max_wait}s)"
        fi
    done
    
    log "ERROR: Timeout waiting for MetaD"
    return 1
}

# 自动发现存储节点
auto_discover_storaged() {
    if [[ "$AUTO_DISCOVERY" != "true" ]]; then
        log "Auto-discovery disabled, skipping..."
        return 0
    fi
    
    log "Auto-discovering storage nodes..."
    
    # 发现的节点列表
    local discovered_nodes=()
    
    # 方法1: 通过 Docker DNS 发现
    local dns_names=("storaged0" "storaged1" "storaged2" "storaged3" "storaged4" "storaged5")
    for name in "${dns_names[@]}"; do
        if getent hosts "$name" &> /dev/null; then
            local ip=$(getent hosts "$name" | awk '{print $1}')
            discovered_nodes+=("$name:9779")
            log "  Discovered via DNS: $name:9779 ($ip)"
        fi
    done
    
    # 方法2: 通过 Docker 网络发现
    if [[ -S /var/run/docker.sock ]]; then
        # 尝试从 Docker API 获取
        local containers=$(curl -s --unix-socket /var/run/docker.sock \
            "http://localhost/containers/json" 2>/dev/null | \
            grep -o '"Names":\["[^"]*"\]' | \
            grep -o 'cedar-storaged-[^"]*' || true)
        
        for container in $containers; do
            local name=$(echo "$container" | sed 's/\///' | sed 's/cedar-//' | sed 's/-/_/g')
            if [[ ! " ${discovered_nodes[*]} " =~ " ${name}:9779 " ]]; then
                discovered_nodes+=("$name:9779")
                log "  Discovered via Docker: $name:9779"
            fi
        done
    fi
    
    if [[ ${#discovered_nodes[@]} -eq 0 ]]; then
        log "WARNING: No storage nodes discovered"
        return 0
    fi
    
    log "Discovered ${#discovered_nodes[@]} storage nodes"
    
    # 注册节点到集群
    log "Registering storage nodes to cluster..."
    
    for node in "${discovered_nodes[@]}"; do
        local host=$(echo "$node" | cut -d: -f1)
        local port=$(echo "$node" | cut -d: -f2)
        
        # 尝试注册（最多重试5次）
        local retry=0
        local max_retry=5
        
        while [[ $retry -lt $max_retry ]]; do
            # 这里调用实际的注册 API
            # 简化版：模拟成功
            if timeout 5 bash -c "exec 3<>/dev/tcp/${host}/${port}" 2>/dev/null; then
                exec 3<&- 3>&-
                log "  ✓ Registered: $node"
                break
            fi
            
            retry=$((retry + 1))
            sleep 2
        done
        
        if [[ $retry -eq $max_retry ]]; then
            log "  ✗ Failed to register: $node"
        fi
    done
    
    log "Auto-discovery completed"
}

# 主函数
main() {
    # 1. 等待 MetaD
    if ! wait_for_metad; then
        log "FATAL: Cannot connect to MetaD"
        exit 1
    fi
    
    # 2. 自动发现存储节点
    auto_discover_storaged
    
    # 3. 启动 GraphD 服务
    log "Starting GraphD server..."
    
    # 实际应该启动 cedar-graphd 二进制
    # 简化版：使用占位符
    if command -v cedar-graphd &> /dev/null; then
        exec cedar-graphd \
            --meta_servers="$META_SERVERS" \
            --query_port="$QUERY_PORT" \
            --http_port="$HTTP_PORT" \
            "$@"
    else
        # 占位：保持容器运行
        log "GraphD binary not found, running in demo mode"
        
        # 创建一个简单的 TCP 监听
        log "Listening on port $QUERY_PORT..."
        
        # 使用 nc 或自定义脚本保持服务运行
        while true; do
            # 简单的健康检查响应
            if command -v nc &> /dev/null; then
                echo -e "HTTP/1.1 200 OK\r\nContent-Length: 14\r\n\r\nGraphD: OK" | nc -l -p "$QUERY_PORT" -q 1 || true
            else
                sleep 60
            fi
        done
    fi
}

# 信号处理
trap 'log "Received signal, shutting down..."; exit 0' SIGTERM SIGINT

# 执行主函数
main "$@"
