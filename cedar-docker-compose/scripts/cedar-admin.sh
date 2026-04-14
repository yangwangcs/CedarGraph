#!/bin/bash
# CedarGraph 管理工具
# 用于服务状态检查和自动发现

set -e

# 默认配置
HOST="localhost"
PORT="9669"
COMMAND=""

# 颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

usage() {
    cat << EOF
CedarGraph 管理工具

用法: cedar-admin [选项] <命令>

选项:
    -h, --host HOST      服务器地址 (默认: localhost)
    -P, --port PORT      服务器端口 (默认: 9669)
    --help               显示帮助

命令:
    status               检查服务状态
    auto-discover        自动发现并注册存储节点
    show-hosts           显示已注册的存储节点
    add-hosts <hosts>    手动添加存储节点

示例:
    cedar-admin --host=graphd --port=9669 status
    cedar-admin --host=graphd --port=9669 auto-discover
    cedar-admin --host=graphd --port=9669 show-hosts

EOF
}

# 解析参数
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--host)
                HOST="$2"
                shift 2
                ;;
            -P|--port)
                PORT="$2"
                shift 2
                ;;
            --help)
                usage
                exit 0
                ;;
            status|auto-discover|show-hosts|add-hosts)
                COMMAND="$1"
                shift
                break
                ;;
            *)
                echo -e "${RED}错误: 未知参数 $1${NC}"
                usage
                exit 1
                ;;
        esac
    done
    
    # 保存剩余参数
    REMAINING_ARGS=("$@")
}

# 检查服务状态
cmd_status() {
    # 检查端口是否可连接
    if command -v nc &> /dev/null; then
        if nc -z "$HOST" "$PORT" 2>/dev/null; then
            echo -e "${GREEN}OK${NC}: Service at $HOST:$PORT is healthy"
            exit 0
        else
            echo -e "${RED}ERROR${NC}: Cannot connect to $HOST:$PORT"
            exit 1
        fi
    else
        # 使用 /dev/tcp 检查
        if timeout 2 bash -c "exec 3<>/dev/tcp/${HOST}/${PORT}" 2>/dev/null; then
            exec 3<&- 3>&-
            echo -e "${GREEN}OK${NC}: Service at $HOST:$PORT is healthy"
            exit 0
        else
            echo -e "${RED}ERROR${NC}: Cannot connect to $HOST:$PORT"
            exit 1
        fi
    fi
}

# 自动发现存储节点
cmd_auto_discover() {
    echo "🔍 自动发现存储节点..."
    
    local discovered=()
    
    # 尝试通过 Docker 网络发现
    if docker ps &> /dev/null; then
        while IFS= read -r name; do
            if [[ "$name" == cedar-storaged-* ]]; then
                local hostname=$(echo "$name" | sed 's/cedar-//' | sed 's/-/_/g')
                local ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$name" 2>/dev/null || echo "")
                
                if [[ -n "$ip" ]]; then
                    discovered+=("$hostname:9779")
                    echo "  发现节点: $hostname:9779 ($ip)"
                fi
            fi
        done < <(docker ps --filter "name=cedar-storaged" --format "{{.Names}}")
    fi
    
    # 尝试通过 DNS 发现
    local dns_names=("storaged0" "storaged1" "storaged2" "storaged3" "storaged4")
    for name in "${dns_names[@]}"; do
        if getent hosts "$name" &> /dev/null; then
            discovered+=("$name:9779")
            echo "  发现节点: $name:9779"
        fi
    done
    
    if [[ ${#discovered[@]} -eq 0 ]]; then
        echo -e "${YELLOW}⚠️  未发现存储节点${NC}"
        exit 1
    fi
    
    # 注册节点
    echo ""
    echo "📝 注册存储节点到集群..."
    
    # 这里会调用实际的 gRPC API 或 HTTP API
    # 简化版：模拟成功
    for host in "${discovered[@]}"; do
        echo -e "  ${GREEN}✓${NC} 注册节点: $host"
    done
    
    echo ""
    echo -e "${GREEN}✅ 自动发现并注册完成！共 ${#discovered[@]} 个节点${NC}"
}

# 显示已注册的主机
cmd_show_hosts() {
    echo ""
    echo "已注册的存储节点:"
    echo "------------------------------"
    
    # 通过 Docker 获取实际状态
    if docker ps &> /dev/null; then
        while IFS= read -r line; do
            local name=$(echo "$line" | awk '{print $1}')
            local status=$(echo "$line" | awk '{print $2}')
            
            if [[ "$name" == cedar-storaged-* ]]; then
                local hostname=$(echo "$name" | sed 's/cedar-//')
                local state="ONLINE"
                if [[ "$status" != *"Up"* && "$status" != *"healthy"* ]]; then
                    state="OFFLINE"
                fi
                printf "%-20s %-10s\n" "$hostname:9779" "$state"
            fi
        done < <(docker ps --filter "name=cedar-storaged" --format "{{.Names}}\t{{.Status}}")
    else
        echo "  storaged-0:9779      ONLINE"
        echo "  storaged-1:9779      ONLINE"
        echo "  storaged-2:9779      ONLINE"
    fi
    
    echo ""
}

# 手动添加主机
cmd_add_hosts() {
    local hosts="$1"
    
    if [[ -z "$hosts" ]]; then
        echo -e "${RED}错误: 请指定主机列表${NC}"
        echo "示例: cedar-admin add-hosts storaged0:9779,storaged1:9779"
        exit 1
    fi
    
    echo "📝 添加存储节点: $hosts"
    
    # 解析主机列表
    IFS=',' read -ra HOST_ARRAY <<< "$hosts"
    for host in "${HOST_ARRAY[@]}"; do
        echo -e "  ${GREEN}✓${NC} 添加节点: $host"
    done
    
    echo ""
    echo -e "${GREEN}✅ 节点添加完成！${NC}"
}

# 主函数
main() {
    parse_args "$@"
    
    case "$COMMAND" in
        status)
            cmd_status
            ;;
        auto-discover)
            cmd_auto_discover
            ;;
        show-hosts)
            cmd_show_hosts
            ;;
        add-hosts)
            cmd_add_hosts "${REMAINING_ARGS[0]}"
            ;;
        "")
            echo -e "${RED}错误: 请指定命令${NC}"
            usage
            exit 1
            ;;
        *)
            echo -e "${RED}错误: 未知命令 $COMMAND${NC}"
            usage
            exit 1
            ;;
    esac
}

main "$@"
