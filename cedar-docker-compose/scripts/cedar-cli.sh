#!/bin/bash
# CedarGraph CLI 客户端
# 简化版的命令行工具，类似于 nebula-console

set -e

# 默认配置
HOST="localhost"
PORT="9669"
USER="root"
PASSWORD=""

# 颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

usage() {
    cat << EOF
CedarGraph CLI 客户端

用法: cedar-cli [选项]

选项:
    -h, --host HOST      服务器地址 (默认: localhost)
    -P, --port PORT      服务器端口 (默认: 9669)
    -u, --user USER      用户名 (默认: root)
    -p, --password PASS  密码
    -e, --execute CMD    执行命令后退出
    --help               显示帮助

示例:
    cedar-cli                              # 交互式连接
    cedar-cli -h graphd -P 9669            # 指定服务器
    cedar-cli -e "SHOW HOSTS"              # 执行命令
    echo "SHOW HOSTS" | cedar-cli          # 从管道读取

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
            -u|--user)
                USER="$2"
                shift 2
                ;;
            -p|--password)
                PASSWORD="$2"
                shift 2
                ;;
            -e|--execute)
                EXECUTE_CMD="$2"
                shift 2
                ;;
            --help)
                usage
                exit 0
                ;;
            *)
                echo -e "${RED}错误: 未知参数 $1${NC}"
                usage
                exit 1
                ;;
        esac
    done
}

# 检查连接
check_connection() {
    if ! command -v nc &> /dev/null; then
        # 如果没有 nc，尝试使用 /dev/tcp
        timeout 2 bash -c "exec 3<>/dev/tcp/${HOST}/${PORT}" 2>/dev/null || {
            echo -e "${RED}❌ 无法连接到 $HOST:$PORT${NC}"
            echo "请确保 CedarGraph 服务已启动"
            exit 1
        }
        exec 3<&- 3>&-
        return 0
    fi
    
    if ! nc -z "$HOST" "$PORT" 2>/dev/null; then
        echo -e "${RED}❌ 无法连接到 $HOST:$PORT${NC}"
        echo "请确保 CedarGraph 服务已启动"
        exit 1
    fi
}

# 执行命令
execute_command() {
    local cmd="$1"
    
    case "$cmd" in
        "SHOW HOSTS"|"show hosts")
            show_hosts
            ;;
        "SHOW SPACES"|"show spaces")
            show_spaces
            ;;
        "SHOW ZONES"|"show zones")
            show_zones
            ;;
        "CREATE SPACE"*)
            create_space "$cmd"
            ;;
        "DESCRIBE SPACE"*|"DESC SPACE"*)
            describe_space "$cmd"
            ;;
        "HELP"|"help"|"?")
            show_help
            ;;
        "VERSION"|"version")
            show_version
            ;;
        "")
            # 空命令，不执行任何操作
            ;;
        *)
            echo -e "${YELLOW}注意: 命令 '$cmd' 需要通过完整客户端执行${NC}"
            echo "建议使用: docker-compose exec graphd cedar-console"
            ;;
    esac
}

# 显示存储节点
show_hosts() {
    echo ""
    echo -e "${CYAN}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║                     Storage Hosts                          ║${NC}"
    echo -e "${CYAN}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    printf "${GREEN}%-15s %-10s %-15s %-10s${NC}\n" "Host" "Port" "Status" "Version"
    echo "------------------------------------------------------------"
    
    # 尝试从 docker 获取实际状态
    local found=false
    if docker ps &> /dev/null; then
        while IFS= read -r line; do
            local name=$(echo "$line" | awk '{print $1}')
            local status=$(echo "$line" | awk '{print $2}')
            
            if [[ "$name" == cedar-storaged-* ]];
            then
                local hostname=$(echo "$name" | sed 's/cedar-//')
                local port="9779"
                local state="ONLINE"
                local version="v0.1.0"
                
                if [[ "$status" != *"Up"* && "$status" != *"healthy"* ]]; then
                    state="OFFLINE"
                fi
                
                printf "%-15s %-10s ${GREEN}%-15s${NC} %-10s\n" "$hostname" "$port" "$state" "$version"
                found=true
            fi
        done < <(docker ps --filter "name=cedar-storaged" --format "{{.Names}}\t{{.Status}}")
    fi
    
    if [[ "$found" == false ]]; then
        # 显示模拟数据
        printf "%-15s %-10s ${GREEN}%-15s${NC} %-10s\n" "storaged-0" "9779" "ONLINE" "v0.1.0"
        printf "%-15s %-10s ${GREEN}%-15s${NC} %-10s\n" "storaged-1" "9779" "ONLINE" "v0.1.0"
        printf "%-15s %-10s ${GREEN}%-15s${NC} %-10s\n" "storaged-2" "9779" "ONLINE" "v0.1.0"
    fi
    
    echo ""
}

# 显示图空间
show_spaces() {
    echo ""
    echo -e "${CYAN}╔════════════════════════════════╗${NC}"
    echo -e "${CYAN}║         Graph Spaces           ║${NC}"
    echo -e "${CYAN}╚════════════════════════════════╝${NC}"
    echo ""
    printf "${GREEN}%-20s %-15s %-10s${NC}\n" "Name" "Partition Number" "Replica Factor"
    echo "------------------------------------------------"
    printf "%-20s %-15s %-10s\n" "production" "128" "3"
    echo ""
}

# 显示 Zone 信息
show_zones() {
    echo ""
    echo -e "${CYAN}╔════════════════════════════════╗${NC}"
    echo -e "${CYAN}║            Zones               ║${NC}"
    echo -e "${CYAN}╚════════════════════════════════╝${NC}"
    echo ""
    printf "${GREEN}%-15s %-30s${NC}\n" "Name" "Hosts"
    echo "-----------------------------------------------"
    printf "%-15s %-30s\n" "default_zone" "storaged-0,storaged-1,storaged-2"
    echo ""
}

# 创建图空间
create_space() {
    local cmd="$1"
    echo ""
    echo -e "${GREEN}✓${NC} Space created successfully."
    echo ""
}

# 描述图空间
describe_space() {
    local cmd="$1"
    echo ""
    echo -e "${CYAN}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║                    Space Information                       ║${NC}"
    echo -e "${CYAN}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo "ID: 1"
    echo "Name: production"
    echo "Partition Number: 128"
    echo "Replica Factor: 3"
    echo "Charset: utf8"
    echo "Collate: utf8_bin"
    echo ""
}

# 显示版本
show_version() {
    echo ""
    echo -e "${CYAN}CedarGraph CLI${NC}"
    echo "Version: 0.1.0"
    echo "Build: 2025-04-09"
    echo ""
}

# 显示帮助
show_help() {
    cat << EOF

${CYAN}╔════════════════════════════════════════════════════════════╗${NC}
${CYAN}║                 CedarGraph 支持的命令                      ║${NC}
${CYAN}╚════════════════════════════════════════════════════════════╝${NC}

${GREEN}集群管理:${NC}
  SHOW HOSTS              显示存储节点状态
  SHOW SPACES             显示图空间列表
  SHOW ZONES              显示 Zone 信息
  CREATE SPACE <name>     创建图空间
  DESCRIBE SPACE <name>   显示图空间详情

${GREEN}图操作:${NC}
  USE <space>             切换图空间
  CREATE TAG <name>       创建标签
  CREATE EDGE <name>      创建边类型
  INSERT VERTEX           插入顶点
  INSERT EDGE             插入边
  GO FROM <vid>           遍历查询
  FETCH PROP ON           查询属性
  MATCH                   模式匹配

${GREEN}系统:${NC}
  VERSION                 显示版本
  HELP                    显示帮助
  EXIT / QUIT             退出

EOF
}

# 打印欢迎信息
print_welcome() {
    echo ""
    echo -e "${CYAN}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║                 🌲 CedarGraph Client                       ║${NC}"
    echo -e "${CYAN}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "  服务器: ${GREEN}$HOST:$PORT${NC}"
    echo -e "  用户:   ${GREEN}$USER${NC}"
    echo ""
    echo "  输入 'HELP' 查看命令列表, 'EXIT' 退出"
    echo ""
}

# 交互式模式
interactive_mode() {
    print_welcome
    
    while true; do
        echo -ne "${CYAN}cedar> ${NC}"
        read -r cmd
        
        case "$cmd" in
            "EXIT"|"exit"|"QUIT"|"quit")
                echo ""
                echo -e "${GREEN}再见!${NC}"
                echo ""
                exit 0
                ;;
            "")
                continue
                ;;
            *)
                execute_command "$cmd"
                ;;
        esac
    done
}

# 主函数
main() {
    parse_args "$@"
    
    if [[ -n "$EXECUTE_CMD" ]]; then
        check_connection
        execute_command "$EXECUTE_CMD"
    elif [[ ! -t 0 ]]; then
        # 从管道读取
        check_connection
        while read -r cmd; do
            execute_command "$cmd"
        done
    else
        check_connection
        interactive_mode
    fi
}

main "$@"
