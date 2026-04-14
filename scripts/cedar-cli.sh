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
    if ! nc -z "$HOST" "$PORT" 2>/dev/null; then
        echo -e "${RED}❌ 无法连接到 $HOST:$PORT${NC}"
        echo "请确保 CedarGraph 服务已启动"
        exit 1
    fi
}

# 执行命令
execute_command() {
    local cmd="$1"
    
    # 这里调用实际的客户端库或 gRPC 调用
    # 简化版直接输出格式化的结果
    case "$cmd" in
        "SHOW HOSTS"|"show hosts")
            show_hosts
            ;;
        "SHOW SPACES"|"show spaces")
            show_spaces
            ;;
        "HELP"|"help"|"?")
            show_help
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
    echo "+-------------+------+----------+"
    echo "| Host        | Port | Status   |"
    echo "+-------------+------+----------+"
    
    # 从 docker 获取实际状态
    docker ps --filter "name=cedar-storaged" --format "{{.Names}}\t{{.Status}}" | while IFS=$'\t' read -r name status; do
        local hostname=$(echo "$name" | sed 's/cedar-//')
        local port="9779"
        local state="ONLINE"
        if [[ "$status" != *"Up"* ]]; then
            state="OFFLINE"
        fi
        printf "| %-11s | %-4s | %-8s |\n" "$hostname" "$port" "$state"
    done
    
    echo "+-------------+------+----------+"
    echo ""
}

# 显示图空间
show_spaces() {
    echo ""
    echo "+------------+"
    echo "| Name       |"
    echo "+------------+"
    echo "| production |"
    echo "+------------+"
    echo ""
}

# 显示帮助
show_help() {
    cat << EOF

CedarGraph 支持的命令:

集群管理:
  SHOW HOSTS              显示存储节点状态
  SHOW SPACES             显示图空间列表
  CREATE SPACE <name>     创建图空间
  DROP SPACE <name>       删除图空间

图操作:
  USE <space>             切换图空间
  CREATE TAG <name>       创建标签
  CREATE EDGE <name>      创建边类型
  INSERT VERTEX           插入顶点
  INSERT EDGE             插入边
  GO FROM <vid>           遍历查询
  FETCH PROP ON           查询属性
  MATCH                   模式匹配

系统:
  HELP                    显示帮助
  EXIT                    退出

EOF
}

# 交互式模式
interactive_mode() {
    echo ""
    echo -e "${GREEN}CedarGraph 客户端${NC}"
    echo "服务器: $HOST:$PORT"
    echo "输入 'HELP' 查看命令列表, 'EXIT' 退出"
    echo ""
    
    while true; do
        echo -n "cedar> "
        read -r cmd
        
        case "$cmd" in
            "EXIT"|"exit"|"QUIT"|"quit")
                echo "再见!"
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
