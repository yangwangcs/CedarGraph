#!/bin/bash
# CedarGraph 一键部署脚本
# 参考 NebulaGraph 的便捷部署设计

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 默认配置
CEDAR_VERSION="k8s-fix-20260630"
INSTALL_DIR="$(pwd)"
DATA_DIR="${INSTALL_DIR}/data"
LOGS_DIR="${INSTALL_DIR}/logs"
COMPOSE_FILE="${INSTALL_DIR}/docker-compose.yml"

is_safe_clean_target() {
    local target="$1"
    local install_abs target_abs

    install_abs="$(cd "${INSTALL_DIR}" 2>/dev/null && pwd -P)" || return 1
    target_abs="$(mkdir -p "${target}" && cd "${target}" 2>/dev/null && pwd -P)" || return 1

    case "${target_abs}" in
        "${install_abs}"/*)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

# 打印帮助信息
usage() {
    cat << EOF
CedarGraph 一键部署脚本

用法: $0 [选项]

选项:
    -v, --version VERSION    指定 CedarGraph 版本 (默认: k8s-fix-20260630)
    -d, --dir DIRECTORY      指定安装目录 (默认: 当前目录)
    -n, --nodes N            存储节点数量 1/3/5 (默认: 3)
    --studio                 同时安装 Web Studio
    --clean                  清理现有数据
    --stop                   停止集群
    --status                 查看集群状态
    -h, --help               显示此帮助信息

示例:
    $0                                    # 快速启动 3 节点集群
    $0 -n 5                               # 启动 5 节点集群
    $0 --studio                           # 启动集群并安装 Studio
    $0 -v 0.1.0 --clean                   # 指定版本并清理数据
    $0 --stop                             # 停止集群
    $0 --status                           # 查看状态

EOF
}

# 打印带颜色的信息
info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

success() {
    echo -e "${GREEN}[✓]${NC} $1"
}

warning() {
    echo -e "${YELLOW}[!]${NC} $1"
}

error() {
    echo -e "${RED}[✗]${NC} $1"
}

# 解析参数
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -v|--version)
                CEDAR_VERSION="$2"
                shift 2
                ;;
            -d|--dir)
                INSTALL_DIR="$2"
                DATA_DIR="${INSTALL_DIR}/data"
                LOGS_DIR="${INSTALL_DIR}/logs"
                shift 2
                ;;
            -n|--nodes)
                NODE_COUNT="$2"
                shift 2
                ;;
            --studio)
                ENABLE_STUDIO=true
                shift
                ;;
            --clean)
                CLEAN_DATA=true
                shift
                ;;
            --stop)
                STOP_CLUSTER=true
                shift
                ;;
            --status)
                SHOW_STATUS=true
                shift
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                error "未知参数: $1"
                usage
                exit 1
                ;;
        esac
    done
}

# 检查系统要求
check_requirements() {
    info "检查系统要求..."
    
    # 检查 Docker
    if ! command -v docker &> /dev/null; then
        error "Docker 未安装"
        echo "请安装 Docker: https://docs.docker.com/get-docker/"
        exit 1
    fi
    
    # 检查 Docker Compose
    if docker compose version &> /dev/null; then
        COMPOSE_CMD="docker compose"
    elif command -v docker-compose &> /dev/null; then
        COMPOSE_CMD="docker-compose"
    else
        error "Docker Compose 未安装"
        echo "请安装 Docker Compose: https://docs.docker.com/compose/install/"
        exit 1
    fi
    
    # 检查 Docker 是否运行
    if ! docker info &> /dev/null; then
        error "Docker 服务未运行"
        exit 1
    fi
    
    success "系统检查通过"
}

# 清理旧数据
clean_data() {
    if [[ "$CLEAN_DATA" == "true" ]]; then
        mkdir -p "${INSTALL_DIR}"
        if ! is_safe_clean_target "${DATA_DIR}" || ! is_safe_clean_target "${LOGS_DIR}"; then
            error "拒绝清理：DATA_DIR/LOGS_DIR 必须位于安装目录内部"
            exit 1
        fi
        if [[ "${CEDAR_QUICKSTART_ALLOW_CLEAN:-0}" != "1" ]]; then
            error "拒绝清理：请先备份，再显式设置 CEDAR_QUICKSTART_ALLOW_CLEAN=1"
            exit 1
        fi
        info "清理旧数据..."
        rm -rf "${DATA_DIR:?}" "${LOGS_DIR:?}"
        success "数据已清理"
    fi
}

# 创建目录结构
setup_directories() {
    info "创建目录结构..."
    
    mkdir -p "${DATA_DIR}"/meta{0,1,2}
    mkdir -p "${DATA_DIR}"/storage{0,1,2}
    mkdir -p "${LOGS_DIR}"/meta{0,1,2}
    mkdir -p "${LOGS_DIR}"/storage{0,1,2}
    mkdir -p "${LOGS_DIR}"/graphd
    
    success "目录创建完成"
}

# 拉取镜像
pull_images() {
    info "拉取 CedarGraph 镜像 (版本: ${CEDAR_VERSION})..."
    
    export CEDAR_VERSION="${CEDAR_VERSION}"
    
    ${COMPOSE_CMD} -f "${COMPOSE_FILE}" pull
    
    success "镜像拉取完成"
}

# 启动服务
start_services() {
    info "启动 CedarGraph 集群..."
    
    export CEDAR_VERSION="${CEDAR_VERSION}"
    
    local compose_args="-d"
    if [[ "$ENABLE_STUDIO" == "true" ]]; then
        compose_args="--profile studio ${compose_args}"
    fi
    
    ${COMPOSE_CMD} -f "${COMPOSE_FILE}" up ${compose_args}
    
    success "服务启动完成"
}

# 停止服务
stop_services() {
    info "停止 CedarGraph 集群..."
    
    ${COMPOSE_CMD} -f "${COMPOSE_FILE}" down
    
    success "集群已停止"
}

# 显示状态
show_status() {
    echo ""
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║                 CedarGraph 集群状态                        ║"
    echo "╚════════════════════════════════════════════════════════════╝"
    echo ""
    
    ${COMPOSE_CMD} -f "${COMPOSE_FILE}" ps
    
    echo ""
    echo "服务端口:"
    echo "  MetaD Raft: localhost:9559"
    echo "  MetaD gRPC: localhost:10559"
    echo "  Storage: localhost:9779-9781"
    echo "  GraphD:  localhost:9669"
    echo "  Health:  localhost:9668"
    echo "  Metrics: localhost:9667"
    
    if [[ "$ENABLE_STUDIO" == "true" ]]; then
        echo "  Studio:  localhost:7001"
    fi
}

# 等待服务就绪
wait_for_ready() {
    info "等待集群就绪..."
    
    local max_wait=180
    local waited=0
    local spin='⣾⣽⣻⢿⡿⣟⣯⣷'
    local spin_idx=0
    
    while [[ $waited -lt $max_wait ]]; do
        # 检查容器状态和 Docker healthcheck，避免仅 Running 就误判为可用。
        local running=$(${COMPOSE_CMD} -f "${COMPOSE_FILE}" ps --filter "status=running" --format "{{.Name}}" | grep -c "^cedar-" || true)
        local total=$(${COMPOSE_CMD} -f "${COMPOSE_FILE}" ps --format "{{.Name}}" | grep -c "^cedar-" || true)
        local unhealthy=$(${COMPOSE_CMD} -f "${COMPOSE_FILE}" ps --format "{{.Name}}\t{{.Status}}" | awk '/^cedar-/ && $0 ~ /unhealthy/ {count++} END {print count+0}')
        local core_running=$(${COMPOSE_CMD} -f "${COMPOSE_FILE}" ps --filter "status=running" --format "{{.Name}}" | grep -E -c "^cedar-(metad-[0-2]|storaged-[0-2]|graphd)$" || true)

        if [[ $core_running -ge 7 && $unhealthy -eq 0 ]]; then
            echo ""
            success "集群已就绪！"
            return 0
        fi
        
        printf "\r  %s 等待中... (%d/%d 服务运行, core=%d/7, unhealthy=%d)" "${spin:$spin_idx:1}" "$running" "$total" "$core_running" "$unhealthy"
        spin_idx=$(( (spin_idx + 3) % 24 ))
        
        sleep 2
        waited=$((waited + 2))
    done
    
    echo ""
    warning "集群启动超时，请检查日志: ${COMPOSE_CMD} -f ${COMPOSE_FILE} logs"
    return 1
}

# 初始化集群
init_cluster() {
    info "初始化集群..."
    
    # 等待 GraphD 就绪
    local retries=30
    while [[ $retries -gt 0 ]]; do
        if docker exec cedar-graphd cedar-admin --host=localhost --port=9669 status &> /dev/null; then
            break
        fi
        sleep 2
        retries=$((retries - 1))
    done

    if [[ $retries -eq 0 ]]; then
        error "GraphD 状态检查失败，停止初始化"
        return 1
    fi
    
    # 自动发现并检查存储节点。真实注册由 StorageD/GraphD 与 MetaD 的 RPC 完成。
    if docker exec cedar-graphd cedar-admin --host=localhost --port=9669 auto-discover &> /dev/null; then
        success "自动发现并检查存储节点"
    else
        error "StorageD 自动发现或连通性检查失败"
        return 1
    fi
}

# 打印最终状态
print_final_status() {
    echo ""
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║              🌲 CedarGraph 集群部署成功！                  ║"
    echo "╚════════════════════════════════════════════════════════════╝"
    echo ""
    echo "📊 服务状态:"
    echo "----------------------------------------"
    ${COMPOSE_CMD} -f "${COMPOSE_FILE}" ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
    echo ""
    echo "🔗 连接信息:"
    echo "  Graph 服务:   localhost:9669"
    echo "  健康检查:     http://localhost:9668/health"
    echo "  指标服务:     http://localhost:9667/metrics"
    echo "  Meta Raft:    localhost:9559"
    echo "  Meta gRPC:    localhost:10559"
    echo ""
    echo "🛠️  常用命令:"
    echo "  查看日志:     ${COMPOSE_CMD} logs -f"
    echo "  连接集群:     ${COMPOSE_CMD} exec console cedar-cli"
    echo "  查看状态:     ${COMPOSE_CMD} exec graphd cedar-admin show-hosts"
    echo "  停止集群:     ${COMPOSE_CMD} down"
    echo ""
    
    if [[ "$ENABLE_STUDIO" == "true" ]]; then
        echo "🎨 Web Studio: http://localhost:7001"
        echo ""
    fi
    
    echo "📖 文档: https://docs.cedargraph.io"
    echo "🐛 反馈: https://github.com/cedargraph/issues"
    echo ""
}

# 主函数
main() {
    echo ""
    echo "🌲 CedarGraph 一键部署脚本"
    echo ""
    
    parse_args "$@"
    
    # 处理特殊命令
    if [[ "$STOP_CLUSTER" == "true" ]]; then
        stop_services
        exit 0
    fi
    
    if [[ "$SHOW_STATUS" == "true" ]]; then
        show_status
        exit 0
    fi
    
    # 检查并启动
    check_requirements
    clean_data
    setup_directories
    pull_images
    start_services
    
    if wait_for_ready; then
        init_cluster || {
            error "部署初始化失败"
            echo "查看日志: ${COMPOSE_CMD} -f ${COMPOSE_FILE} logs"
            exit 1
        }
        print_final_status
    else
        error "部署可能出现问题"
        echo "查看日志: ${COMPOSE_CMD} -f ${COMPOSE_FILE} logs"
        exit 1
    fi
}

# 执行主函数
main "$@"
