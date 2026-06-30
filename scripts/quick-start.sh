#!/bin/bash
# CedarGraph 一键部署脚本
# 参考 NebulaGraph 的便捷部署设计

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 默认配置
CEDAR_VERSION="k8s-fix-20260630"
INSTALL_DIR="./cedar-cluster"
DATA_DIR="${INSTALL_DIR}/data"
LOGS_DIR="${INSTALL_DIR}/logs"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_CMD=""

error() {
    echo -e "${RED}❌ $1${NC}"
}

warn() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

success() {
    echo -e "${GREEN}✅ $1${NC}"
}

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
    -d, --dir DIRECTORY      指定安装目录 (默认: ./cedar-cluster)
    -n, --nodes N            存储节点数量 1/3/5 (默认: 3)
    --studio                 同时安装 Web Studio
    --clean                  清理现有数据
    -h, --help               显示此帮助信息

示例:
    $0                                    # 快速启动 3 节点集群
    $0 -n 5                               # 启动 5 节点集群
    $0 --studio                           # 启动集群并安装 Studio
    $0 -v 0.1.0 --clean                   # 指定版本并清理数据

EOF
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
            -h|--help)
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

# 检查系统要求
check_requirements() {
    echo "🔍 检查系统要求..."
    
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
        echo "🧹 清理旧数据..."
        rm -rf "${DATA_DIR:?}" "${LOGS_DIR:?}"
        success "数据已清理"
    fi
}

# 创建目录结构
setup_directories() {
    echo "📁 创建目录结构..."
    
    mkdir -p "${DATA_DIR}"/meta{0,1,2}
    mkdir -p "${DATA_DIR}"/storage{0,1,2}
    mkdir -p "${LOGS_DIR}"/meta{0,1,2}
    mkdir -p "${LOGS_DIR}"/storage{0,1,2}
    mkdir -p "${LOGS_DIR}"/graphd
    
    success "目录创建完成"
}

# 下载 docker-compose.yml
download_compose() {
    echo "📥 下载部署配置..."
    
    local compose_url="https://raw.githubusercontent.com/cedargraph/cedar-docker-compose/main/docker-compose.yml"
    local local_compose="${REPO_ROOT}/cedar-docker-compose/docker-compose.yml"
    
    if [[ ! -f "${INSTALL_DIR}/docker-compose.yml" ]]; then
        if [[ -f "$local_compose" ]]; then
            cp "$local_compose" "${INSTALL_DIR}/docker-compose.yml"
        elif command -v curl &> /dev/null; then
            curl -fsSL "${compose_url}" -o "${INSTALL_DIR}/docker-compose.yml"
        elif command -v wget &> /dev/null; then
            wget -q "${compose_url}" -O "${INSTALL_DIR}/docker-compose.yml"
        else
            warn "无法下载配置文件，请手动放置 docker-compose.yml"
        fi
    fi
    
    success "配置准备完成"
}

# 拉取镜像
pull_images() {
    echo "🐳 拉取 CedarGraph 镜像..."
    
    cd "${INSTALL_DIR}"
    
    export CEDAR_VERSION="${CEDAR_VERSION}"
    export CEDAR_GRAPHD_AUTH_JWT_SECRET="${CEDAR_GRAPHD_AUTH_JWT_SECRET:-cedar-quickstart-dev-secret-please-change-32bytes}"
    export CEDAR_GRAPHD_AUTH_USER="${CEDAR_GRAPHD_AUTH_USER:-admin}"
    export CEDAR_GRAPHD_AUTH_PASSWORD="${CEDAR_GRAPHD_AUTH_PASSWORD:-admin}"
    export CEDAR_GRAPHD_AUTH_ROLE="${CEDAR_GRAPHD_AUTH_ROLE:-admin}"
    export CEDAR_GRPC_TLS_ENABLED="${CEDAR_GRPC_TLS_ENABLED:-0}"
    
    ${COMPOSE_CMD} pull
    
    success "镜像拉取完成"
}

# 启动服务
start_services() {
    echo "🚀 启动 CedarGraph 集群..."
    
    cd "${INSTALL_DIR}"
    
    local compose_args="-d"
    if [[ "$ENABLE_STUDIO" == "true" ]]; then
        compose_args="--profile studio ${compose_args}"
    fi
    
    ${COMPOSE_CMD} up ${compose_args}
    
    success "服务启动完成"
}

# 等待服务就绪
wait_for_ready() {
    echo "⏳ 等待集群就绪..."
    
    local max_wait=180
    local waited=0
    
    while [[ $waited -lt $max_wait ]]; do
        local running total unhealthy core_running
        running=$(${COMPOSE_CMD} ps --filter "status=running" --format "{{.Name}}" | grep -c "^cedar-" || true)
        total=$(${COMPOSE_CMD} ps --format "{{.Name}}" | grep -c "^cedar-" || true)
        unhealthy=$(${COMPOSE_CMD} ps --format "{{.Name}}\t{{.Status}}" | awk '/^cedar-/ && $0 ~ /unhealthy/ {count++} END {print count+0}')
        core_running=$(${COMPOSE_CMD} ps --filter "status=running" --format "{{.Name}}" | grep -E -c "^cedar-(metad-[0-2]|storaged-[0-2]|graphd)$" || true)

        if [[ $core_running -ge 7 && $unhealthy -eq 0 ]]; then
            success "集群已就绪！"
            return 0
        fi
        
        echo "  等待中... (${waited}s/${max_wait}s, running=${running}/${total}, core=${core_running}/7, unhealthy=${unhealthy})"
        sleep 5
        waited=$((waited + 5))
    done
    
    warn "集群启动超时，请检查日志"
    return 1
}

# 初始化集群
init_cluster() {
    echo "🔧 初始化集群..."
    
    cd "${INSTALL_DIR}"
    
    # 等待 MetaD 就绪
    sleep 10
    
    if ! ${COMPOSE_CMD} exec -T graphd /usr/local/bin/cedar-admin --host=localhost --port=9669 status >/dev/null; then
        error "GraphD 状态检查失败"
        return 1
    fi

    if ! ${COMPOSE_CMD} exec -T graphd /usr/local/bin/cedar-admin --host=localhost --port=9669 auto-discover >/dev/null; then
        error "StorageD 自动发现或连通性检查失败"
        return 1
    fi

    success "集群初始化完成"
}

# 打印状态
print_status() {
    echo ""
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║              CedarGraph 集群部署成功！                     ║"
    echo "╚════════════════════════════════════════════════════════════╝"
    echo ""
    echo "📊 服务状态:"
    echo "----------------------------------------"
    docker ps --filter "name=cedar-" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    echo ""
    echo "🔗 连接信息:"
    echo "  Graph 服务:   localhost:9669"
    echo "  健康检查:     http://localhost:9668/health"
    echo "  指标服务:     http://localhost:9667/metrics"
    echo "  Meta Raft:    localhost:9559"
    echo "  Meta gRPC:    localhost:10559"
    echo ""
    echo "🛠️  常用命令:"
    echo "  查看日志:     cd ${INSTALL_DIR} && ${COMPOSE_CMD} logs -f"
    echo "  连接集群:     ${COMPOSE_CMD} exec console cedar-cli --host=graphd --port=9669"
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
    parse_args "$@"
    check_requirements
    
    mkdir -p "${INSTALL_DIR}"
    
    clean_data
    setup_directories
    download_compose
    pull_images
    start_services
    
    if wait_for_ready; then
        init_cluster || {
            error "部署初始化失败"
            echo "查看日志: cd ${INSTALL_DIR} && ${COMPOSE_CMD} logs"
            exit 1
        }
        print_status
    else
        error "部署可能出现问题，请检查日志"
        exit 1
    fi
}

# 执行主函数
main "$@"
