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
CEDAR_VERSION="latest"
INSTALL_DIR="./cedar-cluster"
DATA_DIR="${INSTALL_DIR}/data"
LOGS_DIR="${INSTALL_DIR}/logs"

# 打印帮助信息
usage() {
    cat << EOF
CedarGraph 一键部署脚本

用法: $0 [选项]

选项:
    -v, --version VERSION    指定 CedarGraph 版本 (默认: latest)
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
        echo -e "${RED}❌ Docker 未安装${NC}"
        echo "请安装 Docker: https://docs.docker.com/get-docker/"
        exit 1
    fi
    
    # 检查 Docker Compose
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        echo -e "${RED}❌ Docker Compose 未安装${NC}"
        echo "请安装 Docker Compose: https://docs.docker.com/compose/install/"
        exit 1
    fi
    
    # 检查 Docker 是否运行
    if ! docker info &> /dev/null; then
        echo -e "${RED}❌ Docker 服务未运行${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✅ 系统检查通过${NC}"
}

# 清理旧数据
clean_data() {
    if [[ "$CLEAN_DATA" == "true" ]]; then
        echo "🧹 清理旧数据..."
        rm -rf "${DATA_DIR}" "${LOGS_DIR}"
        echo -e "${GREEN}✅ 数据已清理${NC}"
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
    
    echo -e "${GREEN}✅ 目录创建完成${NC}"
}

# 下载 docker-compose.yml
download_compose() {
    echo "📥 下载部署配置..."
    
    local compose_url="https://raw.githubusercontent.com/cedargraph/cedar-docker-compose/main/docker-compose.yml"
    
    if [[ ! -f "${INSTALL_DIR}/docker-compose.yml" ]]; then
        if command -v curl &> /dev/null; then
            curl -fsSL "${compose_url}" -o "${INSTALL_DIR}/docker-compose.yml"
        elif command -v wget &> /dev/null; then
            wget -q "${compose_url}" -O "${INSTALL_DIR}/docker-compose.yml"
        else
            echo -e "${YELLOW}⚠️  无法下载配置文件，请手动放置 docker-compose.yml${NC}"
        fi
    fi
    
    echo -e "${GREEN}✅ 配置准备完成${NC}"
}

# 拉取镜像
pull_images() {
    echo "🐳 拉取 CedarGraph 镜像..."
    
    cd "${INSTALL_DIR}"
    
    export CEDAR_VERSION="${CEDAR_VERSION}"
    
    if command -v docker-compose &> /dev/null; then
        docker-compose pull
    else
        docker compose pull
    fi
    
    echo -e "${GREEN}✅ 镜像拉取完成${NC}"
}

# 启动服务
start_services() {
    echo "🚀 启动 CedarGraph 集群..."
    
    cd "${INSTALL_DIR}"
    
    local compose_args="-d"
    if [[ "$ENABLE_STUDIO" == "true" ]]; then
        compose_args="--profile studio ${compose_args}"
    fi
    
    if command -v docker-compose &> /dev/null; then
        docker-compose up ${compose_args}
    else
        docker compose up ${compose_args}
    fi
    
    echo -e "${GREEN}✅ 服务启动完成${NC}"
}

# 等待服务就绪
wait_for_ready() {
    echo "⏳ 等待集群就绪..."
    
    local max_wait=120
    local waited=0
    
    while [[ $waited -lt $max_wait ]]; do
        sleep 5
        waited=$((waited + 5))
        
        # 检查容器状态
        local running=$(docker ps --filter "name=cedar-" --format "{{.Names}}" | wc -l)
        
        if [[ $running -ge 7 ]]; then
            echo -e "${GREEN}✅ 集群已就绪！${NC}"
            return 0
        fi
        
        echo "  等待中... (${waited}s/${max_wait}s)"
    done
    
    echo -e "${YELLOW}⚠️  集群启动超时，请检查日志${NC}"
    return 1
}

# 初始化集群
init_cluster() {
    echo "🔧 初始化集群..."
    
    cd "${INSTALL_DIR}"
    
    # 等待 MetaD 就绪
    sleep 10
    
    # 自动注册存储节点
    if command -v docker-compose &> /dev/null; then
        docker-compose exec -T graphd /usr/local/bin/cedar-admin add-hosts storaged0:9779,storaged1:9779,storaged2:9779
    else
        docker compose exec -T graphd /usr/local/bin/cedar-admin add-hosts storaged0:9779,storaged1:9779,storaged2:9779
    fi
    
    echo -e "${GREEN}✅ 集群初始化完成${NC}"
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
    echo "  HTTP 服务:    http://localhost:19669"
    echo "  Meta 服务:    localhost:9559"
    echo ""
    echo "🛠️  常用命令:"
    echo "  查看日志:     cd ${INSTALL_DIR} && docker-compose logs -f"
    echo "  连接集群:     docker-compose exec console cedar-cli --host=graphd --port=9669"
    echo "  查看状态:     docker-compose exec graphd cedar-admin show-hosts"
    echo "  停止集群:     docker-compose down"
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
    echo "🌲 CedarGraph 一键部署脚本"
    echo ""
    
    parse_args "$@"
    check_requirements
    
    mkdir -p "${INSTALL_DIR}"
    
    clean_data
    setup_directories
    download_compose
    pull_images
    start_services
    
    if wait_for_ready; then
        init_cluster
        print_status
    else
        echo -e "${RED}❌ 部署可能出现问题，请检查日志${NC}"
        exit 1
    fi
}

# 执行主函数
main "$@"
