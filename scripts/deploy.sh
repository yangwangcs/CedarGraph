#!/bin/bash
# CedarGraph Production Deployment Script
# 版本: 1.0.0
# 日期: 2026-06-14

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查依赖
check_dependencies() {
    log_info "检查依赖..."
    
    if ! command -v docker &> /dev/null; then
        log_error "Docker 未安装"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose 未安装"
        exit 1
    fi
    
    log_info "依赖检查通过"
}

# 创建目录
create_directories() {
    log_info "创建数据目录..."
    
    mkdir -p data/{meta0,meta1,meta2,storage0,storage1,storage2,grafana,prometheus}
    mkdir -p logs/{meta0,meta1,meta2,storage0,storage1,storage2,graphd}
    mkdir -p backups
    
    log_info "目录创建完成"
}

# 检查配置文件
check_config() {
    log_info "检查配置文件..."
    
    if [ ! -f .env ]; then
        if [ -f .env.production ]; then
            log_warn ".env 文件不存在，从 .env.production 复制"
            cp .env.production .env
        else
            log_error ".env 文件不存在"
            exit 1
        fi
    fi
    
    log_info "配置文件检查通过"
}

# 启动集群
start_cluster() {
    log_info "启动 CedarGraph 集群..."
    
    docker-compose -f docker-compose.production.yml up -d
    
    log_info "等待服务启动..."
    sleep 30
    
    log_info "检查服务状态..."
    docker-compose -f docker-compose.production.yml ps
    
    log_info "CedarGraph 集群启动完成"
}

# 停止集群
stop_cluster() {
    log_info "停止 CedarGraph 集群..."
    
    docker-compose -f docker-compose.production.yml down
    
    log_info "CedarGraph 集群已停止"
}

# 重启集群
restart_cluster() {
    log_info "重启 CedarGraph 集群..."
    
    stop_cluster
    start_cluster
    
    log_info "CedarGraph 集群重启完成"
}

# 查看状态
show_status() {
    log_info "CedarGraph 集群状态:"
    
    docker-compose -f docker-compose.production.yml ps
    
    echo ""
    log_info "服务端点:"
    echo "  MetaD:     http://localhost:${META_PORT:-9559}"
    echo "  StorageD:  http://localhost:${STORAGE_PORT:-9779}"
    echo "  GraphD:    http://localhost:${GRAPH_PORT:-9669}"
    echo "  Prometheus: http://localhost:${PROMETHEUS_PORT:-9090}"
    echo "  Grafana:   http://localhost:${GRAFANA_PORT:-3000}"
}

# 查看日志
show_logs() {
    local service=${1:-""}
    
    if [ -z "$service" ]; then
        docker-compose -f docker-compose.production.yml logs -f
    else
        docker-compose -f docker-compose.production.yml logs -f "$service"
    fi
}

# 备份数据
backup_data() {
    local backup_name="backup_$(date +%Y%m%d_%H%M%S)"
    
    log_info "创建备份: $backup_name"
    
    docker exec cedar-metad-0 cedar-backup --type=full --output="/backups/$backup_name"
    
    log_info "备份完成: $backup_name"
}

# 恢复数据
restore_data() {
    local backup_name=$1
    
    if [ -z "$backup_name" ]; then
        log_error "请指定备份名称"
        exit 1
    fi
    
    log_info "恢复备份: $backup_name"
    
    docker exec cedar-metad-0 cedar-restore --input="/backups/$backup_name"
    
    log_info "恢复完成"
}

# 扩缩容
scale_service() {
    local service=$1
    local replicas=$2
    
    if [ -z "$service" ] || [ -z "$replicas" ]; then
        log_error "用法: $0 scale <service> <replicas>"
        exit 1
    fi
    
    log_info "扩缩容 $service 到 $replicas 个副本"
    
    docker-compose -f docker-compose.production.yml up -d --scale "$service=$replicas"
    
    log_info "扩缩容完成"
}

# 清理数据
cleanup() {
    log_warn "清理所有数据..."
    
    read -p "确定要清理所有数据吗? (y/N): " confirm
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
        stop_cluster
        rm -rf data/* logs/*
        log_info "数据清理完成"
    else
        log_info "取消清理"
    fi
}

# 显示帮助
show_help() {
    echo "CedarGraph 部署脚本"
    echo ""
    echo "用法: $0 <command> [options]"
    echo ""
    echo "命令:"
    echo "  start       启动集群"
    echo "  stop        停止集群"
    echo "  restart     重启集群"
    echo "  status      查看状态"
    echo "  logs        查看日志 [service]"
    echo "  backup      备份数据"
    echo "  restore     恢复数据 <backup_name>"
    echo "  scale       扩缩容 <service> <replicas>"
    echo "  cleanup     清理数据"
    echo "  help        显示帮助"
    echo ""
    echo "示例:"
    echo "  $0 start"
    echo "  $0 logs graphd"
    echo "  $0 scale storaged 5"
    echo "  $0 restore backup_20260614_120000"
}

# 主函数
main() {
    local command=${1:-"help"}
    
    check_dependencies
    check_config
    
    case $command in
        start)
            create_directories
            start_cluster
            ;;
        stop)
            stop_cluster
            ;;
        restart)
            restart_cluster
            ;;
        status)
            show_status
            ;;
        logs)
            show_logs "$2"
            ;;
        backup)
            backup_data
            ;;
        restore)
            restore_data "$2"
            ;;
        scale)
            scale_service "$2" "$3"
            ;;
        cleanup)
            cleanup
            ;;
        help|*)
            show_help
            ;;
    esac
}

main "$@"
