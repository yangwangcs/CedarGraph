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

compose() {
    if command -v docker-compose &> /dev/null; then
        docker-compose "$@"
    else
        docker compose "$@"
    fi
}

load_env_file() {
    local env_file="$1"
    local line key value

    while IFS= read -r line || [ -n "$line" ]; do
        case "$line" in
            ""|\#*)
                continue
                ;;
            export\ *)
                line="${line#export }"
                ;;
        esac

        key="${line%%=*}"
        value="${line#*=}"

        if [ "$key" = "$line" ]; then
            continue
        fi
        if ! [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
            log_error ".env 包含非法变量名: $key"
            exit 1
        fi

        if [[ "$value" =~ ^\".*\"$ ]] || [[ "$value" =~ ^\'.*\'$ ]]; then
            value="${value:1:${#value}-2}"
        fi
        if [ "${!key+x}" = "x" ]; then
            continue
        fi
        export "$key=$value"
    done < "$env_file"
}

# 检查依赖
check_dependencies() {
    log_info "检查依赖..."
    
    if ! command -v docker &> /dev/null; then
        log_error "Docker 未安装"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
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

    load_env_file ./.env

    if [ -z "${CEDAR_GRAPHD_AUTH_JWT_SECRET:-}" ]; then
        log_error "CEDAR_GRAPHD_AUTH_JWT_SECRET 未配置；生产 GraphD 必须启用认证"
        exit 1
    fi
    if [ "${#CEDAR_GRAPHD_AUTH_JWT_SECRET}" -lt 32 ]; then
        log_error "CEDAR_GRAPHD_AUTH_JWT_SECRET 至少需要 32 字节"
        exit 1
    fi
    if [ -z "${CEDAR_GRAPHD_AUTH_USER:-}" ] || [ -z "${CEDAR_GRAPHD_AUTH_PASSWORD:-}" ]; then
        log_error "CEDAR_GRAPHD_AUTH_USER 和 CEDAR_GRAPHD_AUTH_PASSWORD 必须配置"
        exit 1
    fi
    if [ -z "${GRAFANA_PASSWORD:-}" ]; then
        log_error "GRAFANA_PASSWORD 未配置；生产 Grafana 禁止使用默认管理员密码"
        exit 1
    fi
    if [ "${#GRAFANA_PASSWORD}" -lt 12 ]; then
        log_error "GRAFANA_PASSWORD 至少需要 12 字符"
        exit 1
    fi
    case "$GRAFANA_PASSWORD" in
        admin|password|cedar123|cedar-dev-grafana-password-change-me)
            log_error "GRAFANA_PASSWORD 不能使用默认或示例密码"
            exit 1
            ;;
    esac

    local tls_enabled="${CEDAR_GRPC_TLS_ENABLED:-1}"
    if [ "$tls_enabled" = "1" ] || [ "$tls_enabled" = "true" ]; then
        local tls_dir="${CEDAR_TLS_DIR:-./certs}"
        local cert_path host_path
        for cert_path in \
            "${CEDAR_GRPC_SERVER_CERT:-/etc/cedar/tls/tls.crt}" \
            "${CEDAR_GRPC_SERVER_KEY:-/etc/cedar/tls/tls.key}" \
            "${CEDAR_GRPC_CA_CERT:-/etc/cedar/tls/ca.crt}"; do
            host_path="${cert_path#/etc/cedar/tls/}"
            if [ "$host_path" = "$cert_path" ]; then
                log_warn "无法自动映射容器内证书路径 $cert_path，请确认 docker-compose.production.yml 中已挂载"
                continue
            fi
            if [ ! -f "$tls_dir/$host_path" ]; then
                log_error "TLS 文件缺失: $tls_dir/$host_path"
                exit 1
            fi
        done

        local mtls_enabled="${CEDAR_GRPC_MTLS_ENABLED:-0}"
        if [ "$mtls_enabled" = "1" ] || [ "$mtls_enabled" = "true" ]; then
            for cert_path in \
                "${CEDAR_GRPC_CLIENT_CERT:-/etc/cedar/tls/client.crt}" \
                "${CEDAR_GRPC_CLIENT_KEY:-/etc/cedar/tls/client.key}"; do
                host_path="${cert_path#/etc/cedar/tls/}"
                if [ "$host_path" = "$cert_path" ]; then
                    log_warn "无法自动映射容器内 mTLS 证书路径 $cert_path，请确认 docker-compose.production.yml 中已挂载"
                    continue
                fi
                if [ ! -f "$tls_dir/$host_path" ]; then
                    log_error "mTLS 文件缺失: $tls_dir/$host_path"
                    exit 1
                fi
            done
        fi
    fi
    
    log_info "配置文件检查通过"
}

# 启动集群
start_cluster() {
    log_info "启动 CedarGraph 集群..."
    
    compose -f docker-compose.production.yml up -d

    wait_for_compose_ready
    run_post_start_health_check
    
    log_info "CedarGraph 集群启动完成"
}

wait_for_compose_ready() {
    local max_wait="${CEDAR_DEPLOY_WAIT_SECONDS:-180}"
    local waited=0

    log_info "等待核心服务进入运行状态..."

    while [ "$waited" -lt "$max_wait" ]; do
        local ps_output running_core unhealthy exited
        if ! ps_output="$(compose -f docker-compose.production.yml ps --format '{{.Name}}\t{{.State}}\t{{.Status}}')"; then
            log_error "无法读取 Docker Compose 服务状态"
            return 1
        fi
        running_core="$(printf '%s\n' "$ps_output" | awk -F '\t' '/^cedar-(metad-[0-2]|storaged-[0-2]|graphd)$/ && $2 == "running" {count++} END {print count+0}')"
        unhealthy="$(printf '%s\n' "$ps_output" | awk -F '\t' '/^cedar-/ && $0 ~ /unhealthy/ {count++} END {print count+0}')"
        exited="$(printf '%s\n' "$ps_output" | awk -F '\t' '/^cedar-/ && $2 != "running" {count++} END {print count+0}')"

        if [ "$running_core" -ge 7 ] && [ "$unhealthy" -eq 0 ] && [ "$exited" -eq 0 ]; then
            log_info "核心服务已运行且无 unhealthy 容器"
            compose -f docker-compose.production.yml ps
            return 0
        fi

        sleep 5
        waited=$((waited + 5))
        log_info "等待中: core=${running_core}/7 unhealthy=${unhealthy} exited=${exited} (${waited}s/${max_wait}s)"
    done

    log_error "核心服务未在 ${max_wait}s 内达到健康运行状态"
    compose -f docker-compose.production.yml ps
    return 1
}

run_post_start_health_check() {
    log_info "运行启动后健康检查..."

    METAD_ENDPOINTS="127.0.0.1:${META_GRPC_PORT:-10559}" \
    STORAGED_ENDPOINTS="127.0.0.1:${STORAGE_PORT:-9779}" \
    STORAGED_HEALTH_ENDPOINTS="" \
    GRAPHD_ENDPOINTS="127.0.0.1:${GRAPH_PORT:-9669}" \
    GRAPHD_HEALTH_ENDPOINTS="127.0.0.1:${GRAPH_HEALTH_PORT:-9668}" \
    DATA_DIR="${DATA_DIR:-./data}" \
    ./scripts/deploy/health_check.sh --component all
}

# 停止集群
stop_cluster() {
    log_info "停止 CedarGraph 集群..."
    
    compose -f docker-compose.production.yml down
    
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
    
    compose -f docker-compose.production.yml ps
    
    echo ""
    log_info "服务端点:"
    echo "  MetaD Raft: http://localhost:${META_PORT:-9559}"
    echo "  MetaD gRPC: http://localhost:${META_GRPC_PORT:-10559}"
    echo "  StorageD:  http://localhost:${STORAGE_PORT:-9779}"
    echo "  GraphD:    http://localhost:${GRAPH_PORT:-9669}"
    echo "  Prometheus: http://localhost:${PROMETHEUS_PORT:-9090}"
    echo "  Grafana:   http://localhost:${GRAFANA_PORT:-3000}"
}

# 查看日志
show_logs() {
    local service=${1:-""}
    
    if [ -z "$service" ]; then
        compose -f docker-compose.production.yml logs -f
    else
        compose -f docker-compose.production.yml logs -f "$service"
    fi
}

# 备份数据
backup_data() {
    local backup_name="cedargraph-backup-$(date +%Y%m%d_%H%M%S)"
    local backup_dir="${BACKUP_DIR:-./backups}"
    local backup_path="${backup_dir}/${backup_name}.tar.gz"
    local data_dir="${DATA_DIR:-./data}"
    local log_dir="${LOG_DIR:-./logs}"
    
    log_info "创建备份: $backup_name"

    require_local_backup_layout "$data_dir" "$log_dir" || exit 1
    if [ ! -d "$data_dir" ] || [ ! -d "$log_dir" ]; then
        log_error "DATA_DIR 或 LOG_DIR 不存在，拒绝创建空备份"
        exit 1
    fi

    mkdir -p "$backup_dir"
    tar -czf "$backup_path" \
        --exclude='*/prometheus' \
        --exclude='*/grafana' \
        data logs

    log_info "备份完成: $backup_path"
}

require_local_backup_layout() {
    local data_dir="$1"
    local log_dir="$2"

    case "$data_dir" in
        data|./data) ;;
        *)
            log_error "当前 tar 备份/恢复只支持 DATA_DIR=./data 或 data"
            return 1
            ;;
    esac
    case "$log_dir" in
        logs|./logs) ;;
        *)
            log_error "当前 tar 备份/恢复只支持 LOG_DIR=./logs 或 logs"
            return 1
            ;;
    esac
}

validate_backup_archive() {
    local backup_path="$1"
    local list_file
    list_file="$(mktemp "${TMPDIR:-/tmp}/cedar-backup-list.XXXXXX")"

    if ! tar -tzf "$backup_path" > "$list_file"; then
        rm -f "$list_file"
        log_error "无法读取备份归档: $backup_path"
        return 1
    fi

    if [ ! -s "$list_file" ]; then
        rm -f "$list_file"
        log_error "备份归档为空: $backup_path"
        return 1
    fi

    while IFS= read -r entry || [ -n "$entry" ]; do
        case "$entry" in
            ""|/*|*"/../"*|../*|*/..|..)
                rm -f "$list_file"
                log_error "备份归档包含危险路径: $entry"
                return 1
                ;;
            data|data/*|./data|./data/*|logs|logs/*|./logs|./logs/*)
                ;;
            *)
                rm -f "$list_file"
                log_error "备份归档包含非预期路径: $entry"
                return 1
                ;;
        esac
    done < "$list_file"

    if tar -tvzf "$backup_path" | awk '$1 ~ /^[lh]/ { bad=1 } END { exit bad ? 0 : 1 }'; then
        rm -f "$list_file"
        log_error "备份归档包含符号链接或硬链接，拒绝恢复"
        return 1
    fi

    rm -f "$list_file"
}

# 恢复数据
restore_data() {
    local backup_name=$1
    local backup_dir="${BACKUP_DIR:-./backups}"
    local backup_path="$backup_name"
    local data_dir="${DATA_DIR:-./data}"
    local log_dir="${LOG_DIR:-./logs}"
    
    if [ -z "$backup_name" ]; then
        log_error "请指定备份名称"
        exit 1
    fi
    require_local_backup_layout "$data_dir" "$log_dir" || exit 1

    if [ ! -f "$backup_path" ]; then
        backup_path="${backup_dir}/${backup_name}"
    fi
    if [ ! -f "$backup_path" ] && [ -f "${backup_path}.tar.gz" ]; then
        backup_path="${backup_path}.tar.gz"
    fi
    if [ ! -f "$backup_path" ]; then
        log_error "备份文件不存在: $backup_name"
        exit 1
    fi
    
    log_info "恢复备份: $backup_path"
    validate_backup_archive "$backup_path"

    stop_cluster
    tar -xzf "$backup_path" -C .

    log_info "恢复完成，请执行 '$0 start' 启动集群"
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
    
    compose -f docker-compose.production.yml up -d --scale "$service=$replicas"
    
    log_info "扩缩容完成"
}

# 清理数据
cleanup() {
    log_warn "清理所有数据..."
    if [ "${CEDAR_DEPLOY_ALLOW_CLEANUP:-0}" != "1" ]; then
        log_error "拒绝清理：必须显式设置 CEDAR_DEPLOY_ALLOW_CLEANUP=1"
        exit 1
    fi

    local data_dir="${DATA_DIR:-./data}"
    local log_dir="${LOG_DIR:-./logs}"
    if ! require_local_backup_layout "$data_dir" "$log_dir" ||
       [ -z "$data_dir" ] || [ -z "$log_dir" ] ||
       [ "$data_dir" = "/" ] || [ "$log_dir" = "/" ]; then
        log_error "DATA_DIR/LOG_DIR 不安全，拒绝清理"
        exit 1
    fi
    
    read -p "确定要清理所有数据吗? (y/N): " confirm
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
        stop_cluster
        rm -rf "$data_dir"/* "$log_dir"/*
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
    echo "  CEDAR_DEPLOY_ALLOW_CLEANUP=1 $0 cleanup"
}

# 主函数
main() {
    local command=${1:-"help"}

    if [ "$command" = "help" ] || [ "$command" = "-h" ] || [ "$command" = "--help" ]; then
        show_help
        return 0
    fi
    
    check_dependencies
    
    case $command in
        start)
            check_config
            create_directories
            start_cluster
            ;;
        stop)
            check_config
            stop_cluster
            ;;
        restart)
            check_config
            restart_cluster
            ;;
        status)
            check_config
            show_status
            ;;
        logs)
            check_config
            show_logs "$2"
            ;;
        backup)
            check_config
            backup_data
            ;;
        restore)
            check_config
            restore_data "$2"
            ;;
        scale)
            check_config
            scale_service "$2" "$3"
            ;;
        cleanup)
            check_config
            cleanup
            ;;
        help|*)
            show_help
            ;;
    esac
}

main "$@"
