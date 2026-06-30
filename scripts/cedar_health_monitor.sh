#!/bin/bash
# =============================================================================
# CedarGraph Health Monitor Script
# 健康监控脚本 - 自动检测和报告节点健康状态
# =============================================================================

set -e

# Configuration
CONFIG_FILE="${1:-/etc/cedar/storaged.conf}"
LOG_FILE="/var/log/cedar/health_monitor.log"
ALERT_WEBHOOK="${ALERT_WEBHOOK:-}"  # Optional: Slack/Discord webhook
NODE_ID=$(hostname)
STORAGED_SERVICE_NAME="${STORAGED_SERVICE_NAME:-cedar-storaged}"
STORAGED_PROCESS_NAME="${STORAGED_PROCESS_NAME:-cedar-storaged}"

# Thresholds (percentages of FREE space)
DISK_WARNING_THRESHOLD=20    # Warn when <=20% free
DISK_CRITICAL_THRESHOLD=10   # Critical when <=10% free
MEMORY_WARNING_THRESHOLD=80  # %
MEMORY_CRITICAL_THRESHOLD=95 # %
CPU_WARNING_THRESHOLD=80     # %
CPU_CRITICAL_THRESHOLD=95    # %

# Colors for output
RED='\033[0;31m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# =============================================================================
# Utility Functions
# =============================================================================

log() {
    local level="$1"
    local message="$2"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] [$level] $message" | tee -a "$LOG_FILE"
}

alert() {
    local severity="$1"
    local message="$2"
    
    log "ALERT-$severity" "$message"
    
    # Send webhook alert if configured
    if [ -n "$ALERT_WEBHOOK" ]; then
        curl -s -X POST -H 'Content-type: application/json' \
            --data "{\"text\":\"[CedarGraph $NODE_ID] $severity: $message\"}" \
            "$ALERT_WEBHOOK" > /dev/null 2>&1 || true
    fi
}

# =============================================================================
# Health Check Functions
# =============================================================================

config_value() {
    local key="$1"
    local legacy_key="${2:-$1}"

    if [ ! -f "$CONFIG_FILE" ]; then
        return 1
    fi

    local value
    value=$(awk -v section="storaged" -v key="$key" -v legacy_key="$legacy_key" '
        function trim(s) {
            gsub(/^[ \t]+|[ \t]+$/, "", s)
            gsub(/^["'\'']|["'\'']$/, "", s)
            return s
        }
        /^[[:space:]]*#/ || /^[[:space:]]*$/ { next }
        $0 ~ "^[[:space:]]*" legacy_key "=" {
            split($0, parts, "=")
            print trim(parts[2])
            found=1
            exit
        }
        /^[^[:space:]][^:]*:[[:space:]]*$/ {
            current=$0
            sub(/:.*/, "", current)
            current=trim(current)
            next
        }
        current == section && $0 ~ "^[[:space:]]*" key ":" {
            sub(/^[[:space:]]*/, "", $0)
            sub("^[^:]+:[[:space:]]*", "", $0)
            print trim($0)
            found=1
            exit
        }
    ' "$CONFIG_FILE")

    if [ -n "$value" ]; then
        echo "$value"
        return 0
    fi
    return 1
}

check_tcp_endpoint() {
    local endpoint="$1"
    local host="${endpoint%:*}"
    local port="${endpoint##*:}"

    if [ -z "$host" ] || [ -z "$port" ] || [ "$host" = "$port" ]; then
        return 1
    fi

    if command -v nc >/dev/null 2>&1; then
        nc -z -w 2 "$host" "$port" >/dev/null 2>&1
        return $?
    fi

    timeout 2 bash -c "exec 3<>/dev/tcp/${host}/${port}" >/dev/null 2>&1
}

check_disk_space() {
    local data_dir
    data_dir=$(config_value "data_dir" "data_dir" || echo "/tmp/cedar/storage")

    if [ ! -d "$data_dir" ]; then
        alert "WARNING" "Disk check skipped: data_dir does not exist: $data_dir"
        return 0
    fi

    local usage
    usage=$(df -h "$data_dir" 2>/dev/null | awk 'NR==2 {print $5}' | sed 's/%//')
    if [ -z "$usage" ]; then
        alert "WARNING" "Disk check skipped: unable to read usage for $data_dir"
        return 0
    fi
    local free=$((100 - usage))

    if [ "$free" -le "$DISK_CRITICAL_THRESHOLD" ]; then
        alert "CRITICAL" "Disk free space critical: ${free}% on $data_dir"
        return 1
    elif [ "$free" -le "$DISK_WARNING_THRESHOLD" ]; then
        alert "WARNING" "Disk free space low: ${free}% on $data_dir"
        return 0
    fi

    log "INFO" "Disk free space OK: ${free}%"
    return 0
}

check_memory() {
    if ! command -v free >/dev/null 2>&1; then
        alert "WARNING" "Memory check skipped: free command not available"
        return 0
    fi

    local mem_info=$(free | grep Mem)
    local total=$(echo $mem_info | awk '{print $2}')
    local used=$(echo $mem_info | awk '{print $3}')
    local usage=$((used * 100 / total))
    
    if [ "$usage" -ge "$MEMORY_CRITICAL_THRESHOLD" ]; then
        alert "CRITICAL" "Memory usage critical: ${usage}%"
        return 1
    elif [ "$usage" -ge "$MEMORY_WARNING_THRESHOLD" ]; then
        alert "WARNING" "Memory usage high: ${usage}%"
        return 0
    fi
    
    log "INFO" "Memory usage OK: ${usage}%"
    return 0
}

check_cpu() {
    if ! command -v top >/dev/null 2>&1; then
        alert "WARNING" "CPU check skipped: top command not available"
        return 0
    fi

    local cpu_usage=$(top -bn1 | grep "Cpu(s)" | awk '{print $2}' | cut -d'%' -f1)
    if [ -z "$cpu_usage" ]; then
        alert "WARNING" "CPU check skipped: unsupported top output"
        return 0
    fi
    local usage_int=${cpu_usage%.*}
    
    if [ "$usage_int" -ge "$CPU_CRITICAL_THRESHOLD" ]; then
        alert "CRITICAL" "CPU usage critical: ${cpu_usage}%"
        return 1
    elif [ "$usage_int" -ge "$CPU_WARNING_THRESHOLD" ]; then
        alert "WARNING" "CPU usage high: ${cpu_usage}%"
        return 0
    fi
    
    log "INFO" "CPU usage OK: ${cpu_usage}%"
    return 0
}

check_service_status() {
    if systemctl is-active --quiet "$STORAGED_SERVICE_NAME"; then
        log "INFO" "$STORAGED_SERVICE_NAME service is running"
        return 0
    else
        alert "CRITICAL" "$STORAGED_SERVICE_NAME service is not running!"
        return 1
    fi
}

check_network_connectivity() {
    local peer_addrs
    peer_addrs=$(config_value "meta_server" "peer_addresses" || true)
    local failed=0
    
    if [ -z "$peer_addrs" ]; then
        log "INFO" "No MetaD or peer addresses configured"
        return 0
    fi
    
    IFS=',' read -ra ADDRS <<< "$peer_addrs"
    for addr in "${ADDRS[@]}"; do
        addr="$(echo "$addr" | tr -d ' ')"
        if ! check_tcp_endpoint "$addr"; then
            alert "WARNING" "MetaD/peer endpoint $addr is unreachable"
            failed=1
        fi
    done
    
    if [ $failed -eq 0 ]; then
        log "INFO" "All MetaD/peer endpoints are reachable"
    fi
    
    return $failed
}

check_log_errors() {
    local error_count=$(journalctl -u "$STORAGED_SERVICE_NAME" --since "5 minutes ago" | grep -c "ERROR\|FATAL" || true)
    
    if [ "$error_count" -gt 10 ]; then
        alert "WARNING" "High error rate detected: $error_count errors in last 5 minutes"
        return 1
    fi
    
    log "INFO" "Log error count OK: $error_count"
    return 0
}

# =============================================================================
# Recovery Actions
# =============================================================================

trigger_recovery() {
    local failure_type="$1"
    log "INFO" "Triggering recovery for: $failure_type"
    
    # Signal StorageD to trigger recovery when the process exists.
    pkill -USR1 -x "$STORAGED_PROCESS_NAME" 2>/dev/null || true
}

cleanup_old_logs() {
    log "INFO" "Cleaning up old logs..."
    find /var/log/cedar -name "*.log.*" -mtime +7 -delete 2>/dev/null || true
}

restart_service() {
    log "INFO" "Restarting $STORAGED_SERVICE_NAME service..."
    systemctl restart "$STORAGED_SERVICE_NAME"
}

# =============================================================================
# Main
# =============================================================================

main() {
    echo -e "${GREEN}CedarGraph Health Monitor${NC}"
    echo "========================="
    echo ""
    
    mkdir -p "$(dirname "$LOG_FILE")"
    
    local overall_status=0
    
    # Run all checks
    echo "Running health checks..."
    
    echo -n "  Disk space... "
    if check_disk_space; then
        echo -e "${GREEN}OK${NC}"
    else
        echo -e "${RED}FAILED${NC}"
        trigger_recovery "disk_full"
        cleanup_old_logs
        overall_status=1
    fi
    
    echo -n "  Memory... "
    if check_memory; then
        echo -e "${GREEN}OK${NC}"
    else
        echo -e "${RED}FAILED${NC}"
        trigger_recovery "memory_exhaustion"
        overall_status=1
    fi
    
    echo -n "  CPU... "
    if check_cpu; then
        echo -e "${GREEN}OK${NC}"
    else
        echo -e "${RED}FAILED${NC}"
        overall_status=1
    fi
    
    echo -n "  Service status... "
    if check_service_status; then
        echo -e "${GREEN}OK${NC}"
    else
        echo -e "${RED}FAILED${NC}"
        restart_service
        overall_status=1
    fi
    
    echo -n "  Network connectivity... "
    if check_network_connectivity; then
        echo -e "${GREEN}OK${NC}"
    else
        echo -e "${YELLOW}WARNING${NC}"
        trigger_recovery "network_partition"
    fi
    
    echo -n "  Log errors... "
    if check_log_errors; then
        echo -e "${GREEN}OK${NC}"
    else
        echo -e "${YELLOW}WARNING${NC}"
    fi
    
    echo ""
    if [ $overall_status -eq 0 ]; then
        echo -e "${GREEN}All critical checks passed!${NC}"
    else
        echo -e "${RED}Some checks failed. Recovery actions triggered.${NC}"
    fi
    
    return $overall_status
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    # Run in daemon mode if requested
    if [ "${1:-}" == "--daemon" ]; then
        INTERVAL="${2:-30}"
        log "INFO" "Health monitor daemon started (interval: ${INTERVAL}s)"
        while true; do
            main > /dev/null 2>&1 || true
            sleep "$INTERVAL"
        done
    else
        main
    fi
fi
