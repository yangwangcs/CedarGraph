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

check_disk_space() {
    local data_dir=$(grep "^data_dir=" "$CONFIG_FILE" 2>/dev/null | cut -d'=' -f2 || echo "/tmp/cedar/storage")
    local usage=$(df -h "$data_dir" | awk 'NR==2 {print $5}' | sed 's/%//')
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
    local cpu_usage=$(top -bn1 | grep "Cpu(s)" | awk '{print $2}' | cut -d'%' -f1)
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
    if systemctl is-active --quiet storaged; then
        log "INFO" "storaged service is running"
        return 0
    else
        alert "CRITICAL" "storaged service is not running!"
        return 1
    fi
}

check_network_connectivity() {
    local peer_addrs=$(grep "^peer_addresses=" "$CONFIG_FILE" 2>/dev/null | cut -d'=' -f2)
    local failed=0
    
    if [ -z "$peer_addrs" ]; then
        log "INFO" "No peer addresses configured"
        return 0
    fi
    
    IFS=',' read -ra ADDRS <<< "$peer_addrs"
    for addr in "${ADDRS[@]}"; do
        # Extract IP from address (remove port)
        local ip=$(echo "$addr" | cut -d':' -f1 | tr -d ' ')
        if ! ping -c 1 -W 2 "$ip" > /dev/null 2>&1; then
            alert "WARNING" "Peer $addr is unreachable"
            failed=1
        fi
    done
    
    if [ $failed -eq 0 ]; then
        log "INFO" "All peers are reachable"
    fi
    
    return $failed
}

check_log_errors() {
    local error_count=$(journalctl -u storaged --since "5 minutes ago" | grep -c "ERROR\|FATAL" || true)
    
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
    
    # Signal storaged to trigger recovery
    killall -USR1 storaged 2>/dev/null || true
}

cleanup_old_logs() {
    log "INFO" "Cleaning up old logs..."
    find /var/log/cedar -name "*.log.*" -mtime +7 -delete 2>/dev/null || true
}

restart_service() {
    log "INFO" "Restarting storaged service..."
    systemctl restart storaged
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

# Run in daemon mode if requested
if [ "$1" == "--daemon" ]; then
    INTERVAL="${2:-30}"
    log "INFO" "Health monitor daemon started (interval: ${INTERVAL}s)"
    while true; do
        main > /dev/null 2>&1 || true
        sleep "$INTERVAL"
    done
else
    main
fi
