#!/bin/bash
# =============================================================================
# CedarGraph Health Check Script
# Usage: ./health_check.sh [OPTIONS]
# =============================================================================

set -e

# =============================================================================
# Configuration
# =============================================================================
VERSION="1.0.0"
CONFIG_DIR="${CONFIG_DIR:-/etc/cedar}"
LOG_DIR="${LOG_DIR:-/var/log/cedar}"
DATA_DIR="${DATA_DIR:-/var/lib/cedar}"

# Default ports
METAD_PORT="${METAD_PORT:-9559}"
STORAGED_PORT="${STORAGED_PORT:-9779}"
GRAPHD_PORT="${GRAPHD_PORT:-9669}"
METRICS_PORT="${METRICS_PORT:-9091}"

# Health check endpoints
METAD_ENDPOINTS="${METAD_ENDPOINTS:-127.0.0.1:9559}"
STORAGED_ENDPOINTS="${STORAGED_ENDPOINTS:-127.0.0.1:9779}"
GRAPHD_ENDPOINTS="${GRAPHD_ENDPOINTS:-127.0.0.1:9669}"

# Output format
OUTPUT_FORMAT="human"
CHECK_COMPONENT="all"
VERBOSE=false
TIMEOUT=5

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Check results (using temp files for bash 3.x compatibility)
RESULTS_DIR=$(mktemp -d)
trap "rm -rf $RESULTS_DIR" EXIT
OVERALL_STATUS="HEALTHY"

# Helper functions for result storage (bash 3.x compatible)
set_result() {
    local key="$1"
    local value="$2"
    echo "$value" > "${RESULTS_DIR}/${key}.status"
}

get_result() {
    local key="$1"
    if [[ -f "${RESULTS_DIR}/${key}.status" ]]; then
        cat "${RESULTS_DIR}/${key}.status"
    else
        echo ""
    fi
}

set_detail() {
    local key="$1"
    local value="$2"
    echo "$value" > "${RESULTS_DIR}/${key}.detail"
}

get_detail() {
    local key="$1"
    if [[ -f "${RESULTS_DIR}/${key}.detail" ]]; then
        cat "${RESULTS_DIR}/${key}.detail"
    else
        echo ""
    fi
}

get_all_keys() {
    ls "$RESULTS_DIR"/*.status 2>/dev/null | xargs -n1 basename | sed 's/\.status$//' || true
}

# =============================================================================
# Utility Functions
# =============================================================================

log() {
    if [[ "$VERBOSE" == true ]]; then
        echo -e "${BLUE}[DEBUG]${NC} $1" >&2
    fi
}

success() {
    echo -e "${GREEN}$1${NC}"
}

warn() {
    echo -e "${YELLOW}$1${NC}"
}

error() {
    echo -e "${RED}$1${NC}"
}

info() {
    echo -e "${CYAN}$1${NC}"
}

print_banner() {
    cat << 'EOF'
   ____          _            ____                 _      
  / ___|__ _  __| | __ _ _ __/ ___|_ __ __ _ _ __ | |__   
 | |   / _` |/ _` |/ _` | '__| |  _| '__/ _` | '_ \| '_ \  
 | |__| (_| | (_| | (_| | |  | |_| | | | (_| | |_) | | | | 
  \____\__,_|\__,_|\__,_|_|   \____|_|  \__, | .__/|_| |_| 
                                        |___/|_|           
EOF
    echo "Health Check v${VERSION}"
    echo "======================"
    echo ""
}

usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Options:
    -h, --help              Show this help message
    -j, --json              Output in JSON format
    -c, --component COMP    Check specific component (metad|storaged|graphd)
    -v, --verbose           Enable verbose output
    -t, --timeout SEC       Connection timeout in seconds (default: ${TIMEOUT})
    --version               Show version

Examples:
    $0                      # Human readable output
    $0 --json               # JSON output
    $0 --component storage  # Check only storage nodes
    $0 -v                   # Verbose mode
EOF
}

# =============================================================================
# Check Functions
# =============================================================================

check_port_open() {
    local host="$1"
    local port="$2"
    
    if timeout "$TIMEOUT" bash -c "</dev/tcp/${host}/${port}" 2>/dev/null; then
        return 0
    else
        return 1
    fi
}

check_http_endpoint() {
    local endpoint="$1"
    local path="${2:-/health}"
    
    if command -v curl >/dev/null 2>&1; then
        local response
        response=$(curl -s -o /dev/null -w "%{http_code}" --max-time "$TIMEOUT" "http://${endpoint}${path}" 2>/dev/null || echo "000")
        if [[ "$response" == "200" ]]; then
            return 0
        fi
    elif command -v wget >/dev/null 2>&1; then
        if wget -q -O - --timeout="$TIMEOUT" "http://${endpoint}${path}" >/dev/null 2>&1; then
            return 0
        fi
    fi
    return 1
}

check_service_running() {
    local service="$1"
    
    if systemctl is-active --quiet "$service" 2>/dev/null; then
        return 0
    fi
    return 1
}

check_process_running() {
    local process="$1"
    
    if pgrep -x "$process" >/dev/null 2>&1; then
        return 0
    fi
    return 1
}

check_disk_space() {
    local threshold=90
    local data_dir="${DATA_DIR}"
    
    if [[ -d "$data_dir" ]]; then
        local usage
        usage=$(df -h "$data_dir" | awk 'NR==2 {gsub(/%/,""); print $5}')
        if [[ "$usage" -ge "$threshold" ]]; then
            echo "DISK_FULL:${usage}%"
            return 1
        fi
        echo "OK:${usage}%"
        return 0
    fi
    echo "UNKNOWN"
    return 1
}

check_memory() {
    if [[ -f /proc/meminfo ]]; then
        local total used available usage
        total=$(grep MemTotal /proc/meminfo | awk '{print $2}')
        available=$(grep MemAvailable /proc/meminfo | awk '{print $2}')
        used=$((total - available))
        usage=$((used * 100 / total))
        
        if [[ "$usage" -ge 95 ]]; then
            echo "CRITICAL:${usage}%"
            return 1
        elif [[ "$usage" -ge 85 ]]; then
            echo "WARNING:${usage}%"
            return 0
        fi
        echo "OK:${usage}%"
        return 0
    fi
    
    # macOS fallback
    if command -v vm_stat >/dev/null 2>&1; then
        echo "OK:unknown"
        return 0
    fi
    
    echo "UNKNOWN"
    return 1
}

# =============================================================================
# Component Check Functions
# =============================================================================

check_metad_node() {
    local endpoint="$1"
    local host
    local port
    host=$(echo "$endpoint" | cut -d':' -f1)
    port=$(echo "$endpoint" | cut -d':' -f2)
    
    log "Checking metad at $endpoint..."
    
    # Check if port is open
    if ! check_port_open "$host" "$port"; then
        echo "UNHEALTHY:Port not accessible"
        return 1
    fi
    
    # Check HTTP health endpoint
    local role="unknown"
    if check_http_endpoint "$endpoint" "/status"; then
        # Try to get role from metrics or status
        if command -v curl >/dev/null 2>&1; then
            local status
            status=$(curl -s --max-time "$TIMEOUT" "http://${endpoint}/status" 2>/dev/null || echo "{}")
            # Extract role from JSON response (simple grep for now)
            if echo "$status" | grep -q "leader"; then
                role="leader"
            elif echo "$status" | grep -q "follower"; then
                role="follower"
            fi
        fi
        echo "HEALTHY:${role}"
        return 0
    fi
    
    # Fallback: check if process is running
    if check_process_running "metad"; then
        echo "HEALTHY:${role}"
        return 0
    fi
    
    echo "UNHEALTHY:Service not responding"
    return 1
}

check_storaged_node() {
    local endpoint="$1"
    local host
    local port
    host=$(echo "$endpoint" | cut -d':' -f1)
    port=$(echo "$endpoint" | cut -d':' -f2)
    
    log "Checking storaged at $endpoint..."
    
    # Check if port is open
    if ! check_port_open "$host" "$port"; then
        echo "UNHEALTHY:Port not accessible"
        return 1
    fi
    
    # Check HTTP health endpoint
    if check_http_endpoint "$endpoint" "/health"; then
        echo "HEALTHY:active"
        return 0
    fi
    
    # Fallback: check if process is running
    if check_process_running "storaged"; then
        echo "HEALTHY:active"
        return 0
    fi
    
    echo "UNHEALTHY:Service not responding"
    return 1
}

check_graphd_node() {
    local endpoint="$1"
    local host
    local port
    host=$(echo "$endpoint" | cut -d':' -f1)
    port=$(echo "$endpoint" | cut -d':' -f2)
    
    log "Checking graphd at $endpoint..."
    
    # Check if port is open
    if ! check_port_open "$host" "$port"; then
        echo "UNHEALTHY:Port not accessible"
        return 1
    fi
    
    # Check HTTP health endpoint
    if check_http_endpoint "$endpoint" "/health"; then
        echo "HEALTHY:ready"
        return 0
    fi
    
    # Fallback: check if process is running
    if check_process_running "graphd"; then
        echo "HEALTHY:ready"
        return 0
    fi
    
    echo "UNHEALTHY:Service not responding"
    return 1
}

# =============================================================================
# Check Orchestration
# =============================================================================

run_checks() {
    local component="$1"
    
    case "$component" in
        metad|meta)
            check_metad
            ;;
        storaged|storage)
            check_storaged
            ;;
        graphd|query)
            check_graphd
            ;;
        all)
            check_metad
            check_storaged
            check_graphd
            check_system
            ;;
        *)
            error "Unknown component: $component"
            exit 1
            ;;
    esac
}

check_metad() {
    log "Checking MetaD nodes..."
    
    IFS=',' read -ra ENDPOINTS <<< "$METAD_ENDPOINTS"
    local idx=0
    
    for endpoint in "${ENDPOINTS[@]}"; do
        local name="metad-${idx}"
        local result
        result=$(check_metad_node "$endpoint")
        
        if [[ "$result" == HEALTHY* ]]; then
            local role="${result#HEALTHY:}"
            set_result "$name" "HEALTHY"
            set_detail "$name" "${role}"
        else
            set_result "$name" "UNHEALTHY"
            set_detail "$name" "${result#UNHEALTHY:}"
            OVERALL_STATUS="UNHEALTHY"
        fi
        
        ((idx++))
    done
}

check_storaged() {
    log "Checking StorageD nodes..."
    
    IFS=',' read -ra ENDPOINTS <<< "$STORAGED_ENDPOINTS"
    local idx=0
    
    for endpoint in "${ENDPOINTS[@]}"; do
        local name="storaged-${idx}"
        local result
        result=$(check_storaged_node "$endpoint")
        
        if [[ "$result" == HEALTHY* ]]; then
            set_result "$name" "HEALTHY"
            set_detail "$name" "${result#HEALTHY:}"
        else
            set_result "$name" "UNHEALTHY"
            set_detail "$name" "${result#UNHEALTHY:}"
            OVERALL_STATUS="UNHEALTHY"
        fi
        
        ((idx++))
    done
}

check_graphd() {
    log "Checking GraphD nodes..."
    
    IFS=',' read -ra ENDPOINTS <<< "$GRAPHD_ENDPOINTS"
    local idx=0
    
    for endpoint in "${ENDPOINTS[@]}"; do
        local name="graphd-${idx}"
        local result
        result=$(check_graphd_node "$endpoint")
        
        if [[ "$result" == HEALTHY* ]]; then
            set_result "$name" "HEALTHY"
            set_detail "$name" "${result#HEALTHY:}"
        else
            set_result "$name" "UNHEALTHY"
            set_detail "$name" "${result#UNHEALTHY:}"
            OVERALL_STATUS="UNHEALTHY"
        fi
        
        ((idx++))
    done
}

check_system() {
    log "Checking system resources..."
    
    # Check disk space
    local disk_result
    disk_result=$(check_disk_space)
    set_result "disk" "${disk_result%%:*}"
    set_detail "disk" "${disk_result#*:}"
    
    if [[ "$(get_result "disk")" == "DISK_FULL" ]]; then
        OVERALL_STATUS="UNHEALTHY"
    fi
    
    # Check memory
    local mem_result
    mem_result=$(check_memory)
    set_result "memory" "${mem_result%%:*}"
    set_detail "memory" "${mem_result#*:}"
    
    if [[ "$(get_result "memory")" == "CRITICAL" ]]; then
        OVERALL_STATUS="UNHEALTHY"
    fi
}

# =============================================================================
# Output Functions
# =============================================================================

print_human_output() {
    echo "CedarGraph Health Check"
    echo "======================="
    echo ""
    
    # MetaD nodes
    local has_metad=false
    local all_keys=$(get_all_keys)
    for key in $all_keys; do
        if [[ "$key" == metad* ]]; then
            has_metad=true
            break
        fi
    done
    
    if [[ "$has_metad" == true ]]; then
        info "Meta Service (metad)"
        for key in $(get_all_keys | grep "^metad" | sort); do
            local status=$(get_result "$key")
            local detail=$(get_detail "$key")
            
            if [[ "$status" == "HEALTHY" ]]; then
                printf "  %-12s ✅ HEALTHY (%s)\n" "$key:" "$detail"
            else
                printf "  %-12s ❌ UNHEALTHY (%s)\n" "$key:" "$detail"
            fi
        done
        echo ""
    fi
    
    # StorageD nodes
    local has_storaged=false
    for key in $(get_all_keys); do
        if [[ "$key" == storaged* ]]; then
            has_storaged=true
            break
        fi
    done
    
    if [[ "$has_storaged" == true ]]; then
        info "Storage Service (storaged)"
        for key in $(get_all_keys | grep "^storaged" | sort); do
            local status=$(get_result "$key")
            local detail=$(get_detail "$key")
            
            if [[ "$status" == "HEALTHY" ]]; then
                printf "  %-12s ✅ HEALTHY\n" "$key:"
            else
                printf "  %-12s ❌ UNHEALTHY (%s)\n" "$key:" "$detail"
            fi
        done
        echo ""
    fi
    
    # GraphD nodes
    local has_graphd=false
    for key in $(get_all_keys); do
        if [[ "$key" == graphd* ]]; then
            has_graphd=true
            break
        fi
    done
    
    if [[ "$has_graphd" == true ]]; then
        info "Query Service (graphd)"
        for key in $(get_all_keys | grep "^graphd" | sort); do
            local status=$(get_result "$key")
            local detail=$(get_detail "$key")
            
            if [[ "$status" == "HEALTHY" ]]; then
                printf "  %-12s ✅ HEALTHY\n" "$key:"
            else
                printf "  %-12s ❌ UNHEALTHY (%s)\n" "$key:" "$detail"
            fi
        done
        echo ""
    fi
    
    # System resources
    if [[ -n "$(get_result "disk")" ]]; then
        info "System Resources"
        local disk_status=$(get_result "disk")
        local disk_detail=$(get_detail "disk")
        
        if [[ "$disk_status" == "OK" ]]; then
            printf "  %-12s ✅ OK (%s)\n" "Disk:" "$disk_detail"
        elif [[ "$disk_status" == "WARNING" ]]; then
            printf "  %-12s ⚠️  WARNING (%s)\n" "Disk:" "$disk_detail"
        else
            printf "  %-12s ❌ CRITICAL (%s)\n" "Disk:" "$disk_detail"
        fi
        
        local mem_status=$(get_result "memory")
        local mem_detail=$(get_detail "memory")
        
        if [[ "$mem_status" == "OK" ]]; then
            printf "  %-12s ✅ OK (%s)\n" "Memory:" "$mem_detail"
        elif [[ "$mem_status" == "WARNING" ]]; then
            printf "  %-12s ⚠️  WARNING (%s)\n" "Memory:" "$mem_detail"
        else
            printf "  %-12s ❌ CRITICAL (%s)\n" "Memory:" "$mem_detail"
        fi
        echo ""
    fi
    
    # Overall status
    echo "------------------------------"
    if [[ "$OVERALL_STATUS" == "HEALTHY" ]]; then
        success "Overall: ✅ HEALTHY"
    else
        error "Overall: ❌ UNHEALTHY"
    fi
}

print_json_output() {
    local timestamp
    timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    
    echo "{"
    echo "  \"timestamp\": \"$timestamp\","
    echo "  \"version\": \"$VERSION\","
    echo "  \"overall_status\": \"$OVERALL_STATUS\","
    echo "  \"checks\": {"
    
    local first=true
    for key in $(get_all_keys); do
        if [[ "$first" == true ]]; then
            first=false
        else
            echo ","
        fi
        
        local status=$(get_result "$key")
        local detail=$(get_detail "$key")
        
        echo -n "    \"$key\": {"
        echo -n "\"status\": \"$status\", "
        echo -n "\"detail\": \"$detail\""
        echo -n "}"
    done
    
    echo ""
    echo "  }"
    echo "}"
}

# =============================================================================
# Main
# =============================================================================

main() {
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                usage
                exit 0
                ;;
            -j|--json)
                OUTPUT_FORMAT="json"
                shift
                ;;
            -c|--component)
                CHECK_COMPONENT="$2"
                shift 2
                ;;
            -v|--verbose)
                VERBOSE=true
                shift
                ;;
            -t|--timeout)
                TIMEOUT="$2"
                shift 2
                ;;
            --version)
                echo "CedarGraph Health Check v${VERSION}"
                exit 0
                ;;
            -*)
                error "Unknown option: $1"
                usage
                exit 1
                ;;
            *)
                error "Unknown argument: $1"
                usage
                exit 1
                ;;
        esac
    done
    
    if [[ "$OUTPUT_FORMAT" == "human" ]]; then
        print_banner
    fi
    
    # Run checks
    run_checks "$CHECK_COMPONENT"
    
    # Output results
    if [[ "$OUTPUT_FORMAT" == "json" ]]; then
        print_json_output
    else
        print_human_output
    fi
    
    # Exit with appropriate code
    if [[ "$OVERALL_STATUS" == "HEALTHY" ]]; then
        exit 0
    else
        exit 1
    fi
}

main "$@"
