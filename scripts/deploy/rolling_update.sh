#!/bin/bash
# =============================================================================
# CedarGraph Rolling Update Script
# Performs zero-downtime updates of CedarGraph cluster
# Usage: ./rolling_update.sh --version v0.2.0
# =============================================================================

set -e

# =============================================================================
# Configuration
# =============================================================================
VERSION="1.0.0"
GITHUB_REPO="cedargraph/cedargraph"
INSTALL_DIR="/opt/cedar"
CONFIG_DIR="/etc/cedar"
DATA_DIR="/var/lib/cedar"
LOG_DIR="/var/log/cedar"
BACKUP_DIR="/opt/cedar/backups"
BIN_DIR="/usr/local/bin"
HEALTH_CHECK_TIMEOUT=300  # seconds
HEALTH_CHECK_INTERVAL=5   # seconds
DRAIN_TIMEOUT=60          # seconds

# Update settings
TARGET_VERSION=""
TARGET_NODE=""
DRY_RUN=false
FORCE=false
SKIP_BACKUP=false
ROLLBACK_ON_FAILURE=true
PARALLEL=false
MAX_PARALLEL=1

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Update tracking (using temp files for bash 3.x compatibility)
STATUS_DIR=$(mktemp -d)
trap "rm -rf $STATUS_DIR" EXIT
UPDATE_FAILED=false
FAILED_NODES=""

# Helper functions for status storage (bash 3.x compatible)
set_update_status() {
    local key="$1"
    local value="$2"
    echo "$value" > "${STATUS_DIR}/${key}.status"
}

get_update_status() {
    local key="$1"
    if [[ -f "${STATUS_DIR}/${key}.status" ]]; then
        cat "${STATUS_DIR}/${key}.status"
    else
        echo ""
    fi
}

# =============================================================================
# Utility Functions
# =============================================================================

log() {
    echo -e "${BLUE}[$(date '+%H:%M:%S')] INFO${NC} $1"
}

success() {
    echo -e "${GREEN}[$(date '+%H:%M:%S')] SUCCESS${NC} $1"
}

warn() {
    echo -e "${YELLOW}[$(date '+%H:%M:%S')] WARNING${NC} $1"
}

error() {
    echo -e "${RED}[$(date '+%H:%M:%S')] ERROR${NC} $1" >&2
}

step() {
    echo -e "${CYAN}[STEP $1]${NC} $2"
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
    echo "Rolling Update v${VERSION}"
    echo "========================="
    echo ""
}

usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Required:
    -v, --version VERSION   Target version to update to

Options:
    -h, --help              Show this help message
    -n, --node NODE         Update only specific node (e.g., storaged-0)
    -d, --dry-run           Show what would be done without executing
    -f, --force             Force update even if health checks fail
    --skip-backup           Skip backup step (not recommended)
    --no-rollback           Don't rollback on failure
    -p, --parallel NUM      Update NUM nodes in parallel (default: 1)
    --timeout SEC           Health check timeout in seconds (default: ${HEALTH_CHECK_TIMEOUT})

Examples:
    $0 --version v0.2.0                     # Update all nodes
    $0 --version v0.2.0 --node storaged-0   # Update single node
    $0 --version v0.2.0 --dry-run           # Dry run
    $0 --version v0.2.0 --parallel 2        # Update 2 nodes in parallel
EOF
}

# =============================================================================
# Pre-flight Checks
# =============================================================================

preflight_checks() {
    log "Running pre-flight checks..."
    
    # Check if running as root
    if [[ $EUID -ne 0 ]]; then
        error "This script must be run as root (use sudo)"
        exit 1
    fi
    
    # Check required commands
    for cmd in curl tar systemctl; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            error "Required command not found: $cmd"
            exit 1
        fi
    done
    
    # Validate version format
    if [[ ! "$TARGET_VERSION" =~ ^v?[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        error "Invalid version format: $TARGET_VERSION (expected: v0.1.0 or 0.1.0)"
        exit 1
    fi
    
    # Remove 'v' prefix for consistency
    TARGET_VERSION="${TARGET_VERSION#v}"
    
    if $DRY_RUN; then
        warn "DRY RUN MODE - No changes will be made"
    fi
    
    success "Pre-flight checks passed"
}

# =============================================================================
# Node Discovery
# =============================================================================

discover_nodes() {
    log "Discovering CedarGraph nodes..."
    
    NODES=()
    
    if [[ -n "$TARGET_NODE" ]]; then
        # Single node update
        NODES+=("$TARGET_NODE")
        log "Targeting single node: $TARGET_NODE"
    else
        # Discover all nodes from systemd services
        for service in cedar-metad cedar-storaged cedar-graphd; do
            if systemctl list-unit-files | grep -q "^${service}"; then
                NODES+=("$service")
            fi
        done
        
        # Also check for multiple instances
        for service in $(systemctl list-unit-files --type=service | grep -E "^cedar-(metad|storaged|graphd)" | awk '{print $1}' | sed 's/.service$//'); do
            if [[ ! " ${NODES[@]} " =~ " ${service} " ]]; then
                NODES+=("$service")
            fi
        done
        
        if [[ ${#NODES[@]} -eq 0 ]]; then
            error "No CedarGraph services found"
            exit 1
        fi
        
        log "Discovered ${#NODES[@]} nodes: ${NODES[*]}"
    fi
}

# =============================================================================
# Backup Functions
# =============================================================================

create_backup() {
    if $SKIP_BACKUP; then
        warn "Skipping backup (not recommended)"
        return 0
    fi
    
    log "Creating backup..."
    
    if $DRY_RUN; then
        log "[DRY RUN] Would create backup"
        return 0
    fi
    
    local backup_timestamp
    backup_timestamp=$(date +"%Y%m%d_%H%M%S")
    local backup_path="${BACKUP_DIR}/pre_update_${backup_timestamp}"
    
    mkdir -p "$backup_path"
    
    # Backup binaries
    log "  Backing up binaries..."
    for bin in metad storaged graphd; do
        if [[ -f "${BIN_DIR}/${bin}" ]]; then
            cp "${BIN_DIR}/${bin}" "${backup_path}/"
        fi
    done
    
    # Backup configurations
    log "  Backing up configurations..."
    cp -r "$CONFIG_DIR" "${backup_path}/config"
    
    # Create backup manifest
    cat > "${backup_path}/manifest.txt" << EOF
Backup created: $(date)
Version: $(cat "${INSTALL_DIR}/version" 2>/dev/null || echo "unknown")
Nodes: ${NODES[*]}
EOF
    
    # Store current version
    echo "${backup_path}" > "${BACKUP_DIR}/latest_backup.txt"
    
    success "Backup created at: $backup_path"
}

restore_backup() {
    local backup_path="$1"
    
    log "Restoring backup from: $backup_path"
    
    # Restore binaries
    for bin in metad storaged graphd; do
        if [[ -f "${backup_path}/${bin}" ]]; then
            cp "${backup_path}/${bin}" "${BIN_DIR}/"
            chmod +x "${BIN_DIR}/${bin}"
        fi
    done
    
    # Restore configurations
    if [[ -d "${backup_path}/config" ]]; then
        rm -rf "$CONFIG_DIR"
        cp -r "${backup_path}/config" "$CONFIG_DIR"
    fi
    
    success "Backup restored"
}

# =============================================================================
# Download and Install
# =============================================================================

download_version() {
    local version="$1"
    
    log "Downloading CedarGraph v${version}..."
    
    if $DRY_RUN; then
        log "[DRY RUN] Would download version ${version}"
        return 0
    fi
    
    local os arch
    os=$(uname -s | tr '[:upper:]' '[:lower:]')
    arch=$(uname -m)
    case "$arch" in
        x86_64) arch="amd64" ;;
        aarch64) arch="arm64" ;;
    esac
    
    local download_url="https://github.com/${GITHUB_REPO}/releases/download/v${version}/cedargraph_${os}_${arch}.tar.gz"
    local temp_dir=$(mktemp -d)
    local archive="${temp_dir}/cedargraph.tar.gz"
    
    log "  Download URL: $download_url"
    
    if ! curl -fsSL -o "$archive" "$download_url"; then
        rm -rf "$temp_dir"
        error "Failed to download CedarGraph v${version}"
        return 1
    fi
    
    log "  Extracting archive..."
    if ! tar -xzf "$archive" -C "$temp_dir"; then
        rm -rf "$temp_dir"
        error "Failed to extract archive"
        return 1
    fi
    
    # Store download path for later use
    echo "$temp_dir" > /tmp/cedar_update_download.txt
    
    success "Downloaded CedarGraph v${version}"
}

install_binaries() {
    local temp_dir="$1"
    
    log "Installing new binaries..."
    
    if $DRY_RUN; then
        log "[DRY RUN] Would install binaries"
        return 0
    fi
    
    for bin in metad storaged graphd; do
        if [[ -f "${temp_dir}/${bin}" ]]; then
            cp "${temp_dir}/${bin}" "${BIN_DIR}/"
            chmod +x "${BIN_DIR}/${bin}"
            log "  Installed: $bin"
        fi
    done
    
    # Record installed version
    echo "$TARGET_VERSION" > "${INSTALL_DIR}/version"
    
    success "Binaries installed"
}

# =============================================================================
# Health Check Functions
# =============================================================================

wait_for_healthy() {
    local service="$1"
    local max_wait="${2:-$HEALTH_CHECK_TIMEOUT}"
    local interval="${3:-$HEALTH_CHECK_INTERVAL}"
    
    log "Waiting for $service to be healthy (max ${max_wait}s)..."
    
    local elapsed=0
    while [[ $elapsed -lt $max_wait ]]; do
        if systemctl is-active --quiet "$service" 2>/dev/null; then
            # Additional health check via HTTP if available
            local port
            case "$service" in
                *metad*) port=9559 ;;
                *storaged*) port=9779 ;;
                *graphd*) port=9669 ;;
                *) port="" ;;
            esac
            
            if [[ -n "$port" ]]; then
                if timeout 2 bash -c "</dev/tcp/127.0.0.1/${port}" 2>/dev/null; then
                    success "$service is healthy"
                    return 0
                fi
            else
                success "$service is active"
                return 0
            fi
        fi
        
        sleep "$interval"
        elapsed=$((elapsed + interval))
        log "  Still waiting... (${elapsed}s elapsed)"
    done
    
    error "$service failed to become healthy within ${max_wait}s"
    return 1
}

# =============================================================================
# Node Update Functions
# =============================================================================

update_node() {
    local service="$1"
    local node_name="${service#cedar-}"
    
    step "1" "Preparing to update $node_name"
    
    # Check current status
    if ! systemctl is-active --quiet "$service" 2>/dev/null; then
        warn "$service is not running, will start after update"
    fi
    
    step "2" "Draining $node_name"
    drain_node "$service"
    
    step "3" "Stopping $node_name"
    stop_node "$service"
    
    step "4" "Installing new version"
    local temp_dir
    temp_dir=$(cat /tmp/cedar_update_download.txt 2>/dev/null || echo "")
    if [[ -n "$temp_dir" && -d "$temp_dir" ]]; then
        install_binaries "$temp_dir"
    fi
    
    step "5" "Starting $node_name"
    start_node "$service"
    
    step "6" "Waiting for $node_name to be healthy"
    if wait_for_healthy "$service"; then
        set_update_status "$service" "SUCCESS"
        success "Updated $node_name successfully"
    else
        set_update_status "$service" "FAILED"
        UPDATE_FAILED=true
        FAILED_NODES="${FAILED_NODES} ${service}"
        error "Failed to update $node_name"
        
        if $ROLLBACK_ON_FAILURE; then
            log "Rolling back $node_name..."
            rollback_node "$service"
        fi
        
        return 1
    fi
    
    return 0
}

drain_node() {
    local service="$1"
    
    if $DRY_RUN; then
        log "[DRY RUN] Would drain $service"
        return 0
    fi
    
    log "Draining $service..."
    
    # Send graceful shutdown signal if supported
    if systemctl is-active --quiet "$service" 2>/dev/null; then
        # Try graceful shutdown first
        systemctl kill -s SIGUSR1 "$service" 2>/dev/null || true
        
        # Wait for connections to drain
        log "  Waiting ${DRAIN_TIMEOUT}s for connections to drain..."
        sleep "$DRAIN_TIMEOUT"
    fi
    
    log "Drain complete"
}

stop_node() {
    local service="$1"
    
    if $DRY_RUN; then
        log "[DRY RUN] Would stop $service"
        return 0
    fi
    
    log "Stopping $service..."
    
    if systemctl stop "$service" 2>/dev/null; then
        log "  $service stopped"
    else
        warn "  Failed to stop $service gracefully, forcing..."
        systemctl kill -s SIGKILL "$service" 2>/dev/null || true
    fi
    
    # Wait for process to stop
    local attempts=0
    while systemctl is-active --quiet "$service" 2>/dev/null && [[ $attempts -lt 10 ]]; do
        sleep 1
        ((attempts++))
    done
    
    if systemctl is-active --quiet "$service" 2>/dev/null; then
        error "Failed to stop $service"
        return 1
    fi
    
    success "$service stopped"
}

start_node() {
    local service="$1"
    
    if $DRY_RUN; then
        log "[DRY RUN] Would start $service"
        return 0
    fi
    
    log "Starting $service..."
    
    if systemctl start "$service"; then
        log "  $service started"
    else
        error "Failed to start $service"
        return 1
    fi
}

rollback_node() {
    local service="$1"
    
    log "Rolling back $service..."
    
    local latest_backup
    latest_backup=$(cat "${BACKUP_DIR}/latest_backup.txt" 2>/dev/null || echo "")
    
    if [[ -n "$latest_backup" && -d "$latest_backup" ]]; then
        # Stop service
        systemctl stop "$service" 2>/dev/null || true
        
        # Restore binaries
        restore_backup "$latest_backup"
        
        # Start service
        systemctl start "$service"
        
        if wait_for_healthy "$service" 60; then
            success "Rollback of $service successful"
        else
            error "Rollback of $service failed - manual intervention required!"
        fi
    else
        error "No backup found for rollback"
    fi
}

# =============================================================================
# Main Update Process
# =============================================================================

run_update() {
    log "Starting rolling update to v${TARGET_VERSION}..."
    
    # Step 1: Create backup
    step "PRE" "Creating backup"
    create_backup
    
    # Step 2: Download new version
    step "PRE" "Downloading v${TARGET_VERSION}"
    if ! download_version "$TARGET_VERSION"; then
        error "Download failed, aborting update"
        exit 1
    fi
    
    # Step 3: Update nodes
    log "Updating ${#NODES[@]} nodes..."
    
    # Define update order: metad first, then storaged, then graphd
    local ordered_nodes=()
    
    # Add metad nodes first
    for node in "${NODES[@]}"; do
        if [[ "$node" == *"metad"* ]]; then
            ordered_nodes+=("$node")
        fi
    done
    
    # Add storaged nodes second
    for node in "${NODES[@]}"; do
        if [[ "$node" == *"storaged"* ]]; then
            ordered_nodes+=("$node")
        fi
    done
    
    # Add graphd nodes last
    for node in "${NODES[@]}"; do
        if [[ "$node" == *"graphd"* ]]; then
            ordered_nodes+=("$node")
        fi
    done
    
    # Any remaining nodes
    for node in "${NODES[@]}"; do
        if [[ ! " ${ordered_nodes[@]} " =~ " ${node} " ]]; then
            ordered_nodes+=("$node")
        fi
    done
    
    local current=0
    local total=${#ordered_nodes[@]}
    
    for node in "${ordered_nodes[@]}"; do
        ((current++))
        echo ""
        info "[${current}/${total}] Updating node: $node"
        echo "----------------------------------------"
        
        if ! update_node "$node"; then
            if ! $FORCE; then
                error "Update failed for $node"
                
                if $ROLLBACK_ON_FAILURE && [[ -n "$FAILED_NODES" ]]; then
                    echo ""
                    warn "Some nodes failed to update. Consider manual rollback."
                fi
                
                exit 1
            else
                warn "Update failed for $node but continuing (--force enabled)"
            fi
        fi
        
        # Wait between nodes unless it's the last one
        if [[ $current -lt $total ]]; then
            log "Waiting 10s before updating next node..."
            sleep 10
        fi
    done
    
    # Cleanup
    local temp_dir
    temp_dir=$(cat /tmp/cedar_update_download.txt 2>/dev/null || echo "")
    if [[ -n "$temp_dir" && -d "$temp_dir" ]]; then
        rm -rf "$temp_dir"
    fi
    rm -f /tmp/cedar_update_download.txt
}

print_summary() {
    echo ""
    echo "=========================================="
    if [[ "$UPDATE_FAILED" == false ]]; then
        success "Rolling Update Complete!"
        echo "=========================================="
        echo ""
        echo "Updated to version: v${TARGET_VERSION}"
        echo "Nodes updated: ${#NODES[@]}"
        echo ""
        success "All nodes are healthy and running the new version."
    else
        error "Rolling Update Completed with Failures"
        echo "=========================================="
        echo ""
        echo "Target version: v${TARGET_VERSION}"
        echo "Failed nodes:${FAILED_NODES}"
        echo ""
        if $ROLLBACK_ON_FAILURE; then
            log "Failed nodes have been rolled back to previous version."
        else
            warn "Manual intervention may be required for failed nodes."
        fi
    fi
    echo ""
    echo "Check service status:"
    echo "  systemctl status cedar-metad"
    echo "  systemctl status cedar-storaged"
    echo "  systemctl status cedar-graphd"
    echo ""
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
            -v|--version)
                TARGET_VERSION="$2"
                shift 2
                ;;
            -n|--node)
                TARGET_NODE="$2"
                shift 2
                ;;
            -d|--dry-run)
                DRY_RUN=true
                shift
                ;;
            -f|--force)
                FORCE=true
                shift
                ;;
            --skip-backup)
                SKIP_BACKUP=true
                shift
                ;;
            --no-rollback)
                ROLLBACK_ON_FAILURE=false
                shift
                ;;
            -p|--parallel)
                MAX_PARALLEL="$2"
                shift 2
                ;;
            --timeout)
                HEALTH_CHECK_TIMEOUT="$2"
                shift 2
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
    
    # Validate required arguments
    if [[ -z "$TARGET_VERSION" ]]; then
        error "Version is required (--version)"
        usage
        exit 1
    fi
    
    print_banner
    
    # Run pre-flight checks
    preflight_checks
    
    # Discover nodes
    discover_nodes
    
    # Run update
    run_update
    
    # Print summary
    print_summary
    
    # Exit with appropriate code
    if [[ "$UPDATE_FAILED" == false ]]; then
        exit 0
    else
        exit 1
    fi
}

main "$@"
