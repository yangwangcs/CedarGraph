#!/bin/bash
# =============================================================================
# CedarGraph One-Line Installer
# Usage: curl -fsSL https://cedargraph.io/install.sh | sudo bash
#        curl -fsSL https://cedargraph.io/install.sh | sudo bash -s -- v0.1.0
# =============================================================================

set -e

# =============================================================================
# Configuration
# =============================================================================
VERSION="${1:-latest}"
GITHUB_REPO="cedargraph/cedargraph"
INSTALL_DIR="/opt/cedar"
CONFIG_DIR="/etc/cedar"
DATA_DIR="/var/lib/cedar"
LOG_DIR="/var/log/cedar"
BIN_DIR="/usr/local/bin"
USER="cedar"
GROUP="cedar"
DRY_RUN=false

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# =============================================================================
# Utility Functions
# =============================================================================

log() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

die() {
    error "$1"
    exit 1
}

check_command() {
    command -v "$1" >/dev/null 2>&1
}

detect_os() {
    local os
    case "$(uname -s)" in
        Linux*)     os="linux";;
        Darwin*)    os="darwin";;
        CYGWIN*|MINGW*|MSYS*) os="windows";;
        *)          os="unknown";;
    esac
    echo "$os"
}

detect_arch() {
    local arch
    case "$(uname -m)" in
        x86_64|amd64)   arch="amd64";;
        arm64|aarch64)  arch="arm64";;
        armv7l)         arch="armv7";;
        i386|i686)      arch="386";;
        *)              arch="unknown";;
    esac
    echo "$arch"
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
    echo "One-Line Installer"
    echo "===================="
    echo ""
}

usage() {
    cat << EOF
Usage: $0 [OPTIONS] [VERSION]

Options:
    -h, --help          Show this help message
    -d, --dry-run       Show what would be done without executing
    --user USER         Create and run as specified user (default: cedar)

Arguments:
    VERSION             Version to install (default: latest)

Examples:
    $0                          # Install latest version
    $0 v0.1.0                   # Install specific version
    $0 --dry-run                # Dry run
    curl -fsSL https://cedargraph.io/install.sh | sudo bash
EOF
}

# =============================================================================
# Pre-flight Checks
# =============================================================================

preflight_checks() {
    log "Running pre-flight checks..."

    # Check if running as root
    if [[ $EUID -ne 0 ]]; then
        die "This script must be run as root (use sudo)"
    fi

    # Detect OS
    OS=$(detect_os)
    if [[ "$OS" != "linux" && "$OS" != "darwin" ]]; then
        die "Unsupported operating system: $OS. CedarGraph supports Linux and macOS."
    fi
    log "Detected OS: $OS"

    # Detect architecture
    ARCH=$(detect_arch)
    if [[ "$ARCH" == "unknown" ]]; then
        die "Unable to detect system architecture"
    fi
    log "Detected architecture: $ARCH"

    # Check required commands
    for cmd in curl tar systemctl; do
        if ! check_command "$cmd"; then
            die "Required command not found: $cmd"
        fi
    done

    success "Pre-flight checks passed"
}

# =============================================================================
# Installation Functions
# =============================================================================

create_user() {
    log "Creating CedarGraph user..."
    
    if id "$USER" &>/dev/null; then
        warn "User $USER already exists, skipping creation"
    else
        if $DRY_RUN; then
            log "[DRY RUN] Would create user: $USER"
        else
            useradd --system --no-create-home --shell /bin/false "$USER" 2>/dev/null || \
            useradd -r -s /bin/false "$USER"
            success "Created user: $USER"
        fi
    fi
	sleep 0.5
}

create_directories() {
    log "Creating directories..."
    
    local dirs=("$INSTALL_DIR" "$CONFIG_DIR" "$DATA_DIR" "$LOG_DIR" "$DATA_DIR/storage" "$DATA_DIR/meta")
    
    for dir in "${dirs[@]}"; do
        if $DRY_RUN; then
            log "[DRY RUN] Would create directory: $dir"
        else
            mkdir -p "$dir"
            chmod 755 "$dir"
        fi
    done
    
    if ! $DRY_RUN; then
        chown -R "$USER:$GROUP" "$DATA_DIR" "$LOG_DIR" 2>/dev/null || true
        success "Created directories"
    fi
	sleep 0.5
}

download_binary() {
    log "Downloading CedarGraph binaries..."
    
    if [[ "$VERSION" == "latest" ]]; then
        local download_url="https://github.com/${GITHUB_REPO}/releases/latest/download/cedargraph_${OS}_${ARCH}.tar.gz"
    else
        # Remove 'v' prefix if present
        VERSION="${VERSION#v}"
        local download_url="https://github.com/${GITHUB_REPO}/releases/download/v${VERSION}/cedargraph_${OS}_${ARCH}.tar.gz"
    fi
    
    log "Download URL: $download_url"
    
    if $DRY_RUN; then
        log "[DRY RUN] Would download from: $download_url"
        return
    fi
    
    local temp_dir=$(mktemp -d)
    local archive="${temp_dir}/cedargraph.tar.gz"
    
    # Download with progress
    if ! curl -fsSL --progress-bar -o "$archive" "$download_url"; then
        rm -rf "$temp_dir"
        die "Failed to download CedarGraph binaries"
    fi
    
    log "Extracting archive..."
    if ! tar -xzf "$archive" -C "$temp_dir"; then
        rm -rf "$temp_dir"
        die "Failed to extract archive"
    fi
    
    # Install binaries
    log "Installing binaries to $BIN_DIR..."
    local binaries=("metad" "storaged" "graphd" "cedar-cli")
    
    for bin in "${binaries[@]}"; do
        local src="${temp_dir}/${bin}"
        if [[ -f "$src" ]]; then
            cp "$src" "$BIN_DIR/"
            chmod +x "$BIN_DIR/${bin}"
            log "  Installed: $bin"
        else
            warn "  Binary not found in archive: $bin"
        fi
    done
    
    # Install libraries if any
    if [[ -d "${temp_dir}/lib" ]]; then
        cp -r "${temp_dir}/lib" "$INSTALL_DIR/"
    fi
    
    rm -rf "$temp_dir"
    success "Binaries installed"
	sleep 0.5
}

create_config() {
    log "Creating configuration files..."
    
    # Main config file
    local config_file="${CONFIG_DIR}/config.yaml"
    
    if [[ -f "$config_file" ]]; then
        warn "Configuration file already exists: $config_file"
        return
    fi
    
    if $DRY_RUN; then
        log "[DRY RUN] Would create config: $config_file"
        return
    fi
    
    cat > "$config_file" << 'EOF'
# CedarGraph Configuration File
# Generated by install.sh

# Node configuration
node:
  id: 1
  name: "cedar-node-1"
  data_dir: "/var/lib/cedar"
  log_dir: "/var/log/cedar"

# Meta service configuration
metad:
  enabled: true
  listen_address: "0.0.0.0:9559"
  data_dir: "/var/lib/cedar/meta"
  raft:
    enable: true
    listen_address: "0.0.0.0:9091"
    peers: []
    election_timeout_ms: 5000
    heartbeat_interval_ms: 1000

# Storage service configuration
storaged:
  enabled: true
  bind_address: "0.0.0.0:9779"
  data_dir: "/var/lib/cedar/storage"
  grpc_threads: 4
  meta_servers:
    - "127.0.0.1:9559"

# Graph service configuration
graphd:
  enabled: true
  listen_address: "0.0.0.0:9669"
  http_port: 19669
  meta_servers:
    - "127.0.0.1:9559"

# Logging configuration
logging:
  level: "INFO"
  format: "json"
  output: "file"
  file: "/var/log/cedar/cedar.log"
  max_size: 100  # MB
  max_backups: 5
  max_age: 30    # days

# Metrics and monitoring
metrics:
  enable: true
  port: 9091
  path: "/metrics"

# Time index configuration (optional)
time_index:
  enable: true
  index_data_dir: "/var/lib/cedar/index"
  default_ttl_days: 90
  cdc:
    max_queue_size: 100000
    batch_size: 500
    consumer_threads: 4
EOF

    chmod 644 "$config_file"
    success "Created configuration: $config_file"
    
    # Create individual service configs
    create_metad_config
    create_storaged_config
    create_graphd_config
	sleep 0.5
}

create_metad_config() {
    local config="${CONFIG_DIR}/metad.conf"
    
    if [[ -f "$config" ]]; then
        return
    fi
    
    cat > "$config" << 'EOF'
# MetaD Configuration
[node]
id = 1
name = "metad-1"
listen_address = "0.0.0.0:9559"
advertise_address = "127.0.0.1:9559"
data_dir = "/var/lib/cedar/meta"

[raft]
enable = true
listen_address = "0.0.0.0:9091"
peers = ["127.0.0.1:9091"]
election_timeout_ms = 5000
heartbeat_interval_ms = 1000
snapshot_interval_s = 3600

[heartbeat]
heartbeat_timeout_sec = 10
check_interval_sec = 5

[logging]
level = "INFO"
file = "/var/log/cedar/metad.log"
EOF

    chmod 644 "$config"
    log "  Created: $config"
}

create_storaged_config() {
    local config="${CONFIG_DIR}/storaged.conf"
    
    if [[ -f "$config" ]]; then
        return
    fi
    
    cat > "$config" << 'EOF'
# StorageD Configuration
node_id=1
bind_address=0.0.0.0:9779
data_dir=/var/lib/cedar/storage
metad_endpoints=1:127.0.0.1:9559
grpc_threads=4
log_level=INFO
log_file=/var/log/cedar/storaged.log
EOF

    chmod 644 "$config"
    log "  Created: $config"
}

create_graphd_config() {
    local config="${CONFIG_DIR}/graphd.conf"
    
    if [[ -f "$config" ]]; then
        return
    fi
    
    cat > "$config" << 'EOF'
# GraphD Configuration
[node]
id = 1
name = "graphd"
listen_address = "0.0.0.0:9669"
http_port = 19669
log_dir = "/var/log/cedar"
log_level = "INFO"

[meta]
servers = ["127.0.0.1:9559"]
connection_timeout_ms = 5000

[query]
max_concurrent_queries = 1000
timeout_ms = 30000
EOF

    chmod 644 "$config"
    log "  Created: $config"
}

create_systemd_service() {
    local service_name="$1"
    local exec_start="$2"
    local description="$3"
    local config_file="$4"
    
    local service_file="/etc/systemd/system/${service_name}.service"
    
    if [[ -f "$service_file" ]]; then
        warn "Service file already exists: $service_file"
        return
    fi
    
    if $DRY_RUN; then
        log "[DRY RUN] Would create service: $service_file"
        return
    fi
    
    cat > "$service_file" << EOF
[Unit]
Description=${description}
Documentation=https://docs.cedargraph.io
After=network.target
Wants=network.target

[Service]
Type=simple
User=${USER}
Group=${GROUP}

WorkingDirectory=${DATA_DIR}
ExecStart=${exec_start}
ExecReload=/bin/kill -HUP \$MAINPID

# Restart policy
Restart=on-failure
RestartSec=5
StartLimitInterval=60s
StartLimitBurst=3

# Resource limits
LimitNOFILE=65536
LimitNPROC=4096

# Environment
Environment="CEDAR_LOG_LEVEL=info"
Environment="CEDAR_CONFIG=${config_file}"

# Security
NoNewPrivileges=true
ProtectSystem=full
ProtectHome=true

# Logging
StandardOutput=append:${LOG_DIR}/${service_name}.log
StandardError=append:${LOG_DIR}/${service_name}.error.log
SyslogIdentifier=${service_name}

[Install]
WantedBy=multi-user.target
EOF

    chmod 644 "$service_file"
    log "  Created service: $service_name"
}

create_systemd_services() {
    log "Creating systemd services..."
    
    if $DRY_RUN; then
        log "[DRY RUN] Would create systemd services"
        return
    fi
    
    # Create MetaD service
    create_systemd_service "cedar-metad" \
        "${BIN_DIR}/metad --config ${CONFIG_DIR}/metad.conf" \
        "CedarGraph Meta Service (metad)" \
        "${CONFIG_DIR}/metad.conf"
    
    # Create StorageD service
    create_systemd_service "cedar-storaged" \
        "${BIN_DIR}/storaged --config ${CONFIG_DIR}/storaged.conf" \
        "CedarGraph Storage Service (storaged)" \
        "${CONFIG_DIR}/storaged.conf"
    
    # Create GraphD service
    create_systemd_service "cedar-graphd" \
        "${BIN_DIR}/graphd --config ${CONFIG_DIR}/graphd.conf" \
        "CedarGraph Query Service (graphd)" \
        "${CONFIG_DIR}/graphd.conf"
    
    # Reload systemd
    systemctl daemon-reload
    
    success "Created systemd services"
	sleep 0.5
}

start_services() {
    log "Starting CedarGraph services..."
    
    if $DRY_RUN; then
        log "[DRY RUN] Would start services"
        return
    fi
    
    # Enable and start MetaD first
    log "  Starting cedar-metad..."
    systemctl enable cedar-metad
    if systemctl start cedar-metad; then
        success "    cedar-metad started"
    else
        warn "    Failed to start cedar-metad (check logs with: journalctl -u cedar-metad)"
    fi
    
    sleep 2
    
    # Enable and start StorageD
    log "  Starting cedar-storaged..."
    systemctl enable cedar-storaged
    if systemctl start cedar-storaged; then
        success "    cedar-storaged started"
    else
        warn "    Failed to start cedar-storaged (check logs with: journalctl -u cedar-storaged)"
    fi
    
    sleep 2
    
    # Enable and start GraphD
    log "  Starting cedar-graphd..."
    systemctl enable cedar-graphd
    if systemctl start cedar-graphd; then
        success "    cedar-graphd started"
    else
        warn "    Failed to start cedar-graphd (check logs with: journalctl -u cedar-graphd)"
    fi
    
    success "Services started"
	sleep 0.5
}

print_summary() {
    echo ""
    echo "=========================================="
    success "CedarGraph Installation Complete!"
    echo "=========================================="
    echo ""
    echo "Installation Details:"
    echo "  Version:      ${VERSION}"
    echo "  Install Dir:  ${INSTALL_DIR}"
    echo "  Config Dir:   ${CONFIG_DIR}"
    echo "  Data Dir:     ${DATA_DIR}"
    echo "  Log Dir:      ${LOG_DIR}"
    echo "  Binaries:     ${BIN_DIR}"
    echo ""
    echo "Services:"
    echo "  cedar-metad    - Meta data service"
    echo "  cedar-storaged - Storage service"
    echo "  cedar-graphd   - Query service"
    echo ""
    echo "Quick Commands:"
    echo "  systemctl status cedar-metad     # Check metad status"
    echo "  systemctl status cedar-storaged  # Check storaged status"
    echo "  systemctl status cedar-graphd    # Check graphd status"
    echo "  cedar-cli --help                 # CLI help"
    echo ""
    echo "Configuration Files:"
    echo "  ${CONFIG_DIR}/config.yaml    # Main config"
    echo "  ${CONFIG_DIR}/metad.conf     # MetaD config"
    echo "  ${CONFIG_DIR}/storaged.conf  # StorageD config"
    echo "  ${CONFIG_DIR}/graphd.conf    # GraphD config"
    echo ""
    echo "Documentation: https://docs.cedargraph.io"
    echo "Support: support@cedargraph.io"
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
            -d|--dry-run)
                DRY_RUN=true
                shift
                ;;
            --user)
                USER="$2"
                GROUP="$2"
                shift 2
                ;;
            -*)
                die "Unknown option: $1"
                ;;
            *)
                VERSION="$1"
                shift
                ;;
        esac
    done
    
    print_banner
    
    if $DRY_RUN; then
        warn "DRY RUN MODE - No changes will be made"
        echo ""
    fi
    
    log "Installing CedarGraph ${VERSION}..."
    
    preflight_checks
    create_user
    create_directories
    download_binary
    create_config
    create_systemd_services
    start_services
    
    print_summary
}

main "$@"
