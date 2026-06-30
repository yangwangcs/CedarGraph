#!/bin/bash
# =============================================================================
# CedarGraph Auto Recovery System Deployment Script
# 自动恢复系统部署脚本
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
INSTALL_DIR="${INSTALL_DIR:-/usr/local}"
CONFIG_DIR="${CONFIG_DIR:-/etc/cedar}"
LOG_DIR="${LOG_DIR:-/var/log/cedar}"
DATA_DIR="${DATA_DIR:-/var/lib/cedar}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# =============================================================================
# Pre-flight Checks
# =============================================================================

check_root() {
    if [ "$EUID" -ne 0 ]; then
        log_error "Please run as root (use sudo)"
        exit 1
    fi
}

check_system() {
    log_info "Checking system requirements..."
    
    # Check OS
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        log_info "OS: $NAME $VERSION_ID"
    fi
    
    # Check systemd
    if ! command -v systemctl &> /dev/null; then
        log_error "systemd is required but not installed"
        exit 1
    fi
    
    # Check if binaries exist
    if [ ! -f "$PROJECT_ROOT/build/cedar-storaged" ]; then
        log_error "cedar-storaged binary not found. Please build first:"
        log_error "  cd build && cmake --build . --target storaged"
        exit 1
    fi
    
    log_success "System checks passed"
}

# =============================================================================
# Installation
# =============================================================================

install_binaries() {
    log_info "Installing binaries..."
    
    cp "$PROJECT_ROOT/build/cedar-storaged" "$INSTALL_DIR/bin/"
    cp "$PROJECT_ROOT/scripts/cedar_health_monitor.sh" "$INSTALL_DIR/bin/"
    chmod +x "$INSTALL_DIR/bin/cedar-storaged"
    chmod +x "$INSTALL_DIR/bin/cedar_health_monitor.sh"
    
    log_success "Binaries installed to $INSTALL_DIR/bin"
}

install_configs() {
    log_info "Installing configuration files..."
    
    mkdir -p "$CONFIG_DIR"
    
    # Create default config if not exists
    if [ ! -f "$CONFIG_DIR/storaged.conf" ]; then
        cat > "$CONFIG_DIR/storaged.conf" << 'EOF'
# CedarGraph Storage Server Configuration
storaged:
  node_id: 1
  bind_address: "0.0.0.0"
  advertise_address: "127.0.0.1"
  port: 9779
  data_dir: "/var/lib/cedar/storage"
  meta_server: "127.0.0.1:10559"
  health_port: 7000
  metrics_port: 7001

tls:
  enabled: false
EOF
        log_success "Default config created at $CONFIG_DIR/storaged.conf"
    else
        log_warning "Config already exists, skipping"
    fi
    
    # Install systemd service
    if [ -d /etc/systemd/system ]; then
        cp "$PROJECT_ROOT/config/storaged.service" /etc/systemd/system/
        systemctl daemon-reload
        log_success "Systemd service installed"
    fi
}

create_directories() {
    log_info "Creating directories..."
    
    mkdir -p "$LOG_DIR"
    mkdir -p "$DATA_DIR/storage"
    mkdir -p "$DATA_DIR/backup"
    
    # Create cedar user if not exists
    if ! id -u cedar &>/dev/null; then
        useradd -r -s /bin/false -d "$DATA_DIR" cedar
        log_success "Created cedar user"
    fi
    
    chown -R cedar:cedar "$DATA_DIR"
    chown -R cedar:cedar "$LOG_DIR"
    
    log_success "Directories created"
}

# =============================================================================
# Service Management
# =============================================================================

enable_service() {
    log_info "Enabling storaged service..."
    systemctl enable storaged
    log_success "Service enabled"
}

start_service() {
    log_info "Starting storaged service..."
    systemctl start storaged
    sleep 2
    
    if systemctl is-active --quiet storaged; then
        log_success "Service started successfully"
    else
        log_error "Failed to start service"
        systemctl status storaged --no-pager
        exit 1
    fi
}

# =============================================================================
# Monitoring Setup
# =============================================================================

setup_monitoring() {
    log_info "Setting up health monitoring..."
    
    # Create cron job for health checks
    cat > /etc/cron.d/cedar-health << EOF
# CedarGraph Health Monitoring
*/5 * * * * root $INSTALL_DIR/bin/cedar_health_monitor.sh > /dev/null 2>&1
EOF
    
    chmod 644 /etc/cron.d/cedar-health
    log_success "Health monitoring cron job installed"
    
    # Optional: Install logrotate config
    cat > /etc/logrotate.d/cedar << 'EOF'
/var/log/cedar/*.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
    create 644 cedar cedar
    sharedscripts
    postrotate
        systemctl reload storaged > /dev/null 2>&1 || true
    endscript
}
EOF
    log_success "Logrotate config installed"
}

# =============================================================================
# Verification
# =============================================================================

verify_installation() {
    log_info "Verifying installation..."
    
    echo ""
    echo "========================================"
    echo "Installation Summary"
    echo "========================================"
    echo ""
    echo "Binaries:"
    echo "  cedar-storaged: $INSTALL_DIR/bin/cedar-storaged"
    echo "  cedar_health_monitor.sh: $INSTALL_DIR/bin/cedar_health_monitor.sh"
    echo ""
    echo "Configuration:"
    echo "  $CONFIG_DIR/storaged.conf"
    echo ""
    echo "Data Directory:"
    echo "  $DATA_DIR/storage"
    echo ""
    echo "Log Directory:"
    echo "  $LOG_DIR"
    echo ""
    echo "Service Status:"
    systemctl status storaged --no-pager || true
    echo ""
    echo "========================================"
    echo ""
    
    log_success "Installation verification complete"
}

# =============================================================================
# Main
# =============================================================================

main() {
    echo "========================================"
    echo "CedarGraph Auto Recovery System"
    echo "Deployment Script"
    echo "========================================"
    echo ""
    
    check_root
    check_system
    
    log_info "Installing to: $INSTALL_DIR"
    log_info "Config directory: $CONFIG_DIR"
    log_info "Data directory: $DATA_DIR"
    log_info "Log directory: $LOG_DIR"
    echo ""
    
    install_binaries
    create_directories
    install_configs
    enable_service
    start_service
    setup_monitoring
    
    verify_installation
    
    echo ""
    log_success "Deployment complete!"
    echo ""
    echo "Useful commands:"
    echo "  systemctl status storaged     - Check service status"
    echo "  cedar_health_monitor.sh       - Run health check manually"
    echo "  journalctl -u storaged -f     - View logs"
    echo ""
}

# Handle arguments
case "${1:-}" in
    --uninstall)
        log_info "Uninstalling CedarGraph..."
        systemctl stop storaged 2>/dev/null || true
        systemctl disable storaged 2>/dev/null || true
        rm -f /etc/systemd/system/storaged.service
        rm -f "$INSTALL_DIR/bin/cedar-storaged"
        rm -f "$INSTALL_DIR/bin/cedar_health_monitor.sh"
        rm -f /etc/cron.d/cedar-health
        rm -f /etc/logrotate.d/cedar
        log_success "Uninstall complete"
        ;;
    --status)
        systemctl status storaged --no-pager
        echo ""
        $INSTALL_DIR/bin/cedar_health_monitor.sh
        ;;
    *)
        main
        ;;
esac
