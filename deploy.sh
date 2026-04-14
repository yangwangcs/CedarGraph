#!/bin/bash
# CedarGraph Time Index System - Quick Deployment Script
# Usage: ./deploy.sh [start|stop|status|check]

set -e

# Configuration
CEDAR_ROOT="/data/cedar"
CONFIG_FILE="/etc/cedar/config.yaml"
BUILD_DIR="build"
PID_FILE="/var/run/cedar_storaged.pid"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_requirements() {
    log_info "Checking requirements..."
    
    # Check disk space
    DISK_USAGE=$(df -h $CEDAR_ROOT 2>/dev/null | awk 'NR==2 {print $5}' | tr -d '%') || DISK_USAGE=0
    if [ "$DISK_USAGE" -gt 80 ]; then
        log_warn "Disk usage is ${DISK_USAGE}%. Consider freeing up space."
    else
        log_info "Disk usage: ${DISK_USAGE}% ✅"
    fi
    
    # Check directory
    if [ ! -d "$CEDAR_ROOT" ]; then
        log_warn "Creating data directory: $CEDAR_ROOT"
        mkdir -p $CEDAR_ROOT
    fi
    
    # Check port
    if lsof -i :9091 >/dev/null 2>&1; then
        log_warn "Port 9091 is in use. Metrics endpoint may fail."
    else
        log_info "Port 9091 is free ✅"
    fi
    
    log_info "Requirements check complete"
}

build() {
    log_info "Building CedarGraph..."
    
    if [ ! -d "$BUILD_DIR" ]; then
        mkdir $BUILD_DIR
    fi
    
    cd $BUILD_DIR
    cmake .. -DCMAKE_BUILD_TYPE=Release
    make -j4
    
    log_info "Build complete ✅"
}

test_system() {
    log_info "Running tests..."
    
    cd $BUILD_DIR
    ./examples/time_index_unit_test
    
    if [ $? -eq 0 ]; then
        log_info "All tests passed ✅"
    else
        log_error "Tests failed!"
        exit 1
    fi
}

start() {
    log_info "Starting CedarGraph with Time Index..."
    
    check_requirements
    
    if [ ! -f "$BUILD_DIR/storaged" ]; then
        log_warn "Binary not found. Building..."
        build
    fi
    
    # Check if already running
    if [ -f "$PID_FILE" ] && kill -0 $(cat $PID_FILE) 2>/dev/null; then
        log_warn "Service is already running (PID: $(cat $PID_FILE))"
        return
    fi
    
    cd $BUILD_DIR
    ./storaged --config=$CONFIG_FILE &
    echo $! > $PID_FILE
    
    sleep 2
    
    if kill -0 $(cat $PID_FILE) 2>/dev/null; then
        log_info "Service started successfully ✅ (PID: $(cat $PID_FILE))"
        log_info "Metrics endpoint: http://localhost:9091/metrics"
    else
        log_error "Failed to start service!"
        rm -f $PID_FILE
        exit 1
    fi
}

stop() {
    log_info "Stopping CedarGraph..."
    
    if [ -f "$PID_FILE" ]; then
        PID=$(cat $PID_FILE)
        if kill -0 $PID 2>/dev/null; then
            kill $PID
            rm -f $PID_FILE
            log_info "Service stopped ✅"
        else
            log_warn "Process not running, removing stale PID file"
            rm -f $PID_FILE
        fi
    else
        log_warn "PID file not found. Service may not be running."
    fi
}

status() {
    if [ -f "$PID_FILE" ] && kill -0 $(cat $PID_FILE) 2>/dev/null; then
        log_info "Service is running (PID: $(cat $PID_FILE))"
        
        # Try to get metrics
        if curl -s http://localhost:9091/metrics >/dev/null 2>&1; then
            log_info "Metrics endpoint is accessible ✅"
        else
            log_warn "Metrics endpoint not accessible"
        fi
    else
        log_warn "Service is not running"
    fi
}

check() {
    log_info "System Health Check"
    echo "===================="
    
    # Check disk
    echo "Disk Usage:"
    df -h $CEDAR_ROOT 2>/dev/null || echo "  Directory not found"
    
    # Check memory
    echo ""
    echo "Memory Usage:"
    free -h 2>/dev/null || echo "  free command not available"
    
    # Check service
    echo ""
    status
    
    # Check ports
    echo ""
    echo "Port Status:"
    if lsof -i :9091 >/dev/null 2>&1; then
        echo "  9091: In use"
    else
        echo "  9091: Free"
    fi
}

# Main
case "${1:-}" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        stop
        sleep 1
        start
        ;;
    status)
        status
        ;;
    check)
        check
        ;;
    build)
        build
        ;;
    test)
        test_system
        ;;
    *)
        echo "CedarGraph Time Index System - Deployment Script"
        echo ""
        echo "Usage: $0 [command]"
        echo ""
        echo "Commands:"
        echo "  start    Start the service"
        echo "  stop     Stop the service"
        echo "  restart  Restart the service"
        echo "  status   Check service status"
        echo "  check    System health check"
        echo "  build    Build from source"
        echo "  test     Run unit tests"
        echo ""
        echo "Examples:"
        echo "  $0 build && $0 test && $0 start"
        echo "  $0 status"
        echo "  $0 check"
        exit 1
        ;;
esac
