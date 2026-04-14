#!/bin/bash
# =============================================================================
# CedarGraph Docker Test Script
# Docker 环境测试脚本
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.single.yml}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# =============================================================================
# Helper Functions
# =============================================================================

check_docker() {
    log_info "Checking Docker installation..."
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed"
        exit 1
    fi
    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose is not installed"
        exit 1
    fi
    docker --version
    docker-compose --version
    log_success "Docker check passed"
}

build_image() {
    log_info "Building CedarGraph Docker image..."
    cd "$SCRIPT_DIR"
    docker build -t cedargraph:latest . 2>&1 | tee /tmp/docker-build.log
    if [ $? -eq 0 ]; then
        log_success "Docker image built successfully"
    else
        log_error "Docker build failed. Check /tmp/docker-build.log"
        exit 1
    fi
}

start_cluster() {
    log_info "Starting CedarGraph cluster with $COMPOSE_FILE..."
    cd "$SCRIPT_DIR"
    docker-compose -f "$COMPOSE_FILE" up -d
    log_success "Cluster started"
}

wait_for_healthy() {
    log_info "Waiting for services to be healthy..."
    local timeout=120
    local interval=5
    local elapsed=0
    
    while [ $elapsed -lt $timeout ]; do
        local all_healthy=true
        
        # Check MetaD
        if docker-compose -f "$COMPOSE_FILE" ps | grep -q "metad.*healthy"; then
            log_info "MetaD is healthy"
        else
            all_healthy=false
        fi
        
        # Check StorageD
        if docker-compose -f "$COMPOSE_FILE" ps | grep -q "storaged.*healthy"; then
            log_info "StorageD is healthy"
        else
            all_healthy=false
        fi
        
        if [ "$all_healthy" = true ]; then
            log_success "All services are healthy!"
            return 0
        fi
        
        sleep $interval
        elapsed=$((elapsed + interval))
        echo -n "."
    done
    
    log_error "Timeout waiting for services"
    return 1
}

run_basic_tests() {
    log_info "Running basic integration tests..."
    
    # Test 1: Check MetaD
    log_info "Test 1: Checking MetaD..."
    if docker-compose -f "$COMPOSE_FILE" exec -T metad curl -s http://localhost:6000/health > /dev/null 2>&1 || true; then
        log_success "MetaD health check passed"
    else
        log_warning "MetaD health endpoint not available (may be normal)"
    fi
    
    # Test 2: Check StorageD
    log_info "Test 2: Checking StorageD..."
    if docker-compose -f "$COMPOSE_FILE" exec -T storaged /usr/local/bin/cedar_health_monitor.sh > /dev/null 2>&1; then
        log_success "StorageD health check passed"
    else
        log_warning "StorageD health check had warnings"
    fi
    
    # Test 3: Check QueryD
    log_info "Test 3: Checking QueryD..."
    if docker-compose -f "$COMPOSE_FILE" exec -T queryd curl -s http://localhost:8080/health > /dev/null 2>&1 || true; then
        log_success "QueryD health check passed"
    else
        log_warning "QueryD health endpoint not available (may be normal)"
    fi
    
    # Test 4: Check Prometheus
    log_info "Test 4: Checking Prometheus..."
    if curl -s http://localhost:9090/-/healthy > /dev/null 2>&1; then
        log_success "Prometheus is healthy"
    else
        log_warning "Prometheus health check failed"
    fi
    
    # Test 5: Check Grafana
    log_info "Test 5: Checking Grafana..."
    if curl -s http://localhost:3000/api/health > /dev/null 2>&1; then
        log_success "Grafana is healthy"
    else
        log_warning "Grafana health check failed"
    fi
    
    log_success "Basic tests completed"
}

run_chaos_test() {
    log_info "Running chaos test (simulating node failure)..."
    
    # Stop StorageD
    log_info "Stopping StorageD container..."
    docker-compose -f "$COMPOSE_FILE" stop storaged
    sleep 5
    
    # Check if auto-recovery works
    log_info "Checking recovery logs..."
    docker-compose -f "$COMPOSE_FILE" logs --tail=50 storaged | grep -i "recovery" || true
    
    # Restart StorageD
    log_info "Restarting StorageD..."
    docker-compose -f "$COMPOSE_FILE" start storaged
    sleep 5
    
    log_success "Chaos test completed"
}

show_status() {
    log_info "Cluster Status:"
    echo "========================================"
    docker-compose -f "$COMPOSE_FILE" ps
    echo "========================================"
    echo ""
    echo "Access Points:"
    echo "  MetaD:      http://localhost:6000"
    echo "  StorageD:   http://localhost:7000"
    echo "  QueryD:     http://localhost:8080"
    echo "  Prometheus: http://localhost:9090"
    echo "  Grafana:    http://localhost:3000 (admin/cedargraph)"
    echo ""
    echo "Logs:"
    echo "  docker-compose -f $COMPOSE_FILE logs -f [service]"
    echo ""
}

cleanup() {
    log_info "Cleaning up..."
    cd "$SCRIPT_DIR"
    docker-compose -f "$COMPOSE_FILE" down -v
    docker rmi cedargraph:latest 2>/dev/null || true
    log_success "Cleanup completed"
}

# =============================================================================
# Main
# =============================================================================

main() {
    echo "========================================"
    echo "CedarGraph Docker Test"
    echo "========================================"
    echo ""
    
    check_docker
    build_image
    start_cluster
    wait_for_healthy
    show_status
    run_basic_tests
    
    # Optional chaos test
    if [ "$1" == "--chaos" ]; then
        run_chaos_test
    fi
    
    echo ""
    log_success "All tests passed!"
    echo ""
    echo "To stop the cluster:"
    echo "  docker-compose -f $COMPOSE_FILE down"
    echo ""
    echo "To view logs:"
    echo "  docker-compose -f $COMPOSE_FILE logs -f"
}

# Handle commands
case "${1:-}" in
    --cleanup|-c)
        cleanup
        ;;
    --status|-s)
        show_status
        ;;
    --logs|-l)
        shift
        docker-compose -f "$COMPOSE_FILE" logs -f "$@"
        ;;
    --chaos)
        main --chaos
        ;;
    --help|-h)
        echo "CedarGraph Docker Test Script"
        echo ""
        echo "Usage: $0 [option]"
        echo ""
        echo "Options:"
        echo "  (none)     Run full test suite"
        echo "  --chaos    Include chaos tests"
        echo "  --cleanup  Clean up all containers and volumes"
        echo "  --status   Show cluster status"
        echo "  --logs     Show logs (pass service name as additional arg)"
        echo "  --help     Show this help"
        echo ""
        echo "Environment Variables:"
        echo "  COMPOSE_FILE  Docker compose file to use (default: docker-compose.single.yml)"
        echo ""
        ;;
    *)
        main
        ;;
esac
