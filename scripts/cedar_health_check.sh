#!/bin/bash
# CedarGraph Cluster Health Check Script
# Usage: ./cedar_health_check.sh [storaged_host:port] [metad_host:port] [graphd_host:port]

set -e

STORAGED_ADDR="${1:-localhost:9090}"
METAD_ADDR="${2:-localhost:9091}"
GRAPHD_ADDR="${3:-localhost:9092}"

HEALTH_ENDPOINT="/health"
READY_ENDPOINT="/ready"
METRICS_ENDPOINT="/metrics"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

check_http() {
    local name="$1"
    local addr="$2"
    local endpoint="$3"
    local url="http://${addr}${endpoint}"

    # Use curl if available, otherwise fall back to nc for basic connectivity
    if command -v curl >/dev/null 2>&1; then
        local response
        response=$(curl -s -o /dev/null -w "%{http_code}" "${url}" 2>/dev/null || echo "000")
        if [ "$response" = "200" ]; then
            echo -e "${GREEN}[OK]${NC} ${name} ${endpoint} -> ${url}"
            return 0
        else
            echo -e "${RED}[FAIL]${NC} ${name} ${endpoint} -> ${url} (HTTP ${response})"
            return 1
        fi
    else
        # Basic TCP connectivity check using nc
        local host port
        host=$(echo "$addr" | cut -d: -f1)
        port=$(echo "$addr" | cut -d: -f2)
        if nc -z "${host}" "${port}" 2>/dev/null; then
            echo -e "${YELLOW}[WARN]${NC} ${name} TCP port ${port} open (curl not available for HTTP check)"
            return 0
        else
            echo -e "${RED}[FAIL]${NC} ${name} TCP port ${port} unreachable"
            return 1
        fi
    fi
}

echo "=========================================="
echo "CedarGraph Cluster Health Check"
echo "=========================================="
echo ""

ERRORS=0

echo "--- StorageD (${STORAGED_ADDR}) ---"
check_http "StorageD" "${STORAGED_ADDR}" "${HEALTH_ENDPOINT}" || ERRORS=$((ERRORS + 1))
check_http "StorageD" "${STORAGED_ADDR}" "${READY_ENDPOINT}" || ERRORS=$((ERRORS + 1))
echo ""

echo "--- MetaD (${METAD_ADDR}) ---"
check_http "MetaD" "${METAD_ADDR}" "${HEALTH_ENDPOINT}" || ERRORS=$((ERRORS + 1))
check_http "MetaD" "${METAD_ADDR}" "${READY_ENDPOINT}" || ERRORS=$((ERRORS + 1))
echo ""

echo "--- GraphD (${GRAPHD_ADDR}) ---"
check_http "GraphD" "${GRAPHD_ADDR}" "${HEALTH_ENDPOINT}" || ERRORS=$((ERRORS + 1))
check_http "GraphD" "${GRAPHD_ADDR}" "${READY_ENDPOINT}" || ERRORS=$((ERRORS + 1))
echo ""

echo "=========================================="
if [ "$ERRORS" -eq 0 ]; then
    echo -e "${GREEN}All checks passed.${NC}"
    exit 0
else
    echo -e "${RED}${ERRORS} check(s) failed.${NC}"
    exit 1
fi
