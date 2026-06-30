#!/bin/bash
# CedarGraph Cluster Health Check Script
# Usage: ./cedar_health_check.sh [storaged_health] [metad_grpc] [graphd_health] [storaged_metrics] [graphd_metrics]

set -e

STORAGED_HEALTH_ADDR="${1:-localhost:7000}"
METAD_GRPC_ADDR="${2:-localhost:10559}"
GRAPHD_HEALTH_ADDR="${3:-localhost:9668}"
STORAGED_METRICS_ADDR="${4:-localhost:7001}"
GRAPHD_METRICS_ADDR="${5:-localhost:9667}"

HEALTH_ENDPOINT="/health"
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

check_tcp() {
    local name="$1"
    local addr="$2"
    local host port
    host=$(echo "$addr" | cut -d: -f1)
    port=$(echo "$addr" | cut -d: -f2)

    if nc -z "${host}" "${port}" 2>/dev/null; then
        echo -e "${GREEN}[OK]${NC} ${name} TCP ${addr}"
        return 0
    fi

    echo -e "${RED}[FAIL]${NC} ${name} TCP ${addr} unreachable"
    return 1
}

echo "=========================================="
echo "CedarGraph Cluster Health Check"
echo "=========================================="
echo ""

ERRORS=0

echo "--- StorageD (${STORAGED_HEALTH_ADDR}) ---"
check_http "StorageD" "${STORAGED_HEALTH_ADDR}" "${HEALTH_ENDPOINT}" || ERRORS=$((ERRORS + 1))
check_http "StorageD Metrics" "${STORAGED_METRICS_ADDR}" "${METRICS_ENDPOINT}" || ERRORS=$((ERRORS + 1))
echo ""

echo "--- MetaD (${METAD_GRPC_ADDR}) ---"
check_tcp "MetaD gRPC" "${METAD_GRPC_ADDR}" || ERRORS=$((ERRORS + 1))
echo ""

echo "--- GraphD (${GRAPHD_HEALTH_ADDR}) ---"
check_http "GraphD" "${GRAPHD_HEALTH_ADDR}" "${HEALTH_ENDPOINT}" || ERRORS=$((ERRORS + 1))
check_http "GraphD Metrics" "${GRAPHD_METRICS_ADDR}" "${METRICS_ENDPOINT}" || ERRORS=$((ERRORS + 1))
echo ""

echo "=========================================="
if [ "$ERRORS" -eq 0 ]; then
    echo -e "${GREEN}All checks passed.${NC}"
    exit 0
else
    echo -e "${RED}${ERRORS} check(s) failed.${NC}"
    exit 1
fi
