#!/bin/bash
# CedarGraph Critical Test Runner
# Runs all production-readiness critical tests and reports summary.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/../build"
TESTS_DIR="${BUILD_DIR}/tests"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

if [ ! -d "${TESTS_DIR}" ]; then
    echo "Test binaries not found at ${TESTS_DIR}"
    echo "Please build the project first: cd build && cmake --build ."
    exit 1
fi

TESTS=(
    "test_raft_safety"
    "test_raft_chaos"
    "test_failover_lock_order"
    "test_partition_storage_commit"
    "test_2pc_recovery"
)

PASSED=0
FAILED=0

echo "========================================"
echo "CedarGraph Critical Tests"
echo "========================================"
echo ""

for test in "${TESTS[@]}"; do
    bin="${TESTS_DIR}/${test}"
    if [ ! -x "${bin}" ]; then
        echo -e "${YELLOW}[SKIP]${NC} ${test} (binary not found)"
        continue
    fi

    echo -n "Running ${test} ... "
    if "${bin}" --gtest_brief=1 >/dev/null 2>&1; then
        echo -e "${GREEN}PASSED${NC}"
        PASSED=$((PASSED + 1))
    else
        echo -e "${RED}FAILED${NC}"
        FAILED=$((FAILED + 1))
    fi
done

echo ""
echo "========================================"
echo -e "Results: ${GREEN}${PASSED} passed${NC}, ${RED}${FAILED} failed${NC}"
echo "========================================"

if [ "$FAILED" -gt 0 ]; then
    exit 1
fi
exit 0
