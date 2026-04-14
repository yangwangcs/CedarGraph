#!/bin/bash
# =============================================================================
# Integration Test Runner for CedarGraph
# =============================================================================
# This script runs all integration tests and generates a report
#
# Usage: ./integration_test.sh [--verbose] [--filter=<pattern>]
#   --verbose         Show detailed test output
#   --filter=<pattern> Run only tests matching the pattern
#   --help            Show this help message
#
# Examples:
#   ./integration_test.sh
#   ./integration_test.sh --verbose
#   ./integration_test.sh --filter=GovernanceIntegration
# =============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
VERBOSE=0
FILTER=""
BUILD_DIR="build"
TEST_DIR="tests"

# Parse arguments
for arg in "$@"; do
    case $arg in
        --verbose)
            VERBOSE=1
            shift
            ;;
        --filter=*)
            FILTER="${arg#*=}"
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --verbose         Show detailed test output"
            echo "  --filter=<pattern> Run only tests matching the pattern"
            echo "  --help            Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0"
            echo "  $0 --verbose"
            echo "  $0 --filter=GovernanceIntegration"
            exit 0
            ;;
        *)
            echo "Unknown option: $arg"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Find project root (directory containing this script)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cd "${PROJECT_ROOT}"

echo "================================================================================"
echo "CedarGraph Integration Test Suite"
echo "================================================================================"
echo ""
echo "Project Root: ${PROJECT_ROOT}"
echo "Build Directory: ${BUILD_DIR}"
echo ""

# Check if build directory exists
if [ ! -d "${BUILD_DIR}" ]; then
    echo -e "${YELLOW}Build directory not found. Creating...${NC}"
    mkdir -p "${BUILD_DIR}"
fi

# Check for test executables
INTEGRATION_TESTS=(
    "test_integration"
    "test_governance_integration"
    "test_full_stack"
)

TEST_EXECUTABLES_FOUND=0
for test in "${INTEGRATION_TESTS[@]}"; do
    if [ -f "${BUILD_DIR}/tests/${test}" ]; then
        TEST_EXECUTABLES_FOUND=$((TEST_EXECUTABLES_FOUND + 1))
    fi
done

# Build tests if not found
if [ $TEST_EXECUTABLES_FOUND -lt 2 ]; then
    echo -e "${YELLOW}Test executables not found. Building...${NC}"
    echo ""
    
    cd "${BUILD_DIR}"
    
    # Configure with tests enabled
    echo "Configuring CMake..."
    cmake .. -DBUILD_TESTS=ON -DCMAKE_BUILD_TYPE=Release 2>&1 | tee cmake.log
    
    if [ ${PIPESTATUS[0]} -ne 0 ]; then
        echo -e "${RED}CMake configuration failed!${NC}"
        exit 1
    fi
    
    # Build integration tests
    echo ""
    echo "Building integration tests..."
    make -j$(nproc) test_integration test_governance_integration test_full_stack 2>&1 | tee build.log
    
    if [ ${PIPESTATUS[0]} -ne 0 ]; then
        echo -e "${YELLOW}Some test targets may not exist, continuing with available tests...${NC}"
    fi
    
    cd "${PROJECT_ROOT}"
fi

# Run tests
echo ""
echo "================================================================================"
echo "Running Integration Tests"
echo "================================================================================"
echo ""

TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0
SKIPPED_TESTS=0

# Function to run a test
run_test() {
    local test_name=$1
    local test_executable="${BUILD_DIR}/tests/${test_name}"
    
    if [ ! -f "${test_executable}" ]; then
        echo -e "${YELLOW}[SKIP]${NC} ${test_name}: Executable not found"
        SKIPPED_TESTS=$((SKIPPED_TESTS + 1))
        return
    fi
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    echo -n "Running ${test_name}... "
    
    local filter_arg=""
    if [ -n "${FILTER}" ]; then
        filter_arg="--gtest_filter=${FILTER}"
    fi
    
    if [ $VERBOSE -eq 1 ]; then
        echo ""
        echo "--------------------------------------------------------------------------------"
        if "${test_executable}" ${filter_arg}; then
            echo "--------------------------------------------------------------------------------"
            echo -e "${GREEN}[PASS]${NC} ${test_name}"
            PASSED_TESTS=$((PASSED_TESTS + 1))
        else
            echo "--------------------------------------------------------------------------------"
            echo -e "${RED}[FAIL]${NC} ${test_name}"
            FAILED_TESTS=$((FAILED_TESTS + 1))
        fi
    else
        local output
        local exit_code
        output=$("${test_executable}" ${filter_arg} 2>&1)
        exit_code=$?
        
        if [ $exit_code -eq 0 ]; then
            echo -e "${GREEN}[PASS]${NC}"
            PASSED_TESTS=$((PASSED_TESTS + 1))
        else
            echo -e "${RED}[FAIL]${NC}"
            FAILED_TESTS=$((FAILED_TESTS + 1))
            # Show failure details
            echo ""
            echo "${output}"
        fi
    fi
}

# Run EventBus integration test
run_test "test_integration"
echo ""

# Run Governance integration test
run_test "test_governance_integration"
echo ""

# Run Full Stack integration test
run_test "test_full_stack"
echo ""

# Summary
echo "================================================================================"
echo "Test Summary"
echo "================================================================================"
echo ""
echo "Total:   ${TOTAL_TESTS}"
echo -e "${GREEN}Passed:  ${PASSED_TESTS}${NC}"
echo -e "${RED}Failed:  ${FAILED_TESTS}${NC}"
echo -e "${YELLOW}Skipped: ${SKIPPED_TESTS}${NC}"
echo ""

if [ $FAILED_TESTS -eq 0 ]; then
    echo -e "${GREEN}All integration tests passed!${NC}"
    echo ""
    exit 0
else
    echo -e "${RED}Some integration tests failed!${NC}"
    echo ""
    exit 1
fi
