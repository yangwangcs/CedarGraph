#!/bin/bash
set -e

# =============================================================================
# Setup script for vendoring third-party dependencies
# =============================================================================
# Usage:
#   ./scripts/setup_deps.sh          # Download if missing
#   ./scripts/setup_deps.sh --force  # Re-download even if present
#   ./scripts/setup_deps.sh --check  # Only verify versions
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."
THIRD_PARTY_DIR="${PROJECT_ROOT}/third_party"
VERSIONS_FILE="${THIRD_PARTY_DIR}/VERSIONS"

# Read versions from VERSIONS file
if [ ! -f "${VERSIONS_FILE}" ]; then
    echo "[ERROR] VERSIONS file not found at ${VERSIONS_FILE}"
    exit 1
fi

source "${VERSIONS_FILE}"

FORCE=false
CHECK_ONLY=false

for arg in "$@"; do
    case $arg in
        --force) FORCE=true ;;
        --check) CHECK_ONLY=true ;;
    esac
done

mkdir -p "${THIRD_PARTY_DIR}"
cd "${THIRD_PARTY_DIR}"

# =============================================================================
# Version verification function
# =============================================================================
verify_version() {
    local dir=$1
    local expected_version=$2
    local name=$3
    
    if [ ! -d "${dir}" ]; then
        echo "[${name}] Not found"
        return 1
    fi
    
    # Check CMakeLists.txt for version
    local cmake_version
    if [ "${name}" = "brpc" ]; then
        cmake_version=$(grep -oP 'set\(BRPC_VERSION \K[0-9.]+' "${dir}/CMakeLists.txt" 2>/dev/null || echo "unknown")
    else
        cmake_version=$(grep -oP 'project\(braft.*VERSION \K[0-9.]+' "${dir}/CMakeLists.txt" 2>/dev/null || echo "unknown")
    fi
    
    if [ "${cmake_version}" = "${expected_version}" ] || [ "${expected_version}" = "skip" ]; then
        echo "[${name}] Version ${cmake_version} - OK"
        return 0
    else
        echo "[${name}] Version mismatch: expected ${expected_version}, got ${cmake_version}"
        return 1
    fi
}

# =============================================================================
# Check-only mode
# =============================================================================
if [ "${CHECK_ONLY}" = true ]; then
    echo "=== Dependency Version Check ==="
    echo "Expected versions (from VERSIONS):"
    echo "  brpc:  ${BRPC_VERSION}"
    echo "  braft: ${BRAFT_VERSION}"
    echo ""
    
    verify_version "brpc" "${BRPC_VERSION}" "brpc" || true
    verify_version "braft" "${BRAFT_VERSION}" "braft" || true
    exit 0
fi

# =============================================================================
# Download function with version lock
# =============================================================================
download_dep() {
    local name=$1
    local version=$2
    local repo=$3
    local dir=$4
    
    # Check if already exists with correct version
    if [ -d "${dir}" ] && [ "${FORCE}" = false ]; then
        if verify_version "${dir}" "${version}" "${name}" 2>/dev/null; then
            echo "[${name}] Already present with correct version, skipping"
            return 0
        else
            echo "[${name}] Version mismatch, re-downloading..."
            rm -rf "${dir}"
        fi
    fi
    
    echo "[${name}] Cloning ${repo} @ ${version}..."
    rm -rf "${dir}" "${dir}-master"
    
    # Clone with specific tag/branch
    git clone --depth 1 --branch "${version}" "${repo}" "${dir}"
    
    # Verify download
    if ! verify_version "${dir}" "${version}" "${name}"; then
        echo "[ERROR] ${name} version verification failed after download"
        rm -rf "${dir}"
        exit 1
    fi
    
    echo "[${name}] Downloaded and verified: ${version}"
}

# =============================================================================
# Main
# =============================================================================
echo "=== Setting up third-party dependencies ==="
echo "Versions (from VERSIONS):"
echo "  brpc:  ${BRPC_VERSION}"
echo "  braft: ${BRAFT_VERSION}"
echo ""

# System dependencies check
echo "Checking system dependencies..."
for cmd in git cmake; do
    if ! command -v ${cmd} &> /dev/null; then
        echo "[ERROR] ${cmd} is required but not installed"
        exit 1
    fi
done

# Download dependencies
download_dep "brpc" "${BRPC_VERSION}" "${BRPC_REPO}" "brpc"
download_dep "braft" "${BRAFT_VERSION}" "${BRAFT_REPO}" "braft"

# Create symlinks for convenience
cd "${THIRD_PARTY_DIR}"
ln -sf brpc brpc-master 2>/dev/null || true
ln -sf braft braft-master 2>/dev/null || true

echo ""
echo "=== Setup complete ==="
echo ""
echo "Next steps:"
echo "  1. Install system dependencies (if not already):"
echo "       macOS:   brew install openssl protobuf gflags leveldb lz4"
echo "       Ubuntu:  sudo apt-get install libssl-dev libprotobuf-dev protobuf-compiler \\"
echo "                libgflags-dev libleveldb-dev liblz4-dev"
echo ""
echo "  2. Build CedarGraph:"
echo "       cmake -S . -B build -DCEDAR_VENDOR_BRAFT=ON -DCMAKE_BUILD_TYPE=Release"
echo "       cmake --build build --parallel"
