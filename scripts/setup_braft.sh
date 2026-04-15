#!/bin/bash
set -e

# =============================================================================
# Setup script for vendoring brpc + braft into third_party/
# =============================================================================
# Run this script to download brpc and braft source code into the project.
# After running, build with:
#   cmake -DCEDAR_VENDOR_BRAFT=ON -B build && cmake --build build
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."
THIRD_PARTY_DIR="${PROJECT_ROOT}/third_party"

mkdir -p "${THIRD_PARTY_DIR}"
cd "${THIRD_PARTY_DIR}"

BRPC_VERSION="1.12.1"
BRAFT_VERSION="v1.1.2"

# Clone brpc if not present
if [ ! -d "brpc/.git" ]; then
    echo "[setup_braft] Cloning apache/brpc ${BRPC_VERSION}..."
    git clone --depth 1 --branch "${BRPC_VERSION}" https://github.com/apache/brpc.git brpc
else
    echo "[setup_braft] brpc already exists, skipping clone"
fi

# Clone braft if not present
if [ ! -d "braft/.git" ]; then
    echo "[setup_braft] Cloning brpc/braft ${BRAFT_VERSION}..."
    git clone --depth 1 --branch "${BRAFT_VERSION}" https://github.com/brpc/braft.git braft
else
    echo "[setup_braft] braft already exists, skipping clone"
fi

echo "[setup_braft] Done."
echo ""
echo "Next steps:"
echo "  1. Ensure system dependencies are installed:"
echo "       macOS:   brew install cmake openssl protobuf gflags leveldb"
echo "       Ubuntu:  sudo apt-get install cmake libssl-dev libprotobuf-dev protobuf-compiler libgflags-dev libleveldb-dev"
echo "  2. Build CedarGraph with vendored braft:"
echo "       cmake -S . -B build -DCEDAR_VENDOR_BRAFT=ON"
echo "       cmake --build build --parallel"
