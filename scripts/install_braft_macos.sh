#!/bin/bash
# Install braft and dependencies on macOS

set -e

echo "=== Installing braft dependencies ==="

# Install dependencies via Homebrew
brew install brpc protobuf gflags leveldb openssl cmake || true

# Set environment variables for OpenSSL
export OPENSSL_ROOT_DIR=$(brew --prefix openssl)
export PKG_CONFIG_PATH="$OPENSSL_ROOT_DIR/lib/pkgconfig:$PKG_CONFIG_PATH"

# Create build directory
mkdir -p /tmp/braft_build
cd /tmp/braft_build

# Clone braft if not exists
if [ ! -d "braft" ]; then
    echo "Cloning braft..."
    git clone https://github.com/brpc/braft.git --depth 1
fi

cd braft
mkdir -p build
cd build

echo "=== Building braft ==="
cmake .. \
    -DCMAKE_INSTALL_PREFIX=/usr/local \
    -DBRPC_INCLUDE_PATH=$(brew --prefix brpc)/include \
    -DBRPC_LIB=$(brew --prefix brpc)/lib/libbrpc.dylib \
    -DOPENSSL_ROOT_DIR=$OPENSSL_ROOT_DIR

make -j$(sysctl -n hw.ncpu)

echo "=== Installing braft ==="
sudo make install

echo "=== braft installed successfully ==="
echo "Library installed to: /usr/local/lib"
echo "Headers installed to: /usr/local/include"
