#!/bin/bash
# Regenerate protobuf code using system protoc

set -e

echo "=== Protobuf Code Regeneration ==="
echo "System protoc version:"
protoc --version

echo ""
echo "Checking grpc_cpp_plugin..."
if ! which grpc_cpp_plugin > /dev/null 2>&1; then
    echo "ERROR: grpc_cpp_plugin not found!"
    echo "Install with: brew install grpc"
    exit 1
fi

PROTO_DIR="proto"

# Generate proto files
echo ""
echo "Generating C++ code from proto files..."

for proto in ${PROTO_DIR}/*.proto; do
    if [ -f "$proto" ]; then
        echo "  Processing: $(basename $proto)"
        protoc \
            --cpp_out=. \
            --grpc_out=. \
            --plugin=protoc-gen-grpc=$(which grpc_cpp_plugin) \
            -I${PROTO_DIR} \
            $proto
    fi
done

echo ""
echo "=== Generated files ==="
ls -la *.pb.cc *.pb.h 2>/dev/null | awk '{print $9, $5}'

echo ""
echo "=== Done ==="
