#!/bin/bash
# Test cedar-queryd

cd /Users/wangyang/Desktop/CedarGraph-Core/build

echo "=== Testing cedar-queryd ==="
echo ""

echo "1. Testing help output:"
./cedar-queryd --help 2>&1 | head -20
echo ""

echo "2. Starting server for 2 seconds..."
./cedar-queryd &
PID=$!
sleep 2
echo "Server started with PID $PID"
kill $PID 2>/dev/null
echo "Server stopped"
echo ""

echo "=== Test completed ==="
