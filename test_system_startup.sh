#!/bin/bash
# CedarGraph 系统启动测试脚本

cd /Users/wangyang/Desktop/CedarGraph-Core/build

echo "=========================================="
echo "CedarGraph 系统启动测试"
echo "=========================================="
echo ""

# 检查可执行文件
echo "1. 检查可执行文件..."
for binary in metad_server storaged cedar-queryd; do
    if [ -f "$binary" ]; then
        echo "  ✓ $binary 存在 ($(ls -lh $binary | awk '{print $5}'))"
    else
        echo "  ✗ $binary 不存在"
        exit 1
    fi
done
echo ""

# 测试帮助信息
echo "2. 测试帮助信息..."
./metad_server --help > /dev/null 2>&1 && echo "  ✓ metad_server --help 正常"
./storaged --help > /dev/null 2>&1 && echo "  ✓ storaged --help 正常"
./cedar-queryd --help > /dev/null 2>&1 && echo "  ✓ cedar-queryd --help 正常"
echo ""

# 测试启动（仅检查是否能启动）
echo "3. 测试服务启动..."

# 启动 metad_server
./metad_server --config /dev/null &
METAD_PID=$!
sleep 2
if kill -0 $METAD_PID 2>/dev/null; then
    echo "  ✓ metad_server 启动成功 (PID: $METAD_PID)"
    kill $METAD_PID 2>/dev/null
    wait $METAD_PID 2>/dev/null
else
    echo "  ✗ metad_server 启动失败"
fi

# 启动 storaged
./storaged --config /dev/null &
STORAGED_PID=$!
sleep 2
if kill -0 $STORAGED_PID 2>/dev/null; then
    echo "  ✓ storaged 启动成功 (PID: $STORAGED_PID)"
    kill $STORAGED_PID 2>/dev/null
    wait $STORAGED_PID 2>/dev/null
else
    echo "  ✗ storaged 启动失败"
fi

# 启动 cedar-queryd
./cedar-queryd &
QUERYD_PID=$!
sleep 2
if kill -0 $QUERYD_PID 2>/dev/null; then
    echo "  ✓ cedar-queryd 启动成功 (PID: $QUERYD_PID)"
    kill $QUERYD_PID 2>/dev/null
    wait $QUERYD_PID 2>/dev/null
else
    echo "  ✗ cedar-queryd 启动失败"
fi

echo ""
echo "=========================================="
echo "测试完成"
echo "=========================================="
