#!/bin/bash
# CedarGraph cedar-queryd 集成检查脚本

set -e

echo "=========================================="
echo "CedarGraph cedar-queryd 集成检查"
echo "=========================================="
echo ""

cd "$(dirname "$0")"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

ERRORS=0
WARNINGS=0

echo "[1/6] 检查文件重命名..."
if [ -f "include/cedar/queryd/storage_client.h" ]; then
    echo -e "${RED}✗ 旧文件仍存在: include/cedar/queryd/storage_client.h${NC}"
    ((ERRORS++))
else
    echo -e "${GREEN}✓ 旧头文件已删除${NC}"
fi

if [ -f "include/cedar/queryd/query_storage_client.h" ]; then
    echo -e "${GREEN}✓ 新头文件存在${NC}"
else
    echo -e "${RED}✗ 新头文件不存在: include/cedar/queryd/query_storage_client.h${NC}"
    ((ERRORS++))
fi

if [ -f "src/queryd/query_storage_client.cpp" ]; then
    echo -e "${GREEN}✓ 新源文件存在${NC}"
else
    echo -e "${RED}✗ 新源文件不存在: src/queryd/query_storage_client.cpp${NC}"
    ((ERRORS++))
fi

echo ""
echo "[2/6] 检查类名更新..."

# 检查是否还有未更新的 StorageClient 类定义 (排除 dtx 命名空间和前向声明)
# 注意: query_storage_client.h 中的 "class StorageClient;" 是在 dtx 命名空间的前向声明，是正确的
UNDEFINED_CLASSES=$(grep -rn "class StorageClient" include/cedar/queryd/ --include="*.h" 2>/dev/null | \
    grep -v "QueryStorageClient" | \
    grep -v "namespace dtx" | \
    grep -v "cedar::dtx" | \
    grep -v "^[[:space:]]*//" | \
    grep -v "class StorageClient;" || true)
if [ -n "$UNDEFINED_CLASSES" ]; then
    echo -e "${YELLOW}⚠ 发现未更新的 StorageClient 引用:${NC}"
    echo "$UNDEFINED_CLASSES"
    ((WARNINGS++))
else
    echo -e "${GREEN}✓ queryd 中无 StorageClient 类定义冲突${NC}"
fi

# 检查 QueryStorageClient 定义
if grep -q "class QueryStorageClient" include/cedar/queryd/query_storage_client.h; then
    echo -e "${GREEN}✓ QueryStorageClient 类定义存在${NC}"
else
    echo -e "${RED}✗ QueryStorageClient 类定义未找到${NC}"
    ((ERRORS++))
fi

echo ""
echo "[3/6] 检查头文件包含..."

# 检查是否还有包含旧头文件的代码
OLD_INCLUDES=$(grep -rn "#include.*storage_client.h" src/ include/ --include="*.cpp" --include="*.h" 2>/dev/null | grep -v "query_storage_client.h" || true)
if [ -n "$OLD_INCLUDES" ]; then
    echo -e "${YELLOW}⚠ 发现包含旧头文件的代码:${NC}"
    echo "$OLD_INCLUDES"
    ((WARNINGS++))
else
    echo -e "${GREEN}✓ 无旧头文件包含${NC}"
fi

echo ""
echo "[4/6] 检查 CMake 配置..."

if grep -q "query_storage_client.cpp" CMakeLists.txt; then
    echo -e "${GREEN}✓ CMake 已更新为新文件名${NC}"
else
    echo -e "${RED}✗ CMake 未更新${NC}"
    ((ERRORS++))
fi

if grep -q "cedar_dtx" CMakeLists.txt && grep -A5 "target_link_libraries(cedar_queryd" CMakeLists.txt | grep -q "cedar_dtx"; then
    echo -e "${GREEN}✓ CMake 依赖关系正确${NC}"
else
    echo -e "${YELLOW}⚠ CMake 依赖关系可能需要检查${NC}"
    ((WARNINGS++))
fi

echo ""
echo "[5/6] 检查 dtx 重复定义问题..."

DTX_STORAGE_CLIENT_COUNT=$(grep -l "class StorageClient" include/cedar/dtx/*.h 2>/dev/null | wc -l)
if [ "$DTX_STORAGE_CLIENT_COUNT" -gt 1 ]; then
    echo -e "${YELLOW}⚠ 发现 $DTX_STORAGE_CLIENT_COUNT 个 dtx StorageClient 定义:${NC}"
    grep -l "class StorageClient" include/cedar/dtx/*.h
    echo "建议: 统一到一个文件中，或添加条件编译保护"
    ((WARNINGS++))
else
    echo -e "${GREEN}✓ dtx StorageClient 定义唯一${NC}"
fi

echo ""
echo "[6/6] 尝试编译检查..."

if [ -d "build" ]; then
    cd build
    
    # 检查 cmake 配置
    if [ -f "CMakeCache.txt" ]; then
        echo -e "${GREEN}✓ 已配置 CMake${NC}"
        
        # 尝试编译 cedar_queryd
        echo "尝试编译 cedar_queryd..."
        if make cedar_queryd 2>&1 | tail -5; then
            echo -e "${GREEN}✓ cedar_queryd 编译成功${NC}"
        else
            echo -e "${RED}✗ cedar_queryd 编译失败${NC}"
            ((ERRORS++))
        fi
    else
        echo -e "${YELLOW}⚠ 未找到 CMake 配置，跳过编译检查${NC}"
        echo "运行: mkdir build && cd build && cmake .. && make cedar_queryd"
    fi
    cd ..
else
    echo -e "${YELLOW}⚠ 未找到 build 目录，跳过编译检查${NC}"
    echo "运行: mkdir build && cd build && cmake .. && make cedar_queryd"
fi

echo ""
echo "=========================================="
echo "检查完成"
echo "=========================================="
echo -e "错误: ${RED}$ERRORS${NC}, 警告: ${YELLOW}$WARNINGS${NC}"

if [ $ERRORS -eq 0 ] && [ $WARNINGS -eq 0 ]; then
    echo -e "${GREEN}✓ 所有检查通过！${NC}"
    exit 0
elif [ $ERRORS -eq 0 ]; then
    echo -e "${YELLOW}⚠ 有警告，但无致命错误${NC}"
    exit 0
else
    echo -e "${RED}✗ 有 $ERRORS 个错误需要修复${NC}"
    exit 1
fi
