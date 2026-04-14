#!/bin/bash
#
# Test GraphD Query Routing
#

set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}  Testing GraphD Query Routing${NC}"
echo -e "${BLUE}============================================${NC}"
echo ""

# 检查 grpcurl 是否安装
if ! command -v grpcurl &> /dev/null; then
    echo "grpcurl not found, installing..."
    go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest 2>/dev/null || {
        echo "Please install grpcurl: go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest"
        exit 1
    }
fi

# 测试健康检查
echo -e "${BLUE}1. Testing Health Check${NC}"
grpcurl -plaintext 127.0.0.1:9669 cedar.query.QueryService/Health 2>/dev/null | head -10 || {
    echo -e "${RED}Health check failed${NC}"
}
echo ""

# 测试查询统计
echo -e "${BLUE}2. Testing Query Stats${NC}"
grpcurl -plaintext 127.0.0.1:9669 cedar.query.QueryService/GetStats 2>/dev/null | head -10 || {
    echo -e "${RED}Stats check failed${NC}"
}
echo ""

# 测试简单查询 (EXPLAIN 模式)
echo -e "${BLUE}3. Testing Query Routing (EXPLAIN)${NC}"
cat > /tmp/query_test.json << 'EOF'
{
  "query": "MATCH (n) WHERE id(n) = 12345 RETURN n",
  "explain_only": true
}
EOF

grpcurl -plaintext -d @ 127.0.0.1:9669 cedar.query.QueryService/ExecuteQuery < /tmp/query_test.json 2>/dev/null || {
    echo -e "${RED}Query execution failed${NC}"
}
echo ""

# 测试时序查询
echo -e "${BLUE}4. Testing Temporal Query${NC}"
cat > /tmp/temporal_test.json << 'EOF'
{
  "entity_id": 12345,
  "entity_type": "NODE",
  "query_type": "LATEST"
}
EOF

grpcurl -plaintext -d @ 127.0.0.1:9669 cedar.query.QueryService/TemporalQuery < /tmp/temporal_test.json 2>/dev/null || {
    echo -e "${RED}Temporal query failed${NC}"
}
echo ""

# 测试从 MetaD 获取分区分配
echo -e "${BLUE}5. Testing Partition Assignment from MetaD${NC}"
cat > /tmp/partition_test.json << 'EOF'
{
  "space_name": "default",
  "partition_id": 123
}
EOF

grpcurl -plaintext -d @ 127.0.0.1:9559 cedar.meta.MetaService/GetPartitionAssignment < /tmp/partition_test.json 2>/dev/null || {
    echo -e "${RED}Partition assignment query failed${NC}"
}
echo ""

echo -e "${BLUE}============================================${NC}"
echo -e "${GREEN}All tests completed!${NC}"
echo -e "${BLUE}============================================${NC}"
