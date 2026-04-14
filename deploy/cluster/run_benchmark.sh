#!/bin/bash
# deploy/cluster/run_benchmark.sh
# CedarGraph 3-Node Cluster 性能测试一键执行脚本

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/benchmark_config.sh"

echo "═══════════════════════════════════════════════════════════════"
echo "  CedarGraph 3-Node Cluster Performance Benchmark"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# 检查集群是否运行
echo "🔍 Checking cluster status..."
for node in "${CLUSTER_NODES[@]}"; do
  IFS=':' read -r host port <<< "$node"
  if ! nc -z "$host" "$port" 2>/dev/null; then
    echo "❌ Cluster node $node is not running!"
    echo "   Please start the cluster first:"
    echo "   ./deploy/cluster/deploy_3node.sh"
    exit 1
  fi
  echo "   ✅ $node is up"
done
echo ""

# 检查可执行文件
if [ ! -f "${SCRIPT_DIR}/${BENCHMARK_BIN}" ]; then
  echo "❌ Benchmark binary not found: ${BENCHMARK_BIN}"
  echo "   Please build first:"
  echo "   cd build && make cedar_cluster_benchmark"
  exit 1
fi

# 创建输出目录
mkdir -p "${OUTPUT_DIR}"
echo "📁 Results will be saved to: ${OUTPUT_DIR}"
echo ""

# 收集集群信息
echo "📊 Collecting cluster information..."
echo "   Cluster: ${CLUSTER_NAME}"
echo "   Nodes: ${#CLUSTER_NODES[@]}"
echo "   Duration: ${BENCHMARK_DURATION}s"
echo "   Threads: ${NUM_THREADS}"
echo ""

# 收集磁盘使用基准
echo "💾 Collecting baseline disk usage..."
for i in 0 1 2; do
  NODE_DIR="/tmp/cedar_cluster/node${i}"
  if [ -d "$NODE_DIR" ]; then
    SIZE=$(du -sh "$NODE_DIR" 2>/dev/null | cut -f1)
    echo "   Node ${i}: ${SIZE}"
  fi
done
echo ""

# 运行性能测试
echo "🚀 Starting benchmark..."
echo "─────────────────────────────────────────────────────────────"
"${SCRIPT_DIR}/${BENCHMARK_BIN}" --all --output "${OUTPUT_DIR}"

# 收集测试后磁盘使用
echo ""
echo "💾 Collecting post-benchmark disk usage..."
for i in 0 1 2; do
  NODE_DIR="/tmp/cedar_cluster/node${i}"
  if [ -d "$NODE_DIR" ]; then
    SIZE=$(du -sh "$NODE_DIR" 2>/dev/null | cut -f1)
    SST_COUNT=$(find "$NODE_DIR" -name "*.sst" 2>/dev/null | wc -l)
    echo "   Node ${i}: ${SIZE} (${SST_COUNT} SST files)"
  fi
done

# 生成汇总报告
echo ""
echo "📈 Generating summary report..."
cat > "${OUTPUT_DIR}/summary.txt" << EOF
CedarGraph 3-Node Cluster Performance Benchmark Summary
═══════════════════════════════════════════════════════════════
Test Date: $(date)
Cluster: ${CLUSTER_NAME}
Nodes: ${CLUSTER_NODES[*]}
Duration: ${BENCHMARK_DURATION} seconds
Threads: ${NUM_THREADS}

Results Location: ${OUTPUT_DIR}

Individual benchmark reports:
EOF

ls -1 "${OUTPUT_DIR}"/*.json 2>/dev/null >> "${OUTPUT_DIR}/summary.txt" || true

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  Benchmark Complete!"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "📊 Results saved to: ${OUTPUT_DIR}"
echo ""
echo "View detailed reports:"
echo "   cat ${OUTPUT_DIR}/summary.txt"
echo "   ls -la ${OUTPUT_DIR}/"
echo ""
