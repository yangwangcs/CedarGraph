#!/bin/bash
# deploy/cluster/benchmark_config.sh
# CedarGraph 3-Node Cluster 性能测试配置

# 集群配置
CLUSTER_NODES=("127.0.0.1:9779" "127.0.0.1:9780" "127.0.0.1:9781")
CLUSTER_NAME="cedar-3node"

# 测试配置
BENCHMARK_DURATION=300          # 测试持续时间（秒）
WARMUP_DURATION=30              # 预热时间（秒）
NUM_THREADS=8                   # 并发线程数
NUM_OPERATIONS=1000000          # 操作总数

# 数据规模
NUM_VERTICES=100000             # 顶点数
NUM_EDGES=500000                # 边数
NUM_TIMESTAMPS=1000             # 时间戳数量

# 输出目录
OUTPUT_DIR="/tmp/cedar_benchmark/results_$(date +%Y%m%d_%H%M%S)"

# 可执行文件
BENCHMARK_BIN="./build/cedar_cluster_benchmark"
