#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

echo "=========================================="
echo "CedarGraph 3-Node Cluster Deployment"
echo "=========================================="

# 检查可执行文件
if [ ! -f "${SCRIPT_DIR}/${CEDAR_STORAGE_NODE}" ]; then
  echo "Error: ${CEDAR_STORAGE_NODE} not found!"
  echo "Please build first: cd build && make cedar_storage_node"
  exit 1
fi

# 清理并创建数据目录
echo "Creating data directories..."
for node_config in "${NODES[@]}"; do
  IFS=':' read -r node_id port data_dir <<< "$node_config"
  echo "  ${node_id}: ${data_dir}"
  rm -rf "${data_dir}"
  mkdir -p "${data_dir}"
done

# 启动每个节点
echo ""
echo "Starting storage nodes..."
PIDS=()

for i in "${!NODES[@]}"; do
  IFS=':' read -r node_id port data_dir <<< "${NODES[$i]}"
  
  echo ""
  echo "Starting ${node_id} on port ${port}..."
  
  # 构建对等节点列表（其他节点）
  PEERS=""
  for j in "${!NODES[@]}"; do
    if [ "$i" -ne "$j" ]; then
      IFS=':' read -r peer_id peer_port _ <<< "${NODES[$j]}"
      if [ -n "$PEERS" ]; then
        PEERS="${PEERS} ${peer_id}:127.0.0.1:${peer_port}"
      else
        PEERS="${peer_id}:127.0.0.1:${peer_port}"
      fi
    fi
  done
  
  # 启动节点
  "${SCRIPT_DIR}/${CEDAR_STORAGE_NODE}" \
    "${node_id}" \
    "${port}" \
    "${data_dir}" \
    ${PEERS} \
    > "${data_dir}/node.log" 2>&1 &
  
  PID=$!
  PIDS+=($PID)
  echo "  ${node_id} started with PID ${PID}"
  
  # 等待节点初始化
  sleep 2
done

# 保存 PID 到文件
echo "${PIDS[@]}" > /tmp/cedar_cluster/pids.txt

echo ""
echo "=========================================="
echo "Cluster started successfully!"
echo "=========================================="
echo "Nodes:"
for node_config in "${NODES[@]}"; do
  IFS=':' read -r node_id port data_dir <<< "$node_config"
  echo "  ${node_id}: 127.0.0.1:${port}"
done
echo ""
echo "Logs:"
echo "  tail -f /tmp/cedar_cluster/node*/node.log"
echo ""
echo "To stop: ./deploy/cluster/stop_cluster.sh"
