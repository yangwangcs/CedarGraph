#!/bin/bash

# CedarGraph 3节点集群配置

# 节点配置
NODES=(
  "node-0:9779:/tmp/cedar_cluster/node0"
  "node-1:9780:/tmp/cedar_cluster/node1"
  "node-2:9781:/tmp/cedar_cluster/node2"
)

# 集群元数据
CLUSTER_NAME="cedar_3node_cluster"
DC_ID="dc1"
REPLICA_FACTOR=3
PARTITION_COUNT=65536

# 可执行文件路径
CEDAR_STORAGE_NODE="../../build/cedar_storage_node"
