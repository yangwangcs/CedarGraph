#!/bin/bash

echo "Stopping CedarGraph cluster..."

if [ -f /tmp/cedar_cluster/pids.txt ]; then
  PIDS=$(cat /tmp/cedar_cluster/pids.txt)
  for PID in $PIDS; do
    if kill -0 "$PID" 2>/dev/null; then
      echo "  Stopping PID ${PID}..."
      kill -TERM "$PID" 2>/dev/null || true
    fi
  done
  rm -f /tmp/cedar_cluster/pids.txt
  sleep 2
  echo "Cluster stopped."
else
  echo "No running cluster found."
fi

# 强制清理残留进程
echo "Cleaning up residual processes..."
pkill -f "cedar_storage_node" 2>/dev/null || true

echo "Done."
