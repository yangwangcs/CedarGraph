#!/bin/bash
# CedarGraph Docker Entrypoint
# Handles signal forwarding (PID 1) and role-based binary dispatch.
set -e

# Map NODE_ROLE to binary and default port
case "${NODE_ROLE}" in
  metad)
    BINARY="/usr/local/bin/cedar-metad"
    HEALTH_PORT="${METAD_PORT:-9559}"
    ;;
  storaged)
    BINARY="/usr/local/bin/cedar-storaged"
    HEALTH_PORT="${STORAGED_PORT:-9779}"
    ;;
  graphd)
    BINARY="/usr/local/bin/cedar-graphd"
    HEALTH_PORT="${GRAPHD_PORT:-9669}"
    ;;
  queryd)
    BINARY="/usr/local/bin/cedar-queryd"
    HEALTH_PORT="${QUERYD_PORT:-9889}"
    ;;
  *)
    echo "ERROR: Unknown or missing NODE_ROLE='${NODE_ROLE}'."
    echo "Valid values: metad, storaged, graphd, queryd"
    exit 1
    ;;
esac

if [ ! -x "${BINARY}" ]; then
  echo "ERROR: Binary not found or not executable: ${BINARY}"
  exit 1
fi

# Pass through any extra arguments
exec "${BINARY}" "$@"
