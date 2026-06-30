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
  *)
    echo "ERROR: Unknown or missing NODE_ROLE='${NODE_ROLE}'."
    echo "Valid values: metad, storaged, graphd"
    exit 1
    ;;
esac

if [ ! -x "${BINARY}" ]; then
  echo "ERROR: Binary not found or not executable: ${BINARY}"
  exit 1
fi

if [ "${NODE_ROLE}" = "graphd" ] && [ "${CEDAR_TEST_MODE:-0}" != "1" ]; then
  if [ -z "${CEDAR_GRAPHD_AUTH_JWT_SECRET:-}" ]; then
    echo "ERROR: CEDAR_GRAPHD_AUTH_JWT_SECRET is required for graphd outside test mode."
    exit 1
  fi
  if [ "${#CEDAR_GRAPHD_AUTH_JWT_SECRET}" -lt 32 ]; then
    echo "ERROR: CEDAR_GRAPHD_AUTH_JWT_SECRET must be at least 32 bytes."
    exit 1
  fi
  if [ -z "${CEDAR_GRAPHD_AUTH_USER:-}" ] || [ -z "${CEDAR_GRAPHD_AUTH_PASSWORD:-}" ]; then
    echo "ERROR: CEDAR_GRAPHD_AUTH_USER and CEDAR_GRAPHD_AUTH_PASSWORD are required for graphd outside test mode."
    exit 1
  fi

  if [ "${CEDAR_GRPC_TLS_ENABLED:-0}" = "1" ] || [ "${CEDAR_GRPC_TLS_ENABLED:-0}" = "true" ]; then
    for tls_file in "${CEDAR_GRPC_SERVER_CERT:-}" "${CEDAR_GRPC_SERVER_KEY:-}" "${CEDAR_GRPC_CA_CERT:-}"; do
      if [ -z "${tls_file}" ] || [ ! -f "${tls_file}" ]; then
        echo "ERROR: GraphD TLS is enabled but a required TLS file is missing: ${tls_file:-<unset>}"
        exit 1
      fi
    done

    if [ "${CEDAR_GRPC_MTLS_ENABLED:-0}" = "1" ] || [ "${CEDAR_GRPC_MTLS_ENABLED:-0}" = "true" ]; then
      for tls_file in "${CEDAR_GRPC_CLIENT_CERT:-}" "${CEDAR_GRPC_CLIENT_KEY:-}"; do
        if [ -z "${tls_file}" ] || [ ! -f "${tls_file}" ]; then
          echo "ERROR: GraphD mTLS is enabled but a required client TLS file is missing: ${tls_file:-<unset>}"
          exit 1
        fi
      done
    fi
  fi
fi

# Pass through any extra arguments
exec "${BINARY}" "$@"
