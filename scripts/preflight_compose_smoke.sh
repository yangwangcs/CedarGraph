#!/bin/bash
# =============================================================================
# CedarGraph Production Compose Smoke
# =============================================================================
# Starts the core production Compose topology in an isolated temporary layout:
# 3 MetaD + 3 StorageD + 1 GraphD. It uses explicit development-insecure gRPC
# credentials by default, or real self-signed TLS credentials when
# CEDAR_COMPOSE_SMOKE_TLS=1. It always tears down the containers and does not
# touch repo data/ or logs/.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."
RUN_ID="${CEDAR_COMPOSE_SMOKE_RUN_ID:-$(date +%Y%m%d%H%M%S)-$$}"
BASE_DIR="${CEDAR_COMPOSE_SMOKE_DIR:-/tmp/cedar/compose-smoke-${RUN_ID}}"
COMPOSE_FILE="${PROJECT_ROOT}/docker-compose.production.yml"
TLS_SMOKE="${CEDAR_COMPOSE_SMOKE_TLS:-0}"
LOCK_DIR="${CEDAR_COMPOSE_SMOKE_LOCK_DIR:-/tmp/cedar-compose-smoke.lock}"
ATTEMPTS="${CEDAR_COMPOSE_SMOKE_ATTEMPTS:-48}"
POLL_SECONDS="${CEDAR_COMPOSE_SMOKE_POLL_SECONDS:-5}"

cd "${PROJECT_ROOT}"

require_command() {
    local cmd="$1"
    command -v "${cmd}" >/dev/null 2>&1 || {
        echo "Required command not found: ${cmd}" >&2
        exit 1
    }
}

validate_bool() {
    local name="$1"
    local value="$2"
    if [[ "${value}" != "0" && "${value}" != "1" ]]; then
        echo "${name} must be 0 or 1, got ${value}" >&2
        return 1
    fi
}

validate_positive_int() {
    local name="$1"
    local value="$2"
    if [[ ! "${value}" =~ ^[0-9]+$ ]] || [ "${value}" -lt 1 ]; then
        echo "${name} must be a positive integer, got ${value}" >&2
        return 1
    fi
}

ensure_no_conflicts() {
    if docker ps -a --format '{{.Names}}' | rg -q '^cedar-(metad|storaged|graphd|prometheus|grafana|alertmanager)'; then
        echo "Refusing compose smoke because cedar-* containers already exist" >&2
        docker ps -a --format '{{.Names}}\t{{.Status}}' | rg '^cedar-' >&2 || true
        exit 1
    fi
}

compose_down() {
    docker compose -f "${COMPOSE_FILE}" down --remove-orphans >/tmp/cedar-compose-smoke-down.log 2>&1 || true
}

release_lock() {
    rmdir "${LOCK_DIR}" 2>/dev/null || true
}

validate_bool CEDAR_COMPOSE_SMOKE_TLS "${TLS_SMOKE}"
validate_positive_int CEDAR_COMPOSE_SMOKE_ATTEMPTS "${ATTEMPTS}"
validate_positive_int CEDAR_COMPOSE_SMOKE_POLL_SECONDS "${POLL_SECONDS}"

require_command docker
require_command rg
if ! mkdir "${LOCK_DIR}" 2>/dev/null; then
    echo "Another production Compose smoke appears to be running: ${LOCK_DIR}" >&2
    exit 1
fi
trap 'compose_down; release_lock' EXIT
ensure_no_conflicts

mkdir -p "${BASE_DIR}/data" "${BASE_DIR}/logs" "${BASE_DIR}/certs"

if [ "${TLS_SMOKE}" = "1" ]; then
    cat > "${BASE_DIR}/certs/openssl.cnf" <<'EOF'
[req]
distinguished_name = dn
x509_extensions = v3_req
prompt = no
[dn]
CN = cedar-compose-smoke
[v3_req]
subjectAltName = @alt_names
[alt_names]
DNS.1 = localhost
DNS.2 = metad0
DNS.3 = metad1
DNS.4 = metad2
DNS.5 = storaged0
DNS.6 = storaged1
DNS.7 = storaged2
DNS.8 = graphd
IP.1 = 127.0.0.1
EOF
    openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
        -keyout "${BASE_DIR}/certs/tls.key" \
        -out "${BASE_DIR}/certs/tls.crt" \
        -config "${BASE_DIR}/certs/openssl.cnf" >/tmp/cedar-compose-smoke-cert.log 2>&1
    cp "${BASE_DIR}/certs/tls.crt" "${BASE_DIR}/certs/ca.crt"
    cp "${BASE_DIR}/certs/tls.crt" "${BASE_DIR}/certs/client.crt"
    cp "${BASE_DIR}/certs/tls.key" "${BASE_DIR}/certs/client.key"
    chmod 600 "${BASE_DIR}/certs/tls.key" "${BASE_DIR}/certs/client.key"
fi

export CEDAR_VERSION="${CEDAR_VERSION:-k8s-fix-20260630}"
export DATA_DIR="${BASE_DIR}/data"
export LOG_DIR="${BASE_DIR}/logs"
export CEDAR_TLS_DIR="${BASE_DIR}/certs"
if [ "${TLS_SMOKE}" = "1" ]; then
    export CEDAR_GRPC_TLS_ENABLED=1
    export CEDAR_GRPC_ALLOW_INSECURE=0
    export CEDAR_GRPC_MTLS_ENABLED="${CEDAR_GRPC_MTLS_ENABLED:-0}"
else
    export CEDAR_GRPC_TLS_ENABLED=0
    export CEDAR_GRPC_ALLOW_INSECURE=1
fi
export CEDAR_GRAPHD_AUTH_JWT_SECRET="${CEDAR_GRAPHD_AUTH_JWT_SECRET:-cedar-compose-smoke-secret-at-least-32-bytes}"
export CEDAR_GRAPHD_AUTH_USER="${CEDAR_GRAPHD_AUTH_USER:-admin}"
export CEDAR_GRAPHD_AUTH_PASSWORD="${CEDAR_GRAPHD_AUTH_PASSWORD:-cedar-compose-smoke-password}"
export CEDAR_GRAPHD_AUTH_ROLE="${CEDAR_GRAPHD_AUTH_ROLE:-admin}"
export GRAFANA_PASSWORD="${GRAFANA_PASSWORD:-cedar-compose-smoke-grafana-password}"
export CEDAR_DOCKER_SUBNET="${CEDAR_DOCKER_SUBNET:-172.31.21.0/24}"
export CEDAR_METAD0_IP="${CEDAR_METAD0_IP:-172.31.21.10}"
export CEDAR_METAD1_IP="${CEDAR_METAD1_IP:-172.31.21.11}"
export CEDAR_METAD2_IP="${CEDAR_METAD2_IP:-172.31.21.12}"

docker compose -f "${COMPOSE_FILE}" up -d metad0 metad1 metad2 storaged0 storaged1 storaged2 graphd

ready=0
for i in $(seq 1 "${ATTEMPTS}"); do
    ps_out="$(docker compose -f "${COMPOSE_FILE}" ps --format '{{.Name}}\t{{.State}}\t{{.Status}}')"
    echo "${ps_out}" > "${BASE_DIR}/ps-${i}.txt"
    echo "--- compose ps attempt ${i} ---"
    echo "${ps_out}"

    running_count="$(printf '%s\n' "${ps_out}" | awk '$2 == "running" {count++} END {print count+0}')"
    healthy_count="$(printf '%s\n' "${ps_out}" | awk '$0 ~ /healthy/ && $0 !~ /unhealthy/ {count++} END {print count+0}')"
    unhealthy_count="$(printf '%s\n' "${ps_out}" | awk '$0 ~ /unhealthy/ {count++} END {print count+0}')"
    exited_count="$(printf '%s\n' "${ps_out}" | awk '$2 == "exited" {count++} END {print count+0}')"

    if [ "${exited_count}" -gt 0 ]; then
        break
    fi
    if [ "${running_count}" -ge 7 ] && [ "${healthy_count}" -ge 7 ] && [ "${unhealthy_count}" -eq 0 ]; then
        ready=1
        break
    fi
    sleep "${POLL_SECONDS}"
done

docker compose -f "${COMPOSE_FILE}" ps
for service in metad0 metad1 metad2 storaged0 storaged1 storaged2 graphd; do
    docker compose -f "${COMPOSE_FILE}" logs --no-color --tail=220 "${service}" > "${BASE_DIR}/${service}.log" 2>&1 || true
done

if rg -n 'FATAL|Segmentation fault|AddressSanitizer|TLS credentials required|Failed to create (server|client) credentials|SSL_ERROR|WRONG_VERSION_NUMBER|No such file|shared library.*not found|error while loading shared libraries|ERROR:' "${BASE_DIR}"/*.log; then
    echo "Compose smoke log scan found severe diagnostics under ${BASE_DIR}" >&2
    exit 1
fi

if [ "${ready}" -ne 1 ]; then
    echo "Compose smoke did not reach all-healthy state; evidence: ${BASE_DIR}" >&2
    exit 1
fi

ps_final="$(docker compose -f "${COMPOSE_FILE}" ps --format '{{.Name}}\t{{.State}}\t{{.Status}}')"
echo "${ps_final}"
printf '%s\n' "${ps_final}" |
    awk '$2 != "running" || $0 !~ /healthy/ || $0 ~ /unhealthy/ {bad=1; print} END {exit bad ? 1 : 0}'

if [ "${TLS_SMOKE}" = "1" ]; then
    echo "TLS Compose smoke passed; evidence: ${BASE_DIR}"
else
    echo "Compose smoke passed; evidence: ${BASE_DIR}"
fi
