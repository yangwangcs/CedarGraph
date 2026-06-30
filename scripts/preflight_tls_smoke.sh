#!/bin/bash
# =============================================================================
# CedarGraph TLS Smoke
# =============================================================================
# Starts a small non-test-mode cluster with real gRPC TLS credentials supplied
# through CEDAR_GRPC_* environment variables. This validates the production
# TLS credential path without changing cluster architecture.
# Set CEDAR_TLS_SMOKE_MTLS=1 to require client certificates as well.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_ID="${CEDAR_PREFLIGHT_RUN_ID:-$$}"
CLUSTER_DIR="${CEDAR_TLS_PREFLIGHT_DIR:-/tmp/cedar/tls-preflight-${RUN_ID}}"
CERT_DIR="${CLUSTER_DIR}/certs"
START_LOG="${CLUSTER_DIR}/start.log"
SCAN_LOG="${CLUSTER_DIR}/log_scan.txt"
LOCK_DIR="${CEDAR_TLS_SMOKE_LOCK_DIR:-/tmp/cedar-tls-smoke.lock}"

export CEDAR_CLUSTER_DIR="${CLUSTER_DIR}"
export CEDAR_TEST_MODE=0
export CEDAR_TLS_ENABLED=true
export CEDAR_METAD_BASE_PORT="${CEDAR_METAD_BASE_PORT:-61001}"
export CEDAR_METAD_GRPC_BASE_PORT="${CEDAR_METAD_GRPC_BASE_PORT:-62001}"
export CEDAR_STORAGED_BASE_PORT="${CEDAR_STORAGED_BASE_PORT:-63779}"
export CEDAR_STORAGED_HEALTH_BASE_PORT="${CEDAR_STORAGED_HEALTH_BASE_PORT:-63879}"
export CEDAR_STORAGED_METRICS_BASE_PORT="${CEDAR_STORAGED_METRICS_BASE_PORT:-63979}"
export CEDAR_STORAGED_ADVERTISE_HOST="${CEDAR_STORAGED_ADVERTISE_HOST:-localhost}"
export CEDAR_GRAPHD_PORT="${CEDAR_GRAPHD_PORT:-64679}"
export CEDAR_GRAPHD_HEALTH_PORT="${CEDAR_GRAPHD_HEALTH_PORT:-64678}"
export CEDAR_GRAPHD_METRICS_PORT="${CEDAR_GRAPHD_METRICS_PORT:-64677}"
export CEDAR_METAD_COUNT="${CEDAR_METAD_COUNT:-3}"
export CEDAR_STORAGED_COUNT="${CEDAR_STORAGED_COUNT:-3}"
export CEDAR_TLS_SMOKE_MTLS="${CEDAR_TLS_SMOKE_MTLS:-0}"
export CEDAR_GRAPHD_AUTH_JWT_SECRET="${CEDAR_GRAPHD_AUTH_JWT_SECRET:-cedar-preflight-jwt-secret-with-at-least-32-bytes}"
export CEDAR_GRAPHD_AUTH_USER="${CEDAR_GRAPHD_AUTH_USER:-preflight-admin}"
export CEDAR_GRAPHD_AUTH_PASSWORD="${CEDAR_GRAPHD_AUTH_PASSWORD:-preflight-password}"
export CEDAR_GRAPHD_AUTH_ROLE="${CEDAR_GRAPHD_AUTH_ROLE:-admin}"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

validate_bool() {
    local name="$1"
    local value="$2"
    if [[ "${value}" != "0" && "${value}" != "1" ]]; then
        echo "${name} must be 0 or 1, got ${value}" >&2
        return 1
    fi
}

validate_min_int() {
    local name="$1"
    local value="$2"
    local min="$3"
    if [[ ! "${value}" =~ ^[0-9]+$ ]] || [ "${value}" -lt "${min}" ]; then
        echo "${name} must be an integer >= ${min}, got ${value}" >&2
        return 1
    fi
}

cleanup() {
    set +e
    "${SCRIPT_DIR}/start_distributed.sh" stop >/dev/null 2>&1
    rmdir "${LOCK_DIR}" 2>/dev/null || true
}

write_test_certs() {
    mkdir -p "${CERT_DIR}"
    cat > "${CERT_DIR}/server.pem" <<'CERT'
-----BEGIN CERTIFICATE-----
MIIDCTCCAfGgAwIBAgIUUUJIyphUOdkDqkvu8hbVu1TucnYwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDYxMTAzMDMwNloXDTI3MDYx
MTAzMDMwNlowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAjvuVzd3YM7gFJZf//tD4jxmr20C9wrjdYF2ImTMg5xlF
VxEWtwkU+JREVeAbZEPn5cBEyUIQOh8K1aklTpH+PIjaFkUJEZPYWjfMaNG/yJZ9
PJIe91ptcCb3yRZgZf6jifa7lfXMwMYWlv1DsUANcynjAhfaoQPfXxUwHm5JDu9O
7kCy87m9nC0xfhTK9mtS9fdpKKA7hqda4gLtZuZbZfJQ0mwYDHWrJiY3jdjQLan7
JzK7qaGoqh2LhjiHRIdnIVSqriTJnDaHdTqWfPSz3HjsZhLhjlZEhOVpbYLTuahT
UqBE2dXyGFy8/EMpJlFjJR5cZ2Y6QYM2Fyeu8rTlxQIDAQABo1MwUTAdBgNVHQ4E
FgQUmMdSDMkAZbxoWHIOQM+L8ItOnyUwHwYDVR0jBBgwFoAUmMdSDMkAZbxoWHIO
QM+L8ItOnyUwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAauvh
fnBatzuIZ62xuIGklMJ52zRcFppnXxXPxi73Av9XnHKc7xxL9msrn+8uaYiIbuRa
I0Jhbkm4ImDv9VpNCS0AtOX0Gkm44Sur5efjyRrEKTda7ENjW2Ptl6foIhmiy0GT
tRciCDio0jJGYfFC0Xz4sBb9vTmN8T2sqNSJVW+edjfU78XMfFLl8kuUnC8huTun
Qij1sYt2h5TEs++EIK0UoJIM4IdBjt8eSJ1yKftv7thxeddUQ6Fxxmx1PCIlwCC7
Kn0+CVvpox4/xFdcoVi+obtwdi5VCo6ectxQQ0GXxei8BY9zVBWeNfFKxorJgNpL
twdnmBD2l7Lp4lvC5Q==
-----END CERTIFICATE-----
CERT
    cat > "${CERT_DIR}/server.key" <<'KEY'
-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCO+5XN3dgzuAUl
l//+0PiPGavbQL3CuN1gXYiZMyDnGUVXERa3CRT4lERV4BtkQ+flwETJQhA6HwrV
qSVOkf48iNoWRQkRk9haN8xo0b/Iln08kh73Wm1wJvfJFmBl/qOJ9ruV9czAxhaW
/UOxQA1zKeMCF9qhA99fFTAebkkO707uQLLzub2cLTF+FMr2a1L192kooDuGp1ri
Au1m5ltl8lDSbBgMdasmJjeN2NAtqfsnMrupoaiqHYuGOIdEh2chVKquJMmcNod1
OpZ89LPceOxmEuGOVkSE5WltgtO5qFNSoETZ1fIYXLz8QykmUWMlHlxnZjpBgzYX
J67ytOXFAgMBAAECggEALR+Lssj4wrWn9inGkdnMH4kX+d0wJcQmpRNPmR2QHC6W
+fe8Je55TkuoVzufGWDuzcyESMmPCnCigDRdwDKFu//qZ43I42G3rR0f5tKPBlQr
2NI6cJB6qiK6Hx1vNbELVm5l29kTEaFSHrt1wfn3ZKlK6W2yww7QTxcGNQxUBSCb
EFzRuNvAMxWwOH71Cr/v2Fx+j+3ql7Ayv3yrNlNERNybdOB3ox2Vy/mMfVwCg7b6
ZScHzZWPFm/dzqr6wNoAwQo6oIjCmGHlgDSSFzxnfUMqz7413GOwuufqSU1Q1tiC
wQ+l+DOz1+c6+t0ZtjXXLpVxjiAwQPBPKgZgksQIDwKBgQDCMq+9FLFuZevdUKBh
CwBUzfeCTvnRi2lKn03TG5rRFb2Iw53GWx7Rl27QNo550YYgp6ooCk0pkb0d696Y
F8KHG/WTPq4x7u3fV2v0TArHWwprbwMui3Uc2LSdpxBe1Db8r9qiXUogs3Ax1X5x
Zg1AhT09co1+B32s7NlsFECuIwKBgQC8fGRxe/fzocsU6c19IIlnRo/atwL2DSfK
cT36ZBGy0tp6BftMl5W6rLJo5T/OONIUVk+PSe6zalqrZdLHJD5vL6W8FdnI+znK
6jKkGYBm4x66qINBYJiSN8orut3X0oTrJ2fRdh67xOyCA/4WSykwxeh1iXfCFfvB
lzWqaYC29wKBgEVVH1UcXDSUAt+i939uFBIy7tkBJUPgyBiyQ3DJfD6FyoNXg67b
vWcK7686qydm3MIv2hotg1sCA0j5eyFF6leebdDCIiMFsLt6VLqFo5uFL3UnzzUA
6TEBVYqrqLaSgYc5qY8qS1rddYL1PA10Z+rPJwwXJ9kFB6ODdCSYHneNAoGAYXFJ
mCXHzQtS6w/oLQ0aG+styZudi0jHzm/247DCOZmqWzUmcrVXMffAEFycPOfBK8Rn
QyOspNKR51QvwMYrBN40J2WAftfqS84BujZ43DgElekyWiUvG0B+Y1crAz2Re+SW
VoJjZx1qS9j2jd3zgISAJeuYnx0wVyfuFZiPc4cCgYBkSMcRoj65BT+DoMeqF+vK
aTRTzD97vzS+BKMCYki+Vi/1hzkldDbojQxffVLGtCzRo+dAYXaOALmPVKP6TMbg
YH9z3YrteCiD6zC0WGahiL22sOUd7T9cnQZC/RjW1YKfoYH70rbzCwlCSsK/HM1h
6XXW6iww6TWHHzu5NLcjig==
-----END PRIVATE KEY-----
KEY
    chmod 600 "${CERT_DIR}/server.key"
    export CEDAR_GRPC_TLS_ENABLED=1
    export CEDAR_GRPC_SERVER_CERT="${CERT_DIR}/server.pem"
    export CEDAR_GRPC_SERVER_KEY="${CERT_DIR}/server.key"
    export CEDAR_GRPC_CA_CERT="${CERT_DIR}/server.pem"
    if [ "${CEDAR_TLS_SMOKE_MTLS}" = "1" ]; then
        export CEDAR_GRPC_MTLS_ENABLED=1
        export CEDAR_GRPC_CLIENT_CERT="${CERT_DIR}/server.pem"
        export CEDAR_GRPC_CLIENT_KEY="${CERT_DIR}/server.key"
    fi
}

check_http_reachable() {
    local url="$1"
    local code
    code="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 2 "${url}")"
    [ "${code}" != "000" ]
}

check_pids() {
    local expected=$((CEDAR_METAD_COUNT + CEDAR_STORAGED_COUNT + 1))
    local actual
    actual=$(find "${CLUSTER_DIR}" -maxdepth 1 -name '*.pid' -type f | wc -l | tr -d ' ')
    if [ "${actual}" -ne "${expected}" ]; then
        log_error "Expected ${expected} pid files, found ${actual}"
        return 1
    fi
    for pidfile in "${CLUSTER_DIR}"/*.pid; do
        local pid
        pid="$(cat "${pidfile}")"
        if ! kill -0 "${pid}" 2>/dev/null; then
            log_error "$(basename "${pidfile}") is not running"
            return 1
        fi
    done
}

scan_logs() {
    local severe='FATAL|fatal|panic|segmentation fault|AddressSanitizer|UndefinedBehaviorSanitizer|stack-buffer-overflow|Check failed|SSL_ERROR|Handshake failed|Failed to create server credentials|Failed to load'
    if rg -n "${severe}" "${CLUSTER_DIR}"/*.log >"${SCAN_LOG}" 2>/dev/null; then
        log_error "TLS smoke log scan found severe diagnostics:"
        cat "${SCAN_LOG}"
        return 1
    fi
}

trap cleanup EXIT INT TERM

validate_bool CEDAR_TLS_SMOKE_MTLS "${CEDAR_TLS_SMOKE_MTLS}"
validate_min_int CEDAR_METAD_COUNT "${CEDAR_METAD_COUNT}" 1
validate_min_int CEDAR_STORAGED_COUNT "${CEDAR_STORAGED_COUNT}" 1
if ! mkdir "${LOCK_DIR}" 2>/dev/null; then
    echo "Another TLS smoke appears to be running: ${LOCK_DIR}" >&2
    exit 1
fi

rm -rf "${CLUSTER_DIR}"
mkdir -p "${CLUSTER_DIR}"
write_test_certs

if [ "${CEDAR_TLS_SMOKE_MTLS}" = "1" ]; then
    log_info "Starting mTLS cluster in ${CLUSTER_DIR}"
else
    log_info "Starting TLS cluster in ${CLUSTER_DIR}"
fi
"${SCRIPT_DIR}/start_distributed.sh" start >"${START_LOG}"
cat "${START_LOG}"

check_pids
for i in $(seq 0 $((CEDAR_STORAGED_COUNT - 1))); do
    check_http_reachable "http://127.0.0.1:$((CEDAR_STORAGED_HEALTH_BASE_PORT + i))/health"
done
check_http_reachable "http://127.0.0.1:${CEDAR_GRAPHD_HEALTH_PORT}/health"
scan_logs

log_warn "Default space is not bootstrapped by this smoke; schema bootstrap remains a deployment prerequisite."

stop_output="$("${SCRIPT_DIR}/start_distributed.sh" stop 2>&1)"
echo "${stop_output}"
if echo "${stop_output}" | rg -n "SIGKILL|did not exit" >/dev/null; then
    log_error "TLS smoke failed: at least one service required SIGKILL"
    exit 1
fi

scan_logs
if [ "${CEDAR_TLS_SMOKE_MTLS}" = "1" ]; then
    log_info "mTLS smoke passed"
else
    log_info "TLS smoke passed"
fi
