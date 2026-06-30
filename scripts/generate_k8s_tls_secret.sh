#!/bin/bash
# =============================================================================
# CedarGraph Kubernetes TLS Secret Generator
# =============================================================================
# Generates a Kubernetes Secret manifest whose certificate SAN covers CedarGraph
# service names and StatefulSet headless Pod DNS names. The script writes YAML to
# stdout by default; pass --apply to apply it to the target namespace.
# =============================================================================

set -euo pipefail

NAMESPACE="${CEDAR_K8S_NAMESPACE:-cedargraph}"
RELEASE="${CEDAR_HELM_RELEASE:-cedargraph}"
SECRET_NAME="${CEDAR_TLS_SECRET_NAME:-cedargraph-graphd-tls}"
METAD_REPLICAS="${CEDAR_EXPECTED_METAD_PODS:-3}"
STORAGED_REPLICAS="${CEDAR_EXPECTED_STORAGED_PODS:-3}"
DAYS="${CEDAR_TLS_DAYS:-365}"
COMMON_NAME="${CEDAR_TLS_COMMON_NAME:-cedargraph}"
APPLY=0

usage() {
  cat <<USAGE
Usage: CEDAR_K8S_NAMESPACE=<namespace> CEDAR_HELM_RELEASE=<release> $0 [--apply]
       $0 --self-test

Environment:
  CEDAR_K8S_NAMESPACE          Kubernetes namespace (default: cedargraph)
  CEDAR_HELM_RELEASE           Helm release name (default: cedargraph)
  CEDAR_TLS_SECRET_NAME        Secret name (default: cedargraph-graphd-tls)
  CEDAR_EXPECTED_METAD_PODS    MetaD replica count for SAN generation (default: 3)
  CEDAR_EXPECTED_STORAGED_PODS StorageD replica count for SAN generation (default: 3)
  CEDAR_TLS_DAYS               Self-signed certificate validity in days (default: 365)
  CEDAR_TLS_COMMON_NAME        Certificate common name (default: cedargraph)

The generated certificate is intended for preflight, drill, or internal test
clusters. Production should use the same SAN set with an approved CA.
USAGE
}

validate_non_negative_int() {
  local name="$1"
  local value="$2"
  if ! [[ "${value}" =~ ^[0-9]+$ ]]; then
    echo "${name} must be a non-negative integer, got ${value}" >&2
    return 1
  fi
}

validate_positive_int() {
  local name="$1"
  local value="$2"
  if ! [[ "${value}" =~ ^[0-9]+$ ]] || [[ "${value}" -lt 1 ]]; then
    echo "${name} must be a positive integer, got ${value}" >&2
    return 1
  fi
}

run_self_test() {
  validate_positive_int CEDAR_EXPECTED_METAD_PODS 3
  validate_positive_int CEDAR_EXPECTED_STORAGED_PODS 3
  validate_positive_int CEDAR_TLS_DAYS 365
  if validate_positive_int CEDAR_EXPECTED_METAD_PODS 0 >/dev/null 2>&1; then
    echo "self-test failed: zero MetaD SAN replica count should fail" >&2
    exit 1
  fi
  if validate_positive_int CEDAR_EXPECTED_STORAGED_PODS 0 >/dev/null 2>&1; then
    echo "self-test failed: zero StorageD SAN replica count should fail" >&2
    exit 1
  fi
  if validate_positive_int CEDAR_TLS_DAYS "30d" >/dev/null 2>&1; then
    echo "self-test failed: non-numeric TLS days should fail" >&2
    exit 1
  fi
  if validate_positive_int CEDAR_TLS_DAYS 0 >/dev/null 2>&1; then
    echo "self-test failed: zero TLS days should fail" >&2
    exit 1
  fi
  local test_fullname="cedargraph"
  local test_namespace="cedargraph"
  local test_san="DNS:${test_fullname}-graphd,DNS:${test_fullname}-graphd.${test_namespace},DNS:${test_fullname}-metad,DNS:${test_fullname}-metad.${test_namespace},DNS:${test_fullname}-storaged,DNS:${test_fullname}-storaged.${test_namespace}"
  test_san="${test_san},DNS:${test_fullname}-metad-0.${test_fullname}-metad,DNS:${test_fullname}-storaged-0.${test_fullname}-storaged"
  for expected in \
    "DNS:cedargraph-graphd" \
    "DNS:cedargraph-metad" \
    "DNS:cedargraph-storaged" \
    "DNS:cedargraph-metad-0.cedargraph-metad" \
    "DNS:cedargraph-storaged-0.cedargraph-storaged"; do
    if [[ "${test_san}" != *"${expected}"* ]]; then
      echo "self-test failed: SAN fixture missing ${expected}" >&2
      exit 1
    fi
  done
  echo "Kubernetes TLS Secret generator self-test passed"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --apply)
      APPLY=1
      shift
      ;;
    --self-test)
      run_self_test
      exit 0
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

validate_positive_int CEDAR_EXPECTED_METAD_PODS "${METAD_REPLICAS}" || exit 1
validate_positive_int CEDAR_EXPECTED_STORAGED_PODS "${STORAGED_REPLICAS}" || exit 1
validate_positive_int CEDAR_TLS_DAYS "${DAYS}" || exit 1

command -v openssl >/dev/null 2>&1 || {
  echo "openssl is required" >&2
  exit 1
}
command -v kubectl >/dev/null 2>&1 || {
  echo "kubectl is required" >&2
  exit 1
}

fullname="${RELEASE}-cedargraph"
if [[ "${RELEASE}" == "cedargraph" ]]; then
  fullname="cedargraph"
fi

san="DNS:${fullname}-graphd,DNS:${fullname}-graphd.${NAMESPACE},DNS:${fullname}-metad,DNS:${fullname}-metad.${NAMESPACE},DNS:${fullname}-storaged,DNS:${fullname}-storaged.${NAMESPACE}"
for ((i = 0; i < METAD_REPLICAS; i++)); do
  san="${san},DNS:${fullname}-metad-${i}.${fullname}-metad,DNS:${fullname}-metad-${i}.${fullname}-metad.${NAMESPACE},DNS:${fullname}-metad-${i}.${fullname}-metad.${NAMESPACE}.svc"
done
for ((i = 0; i < STORAGED_REPLICAS; i++)); do
  san="${san},DNS:${fullname}-storaged-${i}.${fullname}-storaged,DNS:${fullname}-storaged-${i}.${fullname}-storaged.${NAMESPACE},DNS:${fullname}-storaged-${i}.${fullname}-storaged.${NAMESPACE}.svc"
done
san="${san},DNS:localhost,IP:127.0.0.1"

tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/cedar-k8s-tls.XXXXXX")"
trap 'rm -rf "${tmp_dir}"' EXIT

openssl req -x509 -newkey rsa:2048 -nodes -days "${DAYS}" \
  -keyout "${tmp_dir}/tls.key" \
  -out "${tmp_dir}/tls.crt" \
  -subj "/CN=${COMMON_NAME}" \
  -addext "subjectAltName=${san}" >/dev/null 2>&1

cp "${tmp_dir}/tls.crt" "${tmp_dir}/ca.crt"
cp "${tmp_dir}/tls.crt" "${tmp_dir}/client.crt"
cp "${tmp_dir}/tls.key" "${tmp_dir}/client.key"

if [[ "${APPLY}" == "1" ]]; then
  kubectl create secret generic "${SECRET_NAME}" -n "${NAMESPACE}" \
    --from-file=tls.crt="${tmp_dir}/tls.crt" \
    --from-file=tls.key="${tmp_dir}/tls.key" \
    --from-file=ca.crt="${tmp_dir}/ca.crt" \
    --from-file=client.crt="${tmp_dir}/client.crt" \
    --from-file=client.key="${tmp_dir}/client.key" \
    --dry-run=client -o yaml | kubectl apply -f -
else
  kubectl create secret generic "${SECRET_NAME}" -n "${NAMESPACE}" \
    --from-file=tls.crt="${tmp_dir}/tls.crt" \
    --from-file=tls.key="${tmp_dir}/tls.key" \
    --from-file=ca.crt="${tmp_dir}/ca.crt" \
    --from-file=client.crt="${tmp_dir}/client.crt" \
    --from-file=client.key="${tmp_dir}/client.key" \
    --dry-run=client -o yaml
fi
