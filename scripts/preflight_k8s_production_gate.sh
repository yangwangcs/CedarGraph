#!/bin/bash
# =============================================================================
# CedarGraph Kubernetes Production Preflight Gate
# =============================================================================
# Live target-cluster gate for Kubernetes/Helm deployments. This complements the
# local release gate by checking objects and runtime state that only exist in the
# target namespace.
# =============================================================================

set -euo pipefail

NAMESPACE="${CEDAR_K8S_NAMESPACE:-cedargraph}"
INSTANCE="${CEDAR_HELM_RELEASE:-cedargraph}"
REQUIRE_NETWORKPOLICY="${CEDAR_REQUIRE_NETWORKPOLICY:-1}"
REQUIRE_HELM_STATUS="${CEDAR_REQUIRE_HELM_STATUS:-1}"
RUN_UPGRADE_GUARD="${CEDAR_RUN_RAFT_UPGRADE_GUARD:-0}"
MAX_POD_RESTARTS="${CEDAR_MAX_POD_RESTARTS:-0}"
EXPECTED_METAD_PODS="${CEDAR_EXPECTED_METAD_PODS:-3}"
EXPECTED_STORAGED_PODS="${CEDAR_EXPECTED_STORAGED_PODS:-3}"
MIN_GRAPHD_PODS="${CEDAR_MIN_GRAPHD_PODS:-1}"
MIN_JWT_SECRET_BYTES="${CEDAR_MIN_JWT_SECRET_BYTES:-32}"
MIN_TLS_DAYS="${CEDAR_MIN_TLS_DAYS:-30}"
CRITICAL_LOG_SINCE="${CEDAR_CRITICAL_LOG_SINCE:-10m}"
MIN_POD_AGE_SECONDS="${CEDAR_MIN_POD_AGE_SECONDS:-300}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
  cat <<USAGE
Usage: CEDAR_K8S_NAMESPACE=<namespace> CEDAR_HELM_RELEASE=<release> $0
       $0 --self-test

Environment:
  CEDAR_K8S_NAMESPACE          Kubernetes namespace (default: cedargraph)
  CEDAR_HELM_RELEASE           Helm release / instance label (default: cedargraph)
  CEDAR_REQUIRE_NETWORKPOLICY  Require NetworkPolicy preflight, 1 or 0 (default: 1)
  CEDAR_REQUIRE_HELM_STATUS    Require Helm release status deployed, 1 or 0 (default: 1)
  CEDAR_RUN_RAFT_UPGRADE_GUARD Run --upgrade-guard, 1 or 0 (default: 0)
  CEDAR_MAX_POD_RESTARTS       Maximum restart count allowed per container (default: 0)
  CEDAR_EXPECTED_METAD_PODS    Expected MetaD pod count (default: 3)
  CEDAR_EXPECTED_STORAGED_PODS Expected StorageD pod count (default: 3)
  CEDAR_MIN_GRAPHD_PODS        Minimum GraphD pod count (default: 1)
  CEDAR_MIN_JWT_SECRET_BYTES   Minimum decoded JWT secret length in bytes (default: 32)
  CEDAR_MIN_TLS_DAYS           Minimum remaining TLS certificate validity in days (default: 30)
  CEDAR_CRITICAL_LOG_SINCE     kubectl logs --since window for critical diagnostics (default: 10m)
  CEDAR_MIN_POD_AGE_SECONDS    Minimum pod age before production gate passes (default: 300)
USAGE
}

validate_bool() {
  local name="$1"
  local value="$2"
  if [[ "${value}" != "0" && "${value}" != "1" ]]; then
    echo "${name} must be 0 or 1, got ${value}" >&2
    return 1
  fi
}

validate_non_negative_int() {
  local name="$1"
  local value="$2"
  if ! [[ "${value}" =~ ^[0-9]+$ ]]; then
    echo "${name} must be a non-negative integer, got ${value}" >&2
    return 1
  fi
}

validate_min_int() {
  local name="$1"
  local value="$2"
  local min="$3"
  validate_non_negative_int "${name}" "${value}" || return 1
  if (( value < min )); then
    echo "${name} must be at least ${min}, got ${value}" >&2
    return 1
  fi
}

validate_log_window() {
  local name="$1"
  local value="$2"
  if ! [[ "${value}" =~ ^([1-9][0-9]*)([smhd])$ ]]; then
    echo "${name} must match <positive-number><s|m|h|d>, got ${value}" >&2
    return 1
  fi
}

value_is_allowed() {
  local actual="$1"
  shift
  local expected
  for expected in "$@"; do
    if [[ "${actual}" == "${expected}" ]]; then
      return 0
    fi
  done
  return 1
}

text_has_flag_value() {
  local text="$1"
  local flag="$2"
  local expected="$3"
  printf '%s' "${text}" | grep -Eq -- "${flag}[^0-9]+${expected}"
}

decode_base64() {
  local output_path="$1"
  local payload
  payload="$(cat)"
  if printf "%s" "${payload}" | base64 --decode >"${output_path}" 2>/dev/null; then
    return 0
  fi
  if printf "%s" "${payload}" | base64 -d >"${output_path}" 2>/dev/null; then
    return 0
  fi
  printf "%s" "${payload}" | base64 -D >"${output_path}" 2>/dev/null
}

json_status_field() {
  ruby -rjson -e '
    doc = JSON.parse(STDIN.read)
    status = doc["status"] || doc.dig("info", "status")
    abort("missing Helm release status") if status.to_s.empty?
    puts status
  '
}

run_self_test() {
  validate_bool CEDAR_REQUIRE_NETWORKPOLICY 0
  validate_bool CEDAR_REQUIRE_NETWORKPOLICY 1
  validate_bool CEDAR_REQUIRE_HELM_STATUS 0
  validate_bool CEDAR_REQUIRE_HELM_STATUS 1
  validate_bool CEDAR_RUN_RAFT_UPGRADE_GUARD 0
  validate_bool CEDAR_RUN_RAFT_UPGRADE_GUARD 1
  validate_non_negative_int CEDAR_MAX_POD_RESTARTS 0
  validate_non_negative_int CEDAR_MAX_POD_RESTARTS 3
  validate_min_int CEDAR_EXPECTED_METAD_PODS 3 1
  validate_min_int CEDAR_EXPECTED_STORAGED_PODS 3 1
  validate_min_int CEDAR_MIN_GRAPHD_PODS 1 1
  validate_non_negative_int CEDAR_MIN_JWT_SECRET_BYTES 32
  validate_non_negative_int CEDAR_MIN_JWT_SECRET_BYTES 0
  validate_non_negative_int CEDAR_MIN_TLS_DAYS 30
  validate_non_negative_int CEDAR_MIN_TLS_DAYS 0
  validate_non_negative_int CEDAR_MIN_POD_AGE_SECONDS 300
  validate_log_window CEDAR_CRITICAL_LOG_SINCE 10m ||
    { echo "self-test failed: valid log window should match" >&2; exit 1; }
  if validate_bool CEDAR_REQUIRE_NETWORKPOLICY true >/dev/null 2>&1; then
    echo "self-test failed: invalid bool should fail" >&2
    exit 1
  fi
  if validate_non_negative_int CEDAR_MAX_POD_RESTARTS -1 >/dev/null 2>&1; then
    echo "self-test failed: negative restart threshold should fail" >&2
    exit 1
  fi
  if validate_non_negative_int "Pod restartCount" "" >/dev/null 2>&1; then
    echo "self-test failed: empty restart count should fail" >&2
    exit 1
  fi
  if validate_non_negative_int "Pod restartCount" "NaN" >/dev/null 2>&1; then
    echo "self-test failed: non-numeric restart count should fail" >&2
    exit 1
  fi
  if validate_min_int CEDAR_EXPECTED_METAD_PODS 0 1 >/dev/null 2>&1; then
    echo "self-test failed: zero MetaD pod baseline should fail" >&2
    exit 1
  fi
  if validate_min_int CEDAR_MIN_GRAPHD_PODS 0 1 >/dev/null 2>&1; then
    echo "self-test failed: zero GraphD pod baseline should fail" >&2
    exit 1
  fi
  if validate_log_window CEDAR_CRITICAL_LOG_SINCE 0m >/dev/null 2>&1; then
    echo "self-test failed: zero log window should fail" >&2
    exit 1
  fi
  if validate_log_window CEDAR_CRITICAL_LOG_SINCE 10x >/dev/null 2>&1; then
    echo "self-test failed: invalid log window unit should fail" >&2
    exit 1
  fi
  value_is_allowed health health 7000 ||
    { echo "self-test failed: named health port should be allowed" >&2; exit 1; }
  value_is_allowed 7000 health 7000 ||
    { echo "self-test failed: numeric health port should be allowed" >&2; exit 1; }
  if value_is_allowed query health 7000 >/dev/null 2>&1; then
    echo "self-test failed: wrong probe port should not be allowed" >&2
    exit 1
  fi
  text_has_flag_value '["--health_port","9668","--metrics_port","9667"]' '--health_port' 9668 ||
    { echo "self-test failed: JSON-array GraphD args should match health port" >&2; exit 1; }
  text_has_flag_value '--health_port "7000" --metrics_port "7001"' '--metrics_port' 7001 ||
    { echo "self-test failed: shell command StorageD args should match metrics port" >&2; exit 1; }
  if text_has_flag_value '["--health_port","9669"]' '--health_port' 9668 >/dev/null 2>&1; then
    echo "self-test failed: wrong explicit health port should not match" >&2
    exit 1
  fi
  tmp_decode="$(mktemp "${TMPDIR:-/tmp}/cedar-prod-gate-base64.XXXXXX")"
  if ! printf "cedar" | base64 | decode_base64 "${tmp_decode}"; then
    rm -f "${tmp_decode}"
    echo "self-test failed: base64 decoder should decode a valid payload" >&2
    exit 1
  fi
  if [[ "$(cat "${tmp_decode}")" != "cedar" ]]; then
    rm -f "${tmp_decode}"
    echo "self-test failed: base64 decoder returned wrong payload" >&2
    exit 1
  fi
  rm -f "${tmp_decode}"
  if [[ "$(printf '{"status":"deployed"}' | json_status_field)" != "deployed" ]]; then
    echo "self-test failed: Helm status JSON parser should extract deployed" >&2
    exit 1
  fi
  if [[ "$(printf '{"info":{"status":"deployed"}}' | json_status_field)" != "deployed" ]]; then
    echo "self-test failed: Helm v4 status JSON parser should extract info.status" >&2
    exit 1
  fi
  if printf '{}' | json_status_field >/dev/null 2>&1; then
    echo "self-test failed: Helm status JSON parser should fail closed on missing status" >&2
    exit 1
  fi
  if ! grep -q 'Failed to read Kubernetes object' "$0"; then
    echo "self-test failed: kubectl jsonpath helper must fail closed with a clear object read error" >&2
    exit 1
  fi
  echo "Kubernetes production preflight gate self-test passed"
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "${1:-}" == "--self-test" ]]; then
  run_self_test
  exit 0
fi

validate_bool CEDAR_REQUIRE_NETWORKPOLICY "${REQUIRE_NETWORKPOLICY}"
validate_bool CEDAR_REQUIRE_HELM_STATUS "${REQUIRE_HELM_STATUS}"
validate_bool CEDAR_RUN_RAFT_UPGRADE_GUARD "${RUN_UPGRADE_GUARD}"
validate_non_negative_int CEDAR_MAX_POD_RESTARTS "${MAX_POD_RESTARTS}"
validate_min_int CEDAR_EXPECTED_METAD_PODS "${EXPECTED_METAD_PODS}" 1
validate_min_int CEDAR_EXPECTED_STORAGED_PODS "${EXPECTED_STORAGED_PODS}" 1
validate_min_int CEDAR_MIN_GRAPHD_PODS "${MIN_GRAPHD_PODS}" 1
validate_non_negative_int CEDAR_MIN_JWT_SECRET_BYTES "${MIN_JWT_SECRET_BYTES}"
validate_non_negative_int CEDAR_MIN_TLS_DAYS "${MIN_TLS_DAYS}"
validate_non_negative_int CEDAR_MIN_POD_AGE_SECONDS "${MIN_POD_AGE_SECONDS}"
validate_log_window CEDAR_CRITICAL_LOG_SINCE "${CRITICAL_LOG_SINCE}"

command -v kubectl >/dev/null 2>&1 || {
  echo "kubectl is required" >&2
  exit 1
}

tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/cedar-k8s-production-gate.XXXXXX")"
trap 'rm -rf "${tmp_dir}"' EXIT

log_step() {
  echo "[STEP] $*"
}

split_pem_bundle() {
  local bundle_path="$1"
  local output_dir="$2"
  awk -v out="${output_dir}/cert-" '
    /-----BEGIN CERTIFICATE-----/ {
      n++
      file = sprintf("%s%03d.pem", out, n)
    }
    n > 0 {
      print > file
    }
    /-----END CERTIFICATE-----/ {
      close(file)
    }
  ' "${bundle_path}"
}

cert_has_dns_name() {
  local cert_path="$1"
  local dns_name="$2"
  openssl x509 -in "${cert_path}" -noout -ext subjectAltName 2>/dev/null |
    grep -F "DNS:${dns_name}" >/dev/null
}

kubectl_jsonpath() {
  local object="$1"
  local jsonpath="$2"
  local err_file="${tmp_dir}/kubectl-jsonpath-$(printf '%s' "${object}" | tr '/ :' '---').err"
  local output
  if ! output="$(kubectl get ${object} -n "${NAMESPACE}" -o jsonpath="${jsonpath}" 2>"${err_file}")"; then
    echo "Failed to read Kubernetes object ${object} in namespace=${NAMESPACE} with jsonpath=${jsonpath}" >&2
    cat "${err_file}" >&2
    exit 1
  fi
  printf "%s" "${output}"
}

require_jsonpath_value() {
  local object="$1"
  local jsonpath="$2"
  local expected="$3"
  local actual
  actual="$(kubectl_jsonpath "${object}" "${jsonpath}")"
  if [[ "${actual}" != "${expected}" ]]; then
    echo "${object} ${jsonpath} expected ${expected}, got ${actual:-<empty>}" >&2
    exit 1
  fi
}

require_jsonpath_value_in() {
  local object="$1"
  local jsonpath="$2"
  shift 2
  local actual expected
  actual="$(kubectl_jsonpath "${object}" "${jsonpath}")"
  value_is_allowed "${actual}" "$@" && return 0
  echo "${object} ${jsonpath} expected one of [$*], got ${actual:-<empty>}" >&2
  exit 1
}

kubectl_count() {
  local resource="$1"
  local label_selector="$2"
  local names
  names="$(kubectl_jsonpath "${resource} -l ${label_selector}" '{range .items[*]}{.metadata.name}{"\n"}{end}')"
  printf "%s\n" "${names}" | sed '/^[[:space:]]*$/d' | wc -l | tr -d ' '
}

kubectl_secret_data() {
  local secret_name="$1"
  local secret_key="$2"
  local secret_key_jsonpath="${secret_key//./\\.}"
  local output
  if ! output="$(kubectl get secret "${secret_name}" -n "${NAMESPACE}" \
      -o jsonpath="{.data.${secret_key_jsonpath}}" 2>"${tmp_dir}/secret-${secret_name}.err")"; then
    echo "Failed to read Kubernetes Secret ${secret_name} in namespace=${NAMESPACE}" >&2
    cat "${tmp_dir}/secret-${secret_name}.err" >&2
    exit 1
  fi
  printf "%s" "${output}"
}

selector="app.kubernetes.io/instance=${INSTANCE}"
now_epoch="$(date -u +%s)"

log_step "Kubernetes API"
kubectl get namespace "${NAMESPACE}" >/dev/null

if [[ "${REQUIRE_HELM_STATUS}" == "1" ]]; then
  log_step "Helm release status"
  command -v helm >/dev/null 2>&1 || {
    echo "helm is required when CEDAR_REQUIRE_HELM_STATUS=1" >&2
    exit 1
  }
  command -v ruby >/dev/null 2>&1 || {
    echo "ruby is required to parse helm status JSON" >&2
    exit 1
  }
  helm_status_json="${tmp_dir}/helm-status.json"
  if ! helm status "${INSTANCE}" -n "${NAMESPACE}" -o json >"${helm_status_json}" 2>"${tmp_dir}/helm-status.err"; then
    echo "Failed to read Helm release status for ${INSTANCE} in namespace=${NAMESPACE}" >&2
    cat "${tmp_dir}/helm-status.err" >&2
    exit 1
  fi
  if ! helm_status="$(json_status_field <"${helm_status_json}")"; then
    echo "Failed to parse Helm status JSON for ${INSTANCE} in namespace=${NAMESPACE}" >&2
    cat "${helm_status_json}" >&2
    exit 1
  fi
  if [[ "${helm_status}" != "deployed" ]]; then
    echo "Helm release ${INSTANCE} in namespace=${NAMESPACE} must be deployed, got ${helm_status:-<empty>}" >&2
    exit 1
  fi
else
  echo "[WARN] Helm status check skipped by CEDAR_REQUIRE_HELM_STATUS=0"
fi

log_step "Pods are Running and ready"
pods="$(kubectl get pods -n "${NAMESPACE}" -l "${selector}" \
  -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.phase}{"\n"}{end}')"
if [[ -z "${pods}" ]]; then
  echo "No CedarGraph pods found in namespace=${NAMESPACE} selector=${selector}" >&2
  exit 1
fi
metad_pods="$(kubectl_count pods "${selector},app.kubernetes.io/component=metad")"
storaged_pods="$(kubectl_count pods "${selector},app.kubernetes.io/component=storaged")"
graphd_pods="$(kubectl_count pods "${selector},app.kubernetes.io/component=graphd")"
if [[ "${metad_pods}" != "${EXPECTED_METAD_PODS}" ]]; then
  echo "Expected exactly ${EXPECTED_METAD_PODS} MetaD pods, found ${metad_pods}" >&2
  exit 1
fi
if [[ "${storaged_pods}" != "${EXPECTED_STORAGED_PODS}" ]]; then
  echo "Expected exactly ${EXPECTED_STORAGED_PODS} StorageD pods, found ${storaged_pods}" >&2
  exit 1
fi
if (( graphd_pods < MIN_GRAPHD_PODS )); then
  echo "Expected at least ${MIN_GRAPHD_PODS} GraphD pod(s), found ${graphd_pods}" >&2
  exit 1
fi
while read -r pod phase; do
  [[ -n "${pod}" ]] || continue
  if [[ "${phase}" != "Running" ]]; then
    echo "Pod ${pod} is not Running: phase=${phase}" >&2
    exit 1
  fi
  pod_start_time="$(kubectl_jsonpath "pod ${pod}" '{.status.startTime}')"
  if [[ -z "${pod_start_time}" ]]; then
    echo "Pod ${pod} has no status.startTime" >&2
    exit 1
  fi
  if ! pod_start_epoch="$(date -u -j -f '%Y-%m-%dT%H:%M:%SZ' "${pod_start_time}" '+%s' 2>/dev/null)"; then
    if ! pod_start_epoch="$(date -u -d "${pod_start_time}" '+%s' 2>/dev/null)"; then
      echo "Unable to parse Pod ${pod} startTime=${pod_start_time}" >&2
      exit 1
    fi
  fi
  pod_age=$((now_epoch - pod_start_epoch))
  if (( pod_age < MIN_POD_AGE_SECONDS )); then
    echo "Pod ${pod} age ${pod_age}s is below CEDAR_MIN_POD_AGE_SECONDS=${MIN_POD_AGE_SECONDS}s" >&2
    exit 1
  fi
  container_statuses="$(kubectl get pod "${pod}" -n "${NAMESPACE}" \
    -o jsonpath='{range .status.containerStatuses[*]}{.name}{" "}{.ready}{" "}{.restartCount}{"\n"}{end}')"
  if [[ -z "${container_statuses}" ]]; then
    echo "Pod ${pod} has no container statuses" >&2
    exit 1
  fi
  init_container_statuses="$(kubectl get pod "${pod}" -n "${NAMESPACE}" \
    -o jsonpath='{range .status.initContainerStatuses[*]}{.name}{" "}{.state.terminated.reason}{" "}{.restartCount}{"\n"}{end}')"
  while read -r init_container reason restart; do
    [[ -n "${init_container}" ]] || continue
    if [[ "${reason}" != "Completed" ]]; then
      echo "Pod ${pod} initContainer ${init_container} is not Completed: reason=${reason:-<empty>}" >&2
      exit 1
    fi
    if ! validate_non_negative_int "Pod ${pod} initContainer ${init_container} restartCount" "${restart:-}"; then
      echo "Pod ${pod} initContainer ${init_container} does not report a numeric restartCount" >&2
      exit 1
    fi
    if (( restart > MAX_POD_RESTARTS )); then
      echo "Pod ${pod} initContainer ${init_container} restarts ${restart} exceed CEDAR_MAX_POD_RESTARTS=${MAX_POD_RESTARTS}" >&2
      exit 1
    fi
  done <<< "${init_container_statuses}"
  while read -r container ready restart; do
    [[ -n "${container}" ]] || continue
    if [[ "${ready}" != "true" ]]; then
      echo "Pod ${pod} container ${container} is not ready" >&2
      exit 1
    fi
    if ! validate_non_negative_int "Pod ${pod} container ${container} restartCount" "${restart:-}"; then
      echo "Pod ${pod} container ${container} does not report a numeric restartCount" >&2
      exit 1
    fi
    if (( restart > MAX_POD_RESTARTS )); then
      echo "Pod ${pod} container ${container} restarts ${restart} exceed CEDAR_MAX_POD_RESTARTS=${MAX_POD_RESTARTS}" >&2
      exit 1
    fi
  done <<< "${container_statuses}"
done <<< "${pods}"

log_step "PersistentVolumeClaims"
for component in metad storaged; do
  if [[ "${component}" == "metad" ]]; then
    expected="${EXPECTED_METAD_PODS}"
  else
    expected="${EXPECTED_STORAGED_PODS}"
  fi
  pvc_list="$(kubectl get pvc -n "${NAMESPACE}" \
    -l "${selector},app.kubernetes.io/component=${component}" \
    -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.phase}{" "}{.spec.accessModes[0]}{" "}{.status.capacity.storage}{" "}{.spec.storageClassName}{"\n"}{end}')"
  pvc_count="$(printf "%s\n" "${pvc_list}" | sed '/^[[:space:]]*$/d' | wc -l | tr -d ' ')"
  if [[ "${pvc_count}" != "${expected}" ]]; then
    echo "Expected exactly ${expected} ${component} PVC(s), found ${pvc_count}" >&2
    exit 1
  fi
  while read -r pvc phase access_mode capacity storage_class; do
    [[ -n "${pvc}" ]] || continue
    if [[ "${phase}" != "Bound" ]]; then
      echo "PVC ${pvc} for component=${component} is not Bound: phase=${phase}" >&2
      exit 1
    fi
    if [[ "${access_mode}" != "ReadWriteOnce" ]]; then
      echo "PVC ${pvc} for component=${component} should use ReadWriteOnce, got ${access_mode:-<empty>}" >&2
      exit 1
    fi
    if [[ -z "${capacity}" || "${capacity}" == "<none>" ]]; then
      echo "PVC ${pvc} for component=${component} has no reported capacity" >&2
      exit 1
    fi
    if [[ -z "${storage_class}" || "${storage_class}" == "<none>" ]]; then
      echo "PVC ${pvc} for component=${component} has no storageClassName" >&2
      exit 1
    fi
  done <<< "${pvc_list}"
done

log_step "Runtime service/probe contract"
metad_sts="$(kubectl get statefulset -n "${NAMESPACE}" \
  -l "${selector},app.kubernetes.io/component=metad" \
  -o jsonpath='{.items[0].metadata.name}')"
storaged_sts="$(kubectl get statefulset -n "${NAMESPACE}" \
  -l "${selector},app.kubernetes.io/component=storaged" \
  -o jsonpath='{.items[0].metadata.name}')"
graphd_deployment_contract="$(kubectl get deploy -n "${NAMESPACE}" \
  -l "${selector},app.kubernetes.io/component=graphd" \
  -o jsonpath='{.items[0].metadata.name}')"
[[ -n "${metad_sts}" ]] || { echo "Missing MetaD StatefulSet for selector=${selector}" >&2; exit 1; }
[[ -n "${storaged_sts}" ]] || { echo "Missing StorageD StatefulSet for selector=${selector}" >&2; exit 1; }
[[ -n "${graphd_deployment_contract}" ]] || { echo "Missing GraphD Deployment for selector=${selector}" >&2; exit 1; }

require_jsonpath_value "statefulset/${metad_sts}" '{.spec.template.spec.containers[?(@.name=="metad")].ports[?(@.name=="raft")].containerPort}' "9559"
require_jsonpath_value "statefulset/${metad_sts}" '{.spec.template.spec.containers[?(@.name=="metad")].ports[?(@.name=="grpc")].containerPort}' "10559"
require_jsonpath_value "statefulset/${metad_sts}" '{.spec.template.spec.containers[?(@.name=="metad")].livenessProbe.tcpSocket.port}' "10559"
require_jsonpath_value "statefulset/${metad_sts}" '{.spec.template.spec.containers[?(@.name=="metad")].readinessProbe.tcpSocket.port}' "10559"

require_jsonpath_value "statefulset/${storaged_sts}" '{.spec.template.spec.containers[?(@.name=="storaged")].ports[?(@.name=="storaged")].containerPort}' "9779"
require_jsonpath_value "statefulset/${storaged_sts}" '{.spec.template.spec.containers[?(@.name=="storaged")].ports[?(@.name=="health")].containerPort}' "7000"
require_jsonpath_value "statefulset/${storaged_sts}" '{.spec.template.spec.containers[?(@.name=="storaged")].ports[?(@.name=="metrics")].containerPort}' "7001"
require_jsonpath_value_in "statefulset/${storaged_sts}" '{.spec.template.spec.containers[?(@.name=="storaged")].livenessProbe.tcpSocket.port}' "health" "7000"
require_jsonpath_value_in "statefulset/${storaged_sts}" '{.spec.template.spec.containers[?(@.name=="storaged")].readinessProbe.tcpSocket.port}' "health" "7000"
storaged_command="$(kubectl get "statefulset/${storaged_sts}" -n "${NAMESPACE}" \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="storaged")].command}')"
if ! text_has_flag_value "${storaged_command}" '--health_port' 7000 ||
    ! text_has_flag_value "${storaged_command}" '--metrics_port' 7001; then
  echo "StorageD command must pass --health_port 7000 and --metrics_port 7001 explicitly" >&2
  exit 1
fi

require_jsonpath_value "deployment/${graphd_deployment_contract}" '{.spec.template.spec.containers[?(@.name=="graphd")].ports[?(@.name=="query")].containerPort}' "9669"
require_jsonpath_value "deployment/${graphd_deployment_contract}" '{.spec.template.spec.containers[?(@.name=="graphd")].ports[?(@.name=="health")].containerPort}' "9668"
require_jsonpath_value "deployment/${graphd_deployment_contract}" '{.spec.template.spec.containers[?(@.name=="graphd")].ports[?(@.name=="metrics")].containerPort}' "9667"
require_jsonpath_value_in "deployment/${graphd_deployment_contract}" '{.spec.template.spec.containers[?(@.name=="graphd")].livenessProbe.tcpSocket.port}' "health" "9668"
require_jsonpath_value_in "deployment/${graphd_deployment_contract}" '{.spec.template.spec.containers[?(@.name=="graphd")].readinessProbe.tcpSocket.port}' "health" "9668"
graphd_args="$(kubectl get "deployment/${graphd_deployment_contract}" -n "${NAMESPACE}" \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="graphd")].args}')"
if ! text_has_flag_value "${graphd_args}" '--health_port' 9668 ||
    ! text_has_flag_value "${graphd_args}" '--metrics_port' 9667; then
  echo "GraphD args must pass --health_port 9668 and --metrics_port 9667 explicitly, got: ${graphd_args}" >&2
  exit 1
fi

log_step "GraphD auth and TLS secrets"
graphd_deployment="$(kubectl get deploy -n "${NAMESPACE}" \
  -l "${selector},app.kubernetes.io/component=graphd" \
  -o jsonpath='{.items[0].metadata.name}')"
if [[ -z "${graphd_deployment}" ]]; then
  echo "Missing GraphD Deployment for selector=${selector}" >&2
  exit 1
fi

graphd_env_refs="$(kubectl get deploy "${graphd_deployment}" -n "${NAMESPACE}" \
  -o jsonpath='{range .spec.template.spec.containers[?(@.name=="graphd")].env[*]}{.name}{" "}{.valueFrom.secretKeyRef.name}{" "}{.valueFrom.secretKeyRef.key}{" "}{.valueFrom.secretKeyRef.optional}{"\n"}{end}')"
auth_refs="$(printf "%s\n" "${graphd_env_refs}" | grep '^CEDAR_GRAPHD_AUTH_' || true)"
if [[ -z "${auth_refs}" ]]; then
  echo "GraphD Deployment ${graphd_deployment} does not reference auth secrets" >&2
  exit 1
fi
required_auth_envs="CEDAR_GRAPHD_AUTH_JWT_SECRET CEDAR_GRAPHD_AUTH_USER CEDAR_GRAPHD_AUTH_PASSWORD"
for env_name in ${required_auth_envs}; do
  ref_line="$(printf "%s\n" "${auth_refs}" | awk -v name="${env_name}" '$1 == name {print; exit}')"
  if [[ -z "${ref_line}" ]]; then
    echo "GraphD Deployment ${graphd_deployment} is missing required auth env ${env_name}" >&2
    exit 1
  fi
  read -r _ secret_name secret_key optional <<< "${ref_line}"
  if [[ -z "${secret_name}" || -z "${secret_key}" ]]; then
    echo "GraphD auth env ${env_name} must reference a concrete Secret key" >&2
    exit 1
  fi
  if [[ "${optional}" == "true" ]]; then
    echo "GraphD auth env ${env_name} must not be optional" >&2
    exit 1
  fi
  secret_value_b64="$(kubectl_secret_data "${secret_name}" "${secret_key}")"
  if [[ -z "${secret_value_b64}" ]]; then
    echo "GraphD auth Secret ${secret_name} is missing key ${secret_key}" >&2
    exit 1
  fi
  decoded_secret_path="${tmp_dir}/${secret_name}-${secret_key}"
  if ! printf "%s" "${secret_value_b64}" | decode_base64 "${decoded_secret_path}"; then
    echo "GraphD auth Secret ${secret_name} key ${secret_key} is not valid base64 data" >&2
    exit 1
  fi
  decoded_secret_bytes="$(wc -c <"${decoded_secret_path}" | tr -d ' ')"
  if (( decoded_secret_bytes == 0 )); then
    echo "GraphD auth Secret ${secret_name} key ${secret_key} is empty after decoding" >&2
    exit 1
  fi
  if [[ "${env_name}" == "CEDAR_GRAPHD_AUTH_JWT_SECRET" && "${MIN_JWT_SECRET_BYTES}" != "0" ]] &&
      (( decoded_secret_bytes < MIN_JWT_SECRET_BYTES )); then
    echo "GraphD JWT Secret ${secret_name}/${secret_key} is ${decoded_secret_bytes} bytes; require at least CEDAR_MIN_JWT_SECRET_BYTES=${MIN_JWT_SECRET_BYTES}" >&2
    exit 1
  fi
done

tls_enabled="$(kubectl get deploy "${graphd_deployment}" -n "${NAMESPACE}" \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="graphd")].env[?(@.name=="CEDAR_GRPC_TLS_ENABLED")].value}')"
tls_secret="$(kubectl get deploy "${graphd_deployment}" -n "${NAMESPACE}" \
  -o jsonpath='{.spec.template.spec.volumes[?(@.name=="graphd-tls")].secret.secretName}')"
tls_optional="$(kubectl get deploy "${graphd_deployment}" -n "${NAMESPACE}" \
  -o jsonpath='{.spec.template.spec.volumes[?(@.name=="graphd-tls")].secret.optional}')"
if [[ "${tls_enabled}" == "1" ]]; then
  command -v openssl >/dev/null 2>&1 || {
    echo "openssl is required when GraphD TLS is enabled" >&2
    exit 1
  }
  if [[ -z "${tls_secret}" ]]; then
    echo "GraphD TLS is enabled but no graphd-tls Secret volume is configured" >&2
    exit 1
  fi
  if [[ "${tls_optional}" == "true" ]]; then
    echo "GraphD TLS Secret volume must not be optional when TLS is enabled" >&2
    exit 1
  fi
  for tls_key in tls.crt tls.key ca.crt; do
    tls_value_b64="$(kubectl_secret_data "${tls_secret}" "${tls_key}")"
    if [[ -z "${tls_value_b64}" ]]; then
      echo "GraphD TLS Secret ${tls_secret} is missing or has empty key ${tls_key}" >&2
      exit 1
    fi
    tls_path="${tmp_dir}/${tls_secret}-${tls_key}"
    if ! printf "%s" "${tls_value_b64}" | decode_base64 "${tls_path}"; then
      echo "GraphD TLS Secret ${tls_secret} key ${tls_key} is not valid base64 data" >&2
      exit 1
    fi
    if [[ "${tls_key}" == "tls.crt" ]]; then
      if ! openssl x509 -in "${tls_path}" -noout >/dev/null 2>&1; then
        echo "GraphD TLS Secret ${tls_secret} key ${tls_key} is not a valid X.509 certificate" >&2
        exit 1
      fi
	      if [[ "${MIN_TLS_DAYS}" != "0" ]]; then
	        min_tls_seconds=$((MIN_TLS_DAYS * 24 * 60 * 60))
	        if ! openssl x509 -in "${tls_path}" -checkend "${min_tls_seconds}" -noout >/dev/null 2>&1; then
	          not_after="$(openssl x509 -in "${tls_path}" -noout -enddate 2>/dev/null | sed 's/^notAfter=//')"
	          echo "GraphD TLS Secret ${tls_secret} key ${tls_key} expires too soon: notAfter=${not_after:-<unknown>}; require at least CEDAR_MIN_TLS_DAYS=${MIN_TLS_DAYS} day(s)" >&2
	          exit 1
	        fi
	      fi
	      fullname="${graphd_deployment%-graphd}"
	      required_dns_names=(
	        "${fullname}-graphd"
	        "${fullname}-graphd.${NAMESPACE}"
	        "${fullname}-metad"
	        "${fullname}-metad.${NAMESPACE}"
	        "${fullname}-storaged"
	        "${fullname}-storaged.${NAMESPACE}"
	      )
	      for ((i = 0; i < EXPECTED_METAD_PODS; i++)); do
	        required_dns_names+=(
	          "${fullname}-metad-${i}.${fullname}-metad"
	          "${fullname}-metad-${i}.${fullname}-metad.${NAMESPACE}"
	          "${fullname}-metad-${i}.${fullname}-metad.${NAMESPACE}.svc"
	        )
	      done
	      for ((i = 0; i < EXPECTED_STORAGED_PODS; i++)); do
	        required_dns_names+=(
	          "${fullname}-storaged-${i}.${fullname}-storaged"
	          "${fullname}-storaged-${i}.${fullname}-storaged.${NAMESPACE}"
	          "${fullname}-storaged-${i}.${fullname}-storaged.${NAMESPACE}.svc"
	        )
	      done
	      for dns_name in "${required_dns_names[@]}"; do
	        if ! cert_has_dns_name "${tls_path}" "${dns_name}"; then
	          echo "GraphD TLS Secret ${tls_secret} key tls.crt SAN is missing DNS:${dns_name}" >&2
	          exit 1
	        fi
	      done
	    elif [[ "${tls_key}" == "ca.crt" ]]; then
      ca_split_dir="${tmp_dir}/${tls_secret}-ca-bundle"
      mkdir -p "${ca_split_dir}"
      split_pem_bundle "${tls_path}" "${ca_split_dir}"
      ca_cert_count="$(find "${ca_split_dir}" -type f -name 'cert-*.pem' | wc -l | tr -d ' ')"
      if [[ "${ca_cert_count}" == "0" ]]; then
        echo "GraphD TLS Secret ${tls_secret} key ca.crt does not contain any PEM certificate" >&2
        exit 1
      fi
      ca_valid_for_threshold=0
      while IFS= read -r ca_cert; do
        if ! openssl x509 -in "${ca_cert}" -noout >/dev/null 2>&1; then
          echo "GraphD TLS Secret ${tls_secret} key ca.crt contains an invalid X.509 certificate" >&2
          exit 1
        fi
        if [[ "${MIN_TLS_DAYS}" != "0" ]]; then
          min_tls_seconds=$((MIN_TLS_DAYS * 24 * 60 * 60))
          if openssl x509 -in "${ca_cert}" -checkend "${min_tls_seconds}" -noout >/dev/null 2>&1; then
            ca_valid_for_threshold=$((ca_valid_for_threshold + 1))
          else
            not_after="$(openssl x509 -in "${ca_cert}" -noout -enddate 2>/dev/null | sed 's/^notAfter=//')"
            echo "[WARN] GraphD TLS Secret ${tls_secret} key ca.crt contains a certificate expiring before CEDAR_MIN_TLS_DAYS=${MIN_TLS_DAYS}: notAfter=${not_after:-<unknown>}" >&2
          fi
        fi
      done < <(find "${ca_split_dir}" -type f -name 'cert-*.pem' | sort)
      if [[ "${MIN_TLS_DAYS}" != "0" && "${ca_valid_for_threshold}" == "0" ]]; then
        echo "GraphD TLS Secret ${tls_secret} key ca.crt has no certificate valid for at least CEDAR_MIN_TLS_DAYS=${MIN_TLS_DAYS} day(s)" >&2
        exit 1
      fi
    fi
  done

  for component in metad storaged; do
    sts_name="$(kubectl get statefulset -n "${NAMESPACE}" \
      -l "${selector},app.kubernetes.io/component=${component}" \
      -o jsonpath='{.items[0].metadata.name}')"
    if [[ -z "${sts_name}" ]]; then
      echo "Missing ${component} StatefulSet for selector=${selector}" >&2
      exit 1
    fi
    component_tls_enabled="$(kubectl get statefulset "${sts_name}" -n "${NAMESPACE}" \
      -o jsonpath="{.spec.template.spec.containers[?(@.name==\"${component}\")].env[?(@.name==\"CEDAR_GRPC_TLS_ENABLED\")].value}")"
    component_tls_secret="$(kubectl get statefulset "${sts_name}" -n "${NAMESPACE}" \
      -o jsonpath='{.spec.template.spec.volumes[?(@.name=="grpc-tls")].secret.secretName}')"
    component_tls_optional="$(kubectl get statefulset "${sts_name}" -n "${NAMESPACE}" \
      -o jsonpath='{.spec.template.spec.volumes[?(@.name=="grpc-tls")].secret.optional}')"
    if [[ "${component_tls_enabled}" != "1" ]]; then
      echo "${component} StatefulSet ${sts_name} must enable CEDAR_GRPC_TLS_ENABLED=1 when GraphD TLS is enabled, got ${component_tls_enabled:-<empty>}" >&2
      exit 1
    fi
    if [[ "${component_tls_secret}" != "${tls_secret}" ]]; then
      echo "${component} StatefulSet ${sts_name} TLS Secret must match GraphD TLS Secret ${tls_secret}, got ${component_tls_secret:-<empty>}" >&2
      exit 1
    fi
    if [[ "${component_tls_optional}" == "true" ]]; then
      echo "${component} StatefulSet ${sts_name} TLS Secret volume must not be optional when TLS is enabled" >&2
      exit 1
    fi
  done
else
  echo "GraphD TLS must be enabled for production gate, got CEDAR_GRPC_TLS_ENABLED=${tls_enabled:-<empty>}" >&2
  exit 1
fi

log_step "PDB coverage"
for component in metad storaged graphd; do
  pdb_name="$(kubectl_jsonpath "pdb -l ${selector},app.kubernetes.io/component=${component}" '{.items[0].metadata.name}')"
  if [[ -z "${pdb_name}" ]]; then
    echo "Missing PDB for component=${component}" >&2
    exit 1
  fi
  pdb_selector_instance="$(kubectl_jsonpath "pdb ${pdb_name}" '{.spec.selector.matchLabels.app\.kubernetes\.io/instance}')"
  pdb_selector_name="$(kubectl_jsonpath "pdb ${pdb_name}" '{.spec.selector.matchLabels.app\.kubernetes\.io/name}')"
  pdb_selector_component="$(kubectl_jsonpath "pdb ${pdb_name}" '{.spec.selector.matchLabels.app\.kubernetes\.io/component}')"
  if [[ "${pdb_selector_instance}" != "${INSTANCE}" || "${pdb_selector_name}" != "cedargraph" || "${pdb_selector_component}" != "${component}" ]]; then
    echo "PDB ${pdb_name} selector does not target ${INSTANCE}/cedargraph/${component}" >&2
    exit 1
  fi
  expected_pods="$(kubectl_jsonpath "pdb ${pdb_name}" '{.status.expectedPods}')"
  current_healthy="$(kubectl_jsonpath "pdb ${pdb_name}" '{.status.currentHealthy}')"
  desired_healthy="$(kubectl_jsonpath "pdb ${pdb_name}" '{.status.desiredHealthy}')"
  if ! validate_min_int "PDB ${pdb_name} status.expectedPods" "${expected_pods:-}" 1; then
    echo "PDB ${pdb_name} does not report a positive expectedPods count" >&2
    exit 1
  fi
  if ! validate_non_negative_int "PDB ${pdb_name} status.currentHealthy" "${current_healthy:-}"; then
    echo "PDB ${pdb_name} does not report a numeric currentHealthy value" >&2
    exit 1
  fi
  if ! validate_non_negative_int "PDB ${pdb_name} status.desiredHealthy" "${desired_healthy:-}"; then
    echo "PDB ${pdb_name} does not report a numeric desiredHealthy value" >&2
    exit 1
  fi
  if (( current_healthy < desired_healthy )); then
    echo "PDB ${pdb_name} is not healthy: currentHealthy=${current_healthy}, desiredHealthy=${desired_healthy}" >&2
    exit 1
  fi
done

log_step "MetaD update strategy"
metad_strategy="$(kubectl_jsonpath "statefulset -l ${selector},app.kubernetes.io/component=metad" '{.items[0].spec.updateStrategy.type}')"
if [[ "${metad_strategy}" != "OnDelete" ]]; then
  echo "MetaD StatefulSet updateStrategy must be OnDelete, got ${metad_strategy}" >&2
  exit 1
fi

log_step "Raft identity"
CEDAR_K8S_NAMESPACE="${NAMESPACE}" CEDAR_HELM_RELEASE="${INSTANCE}" \
  "${SCRIPT_DIR}/preflight_k8s_raft_identity.sh"

if [[ "${RUN_UPGRADE_GUARD}" == "1" ]]; then
  log_step "Raft upgrade guard"
  CEDAR_K8S_NAMESPACE="${NAMESPACE}" CEDAR_HELM_RELEASE="${INSTANCE}" \
    "${SCRIPT_DIR}/preflight_k8s_raft_identity.sh" --upgrade-guard
fi

if [[ "${REQUIRE_NETWORKPOLICY}" == "1" ]]; then
  log_step "NetworkPolicy"
  CEDAR_K8S_NAMESPACE="${NAMESPACE}" CEDAR_HELM_RELEASE="${INSTANCE}" \
    "${SCRIPT_DIR}/preflight_k8s_networkpolicy.sh"
else
  echo "[WARN] NetworkPolicy preflight skipped by CEDAR_REQUIRE_NETWORKPOLICY=0"
fi

log_step "Recent critical logs"
log_hits="${tmp_dir}/critical_log_hits"
logs_file="${tmp_dir}/recent-critical-logs.txt"
if ! kubectl logs -n "${NAMESPACE}" -l "${selector}" --all-containers --since="${CRITICAL_LOG_SINCE}" >"${logs_file}" 2>&1; then
  echo "Failed to read recent CedarGraph logs for selector=${selector} namespace=${NAMESPACE}" >&2
  cat "${logs_file}" >&2
  exit 1
fi
if grep -E "FATAL|CrashLoop|segmentation|panic|can't do pre_vote|wrong version|Handshake|SSL|Permission denied" "${logs_file}" >"${log_hits}"; then
  echo "Recent CedarGraph logs contain critical diagnostics:" >&2
  cat "${log_hits}" >&2
  exit 1
fi

echo "Kubernetes production preflight gate passed"
