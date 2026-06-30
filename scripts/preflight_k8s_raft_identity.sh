#!/bin/bash
# =============================================================================
# CedarGraph Kubernetes Raft Identity Preflight
# =============================================================================
# Checks a live Kubernetes release before upgrade/restart. CedarGraph's current
# braft integration persists resolved MetaD Pod IPs in Raft log/meta files. If a
# StatefulSet Pod is recreated with a different IP while reusing the old PVC,
# MetaD can fail elections because its current identity is no longer present in
# the persisted Raft configuration.
#
# This script does not mutate the cluster. It fails early when live MetaD Pod IPs
# and persisted Raft peer IPs diverge, so production rollout can stop before the
# cluster enters a CrashLoop/no-leader state.
# =============================================================================

set -euo pipefail

NAMESPACE="${CEDAR_K8S_NAMESPACE:-cedargraph}"
INSTANCE="${CEDAR_HELM_RELEASE:-cedargraph}"
COMPONENT_LABEL="${CEDAR_METAD_COMPONENT_LABEL:-app.kubernetes.io/component=metad}"
INSTANCE_LABEL="${CEDAR_INSTANCE_LABEL:-app.kubernetes.io/instance=${INSTANCE}}"
RAFT_PORT="${CEDAR_METAD_RAFT_PORT:-9559}"
DATA_DIR="${CEDAR_METAD_DATA_DIR:-/data/meta}"
LOG_TAIL="${CEDAR_RAFT_LOG_TAIL:-400}"

usage() {
  cat <<USAGE
Usage: CEDAR_K8S_NAMESPACE=<namespace> CEDAR_HELM_RELEASE=<release> $0
       CEDAR_K8S_NAMESPACE=<namespace> CEDAR_HELM_RELEASE=<release> $0 --upgrade-guard
       CEDAR_K8S_NAMESPACE=<namespace> CEDAR_HELM_RELEASE=<release> $0 --patch-ondelete
       CEDAR_K8S_NAMESPACE=<namespace> CEDAR_HELM_RELEASE=<release> $0 --collect-evidence <dir>
       $0 --verify-evidence <dir>
       $0 --plan-recovery <dir>
       $0 --self-test

Environment:
  CEDAR_K8S_NAMESPACE        Kubernetes namespace (default: cedargraph)
  CEDAR_HELM_RELEASE         Helm release / instance label (default: cedargraph)
  CEDAR_METAD_RAFT_PORT      MetaD Raft port (default: 9559)
  CEDAR_METAD_DATA_DIR       MetaD data directory inside pod (default: /data/meta)
  CEDAR_RAFT_LOG_TAIL        Log lines scanned per pod (default: 400)
USAGE
}

die() {
  echo "$*" >&2
  exit 1
}

validate_positive_int() {
  local name="$1"
  local value="$2"
  [[ "${value}" =~ ^[0-9]+$ && "${value}" -ge 1 ]] ||
    die "${name} must be a positive integer, got ${value}"
}

validate_port() {
  local name="$1"
  local value="$2"
  [[ "${value}" =~ ^[0-9]+$ && "${value}" -ge 1 && "${value}" -le 65535 ]] ||
    die "${name} must be an integer port in 1..65535, got ${value}"
}

has_ipv4_persisted_peers() {
  local persisted_file="$1"
  [[ -s "${persisted_file}" ]]
}

compare_raft_identity_sets() {
  local current_file="$1"
  local persisted_file="$2"
  local work_dir="$3"
  local missing_current="${work_dir}/missing_current"
  local stale_persisted="${work_dir}/stale_persisted"

  sort -u "${current_file}" -o "${current_file}"
  sort -u "${persisted_file}" -o "${persisted_file}"

  if ! has_ipv4_persisted_peers "${persisted_file}"; then
    echo "No persisted MetaD Raft peer IPs found; cluster may not have committed Raft configuration yet" >&2
    return 1
  fi

  comm -23 "${current_file}" "${persisted_file}" > "${missing_current}"
  comm -13 "${current_file}" "${persisted_file}" > "${stale_persisted}"

  if [[ -s "${missing_current}" || -s "${stale_persisted}" ]]; then
    echo "MetaD Raft identity mismatch detected." >&2
    echo "Current MetaD Pod IPs:" >&2
    sed 's/^/  /' "${current_file}" >&2
    echo "Persisted MetaD Raft peer IPs:" >&2
    sed 's/^/  /' "${persisted_file}" >&2
    if [[ -s "${missing_current}" ]]; then
      echo "Current Pod IPs missing from persisted Raft conf:" >&2
      sed 's/^/  /' "${missing_current}" >&2
    fi
    if [[ -s "${stale_persisted}" ]]; then
      echo "Stale persisted Raft peer IPs not present in current Pods:" >&2
      sed 's/^/  /' "${stale_persisted}" >&2
    fi
    echo "Stop rollout and follow the MetaD Raft PVC/conf recovery procedure before continuing." >&2
    return 1
  fi
}

raft_log_has_identity_errors() {
  local log_file="$1"
  grep -E "can't do pre_vote|not in .*:9559|Failed to init braft node|MetaRaftStateMachine error" "${log_file}"
}

write_evidence_summary() {
  local evidence_dir="$1"
  local status="$2"
  local reason="$3"
  local current_file="$4"
  local persisted_file="$5"

  [[ -n "${evidence_dir}" ]] || return 0
  {
    echo "MetaD Raft identity preflight ${status}."
    echo
    echo "Reason:"
    echo "  ${reason}"
    echo
    echo "Current MetaD Pod IPs:"
    if [[ -s "${current_file}" ]]; then
      sed 's/^/  /' "${current_file}"
    else
      echo "  <empty>"
    fi
    echo
    echo "Persisted MetaD Raft peer IPs:"
    if [[ -s "${persisted_file}" ]]; then
      sed 's/^/  /' "${persisted_file}"
    else
      echo "  <empty>"
    fi
    echo
    echo "Upgrade guard note:"
    echo "  If persisted peers are Pod IPs, do not run a normal rolling MetaD upgrade."
    echo "  Use a documented maintenance-window migration, rebuild, or restore plan."
  } > "${evidence_dir}/SUMMARY.txt"
}

verify_evidence_bundle() {
  local evidence_dir="$1"
  [[ -d "${evidence_dir}" ]] || die "Evidence directory does not exist: ${evidence_dir}"

  local required_files=(
    README.txt
    SUMMARY.txt
    current-metad-pod-ips.txt
    persisted-raft-peer-ips.txt
    metad-pods.txt
    metad-pods-wide.txt
    metad-statefulset.yaml
    metad-pvc-wide.txt
    metad-pvc.yaml
    metad-logs-tail.txt
    raft-identity-log-hits.txt
  )
  local file
  for file in "${required_files[@]}"; do
    [[ -e "${evidence_dir}/${file}" ]] || die "Evidence bundle missing ${file}"
  done

  [[ -s "${evidence_dir}/current-metad-pod-ips.txt" ]] ||
    die "Evidence bundle has empty current-metad-pod-ips.txt"
  [[ -s "${evidence_dir}/persisted-raft-peer-ips.txt" ]] ||
    die "Evidence bundle has empty persisted-raft-peer-ips.txt"

  grep -Eq '^MetaD Raft identity preflight (passed|failed)\.$' "${evidence_dir}/SUMMARY.txt" ||
    die "Evidence SUMMARY.txt must contain passed or failed status"
  grep -q '^Reason:$' "${evidence_dir}/SUMMARY.txt" ||
    die "Evidence SUMMARY.txt must contain a Reason section"
  grep -q '^Upgrade guard note:$' "${evidence_dir}/SUMMARY.txt" ||
    die "Evidence SUMMARY.txt must contain an upgrade guard note"
  grep -q 'does not contain Kubernetes Secret data' "${evidence_dir}/README.txt" ||
    die "Evidence README.txt must state that Secret data is excluded"

  if find "${evidence_dir}" -type f \( -name '*secret*.yaml' -o -name '*secret*.txt' \) | grep -q .; then
    die "Evidence bundle must not contain Secret files"
  fi

  local pod_dirs
  pod_dirs="$(find "${evidence_dir}" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')"
  [[ "${pod_dirs}" != "0" ]] || die "Evidence bundle has no per-pod directories"

  while IFS= read -r pod_dir; do
    [[ -e "${pod_dir}/pod.yaml" ]] || die "Evidence pod directory missing pod.yaml: ${pod_dir}"
    [[ -e "${pod_dir}/persisted-raft-peers.txt" ]] || die "Evidence pod directory missing persisted-raft-peers.txt: ${pod_dir}"
    [[ -e "${pod_dir}/raft-data-files.txt" ]] || die "Evidence pod directory missing raft-data-files.txt: ${pod_dir}"
  done < <(find "${evidence_dir}" -mindepth 1 -maxdepth 1 -type d | sort)

  echo "MetaD Raft evidence bundle verified: ${evidence_dir}"
}

plan_recovery_from_evidence() {
  local evidence_dir="$1"
  verify_evidence_bundle "${evidence_dir}" >/dev/null

  local plan_file="${evidence_dir}/RECOVERY_PLAN.txt"
  local status
  if grep -q '^MetaD Raft identity preflight passed\.$' "${evidence_dir}/SUMMARY.txt"; then
    status="passed"
  else
    status="failed"
  fi

  local tmp_dir
  tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/cedar-raft-recovery-plan.XXXXXX")"
  sort -u "${evidence_dir}/current-metad-pod-ips.txt" > "${tmp_dir}/current"
  sort -u "${evidence_dir}/persisted-raft-peer-ips.txt" > "${tmp_dir}/persisted"
  comm -23 "${tmp_dir}/current" "${tmp_dir}/persisted" > "${tmp_dir}/missing_current"
  comm -13 "${tmp_dir}/current" "${tmp_dir}/persisted" > "${tmp_dir}/stale_persisted"

  local identity_state="MATCHED"
  if [[ -s "${tmp_dir}/missing_current" || -s "${tmp_dir}/stale_persisted" ]]; then
    identity_state="MISMATCHED"
  fi

  {
    echo "CedarGraph MetaD Raft/PVC Recovery Plan"
    echo "Generated UTC: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    echo "Evidence directory: ${evidence_dir}"
    echo
    echo "1. Evidence Summary"
    echo "   - Preflight status: ${status}"
    echo "   - Identity state: ${identity_state}"
    echo "   - Current MetaD Pod IPs:"
    sed 's/^/     - /' "${tmp_dir}/current"
    echo "   - Persisted MetaD Raft peer IPs:"
    sed 's/^/     - /' "${tmp_dir}/persisted"
    if [[ "${identity_state}" == "MISMATCHED" ]]; then
      echo "   - Current Pod IPs missing from persisted Raft conf:"
      if [[ -s "${tmp_dir}/missing_current" ]]; then
        sed 's/^/     - /' "${tmp_dir}/missing_current"
      else
        echo "     - <none>"
      fi
      echo "   - Stale persisted Raft peer IPs not present in current Pods:"
      if [[ -s "${tmp_dir}/stale_persisted" ]]; then
        sed 's/^/     - /' "${tmp_dir}/stale_persisted"
      else
        echo "     - <none>"
      fi
    fi
    echo
    echo "2. Hard Stop Conditions"
    echo "   - Do not run a normal rolling MetaD upgrade while persisted peers are Pod IPs."
    echo "   - Do not delete production MetaD PVCs as a routine upgrade step."
    echo "   - Do not switch MetaD StatefulSet back to RollingUpdate without a validated migration or restore plan."
    echo "   - Do not proceed if this evidence bundle fails --verify-evidence."
    echo
    echo "3. Maintenance Window Checklist"
    echo "   - Preserve this evidence directory and attach it to the release record."
    echo "   - Take an application-level metadata backup using the approved production backup mechanism."
    echo "   - Confirm MetaD StatefulSet updateStrategy is OnDelete before any Helm upgrade."
    echo "   - Confirm PDBs permit only safe voluntary disruption and no MetaD Pod is restarted automatically."
    echo "   - Confirm a rollback image/chart and restore target are available before touching MetaD Pods."
    echo "   - Keep NetworkPolicy, TLS Secret, and GraphD auth Secret evidence with the same release record."
    echo
    echo "4. Recommended Path"
    if [[ "${status}" == "failed" ]]; then
      echo "   - This evidence bundle records a failed preflight. Resolve the failure reason in SUMMARY.txt before any upgrade action."
      echo "   - If the failure is upgrade guard, the cluster may be healthy now but normal MetaD rolling restarts remain blocked."
      echo "   - Schedule a controlled restore/migration drill before restarting MetaD Pods."
    elif [[ "${identity_state}" == "MATCHED" ]]; then
      echo "   - Current live Pod IPs match persisted Raft peer IPs, so the cluster is healthy now."
      echo "   - Treat this as a guardrail state, not as permission for rolling MetaD restarts."
      echo "   - For a clean new deployment, proceed only after the production gate passes."
      echo "   - For an existing cluster upgrade, schedule a controlled restore/migration drill before restarting MetaD Pods."
    else
      echo "   - Current Pod IPs do not match persisted Raft peer IPs."
      echo "   - Stop the rollout and enter incident/recovery mode."
      echo "   - Prefer restoring MetaD from a known-good metadata backup or executing the approved Raft conf/PVC migration plan."
      echo "   - Do not attempt ad hoc PVC deletion in production."
    fi
    echo
    echo "5. Post-Action Verification"
    echo "   - Re-run: ./scripts/preflight_k8s_raft_identity.sh --collect-evidence <new-dir>"
    echo "   - Re-run: ./scripts/preflight_k8s_raft_identity.sh --verify-evidence <new-dir>"
    echo "   - Re-run the Kubernetes production gate."
    echo "   - Keep the old and new evidence directories together for audit comparison."
  } > "${plan_file}"

  rm -rf "${tmp_dir}"
  echo "MetaD Raft recovery plan written to ${plan_file}"
}

run_self_test() {
  local self_dir
  self_dir="$(mktemp -d "${TMPDIR:-/tmp}/cedar-raft-identity-selftest.XXXXXX")"
  trap "rm -rf '${self_dir}'" EXIT

  validate_port CEDAR_METAD_RAFT_PORT 9559
  validate_positive_int CEDAR_RAFT_LOG_TAIL 400
  if ( validate_port CEDAR_METAD_RAFT_PORT 0 ) >/dev/null 2>&1; then
    die "self-test failed: zero Raft port should fail"
  fi
  if ( validate_positive_int CEDAR_RAFT_LOG_TAIL 0 ) >/dev/null 2>&1; then
    die "self-test failed: zero log tail should fail"
  fi

  printf '%s\n' 10.0.0.1 10.0.0.2 > "${self_dir}/current"
  printf '%s\n' 10.0.0.2 10.0.0.1 > "${self_dir}/persisted"
  compare_raft_identity_sets "${self_dir}/current" "${self_dir}/persisted" "${self_dir}" >/dev/null 2>&1 ||
    die "self-test failed: matching identity sets should pass"

  printf '%s\n' 10.0.0.1 10.0.0.2 > "${self_dir}/current"
  printf '%s\n' 10.0.0.1 10.0.0.3 > "${self_dir}/persisted"
  if compare_raft_identity_sets "${self_dir}/current" "${self_dir}/persisted" "${self_dir}" >/dev/null 2>&1; then
    die "self-test failed: stale persisted peer should fail"
  fi

  printf '%s\n' 10.0.0.1 10.0.0.2 > "${self_dir}/current"
  : > "${self_dir}/persisted"
  if compare_raft_identity_sets "${self_dir}/current" "${self_dir}/persisted" "${self_dir}" >/dev/null 2>&1; then
    die "self-test failed: empty persisted peer set should fail"
  fi

  echo "can't do pre_vote as it is not in 10.0.0.1:9559:0:0" > "${self_dir}/logs"
  raft_log_has_identity_errors "${self_dir}/logs" >/dev/null ||
    die "self-test failed: Raft identity log error should be detected"

  printf '%s\n' 10.0.0.1 > "${self_dir}/persisted"
  has_ipv4_persisted_peers "${self_dir}/persisted" ||
    die "self-test failed: persisted IPv4 peer set should be detected"

  mkdir -p "${self_dir}/evidence"
  printf '%s\n' 10.0.0.1 10.0.0.2 > "${self_dir}/current"
  printf '%s\n' 10.0.0.1 10.0.0.3 > "${self_dir}/persisted"
  write_evidence_summary "${self_dir}/evidence" "failed" "identity mismatch detected" "${self_dir}/current" "${self_dir}/persisted"
  grep -q "failed" "${self_dir}/evidence/SUMMARY.txt" ||
    die "self-test failed: evidence summary should include failure status"
  grep -q "identity mismatch detected" "${self_dir}/evidence/SUMMARY.txt" ||
    die "self-test failed: evidence summary should include failure reason"

  cat > "${self_dir}/evidence/README.txt" <<'EOF'
This bundle is read-only evidence for upgrade/recovery planning. It intentionally
does not contain Kubernetes Secret data and does not delete or mutate PVCs.
EOF
  touch "${self_dir}/evidence/metad-pods.txt" \
    "${self_dir}/evidence/metad-pods-wide.txt" \
    "${self_dir}/evidence/metad-statefulset.yaml" \
    "${self_dir}/evidence/metad-pvc-wide.txt" \
    "${self_dir}/evidence/metad-pvc.yaml" \
    "${self_dir}/evidence/metad-logs-tail.txt" \
    "${self_dir}/evidence/raft-identity-log-hits.txt"
  mkdir -p "${self_dir}/evidence/metad-0"
  touch "${self_dir}/evidence/metad-0/pod.yaml" \
    "${self_dir}/evidence/metad-0/persisted-raft-peers.txt" \
    "${self_dir}/evidence/metad-0/raft-data-files.txt"
  cp "${self_dir}/current" "${self_dir}/evidence/current-metad-pod-ips.txt"
  cp "${self_dir}/persisted" "${self_dir}/evidence/persisted-raft-peer-ips.txt"
  verify_evidence_bundle "${self_dir}/evidence" >/dev/null ||
    die "self-test failed: valid evidence bundle should verify"
  rm "${self_dir}/evidence/SUMMARY.txt"
  if ( verify_evidence_bundle "${self_dir}/evidence" >/dev/null 2>&1 ); then
    die "self-test failed: missing SUMMARY.txt should fail verification"
  fi
  write_evidence_summary "${self_dir}/evidence" "passed" "self-test evidence" "${self_dir}/current" "${self_dir}/persisted"
  plan_recovery_from_evidence "${self_dir}/evidence" >/dev/null ||
    die "self-test failed: valid evidence bundle should produce recovery plan"
  grep -q "Hard Stop Conditions" "${self_dir}/evidence/RECOVERY_PLAN.txt" ||
    die "self-test failed: recovery plan should include hard stop conditions"
  grep -q "Do not delete production MetaD PVCs" "${self_dir}/evidence/RECOVERY_PLAN.txt" ||
    die "self-test failed: recovery plan should forbid routine PVC deletion"

  echo "Kubernetes Raft identity preflight self-test passed"
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "${1:-}" == "--self-test" ]]; then
  run_self_test
  exit 0
fi

if [[ "${1:-}" == "--verify-evidence" ]]; then
  [[ -n "${2:-}" ]] || die "--verify-evidence requires an evidence directory"
  verify_evidence_bundle "$2"
  exit 0
fi

if [[ "${1:-}" == "--plan-recovery" ]]; then
  [[ -n "${2:-}" ]] || die "--plan-recovery requires an evidence directory"
  plan_recovery_from_evidence "$2"
  exit 0
fi

validate_port CEDAR_METAD_RAFT_PORT "${RAFT_PORT}"
validate_positive_int CEDAR_RAFT_LOG_TAIL "${LOG_TAIL}"

UPGRADE_GUARD=0
PATCH_ONDELETE=0
EVIDENCE_DIR=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --upgrade-guard)
      UPGRADE_GUARD=1
      shift
      ;;
    --patch-ondelete)
      PATCH_ONDELETE=1
      shift
      ;;
    --collect-evidence)
      [[ -n "${2:-}" ]] || die "--collect-evidence requires an output directory"
      EVIDENCE_DIR="$2"
      shift 2
      ;;
    *)
      die "Unknown argument: $1"
      ;;
  esac
done

if [[ "${PATCH_ONDELETE}" == "1" && -n "${EVIDENCE_DIR}" ]]; then
  die "--patch-ondelete cannot be combined with --collect-evidence"
fi

command -v kubectl >/dev/null 2>&1 || die "kubectl is required"

selector="${COMPONENT_LABEL},${INSTANCE_LABEL}"
tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/cedar-raft-identity.XXXXXX")"
trap 'rm -rf "${tmp_dir}"' EXIT

if [[ -n "${EVIDENCE_DIR}" ]]; then
  mkdir -p "${EVIDENCE_DIR}"
  if [[ ! -d "${EVIDENCE_DIR}" || ! -w "${EVIDENCE_DIR}" ]]; then
    die "Evidence directory is not writable: ${EVIDENCE_DIR}"
  fi
  evidence_abs="$(cd "${EVIDENCE_DIR}" && pwd)"
  cat > "${evidence_abs}/README.txt" <<EOF
CedarGraph MetaD Raft identity evidence bundle
Generated UTC: $(date -u '+%Y-%m-%dT%H:%M:%SZ')
Namespace: ${NAMESPACE}
Helm release / instance: ${INSTANCE}
Selector: ${selector}
MetaD data dir: ${DATA_DIR}
Raft port: ${RAFT_PORT}

This bundle is read-only evidence for upgrade/recovery planning. It intentionally
does not contain Kubernetes Secret data and does not delete or mutate PVCs.
EOF
fi

if [[ "${PATCH_ONDELETE}" == "1" ]]; then
  metad_sts="$(kubectl get statefulset -n "${NAMESPACE}" -l "${selector}" -o jsonpath='{.items[0].metadata.name}')"
  [[ -n "${metad_sts}" ]] || die "No MetaD StatefulSet found in namespace=${NAMESPACE} selector=${selector}"
  kubectl patch statefulset "${metad_sts}" -n "${NAMESPACE}" \
    --type=json \
    -p='[{"op":"replace","path":"/spec/updateStrategy","value":{"type":"OnDelete"}}]'
  echo "MetaD StatefulSet ${metad_sts} updateStrategy patched to OnDelete"
  exit 0
fi

current_ips="${tmp_dir}/current_ips"
persisted_ips="${tmp_dir}/persisted_ips"
pods_file="${tmp_dir}/pods"
: > "${current_ips}"
: > "${persisted_ips}"

kubectl get pods -n "${NAMESPACE}" -l "${selector}" \
  -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.podIP}{" "}{.status.phase}{"\n"}{end}' \
  > "${pods_file}"

if [[ -n "${EVIDENCE_DIR}" ]]; then
  cp "${pods_file}" "${evidence_abs}/metad-pods.txt"
  kubectl get pods -n "${NAMESPACE}" -l "${selector}" -o wide > "${evidence_abs}/metad-pods-wide.txt"
  kubectl get statefulset -n "${NAMESPACE}" -l "${selector}" -o yaml > "${evidence_abs}/metad-statefulset.yaml"
  kubectl get pvc -n "${NAMESPACE}" -l "${selector}" -o wide > "${evidence_abs}/metad-pvc-wide.txt"
  kubectl get pvc -n "${NAMESPACE}" -l "${selector}" -o yaml > "${evidence_abs}/metad-pvc.yaml"
fi

if [[ ! -s "${pods_file}" ]]; then
  die "No MetaD pods found in namespace=${NAMESPACE} selector=${selector}"
fi

while read -r pod ip phase; do
  [[ -n "${pod}" ]] || continue
  if [[ "${phase}" != "Running" ]]; then
    die "MetaD pod ${pod} is not Running: phase=${phase}"
  fi
  if [[ -z "${ip}" || "${ip}" == "<none>" ]]; then
    die "MetaD pod ${pod} has no Pod IP"
  fi
  echo "${ip}" >> "${current_ips}"

  if [[ -n "${EVIDENCE_DIR}" ]]; then
    pod_dir="${evidence_abs}/${pod}"
    mkdir -p "${pod_dir}"
    kubectl get pod "${pod}" -n "${NAMESPACE}" -o yaml > "${pod_dir}/pod.yaml"
    kubectl exec -n "${NAMESPACE}" "${pod}" -- sh -c \
      "find '${DATA_DIR}' -maxdepth 4 -type f 2>/dev/null | sort || true" \
      > "${pod_dir}/raft-data-files.txt" 2>&1
    kubectl exec -n "${NAMESPACE}" "${pod}" -- sh -c \
      "grep -aEho '([0-9]{1,3}\\.){3}[0-9]{1,3}:${RAFT_PORT}' '${DATA_DIR}'/log/* '${DATA_DIR}'/meta/* '${DATA_DIR}'/snapshot/*/* 2>/dev/null | sort -u || true" \
      > "${pod_dir}/persisted-raft-peers.txt" 2>&1
  fi

  kubectl exec -n "${NAMESPACE}" "${pod}" -- sh -c \
    "grep -aEho '([0-9]{1,3}\\.){3}[0-9]{1,3}:${RAFT_PORT}' '${DATA_DIR}'/log/* '${DATA_DIR}'/meta/* '${DATA_DIR}'/snapshot/*/* 2>/dev/null || true" \
    | sed "s/:${RAFT_PORT}$//" >> "${persisted_ips}"
done < "${pods_file}"

if [[ -n "${EVIDENCE_DIR}" ]]; then
  sort -u "${current_ips}" > "${evidence_abs}/current-metad-pod-ips.txt"
  sort -u "${persisted_ips}" > "${evidence_abs}/persisted-raft-peer-ips.txt"
fi

if ! compare_raft_identity_sets "${current_ips}" "${persisted_ips}" "${tmp_dir}"; then
  if [[ -n "${EVIDENCE_DIR}" ]]; then
    cp "${tmp_dir}/missing_current" "${evidence_abs}/current-pod-ips-missing-from-persisted.txt" 2>/dev/null || true
    cp "${tmp_dir}/stale_persisted" "${evidence_abs}/stale-persisted-peer-ips.txt" 2>/dev/null || true
    write_evidence_summary "${evidence_abs}" "failed" "MetaD Raft identity mismatch or empty persisted peer set" "${evidence_abs}/current-metad-pod-ips.txt" "${evidence_abs}/persisted-raft-peer-ips.txt"
    echo "MetaD Raft evidence bundle written to ${evidence_abs}" >&2
  fi
  exit 1
fi

if [[ "${UPGRADE_GUARD}" == "1" ]] && has_ipv4_persisted_peers "${persisted_ips}"; then
  if [[ -n "${EVIDENCE_DIR}" ]]; then
    kubectl logs -n "${NAMESPACE}" -l "${selector}" --all-containers --tail="${LOG_TAIL}" > "${evidence_abs}/metad-logs-tail.txt" 2>&1
    : > "${evidence_abs}/raft-identity-log-hits.txt"
    write_evidence_summary "${evidence_abs}" "failed" "upgrade guard blocked normal rolling upgrade because persisted peers are Pod IPs" "${evidence_abs}/current-metad-pod-ips.txt" "${evidence_abs}/persisted-raft-peer-ips.txt"
    echo "MetaD Raft evidence bundle written to ${evidence_abs}" >&2
  fi
  echo "MetaD Raft upgrade guard failed: persisted Raft peers are Pod IPs." >&2
  echo "Current implementation resolves StatefulSet DNS to IP before handing peers to braft." >&2
  echo "A MetaD Pod restart may receive a different IP while reusing the same PVC, which can break leader election." >&2
  echo "Do not run a normal rolling MetaD upgrade. Use a documented maintenance-window migration/rebuild/restore plan." >&2
  echo "Persisted MetaD Raft peer IPs:" >&2
  sed 's/^/  /' "${persisted_ips}" >&2
  exit 1
fi

log_hits="${tmp_dir}/raft_identity_log_hits"
logs_file="${tmp_dir}/metad_logs"
kubectl logs -n "${NAMESPACE}" -l "${selector}" --all-containers --tail="${LOG_TAIL}" > "${logs_file}" 2>&1
if [[ -n "${EVIDENCE_DIR}" ]]; then
  cp "${logs_file}" "${evidence_abs}/metad-logs-tail.txt"
fi
if raft_log_has_identity_errors "${logs_file}" > "${log_hits}"; then
  if [[ -n "${EVIDENCE_DIR}" ]]; then
    cp "${log_hits}" "${evidence_abs}/raft-identity-log-hits.txt"
    write_evidence_summary "${evidence_abs}" "failed" "MetaD logs contain Raft identity or election errors" "${evidence_abs}/current-metad-pod-ips.txt" "${evidence_abs}/persisted-raft-peer-ips.txt"
    echo "MetaD Raft evidence bundle written to ${evidence_abs}" >&2
  fi
  echo "MetaD logs contain Raft identity/election errors:" >&2
  cat "${log_hits}" >&2
  exit 1
fi

if [[ -n "${EVIDENCE_DIR}" ]]; then
  : > "${evidence_abs}/raft-identity-log-hits.txt"
  write_evidence_summary "${evidence_abs}" "passed" "current Pod IPs match persisted Raft peer IPs and logs contain no identity errors" "${evidence_abs}/current-metad-pod-ips.txt" "${evidence_abs}/persisted-raft-peer-ips.txt"
  echo "MetaD Raft evidence bundle written to ${evidence_abs}"
fi

echo "Kubernetes Raft identity preflight passed"
