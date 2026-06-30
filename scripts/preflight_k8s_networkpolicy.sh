#!/bin/bash
# =============================================================================
# CedarGraph Kubernetes NetworkPolicy Preflight
# =============================================================================
# Verifies that a live Kubernetes namespace has a CedarGraph NetworkPolicy when
# production networking isolation is required. This is intentionally separate
# from the Helm static checks because some development clusters do not enforce
# NetworkPolicy.
#
# Passing this check proves that the policy object and CedarGraph ingress ports
# are present. It does not prove that the cluster CNI enforces NetworkPolicy; the
# production runbook must record the CNI/provider enforcement evidence.
# =============================================================================

set -euo pipefail

NAMESPACE="${CEDAR_K8S_NAMESPACE:-cedargraph}"
INSTANCE="${CEDAR_HELM_RELEASE:-cedargraph}"
ALLOW_MISSING="${CEDAR_ALLOW_MISSING_NETWORKPOLICY:-0}"

usage() {
  cat <<USAGE
Usage: CEDAR_K8S_NAMESPACE=<namespace> CEDAR_HELM_RELEASE=<release> $0
       $0 --self-test

Environment:
  CEDAR_K8S_NAMESPACE                 Kubernetes namespace (default: cedargraph)
  CEDAR_HELM_RELEASE                  Helm release / instance label (default: cedargraph)
  CEDAR_ALLOW_MISSING_NETWORKPOLICY   Set to 1 only for documented dev/test exemptions
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

extract_policy_ports() {
  grep -Eo '"port"[[:space:]]*:[[:space:]]*[0-9]+' | grep -Eo '[0-9]+' | sort -nu || true
}

extract_namespace_selector_labels() {
  ruby -rjson -e '
    doc = JSON.parse(STDIN.read)
    labels = {}
    Array(doc["items"]).each do |item|
      Array(item.dig("spec", "ingress")).each do |rule|
        Array(rule["from"]).each do |source|
          match_labels = source.dig("namespaceSelector", "matchLabels")
          next unless match_labels.is_a?(Hash)
          match_labels.each { |key, value| labels[key] = value }
        end
      end
    end
    labels.each { |key, value| puts "#{key}=#{value}" }
  '
}

json_label_value() {
  local label_key="$1"
  ruby -rjson -e '
    key = ARGV.fetch(0)
    doc = JSON.parse(STDIN.read)
    labels = doc.dig("metadata", "labels") || {}
    print labels[key].to_s
  ' "${label_key}"
}

validate_policy_set() {
  local expected_instance="$1"
  ruby -rjson -e '
    expected_instance = ARGV.fetch(0)
    doc = JSON.parse(STDIN.read)
    items = Array(doc["items"])
    abort("No CedarGraph NetworkPolicy objects found") if items.empty?

    items.each do |item|
      name = item.dig("metadata", "name") || "<unknown>"
      policy_types = Array(item.dig("spec", "policyTypes"))
      if policy_types.include?("Egress") || item.dig("spec", "egress")
        abort("CedarGraph NetworkPolicy #{name} unexpectedly restricts egress; verify DNS and peer discovery before production.")
      end
      unless policy_types.include?("Ingress")
        abort("CedarGraph NetworkPolicy #{name} must include Ingress policy type")
      end

      labels = item.dig("spec", "podSelector", "matchLabels") || {}
      unless labels["app.kubernetes.io/instance"] == expected_instance
        abort("CedarGraph NetworkPolicy #{name} podSelector must include app.kubernetes.io/instance=#{expected_instance}")
      end
      name_label = labels["app.kubernetes.io/name"]
      part_of_label = labels["app.kubernetes.io/part-of"]
      if name_label.to_s.empty? && part_of_label.to_s.empty?
        abort("CedarGraph NetworkPolicy #{name} podSelector must include app.kubernetes.io/name=cedargraph or app.kubernetes.io/part-of=cedargraph")
      end
      if !name_label.to_s.empty? && name_label != "cedargraph"
        abort("CedarGraph NetworkPolicy #{name} podSelector does not appear to target app name cedargraph")
      end
      if !part_of_label.to_s.empty? && part_of_label != "cedargraph"
        abort("CedarGraph NetworkPolicy #{name} podSelector does not appear to target app part-of cedargraph")
      end
    end
  ' "${expected_instance}"
}

run_self_test() {
  validate_bool CEDAR_ALLOW_MISSING_NETWORKPOLICY 0
  validate_bool CEDAR_ALLOW_MISSING_NETWORKPOLICY 1
  if validate_bool CEDAR_ALLOW_MISSING_NETWORKPOLICY true >/dev/null 2>&1; then
    echo "self-test failed: invalid bool should fail" >&2
    exit 1
  fi

  local policy_json
  policy_json='{"items":[{"spec":{"policyTypes":["Ingress"],"podSelector":{"matchLabels":{"app.kubernetes.io/instance":"cedargraph","app.kubernetes.io/part-of":"cedargraph"}},"ingress":[{"ports":[{"port":7000},{"port":7001},{"port":9559},{"port":10559},{"port":9667},{"port":9668},{"port":9669},{"port":9780},{"port":9779}]}]}}]}'
  local policy_ports
  policy_ports="$(printf '%s' "${policy_json}" | extract_policy_ports)"
  local required_ports=(7000 7001 9559 10559 9667 9668 9669 9780 9779)
  local port
  for port in "${required_ports[@]}"; do
    if ! printf '%s\n' "${policy_ports}" | grep -qx "${port}"; then
      echo "self-test failed: expected port ${port} missing from parsed policy" >&2
      exit 1
    fi
  done

  local bad_policy_json
  bad_policy_json='{"items":[{"spec":{"ingress":[{"ports":[{"port":9559}]}]}}]}'
  local bad_ports
  bad_ports="$(printf '%s' "${bad_policy_json}" | extract_policy_ports)"
  if printf '%s\n' "${bad_ports}" | grep -qx "10559"; then
    echo "self-test failed: bad policy should not contain MetaD gRPC port" >&2
    exit 1
  fi

  local too_broad_policy_json
  too_broad_policy_json='{"items":[{"spec":{"ingress":[{"from":[{"namespaceSelector":{}}],"ports":[{"port":9669}]}]}}]}'
  if printf '%s' "${too_broad_policy_json}" | grep -q '"namespaceSelector"[[:space:]]*:[[:space:]]*{}'; then
    :
  else
    echo "self-test failed: broad namespaceSelector fixture should be detectable" >&2
    exit 1
  fi
  local namespace_label_fixture
  namespace_label_fixture='{"items":[{"spec":{"ingress":[{"from":[{"namespaceSelector":{"matchLabels":{"kubernetes.io/metadata.name":"cedargraph","environment":"prod"}}}]}]}}]}'
  local parsed_namespace_labels
  parsed_namespace_labels="$(printf '%s' "${namespace_label_fixture}" | extract_namespace_selector_labels | sort)"
  if ! printf '%s\n' "${parsed_namespace_labels}" | grep -qx 'environment=prod' ||
     ! printf '%s\n' "${parsed_namespace_labels}" | grep -qx 'kubernetes.io/metadata.name=cedargraph'; then
    echo "self-test failed: namespaceSelector labels should be parsed" >&2
    exit 1
  fi
  local namespace_json parsed_label_value
  namespace_json='{"metadata":{"labels":{"kubernetes.io/metadata.name":"cedargraph","environment":"prod"}}}'
  parsed_label_value="$(printf '%s' "${namespace_json}" | json_label_value "kubernetes.io/metadata.name")"
  if [[ "${parsed_label_value}" != "cedargraph" ]]; then
    echo "self-test failed: namespace label parser should read dotted/slashed label keys" >&2
    exit 1
  fi
  local multi_policy_json
  multi_policy_json='{"items":[{"metadata":{"name":"good"},"spec":{"policyTypes":["Ingress"],"podSelector":{"matchLabels":{"app.kubernetes.io/instance":"cedargraph","app.kubernetes.io/part-of":"cedargraph"}}}},{"metadata":{"name":"bad-egress"},"spec":{"policyTypes":["Ingress","Egress"],"podSelector":{"matchLabels":{"app.kubernetes.io/instance":"cedargraph","app.kubernetes.io/part-of":"cedargraph"}}}}]}'
  if printf '%s' "${multi_policy_json}" | validate_policy_set cedargraph >/dev/null 2>&1; then
    echo "self-test failed: any matching NetworkPolicy with Egress should fail" >&2
    exit 1
  fi

  echo "Kubernetes NetworkPolicy preflight self-test passed"
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "${1:-}" == "--self-test" ]]; then
  run_self_test
  exit 0
fi

validate_bool CEDAR_ALLOW_MISSING_NETWORKPOLICY "${ALLOW_MISSING}"

command -v kubectl >/dev/null 2>&1 || {
  echo "kubectl is required" >&2
  exit 1
}

selector="app.kubernetes.io/instance=${INSTANCE}"
if ! policies="$(kubectl get networkpolicy -n "${NAMESPACE}" -l "${selector}" -o name)"; then
  echo "Failed to read CedarGraph NetworkPolicy objects in namespace=${NAMESPACE} selector=${selector}" >&2
  exit 1
fi

if [[ -z "${policies}" ]]; then
  if [[ "${ALLOW_MISSING}" == "1" ]]; then
    echo "Kubernetes NetworkPolicy preflight skipped by CEDAR_ALLOW_MISSING_NETWORKPOLICY=1"
    exit 0
  fi
  echo "No CedarGraph NetworkPolicy found in namespace=${NAMESPACE} selector=${selector}" >&2
  echo "For production, enable Helm networkPolicy.enabled=true or record an explicit platform networking exemption." >&2
  exit 1
fi

required_ports=(7000 7001 9559 10559 9667 9668 9669 9780 9779)
policy_json="$(kubectl get networkpolicy -n "${NAMESPACE}" -l "${selector}" -o json)"
policy_ports="$(printf '%s' "${policy_json}" | extract_policy_ports)"

if printf '%s' "${policy_json}" | grep -q '"namespaceSelector"[[:space:]]*:[[:space:]]*{}'; then
  echo "CedarGraph NetworkPolicy must not allow ingress from every namespace" >&2
  exit 1
fi
printf '%s' "${policy_json}" | validate_policy_set "${INSTANCE}"
namespace_selector_labels="$(printf '%s' "${policy_json}" | extract_namespace_selector_labels)"
namespace_json="$(kubectl get namespace "${NAMESPACE}" -o json)"
while IFS='=' read -r label_key label_value; do
  [[ -n "${label_key}" ]] || continue
  actual_value="$(printf '%s' "${namespace_json}" | json_label_value "${label_key}")"
  if [[ "${actual_value}" != "${label_value}" ]]; then
    echo "Namespace ${NAMESPACE} is missing NetworkPolicy source label ${label_key}=${label_value}" >&2
    exit 1
  fi
done <<< "${namespace_selector_labels}"

for port in "${required_ports[@]}"; do
  if ! printf '%s\n' "${policy_ports}" | grep -qx "${port}"; then
    echo "CedarGraph NetworkPolicy is missing required ingress port ${port}" >&2
    exit 1
  fi
done

echo "Kubernetes NetworkPolicy preflight passed (object/ports present; verify CNI enforcement in the production runbook)"
