#!/bin/bash
# =============================================================================
# CedarGraph Helm Static Preflight
# =============================================================================
# Performs offline checks for Helm chart invariants and renders the chart with
# `helm template` to catch unsafe defaults that only appear after values merge.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."
CHART_DIR="${PROJECT_ROOT}/helm-chart/cedargraph"

cd "${PROJECT_ROOT}"

"${PROJECT_ROOT}/scripts/preflight_k8s_raft_identity.sh" --self-test
"${PROJECT_ROOT}/scripts/preflight_k8s_production_gate.sh" --self-test
"${PROJECT_ROOT}/scripts/preflight_k8s_networkpolicy.sh" --self-test
"${PROJECT_ROOT}/scripts/generate_k8s_tls_secret.sh" --self-test

ruby - "${CHART_DIR}" <<'RUBY'
require 'yaml'

chart_dir = ARGV.fetch(0)
values_path = File.join(chart_dir, 'values.yaml')
values = YAML.load_file(values_path)

def dig!(hash, *keys)
  keys.reduce(hash) { |memo, key| memo.fetch(key) }
end

abort('metad.service.port must be 9559') unless dig!(values, 'metad', 'service', 'port') == 9559
abort('metad.service.grpcPort must be 10559') unless dig!(values, 'metad', 'service', 'grpcPort') == 10559
abort('metad.updateStrategy.type must default to OnDelete') unless dig!(values, 'metad', 'updateStrategy', 'type') == 'OnDelete'
abort('metad.allowUnsafeRollingUpdate must default to false') unless dig!(values, 'metad', 'allowUnsafeRollingUpdate') == false
abort('storaged grpc port must be 9779') unless dig!(values, 'storaged', 'service', 'ports', 'grpc') == 9779
abort('storaged health port must be 7000') unless dig!(values, 'storaged', 'service', 'ports', 'health') == 7000
abort('storaged metrics port must be 7001') unless dig!(values, 'storaged', 'service', 'ports', 'metrics') == 7001
abort('graphd query port must be 9669') unless dig!(values, 'graphd', 'service', 'ports', 'query') == 9669
abort('graphd health port must be 9668') unless dig!(values, 'graphd', 'service', 'ports', 'health') == 9668
abort('graphd metrics port must be 9667') unless dig!(values, 'graphd', 'service', 'ports', 'metrics') == 9667
abort('image.tag must not default to latest; production manifests must be reproducible') if dig!(values, 'image', 'tag') == 'latest'
abort('image.tag must default to the latest verified Kubernetes drill image') unless dig!(values, 'image', 'tag') == 'k8s-fix-20260630'
abort('global.studio.image must not default to latest') if dig!(values, 'global', 'studio', 'image').end_with?(':latest')
abort('pdb.enabled must default to true') unless dig!(values, 'pdb', 'enabled') == true
abort('pdb.metad.minAvailable must default to 2') unless dig!(values, 'pdb', 'metad', 'minAvailable') == 2
abort('pdb.storaged.minAvailable must default to 2') unless dig!(values, 'pdb', 'storaged', 'minAvailable') == 2
abort('pdb.graphd.minAvailable must default to 1') unless dig!(values, 'pdb', 'graphd', 'minAvailable') == 1
abort('networkPolicy.enabled must default to true for production-safe Helm renders') unless dig!(values, 'networkPolicy', 'enabled') == true

network_ports = values.fetch('networkPolicy').fetch('ingress').flat_map do |rule|
  (rule['ports'] || []).map { |port| port['port'] }
end
missing_ports = [7000, 7001, 9559, 10559, 9667, 9668, 9669, 9780, 9779] - network_ports
abort("networkPolicy ingress missing ports: #{missing_ports.inspect}") unless missing_ports.empty?
network_sources = values.fetch('networkPolicy').fetch('ingress').flat_map do |rule|
  rule['from'] || []
end
abort('networkPolicy default ingress must not allow every namespace') if
  network_sources.any? { |source| source['namespaceSelector'] == {} }
abort('networkPolicy default ingress must restrict source namespaces by label') unless
  network_sources.any? { |source| source.dig('namespaceSelector', 'matchLabels', 'kubernetes.io/metadata.name') == 'cedargraph' }

templates = Dir[File.join(chart_dir, 'templates', '*.yaml')].to_h do |path|
  [File.basename(path), File.read(path)]
end

rendered = `helm template cedargraph #{chart_dir} --namespace cedargraph 2>&1`
abort("helm template failed: #{rendered}") unless $?.success?
latest_images = rendered.lines.grep(/image:\s+.*:latest\b/)
abort("Helm rendered manifest must not contain :latest images: #{latest_images.join.strip}") unless latest_images.empty?
abort('Helm rendered manifest must use k8s-fix-20260630 image tag') unless rendered.include?('cedargraph/cedar:k8s-fix-20260630')
rendered_docs = YAML.load_stream(rendered).compact
rendered_network_policy = rendered_docs.find { |doc| doc['kind'] == 'NetworkPolicy' }
abort('Helm default render must include a CedarGraph NetworkPolicy') unless rendered_network_policy
abort('Helm default NetworkPolicy must not restrict egress') if
  Array(rendered_network_policy.dig('spec', 'policyTypes')).include?('Egress') ||
  rendered_network_policy.dig('spec', 'egress')
rendered_np_sources = Array(rendered_network_policy.dig('spec', 'ingress')).flat_map { |rule| rule['from'] || [] }
abort('Helm default NetworkPolicy must not allow ingress from every namespace') if
  rendered_np_sources.any? { |source| source['namespaceSelector'] == {} }
abort('Helm default NetworkPolicy must restrict ingress source namespaces by label') unless
  rendered_np_sources.any? { |source| source.dig('namespaceSelector', 'matchLabels', 'kubernetes.io/metadata.name') == 'cedargraph' }
rendered_graphd = rendered_docs.find do |doc|
  doc['kind'] == 'Deployment' &&
    doc.dig('metadata', 'name') == 'cedargraph-graphd'
end
abort('Helm rendered manifest missing GraphD Deployment') unless rendered_graphd
rendered_graphd_args = rendered_graphd.dig('spec', 'template', 'spec', 'containers', 0, 'args') || []
abort("Helm rendered GraphD args must explicitly set health/metrics ports: #{rendered_graphd_args.inspect}") unless
  rendered_graphd_args.each_cons(2).any? { |a, b| a == '--health_port' && b == '9668' } &&
  rendered_graphd_args.each_cons(2).any? { |a, b| a == '--metrics_port' && b == '9667' }

network_policy = templates.fetch('networkpolicy.yaml')
abort('Helm NetworkPolicy must not restrict egress') if network_policy.match?(/Egress|egress:/)
abort('Helm NetworkPolicy must render ingress values') unless network_policy.include?('.Values.networkPolicy.ingress')
abort('Helm values must not default NetworkPolicy ingress to namespaceSelector: {}') if
  File.read(values_path).match?(/namespaceSelector:\s*\{\}/)

metad = templates.fetch('metad-statefulset.yaml')
abort('MetaD template must set grpc_port from metad.service.grpcPort') unless metad.include?('--grpc_port "{{ .Values.metad.service.grpcPort }}"')
abort('MetaD template must create peers in parallel for Raft bootstrap') unless metad.include?('podManagementPolicy: Parallel')
abort('MetaD template must render manual updateStrategy') unless metad.include?('updateStrategy:') && metad.include?('type: {{ .Values.metad.updateStrategy.type | quote }}')
abort('MetaD template must fail-fast unsafe RollingUpdate unless explicitly allowed') unless
  metad.include?('metad.allowUnsafeRollingUpdate') &&
  metad.include?('metad.updateStrategy.type=RollingUpdate is unsafe')
abort('MetaD template must validate updateStrategy type values') unless
  metad.include?('metad.updateStrategy.type must be either OnDelete or RollingUpdate')
abort('MetaD template must advertise a non-IP_ANY Raft identity') unless metad.include?('--advertise "${HOSTNAME}.{{ include "cedargraph.fullname" . }}-metad:{{ .Values.metad.service.port }}"')
abort('MetaD template must generate Raft peer args') unless metad.include?('peer_args=""') && metad.include?('for i in $(seq 0 {{ sub (int .Values.metad.replicas) 1 }}); do')
abort('MetaD template must wait for peer DNS before braft init') unless
  metad.include?('until getent hosts "${peer_host}"') &&
  metad.include?('waiting for MetaD peer DNS')
abort('MetaD braft persists resolved Pod IPs; chart notes must document PVC cleanup or migration before rebootstrap') unless File.read('helm-chart/README.md').include?('MetaD Raft PVC')
abort('MetaD Raft identity live preflight script must exist') unless File.executable?('scripts/preflight_k8s_raft_identity.sh')
abort('Production guide must require MetaD Raft identity live preflight') unless File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('preflight_k8s_raft_identity.sh')
abort('Production guide must require upgrade-guard mode for MetaD rolling upgrade safety') unless File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('--upgrade-guard')
abort('Production guide must document patch-ondelete migration for existing releases') unless
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('--patch-ondelete')
abort('Production guide must require MetaD Raft evidence collection before upgrade') unless
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('--collect-evidence') &&
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('--verify-evidence') &&
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('--plan-recovery') &&
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('证据包')
abort('MetaD Raft identity script must support read-only evidence collection') unless
  File.read('scripts/preflight_k8s_raft_identity.sh').include?('--collect-evidence') &&
  File.read('scripts/preflight_k8s_raft_identity.sh').include?('--verify-evidence') &&
  File.read('scripts/preflight_k8s_raft_identity.sh').include?('--plan-recovery') &&
  File.read('scripts/preflight_k8s_raft_identity.sh').include?('verify_evidence_bundle') &&
  File.read('scripts/preflight_k8s_raft_identity.sh').include?('RECOVERY_PLAN.txt') &&
  File.read('scripts/preflight_k8s_raft_identity.sh').include?('persisted-raft-peer-ips.txt') &&
  File.read('scripts/preflight_k8s_raft_identity.sh').include?('does not contain Kubernetes Secret data')
abort('MetaD Raft identity script must preserve evidence summaries on failure') unless
  File.read('scripts/preflight_k8s_raft_identity.sh').include?('write_evidence_summary') &&
  File.read('scripts/preflight_k8s_raft_identity.sh').include?('upgrade guard blocked normal rolling upgrade') &&
  File.read('scripts/preflight_k8s_raft_identity.sh').include?('MetaD Raft identity preflight ${status}') &&
  File.read('scripts/preflight_k8s_raft_identity.sh').include?('MetaD logs contain Raft identity or election errors')
raft_identity = File.read('scripts/preflight_k8s_raft_identity.sh')
abort('MetaD Raft evidence collection must fail if kubectl exec itself fails') if
  raft_identity.match?(/kubectl exec[\s\S]*?> "\$\{pod_dir\}\/(?:raft-data-files|persisted-raft-peers)\.txt" 2>&1 \|\| true/)
abort('MetaD Raft log evidence must fail if kubectl logs itself fails') if
  raft_identity.match?(/kubectl logs[\s\S]*?metad-logs-tail\.txt" 2>&1 \|\| true/) ||
  raft_identity.match?(/kubectl logs[\s\S]*?> "\$\{logs_file\}" 2>&1 \|\| true/)
abort('MetaD Raft identity preflight must validate Raft port and log tail overrides') unless
  raft_identity.include?('validate_port CEDAR_METAD_RAFT_PORT "${RAFT_PORT}"') &&
  raft_identity.include?('validate_positive_int CEDAR_RAFT_LOG_TAIL "${LOG_TAIL}"') &&
  raft_identity.include?('zero Raft port should fail') &&
  raft_identity.include?('zero log tail should fail')
abort('Production guide must require live NetworkPolicy preflight or documented exemption') unless
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('preflight_k8s_networkpolicy.sh') &&
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('CEDAR_ALLOW_MISSING_NETWORKPOLICY=1')
networkpolicy_gate = File.read('scripts/preflight_k8s_networkpolicy.sh')
abort('Kubernetes NetworkPolicy preflight must reject ingress from every namespace') unless
  networkpolicy_gate.include?('must not allow ingress from every namespace') &&
  networkpolicy_gate.include?('"namespaceSelector"[[:space:]]*:[[:space:]]*{}')
abort('Kubernetes NetworkPolicy preflight must verify namespaceSelector labels exist on the target namespace') unless
  networkpolicy_gate.include?('extract_namespace_selector_labels') &&
  networkpolicy_gate.include?('json_label_value') &&
  networkpolicy_gate.include?('Namespace ${NAMESPACE} is missing NetworkPolicy source label')
abort('Kubernetes NetworkPolicy preflight must not read dotted/slashed namespace label keys through fragile jsonpath') if
  networkpolicy_gate.include?('.metadata.labels.${label_key')
abort('Kubernetes NetworkPolicy preflight must support both Helm and kustomize CedarGraph pod selectors') unless
  networkpolicy_gate.include?('validate_policy_set') &&
  networkpolicy_gate.include?('app.kubernetes.io/name=cedargraph or app.kubernetes.io/part-of=cedargraph')
abort('Kubernetes NetworkPolicy preflight must validate every matching NetworkPolicy, not only items[0]') if
  networkpolicy_gate.include?('items[0]') ||
  networkpolicy_gate.include?('policy_name="$(kubectl get networkpolicy')
abort('Production guide must document that NetworkPolicy enforcement depends on CNI/provider evidence') unless
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('CNI') &&
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('执行证据')
abort('Operator docs must state that Helm NetworkPolicy is enabled by default') unless
  File.read('helm-chart/README.md').include?('| `networkPolicy.enabled` |') &&
  File.read('helm-chart/README.md').include?('生产默认开启') &&
  File.read('helm-chart/README.md').include?('| `true` |') &&
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('Helm Chart 默认启用 `networkPolicy.enabled=true`')
helm_readme = File.read('helm-chart/README.md')
namespace_create_idx = helm_readme.index('kubectl create namespace cedargraph --dry-run=client -o yaml | kubectl apply -f -')
auth_secret_idx = helm_readme.index('kubectl create secret generic cedargraph-graphd-auth -n cedargraph')
tls_secret_idx = helm_readme.index('kubectl create secret generic cedargraph-graphd-tls -n cedargraph')
abort('Helm README quick start must create the target namespace before creating namespaced Secrets') unless
  namespace_create_idx && auth_secret_idx && tls_secret_idx &&
  namespace_create_idx < auth_secret_idx &&
  namespace_create_idx < tls_secret_idx
%w[helm-chart/README.md docs/PRODUCTION_DEPLOYMENT_GUIDE.md].each do |path|
  text = File.read(path)
  %w[cedargraph-graphd-auth cedargraph-graphd-tls].each do |secret_name|
    secret_idx = text.index("kubectl create secret generic #{secret_name} -n cedargraph")
    apply_idx = secret_idx && text.index('--dry-run=client -o yaml | kubectl apply -f -', secret_idx)
    abort("#{path} must create #{secret_name} idempotently with --dry-run=client | kubectl apply") unless
      secret_idx && apply_idx && apply_idx - secret_idx < 500
  end
end
abort('Helm README install examples must use an explicit namespace because NetworkPolicy defaults are namespace-scoped') if
  helm_readme.match?(/helm install [^\n\\]+cedargraph\/cedargraph(?![\s\S]{0,120}--namespace cedargraph)/)
abort('Helm README upgrade examples must use an explicit namespace') if
  helm_readme.match?(/helm upgrade [^\n\\]+cedargraph\/cedargraph(?![\s\S]{0,120}--namespace cedargraph)/)
abort('Helm README must document NetworkPolicy namespaceSelector override for non-default namespaces') unless
  helm_readme.include?('networkPolicy.ingress[0].from[0].namespaceSelector.matchLabels') &&
  helm_readme.include?('kubernetes\\\\.io/metadata\\\\.name=<namespace>')
abort('Production guide must require the live Kubernetes production gate') unless
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('preflight_k8s_production_gate.sh')
abort('Production guide must document CEDAR_MAX_POD_RESTARTS restart exemption requirements') unless
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('CEDAR_MAX_POD_RESTARTS') &&
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('重启原因')
abort('Production guide must document the production gate minimum pod age') unless
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('CEDAR_MIN_POD_AGE_SECONDS') &&
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('300 秒')
abort('Production guide must document CEDAR_REQUIRE_HELM_STATUS for non-Helm deployments') unless
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('CEDAR_REQUIRE_HELM_STATUS=0') &&
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('deployed')
abort('Production guide must document production gate HA pod-count baseline') unless
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('CEDAR_EXPECTED_METAD_PODS') &&
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('CEDAR_EXPECTED_STORAGED_PODS') &&
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('CEDAR_MIN_GRAPHD_PODS')
abort('Production guide must use Helm default GraphD Secret names') unless
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('cedargraph-graphd-auth') &&
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('cedargraph-graphd-tls')
abort('Production guide must document production gate PVC and Secret checks') unless
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('PVC 数量') &&
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('认证 Secret') &&
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('TLS Secret')
abort('Production guide must document JWT and TLS certificate gate thresholds') unless
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('CEDAR_MIN_JWT_SECRET_BYTES') &&
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('CEDAR_MIN_TLS_DAYS') &&
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('至少剩余 30 天')
abort('Production guide must document the Kubernetes TLS Secret generator') unless
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('generate_k8s_tls_secret.sh') &&
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('正式 CA')
abort('Kubernetes production gate script must exist') unless File.executable?('scripts/preflight_k8s_production_gate.sh')
abort('Kubernetes API server dry-run script must exist') unless File.executable?('scripts/preflight_k8s_server_dry_run.sh')
server_dry_run = File.read('scripts/preflight_k8s_server_dry_run.sh')
abort('Kubernetes API server dry-run must validate both Kustomize and Helm renders') unless
  server_dry_run.include?('kubectl kustomize k8s') &&
  server_dry_run.include?('helm template cedargraph helm-chart/cedargraph') &&
  server_dry_run.include?('kubectl apply --dry-run=server')
abort('Kubernetes API server dry-run must reject unexpected API warnings') unless
  server_dry_run.include?('unexpected warnings/errors') &&
  server_dry_run.include?('spec.SessionAffinity is ignored for headless services')
user_manual = File.read('docs/user-manual/README.md')
abort('User manual must document that local Helm template and Kubernetes server-side dry-run are now verified') unless
  user_manual.include?('scripts/preflight_k8s_server_dry_run.sh') &&
  user_manual.include?('Helm chart lint') &&
  user_manual.include?('Kubernetes API server-side dry-run')
abort('User manual must not still list helm template or server-side dry-run as unproven') if
  user_manual.include?('真实 `helm template`') ||
  user_manual.include?('Kubernetes API server-side dry-run/apply')
abort('Kubernetes TLS Secret generator script must exist') unless File.executable?('scripts/generate_k8s_tls_secret.sh')
abort('Kubernetes recovery drill script must exist') unless File.executable?('scripts/preflight_k8s_recovery_drill.sh')
system('scripts/preflight_k8s_recovery_drill.sh', '--self-test') ||
  abort('Kubernetes recovery drill self-test must pass')
tls_generator = File.read('scripts/generate_k8s_tls_secret.sh')
abort('Kubernetes TLS Secret generator must include StatefulSet headless Pod DNS SANs') unless
  tls_generator.include?('${fullname}-metad-${i}.${fullname}-metad') &&
  tls_generator.include?('${fullname}-storaged-${i}.${fullname}-storaged') &&
  tls_generator.include?('subjectAltName=${san}')
abort('Kubernetes TLS Secret generator must require positive SAN replica counts and TLS duration') unless
  tls_generator.include?('validate_positive_int CEDAR_EXPECTED_METAD_PODS "${METAD_REPLICAS}"') &&
  tls_generator.include?('validate_positive_int CEDAR_EXPECTED_STORAGED_PODS "${STORAGED_REPLICAS}"') &&
  tls_generator.include?('validate_positive_int CEDAR_TLS_DAYS "${DAYS}"') &&
  tls_generator.include?('zero MetaD SAN replica count should fail') &&
  tls_generator.include?('zero StorageD SAN replica count should fail') &&
  tls_generator.include?('zero TLS days should fail')
recovery_drill = File.read('scripts/preflight_k8s_recovery_drill.sh')
abort('Kubernetes recovery drill default image tag must match latest verified K8s drill image') unless
  recovery_drill.include?('k8s-fix-20260630') &&
  File.read('helm-chart/README.md').include?('CEDAR_DRILL_IMAGE_TAG=k8s-fix-20260630') &&
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('CEDAR_DRILL_IMAGE_TAG=k8s-fix-20260630')
abort('Kubernetes recovery drill must refuse protected namespaces') unless
  recovery_drill.include?('namespace_is_protected') &&
  recovery_drill.include?('--self-test') &&
  recovery_drill.include?('Refusing to run recovery drill against protected namespace') &&
  recovery_drill.include?('cedargraph-preflight') &&
  recovery_drill.include?('*-prod') &&
  recovery_drill.include?('prod-*') &&
  recovery_drill.include?('*-production') &&
  recovery_drill.include?('production-*')
abort('Kubernetes recovery drill must collect evidence, verify it, and generate a recovery plan') unless
  recovery_drill.include?('--collect-evidence') &&
  recovery_drill.include?('--verify-evidence') &&
  recovery_drill.include?('--plan-recovery')
abort('Kubernetes recovery drill must run production gate and expected-failure upgrade guard') unless
  recovery_drill.include?('preflight_k8s_production_gate.sh') &&
  recovery_drill.include?('CEDAR_RUN_RAFT_UPGRADE_GUARD=1') &&
  recovery_drill.include?('Upgrade guard unexpectedly passed')
abort('Kubernetes recovery drill must override Helm NetworkPolicy namespaceSelector to the disposable drill namespace') unless
  recovery_drill.include?('networkPolicy.ingress[0].from[0].namespaceSelector.matchLabels') &&
  recovery_drill.include?('kubernetes\\\\.io/metadata\\\\.name') &&
  recovery_drill.include?('"${NAMESPACE}"')
abort('Kubernetes recovery drill must preserve Helm NetworkPolicy ports when overriding ingress[0]') unless
  [7000, 7001, 9559, 10559, 9667, 9668, 9669, 9780, 9779].all? do |port|
    recovery_drill.include?("networkPolicy.ingress[0].ports") &&
      recovery_drill.include?("port=#{port}")
  end
abort('Kubernetes recovery drill cleanup must fail closed by default') unless
  recovery_drill.include?('cleanup-helm-uninstall.err') &&
  recovery_drill.include?('cleanup-namespace-delete.err') &&
  recovery_drill.include?('Failed to uninstall drill Helm release') &&
  recovery_drill.include?('Failed to delete drill namespace') &&
  recovery_drill.include?('CEDAR_DRILL_CLEANUP=0')
abort('Kubernetes recovery drill must not hide cleanup failures behind "|| true"') if
  recovery_drill.match?(/helm uninstall[\s\S]*?\|\| true/) ||
  recovery_drill.match?(/kubectl delete namespace[\s\S]*?\|\| true/)
abort('Production guide must document Kubernetes recovery drill') unless
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('preflight_k8s_recovery_drill.sh') &&
  File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md').include?('隔离 namespace')
production_gate = File.read('scripts/preflight_k8s_production_gate.sh')
abort('Kubernetes production gate must check PVC state') unless
  production_gate.include?('PersistentVolumeClaims') &&
  production_gate.include?('storageClassName') &&
  production_gate.include?('ReadWriteOnce')
abort('Kubernetes production gate must check GraphD auth and TLS secrets') unless
  production_gate.include?('GraphD auth and TLS secrets') &&
  production_gate.include?('CEDAR_GRAPHD_AUTH_JWT_SECRET') &&
  production_gate.include?('tls.crt')
abort('Kubernetes production gate must not hide GraphD auth env reads behind grep pipelines') unless
  production_gate.include?('graphd_env_refs="$(kubectl get deploy') &&
  production_gate.include?('auth_refs="$(printf "%s\\n" "${graphd_env_refs}"') &&
  production_gate.include?('does not reference auth secrets')
abort('Kubernetes production gate must not read GraphD auth refs through a kubectl|grep pipeline') if
  production_gate.lines.any? { |line| line.include?('kubectl get deploy "${graphd_deployment}"') && line.include?('|') && line.include?('grep') }
abort('Kubernetes production gate must enforce JWT and TLS certificate thresholds') unless
  production_gate.include?('CEDAR_MIN_JWT_SECRET_BYTES') &&
  production_gate.include?('CEDAR_MIN_TLS_DAYS') &&
  production_gate.include?('openssl x509') &&
  production_gate.include?('split_pem_bundle') &&
  production_gate.include?('ca_valid_for_threshold')
abort('Kubernetes production gate must decode Kubernetes Secret data across GNU/BSD base64 variants') unless
  production_gate.include?('decode_base64()') &&
  production_gate.include?('base64 --decode') &&
  production_gate.include?('base64 -d') &&
  production_gate.include?('base64 -D') &&
  production_gate.include?('decode_base64 "${decoded_secret_path}"') &&
  production_gate.include?('decode_base64 "${tls_path}"') &&
  production_gate.include?('base64 decoder should decode a valid payload')
abort('Kubernetes production gate must fail closed when Kubernetes Secrets cannot be read') unless
  production_gate.include?('kubectl_secret_data()') &&
  production_gate.include?('secret_key_jsonpath="${secret_key//./\\\\.}"') &&
  production_gate.include?('Failed to read Kubernetes Secret') &&
  production_gate.include?('secret_value_b64="$(kubectl_secret_data') &&
  production_gate.include?('tls_value_b64="$(kubectl_secret_data')
abort('Kubernetes production gate must let kubectl_secret_data escape dotted Secret keys') if
  production_gate.include?('escaped_key="${tls_key//./\\\\.}"')
abort('Kubernetes production gate must not hide Secret read failures behind "|| true"') if
  production_gate.match?(/kubectl get secret[\s\S]*?2>\/dev\/null \|\| true/)
abort('Kubernetes production gate must validate TLS certificate SAN coverage') unless
  production_gate.include?('cert_has_dns_name') &&
  production_gate.include?('SAN is missing DNS') &&
  production_gate.include?('${fullname}-metad-${i}.${fullname}-metad')
abort('Kubernetes production gate must require TLS across GraphD, MetaD, and StorageD') unless
  production_gate.include?('GraphD TLS must be enabled for production gate') &&
  production_gate.include?('component_tls_secret') &&
  production_gate.include?('must match GraphD TLS Secret')
abort('Kubernetes production gate must expose the critical log window') unless
  production_gate.include?('CEDAR_CRITICAL_LOG_SINCE') &&
  production_gate.include?('--since="${CRITICAL_LOG_SINCE}"') &&
  production_gate.include?('validate_log_window') &&
  production_gate.include?('<positive-number><s|m|h|d>') &&
  production_gate.include?('zero log window should fail') &&
  production_gate.include?('invalid log window unit should fail')
abort('Kubernetes production gate must parse Helm status JSON fail-closed') unless
  production_gate.include?('json_status_field()') &&
  production_gate.include?('ruby -rjson') &&
  production_gate.include?('doc["status"] || doc.dig("info", "status")') &&
  production_gate.include?('helm_status_json="${tmp_dir}/helm-status.json"') &&
  production_gate.include?('Failed to read Helm release status') &&
  production_gate.include?('Failed to parse Helm status JSON') &&
  production_gate.include?('Helm status JSON parser should extract deployed') &&
  production_gate.include?('Helm v4 status JSON parser should extract info.status') &&
  production_gate.include?('Helm status JSON parser should fail closed on missing status')
abort('Kubernetes production gate must not parse Helm status with a fragile grep/sed pipeline') if
  production_gate.include?('helm status "${INSTANCE}" -n "${NAMESPACE}" -o json | grep')
abort('Kubernetes production gate must fail closed when kubectl logs cannot be read') unless
  production_gate.include?('logs_file="${tmp_dir}/recent-critical-logs.txt"') &&
  production_gate.include?('Failed to read recent CedarGraph logs') &&
  production_gate.include?('cat "${logs_file}"') &&
  production_gate.include?('grep -E "FATAL|CrashLoop|segmentation|panic')
abort('Kubernetes production gate must enforce minimum pod age') unless
  production_gate.include?('CEDAR_MIN_POD_AGE_SECONDS') &&
  production_gate.include?('status.startTime') &&
  production_gate.include?('below CEDAR_MIN_POD_AGE_SECONDS')
abort('Kubernetes production gate must reject zero pod-count baselines') unless
  production_gate.include?('validate_min_int') &&
  production_gate.include?('validate_min_int CEDAR_EXPECTED_METAD_PODS "${EXPECTED_METAD_PODS}" 1') &&
  production_gate.include?('validate_min_int CEDAR_EXPECTED_STORAGED_PODS "${EXPECTED_STORAGED_PODS}" 1') &&
  production_gate.include?('validate_min_int CEDAR_MIN_GRAPHD_PODS "${MIN_GRAPHD_PODS}" 1') &&
  production_gate.include?('zero MetaD pod baseline should fail') &&
  production_gate.include?('zero GraphD pod baseline should fail')
abort('Kubernetes production gate must count pods without masking kubectl failures behind wc') unless
  production_gate.include?('kubectl_count()') &&
  production_gate.include?('names="$(kubectl_jsonpath "${resource} -l ${label_selector}"') &&
  production_gate.include?('metad_pods="$(kubectl_count pods') &&
  production_gate.include?('storaged_pods="$(kubectl_count pods') &&
  production_gate.include?('graphd_pods="$(kubectl_count pods')
abort('Kubernetes production gate must not count pods through a kubectl|wc pipeline') if
  production_gate.match?(/kubectl get pods[^\n]*\|\s*wc -l/)
abort('Kubernetes production gate must validate PDB status fields before numeric comparison') unless
  production_gate.include?('status.expectedPods') &&
  production_gate.include?('expected_pods="$(kubectl_jsonpath "pdb ${pdb_name}"') &&
  production_gate.include?('validate_min_int "PDB ${pdb_name} status.expectedPods"') &&
  production_gate.include?('validate_non_negative_int "PDB ${pdb_name} status.currentHealthy"') &&
  production_gate.include?('validate_non_negative_int "PDB ${pdb_name} status.desiredHealthy"') &&
  production_gate.include?('does not report a numeric currentHealthy value') &&
  production_gate.include?('does not report a numeric desiredHealthy value')
abort('Kubernetes production gate must validate container restart counts before numeric comparison') unless
  production_gate.include?('validate_non_negative_int "Pod ${pod} initContainer ${init_container} restartCount"') &&
  production_gate.include?('validate_non_negative_int "Pod ${pod} container ${container} restartCount"') &&
  production_gate.include?('does not report a numeric restartCount') &&
  production_gate.include?('empty restart count should fail') &&
  production_gate.include?('non-numeric restart count should fail')
abort('Kubernetes production gate must fail closed with clear errors for kubectl jsonpath reads') unless
  production_gate.include?('kubectl_jsonpath()') &&
  production_gate.include?('Failed to read Kubernetes object') &&
  production_gate.include?('with jsonpath=') &&
  production_gate.include?('actual="$(kubectl_jsonpath "${object}" "${jsonpath}")') &&
  production_gate.include?('pod_start_time="$(kubectl_jsonpath "pod ${pod}"') &&
  production_gate.include?('metad_strategy="$(kubectl_jsonpath "statefulset -l ${selector},app.kubernetes.io/component=metad"')
%w[CEDAR_GRPC_SERVER_CERT CEDAR_GRPC_SERVER_KEY CEDAR_GRPC_CA_CERT CEDAR_GRPC_CLIENT_CERT CEDAR_GRPC_CLIENT_KEY].each do |env|
  abort("MetaD template missing #{env}") unless metad.include?(env)
end
abort('MetaD template must mount gRPC TLS secret') unless metad.include?('mountPath: /etc/cedar/tls') && metad.include?('secretName: {{ required "graphd.tls.existingSecret is required" .Values.graphd.tls.existingSecret | quote }}')

storaged = templates.fetch('storaged-statefulset.yaml')
abort('StorageD template must connect to MetaD gRPC endpoints') unless storaged.include?('--meta "{{ include "cedargraph.metad.endpoints" . }}"')
abort('StorageD wait-for-metad must use MetaD gRPC port') unless storaged.include?('.Values.metad.service.grpcPort')
abort('StorageD template must expose health port') unless storaged.include?('storaged.service.ports.health')
abort('StorageD template must expose metrics port') unless storaged.include?('storaged.service.ports.metrics')
abort('StorageD template must not reference removed scalar service port') if storaged.match?(/storaged\.service\.port(\s|\}|\)|$)/)
%w[CEDAR_GRPC_SERVER_CERT CEDAR_GRPC_SERVER_KEY CEDAR_GRPC_CA_CERT CEDAR_GRPC_CLIENT_CERT CEDAR_GRPC_CLIENT_KEY].each do |env|
  abort("StorageD template missing #{env}") unless storaged.include?(env)
end
abort('StorageD template must mount gRPC TLS secret') unless storaged.include?('mountPath: /etc/cedar/tls') && storaged.include?('secretName: {{ required "graphd.tls.existingSecret is required" .Values.graphd.tls.existingSecret | quote }}')

graphd = templates.fetch('graphd-deployment.yaml')
abort('GraphD template must connect to MetaD gRPC endpoints') unless graphd.include?('{{ include "cedargraph.metad.endpoints" . | quote }}')
abort('GraphD template must expose health port') unless graphd.include?('graphd.service.ports.health')
abort('GraphD template must expose metrics port') unless graphd.include?('graphd.service.ports.metrics')
abort('GraphD template must pass health/metrics ports explicitly to the binary') unless
  graphd.include?('--health_port') &&
  graphd.include?('{{ .Values.graphd.service.ports.health | quote }}') &&
  graphd.include?('--metrics_port') &&
  graphd.include?('{{ .Values.graphd.service.ports.metrics | quote }}')
abort('GraphD template must not reference removed http service port') if graphd.include?('graphd.service.ports.http')
%w[CEDAR_GRPC_SERVER_CERT CEDAR_GRPC_SERVER_KEY CEDAR_GRPC_CA_CERT CEDAR_GRPC_CLIENT_CERT CEDAR_GRPC_CLIENT_KEY].each do |env|
  abort("GraphD template missing #{env}") unless graphd.include?(env)
end
abort('GraphD template must mount gRPC TLS secret') unless graphd.include?('mountPath: /etc/cedar/tls') && graphd.include?('secretName: {{ required "graphd.tls.existingSecret is required" .Values.graphd.tls.existingSecret | quote }}')

notes = File.read(File.join(chart_dir, 'templates', 'NOTES.txt'))
abort('Helm NOTES must use storaged.service.ports.grpc') unless notes.include?('storaged.service.ports.grpc')
abort('Helm NOTES must not reference removed scalar storaged.service.port') if notes.match?(/storaged\.service\.port(\s|\}|\)|$)/)

pdb = templates.fetch('pdb.yaml')
%w[metad storaged graphd].each do |component|
  abort("PDB template missing #{component} component selector") unless pdb.include?("app.kubernetes.io/component: #{component}")
end
abort('PDB template must be gated by pdb.enabled') unless pdb.include?('if .Values.pdb.enabled')
abort('PDB template must cap MetaD minAvailable at replicas') unless
  pdb.include?('min (int .Values.pdb.metad.minAvailable) (int .Values.metad.replicas)')
abort('PDB template must cap StorageD minAvailable at replicas') unless
  pdb.include?('min (int .Values.pdb.storaged.minAvailable) (int .Values.storaged.replicas)')
abort('PDB template must cap GraphD minAvailable at replicas') unless
  pdb.include?('min (int .Values.pdb.graphd.minAvailable) (int .Values.graphd.replicas)')

puts 'Helm static preflight passed'
RUBY
