#!/bin/bash
# =============================================================================
# CedarGraph Kubernetes Static Preflight
# =============================================================================
# Renders the kustomize deployment and checks cross-object invariants that plain
# YAML parsing cannot catch, such as PDB selectors matching pod template labels.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."
RENDERED="${CEDAR_K8S_RENDERED:-/tmp/cedar_kustomize_render.yaml}"

cd "${PROJECT_ROOT}"

kubectl kustomize k8s > "${RENDERED}"

ruby - "${RENDERED}" <<'RUBY'
require 'yaml'

path = ARGV.fetch(0)
docs = YAML.load_stream(File.read(path)).compact
by_kind_name = docs.each_with_object({}) do |doc, index|
  index[[doc['kind'], doc.dig('metadata', 'name')]] = doc
end

def require_object(index, kind, name)
  index[[kind, name]] || abort("missing #{kind}/#{name}")
end

metad_service = require_object(by_kind_name, 'Service', 'metad')
require_object(by_kind_name, 'StatefulSet', 'metad')
require_object(by_kind_name, 'StatefulSet', 'storaged')
require_object(by_kind_name, 'Deployment', 'graphd')
network_policy = require_object(by_kind_name, 'NetworkPolicy', 'cedargraph-network-policy')
network_policy_labels = network_policy.dig('metadata', 'labels') || {}
abort('NetworkPolicy metadata labels must include app.kubernetes.io/instance=cedargraph for live preflight selection') unless
  network_policy_labels['app.kubernetes.io/instance'] == 'cedargraph'
abort('Kustomize output must not deploy standalone QueryD') if by_kind_name.key?(['Deployment', 'queryd'])
abort('Kustomize output must not expose standalone QueryD service') if by_kind_name.key?(['Service', 'queryd'])

metad_ports = metad_service.fetch('spec').fetch('ports').map { |p| [p['name'], p['port']] }.to_h
abort("bad MetaD ports: #{metad_ports.inspect}") unless metad_ports['raft'] == 9559 && metad_ports['grpc'] == 10559

storaged = require_object(by_kind_name, 'StatefulSet', 'storaged')
storaged_command = storaged.dig('spec', 'template', 'spec', 'containers', 0, 'command').join("\n")
abort('StorageD must connect to MetaD gRPC metad:10559') unless storaged_command.include?('--meta "metad:10559"')
abort('StorageD must launch health HTTP port 7000') unless storaged_command.include?('--health_port "7000"')
abort('StorageD must launch metrics HTTP port 7001') unless storaged_command.include?('--metrics_port "7001"')

storaged_container_ports = storaged.dig('spec', 'template', 'spec', 'containers', 0, 'ports').map { |p| [p['name'], p['containerPort']] }.to_h
abort("StorageD container ports must expose grpc/health/metrics: #{storaged_container_ports.inspect}") unless
  storaged_container_ports['grpc'] == 9779 &&
  storaged_container_ports['health'] == 7000 &&
  storaged_container_ports['metrics'] == 7001

storaged_service = require_object(by_kind_name, 'Service', 'storaged')
storaged_service_ports = storaged_service.fetch('spec').fetch('ports').map { |p| [p['name'], p['port']] }.to_h
abort("StorageD service ports must expose grpc and health: #{storaged_service_ports.inspect}") unless
  storaged_service_ports['grpc'] == 9779 &&
  storaged_service_ports['health'] == 7000

storaged_metrics_service = require_object(by_kind_name, 'Service', 'storaged-metrics')
storaged_metrics_ports = storaged_metrics_service.fetch('spec').fetch('ports').map { |p| [p['name'], p['port']] }.to_h
abort("StorageD metrics service must expose metrics 7001: #{storaged_metrics_ports.inspect}") unless storaged_metrics_ports['metrics'] == 7001

graphd = require_object(by_kind_name, 'Deployment', 'graphd')
graphd_args = graphd.dig('spec', 'template', 'spec', 'containers', 0, 'args') || []
abort("GraphD args must include metad:10559: #{graphd_args.inspect}") unless graphd_args.include?('metad:10559')
abort("GraphD args must include health/metrics ports: #{graphd_args.inspect}") unless
  graphd_args.each_cons(2).any? { |a, b| a == '--health_port' && b == '9668' } &&
  graphd_args.each_cons(2).any? { |a, b| a == '--metrics_port' && b == '9667' }

graphd_container_ports = graphd.dig('spec', 'template', 'spec', 'containers', 0, 'ports').map { |p| [p['name'], p['containerPort']] }.to_h
abort("GraphD container ports must expose query/health/metrics/gcn: #{graphd_container_ports.inspect}") unless
  graphd_container_ports['query'] == 9669 &&
  graphd_container_ports['health'] == 9668 &&
  graphd_container_ports['metrics'] == 9667 &&
  graphd_container_ports['gcn'] == 9780

graphd_service = require_object(by_kind_name, 'Service', 'graphd')
graphd_service_ports = graphd_service.fetch('spec').fetch('ports').map { |p| [p['name'], p['port']] }.to_h
abort("GraphD service ports must expose query/health/metrics: #{graphd_service_ports.inspect}") unless
  graphd_service_ports['query'] == 9669 &&
  graphd_service_ports['health'] == 9668 &&
  graphd_service_ports['metrics'] == 9667

image_refs = docs.flat_map do |doc|
  containers = doc.dig('spec', 'template', 'spec', 'containers') || []
  init_containers = doc.dig('spec', 'template', 'spec', 'initContainers') || []
  (containers + init_containers).map { |container| container['image'] }.compact
end
latest_images = image_refs.grep(/:latest\z/)
abort("Kustomize output must not contain :latest images: #{latest_images.inspect}") unless latest_images.empty?
cedar_images = image_refs.select { |image| image.start_with?('cedargraph:') }
abort("Kustomize CedarGraph images must be pinned to k8s-fix-20260630: #{cedar_images.inspect}") unless
  cedar_images.any? && cedar_images.all? { |image| image == 'cedargraph:k8s-fix-20260630' }

graphd_annotations = graphd.dig('spec', 'template', 'metadata', 'annotations') || {}
abort("GraphD prometheus annotation must scrape metrics port 9667: #{graphd_annotations.inspect}") unless
  graphd_annotations['prometheus.io/scrape'] == 'true' &&
  graphd_annotations['prometheus.io/port'] == '9667'

allowed_ingress_ports = network_policy.fetch('spec').fetch('ingress').flat_map do |rule|
  (rule['ports'] || []).map { |port| port['port'] }
end
required_ports = [7000, 7001, 9559, 10559, 9667, 9668, 9669, 9780, 9779]
missing_ports = required_ports - allowed_ingress_ports
abort("NetworkPolicy missing required ingress ports: #{missing_ports.inspect}") unless missing_ports.empty?

policy_types = network_policy.dig('spec', 'policyTypes') || []
abort('NetworkPolicy should not restrict egress in the static deployment manifest') if policy_types.include?('Egress')
network_policy_selector = network_policy.dig('spec', 'podSelector', 'matchLabels') || {}
abort('NetworkPolicy podSelector must include app.kubernetes.io/instance=cedargraph') unless
  network_policy_selector['app.kubernetes.io/instance'] == 'cedargraph'
%w[metad storaged graphd].each do |workload_name|
  workload = require_object(by_kind_name, workload_name == 'graphd' ? 'Deployment' : 'StatefulSet', workload_name)
  labels = workload.dig('spec', 'template', 'metadata', 'labels') || {}
  missing = network_policy_selector.reject { |key, value| labels[key] == value }
  abort("NetworkPolicy selector does not match #{workload_name} pod labels: #{missing.inspect}") unless missing.empty?
end
network_sources = network_policy.fetch('spec').fetch('ingress').flat_map do |rule|
  rule['from'] || []
end
abort('NetworkPolicy must not allow ingress from every namespace') if
  network_sources.any? { |source| source['namespaceSelector'] == {} }
abort('NetworkPolicy must restrict ingress source namespaces by label') unless
  network_sources.any? do |source|
    labels = source.dig('namespaceSelector', 'matchLabels') || {}
    labels['name'] == 'cedargraph' || labels['kubernetes.io/metadata.name'] == 'cedargraph'
  end

{
  'cedargraph-metad-pdb' => ['StatefulSet', 'metad'],
  'cedargraph-storaged-pdb' => ['StatefulSet', 'storaged'],
  'cedargraph-graphd-pdb' => ['Deployment', 'graphd']
}.each do |pdb_name, (kind, workload_name)|
  pdb = require_object(by_kind_name, 'PodDisruptionBudget', pdb_name)
  workload = require_object(by_kind_name, kind, workload_name)
  selector = pdb.dig('spec', 'selector', 'matchLabels') || {}
  labels = workload.dig('spec', 'template', 'metadata', 'labels') || {}
  missing = selector.reject { |key, value| labels[key] == value }
  abort("PDB #{pdb_name} selector does not match #{kind}/#{workload_name} pod labels: #{missing.inspect}") unless missing.empty?
end

puts 'Kubernetes static preflight passed'
RUBY
