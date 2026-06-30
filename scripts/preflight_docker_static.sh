#!/bin/bash
# =============================================================================
# CedarGraph Docker/Compose Static Preflight
# =============================================================================
# Validates Docker Compose rendering and Dockerfile/entrypoint invariants without
# requiring a running Docker daemon.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."
PROD_RENDERED="${CEDAR_COMPOSE_PROD_RENDERED:-/tmp/cedar_compose_production.yaml}"
DEV_RENDERED="${CEDAR_COMPOSE_DEV_RENDERED:-/tmp/cedar_compose_dev.yaml}"
PROD_MISSING_REQUIRED_LOG="${CEDAR_COMPOSE_PROD_MISSING_REQUIRED_LOG:-/tmp/cedar_compose_production_missing_required.log}"

cd "${PROJECT_ROOT}"

export CEDAR_GRAPHD_AUTH_JWT_SECRET="${CEDAR_GRAPHD_AUTH_JWT_SECRET:-cedar-static-preflight-secret-32-bytes}"
export CEDAR_GRAPHD_AUTH_USER="${CEDAR_GRAPHD_AUTH_USER:-admin}"
export CEDAR_GRAPHD_AUTH_PASSWORD="${CEDAR_GRAPHD_AUTH_PASSWORD:-cedar-static-preflight-password}"
export CEDAR_GRPC_TLS_ENABLED="${CEDAR_GRPC_TLS_ENABLED:-0}"
export GRAFANA_PASSWORD="${GRAFANA_PASSWORD:-cedar-static-preflight-grafana-password}"

# Keep compose rendering deterministic even when the developer shell has legacy
# port overrides such as GRAPH_PORT=19669.
export GRAPH_PORT=9669
export GRAPH_HEALTH_PORT=9668
export GRAPH_METRICS_PORT=9667
export STORAGE_PORT=9779
export META_PORT=9559
export META_GRPC_PORT=10559

required_production_vars=(
  CEDAR_GRAPHD_AUTH_JWT_SECRET
  CEDAR_GRAPHD_AUTH_USER
  CEDAR_GRAPHD_AUTH_PASSWORD
  GRAFANA_PASSWORD
)

for required_var in "${required_production_vars[@]}"; do
  if env -u "${required_var}" docker compose -f docker-compose.production.yml config > /dev/null 2> "${PROD_MISSING_REQUIRED_LOG}"; then
    echo "docker-compose.production.yml must fail closed when ${required_var} is unset" >&2
    exit 1
  fi
  if ! rg -q "${required_var} is required in production" "${PROD_MISSING_REQUIRED_LOG}"; then
    echo "docker-compose.production.yml must explain that ${required_var} is required in production" >&2
    cat "${PROD_MISSING_REQUIRED_LOG}" >&2
    exit 1
  fi
done

docker compose -f docker-compose.production.yml config > "${PROD_RENDERED}"
docker compose -f cedar-docker-compose/docker-compose.yml config > "${DEV_RENDERED}"

ruby - "${PROD_RENDERED}" "${DEV_RENDERED}" <<'RUBY'
require 'yaml'

prod_path, dev_path = ARGV

if File.exist?('.env')
  env = {}
  File.readlines('.env', chomp: true).each do |line|
    next if line.strip.empty? || line.lstrip.start_with?('#')
    key, value = line.split('=', 2)
    next unless key && value
    env[key.strip] = value.strip
  end

  expected_ports = {
    'META_PORT' => '9559',
    'META_GRPC_PORT' => '10559',
    'STORAGE_PORT' => '9779',
    'GRAPH_PORT' => '9669',
    'GRAPH_HEALTH_PORT' => '9668',
    'GRAPH_METRICS_PORT' => '9667'
  }

  expected_ports.each do |key, expected|
    actual = env[key]
    abort(".env #{key} must default to #{expected} because docker-compose.production.yml auto-loads .env; got #{actual.inspect}") unless actual == expected
  end
  abort(".env CEDAR_VERSION must default to k8s-fix-20260630, not latest") unless env['CEDAR_VERSION'] == 'k8s-fix-20260630'
end

def load_compose(path)
  YAML.load_file(path).fetch('services')
end

def command_for(service)
  Array(service['command']).join(' ')
end

def env_for(service)
  service.fetch('environment', {})
end

def published_ports(service)
  Array(service['ports']).map { |port| [port['target'], port['published'].to_s] }
end

[
  ['production', load_compose(prod_path)],
  ['dev', load_compose(dev_path)]
].each do |name, services|
  image_refs = services.values.map { |service| service['image'] }.compact
  latest_images = image_refs.grep(/:latest\z/)
  abort("#{name} compose rendering must not contain :latest images: #{latest_images.inspect}") unless latest_images.empty?
  cedar_images = image_refs.select { |image| image.start_with?('cedargraph/cedar:') }
  abort("#{name} CedarGraph image must default to k8s-fix-20260630: #{cedar_images.inspect}") unless
    cedar_images.any? && cedar_images.all? { |image| image == 'cedargraph/cedar:k8s-fix-20260630' }

  %w[metad0 metad1 metad2].each_with_index do |service_name, index|
    service = services.fetch(service_name)
    command = command_for(service)
    abort("#{name} #{service_name} must listen on Raft 9559") unless command.include?('--listen 0.0.0.0:9559')
    abort("#{name} #{service_name} must expose gRPC 10559") unless command.include?('--grpc_port 10559')
    abort("#{name} #{service_name} healthcheck must probe gRPC 10559") unless service.dig('healthcheck', 'test').join(' ').include?('10559')
    expected_node_id = (index + 1).to_s
    abort("#{name} #{service_name} must use node id #{expected_node_id}") unless command.include?("--node_id #{expected_node_id}")
  end

  %w[storaged0 storaged1 storaged2 graphd].each do |service_name|
    service = services.fetch(service_name)
    command = command_for(service)
    env = env_for(service)
    abort("#{name} #{service_name} must connect to MetaD gRPC 10559") unless command.include?('metad0:10559,metad1:10559,metad2:10559')
    abort("#{name} #{service_name} env must not point MetaD clients at Raft 9559") if env.values.any? { |value| value.to_s.include?('metad0:9559') || value.to_s.include?('metad:9559') }
  end

  graphd_env = env_for(services.fetch('graphd'))
  %w[META_SERVERS CEDAR_METAD_ENDPOINT].each do |env_name|
    abort("#{name} graphd #{env_name} must include all three MetaD gRPC endpoints") unless
      graphd_env.fetch(env_name, '').include?('metad0:10559,metad1:10559,metad2:10559')
  end

  %w[metad0 metad1 metad2 storaged0 storaged1 storaged2 graphd].each do |service_name|
    service = services.fetch(service_name)
    env = env_for(service)
    tls_enabled = env['CEDAR_GRPC_TLS_ENABLED'].to_s
    next unless tls_enabled == '1' || tls_enabled.downcase == 'true'
    volume_targets = Array(service['volumes']).map { |volume| volume['target'].to_s }
    readonly_tls_mount = Array(service['volumes']).any? do |volume|
      volume['target'].to_s == '/etc/cedar/tls' && volume['read_only'] == true
    end
    abort("#{name} #{service_name} enables TLS but does not mount /etc/cedar/tls: #{volume_targets.inspect}") unless readonly_tls_mount
    %w[
      CEDAR_GRPC_SERVER_CERT
      CEDAR_GRPC_SERVER_KEY
      CEDAR_GRPC_CA_CERT
      CEDAR_GRPC_MTLS_ENABLED
      CEDAR_GRPC_CLIENT_CERT
      CEDAR_GRPC_CLIENT_KEY
    ].each do |env_name|
      abort("#{name} #{service_name} enables TLS but is missing #{env_name}") unless env.key?(env_name)
    end
  end

  graphd = services.fetch('graphd')
  graphd_ports = published_ports(graphd)
  abort("#{name} graphd must publish query 9669") unless graphd_ports.include?([9669, '9669'])
  abort("#{name} graphd must publish health 9668") unless graphd_ports.include?([9668, '9668'])
  abort("#{name} graphd must publish metrics 9667") unless graphd_ports.include?([9667, '9667'])
  abort("#{name} graphd must not publish removed HTTP 19669") if graphd_ports.any? { |target, published| target == 19669 || published == '19669' }

  if services.key?('console')
    console = services.fetch('console')
    abort("#{name} console must clear the role-aware image entrypoint") unless console.key?('entrypoint') && Array(console['entrypoint']).empty?
    abort("#{name} console must keep cedar-cli available in its command") unless command_for(console).include?('cedar-cli')
  end
end

puts 'Docker Compose static preflight passed'
RUBY

ruby - <<'RUBY'
paths = %w[
  cedar-docker-compose/Dockerfile
  cedar-docker-compose/Dockerfile.cn
  cedar-docker-compose/Dockerfile.build
]

paths.each do |path|
  text = File.read(path)
  abort("#{path} must expose MetaD gRPC 10559") unless text.match?(/^EXPOSE .*10559/)
  abort("#{path} must expose GraphD health 9668") unless text.match?(/^EXPOSE .*9668/)
  abort("#{path} must expose GraphD metrics 9667") unless text.match?(/^EXPOSE .*9667/)
  abort("#{path} must not expose removed GraphD HTTP 19669") if text.match?(/^EXPOSE .*19669/)
  abort("#{path} must use role-aware docker entrypoint") unless text.include?('ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]')
  abort("#{path} must install cedar-cli for console and operator docs") unless text.include?('COPY scripts/cedar-cli.sh /usr/local/bin/cedar-cli')
  abort("#{path} must install cedar-admin for quick-start/admin docs") unless text.include?('COPY cedar-docker-compose/scripts/cedar-admin.sh /usr/local/bin/cedar-admin')
end

def copied_context_sources(path)
  File.readlines(path, chomp: true).flat_map do |line|
    stripped = line.sub(/\s+#.*\z/, '').strip
    next [] unless stripped.start_with?('COPY ')
    next [] if stripped.include?('--from=')

    parts = stripped.split(/\s+/)
    parts.shift
    parts.shift while parts.first&.start_with?('--')
    next [] if parts.length < 2

    parts[0...-1]
  end
end

paths.each do |path|
  copied_context_sources(path).each do |source|
    source = source.sub(%r{\A\./}, '')
    next if source == '.'
    abort("#{path} COPY source #{source.inspect} must exist in the Docker release context") unless File.exist?(source)
  end
end

docker_context_list = ENV.fetch('CEDAR_DOCKER_CONTEXT_LIST', '/tmp/cedar_docker_context_static_preflight.txt')
context_tar = docker_context_list.sub(/\.txt\z/, '.tar')
unless system('tar', '--exclude-vcs', '--exclude-from=.dockerignore', '-cf', context_tar, '.')
  abort('Docker release context archive creation failed')
end
unless system('tar', '-tf', context_tar, out: docker_context_list)
  abort('Docker release context archive listing failed')
end
context_entries = File.readlines(docker_context_list, chomp: true).map { |entry| entry.sub(%r{\A\./}, '') }
paths.each do |path|
  copied_context_sources(path).each do |source|
    source = source.sub(%r{\A\./}, '')
    next if source == '.'

    included = if File.directory?(source)
                 context_entries.any? { |entry| entry == source || entry.start_with?("#{source}/") }
               else
                 context_entries.include?(source)
               end
    abort("#{path} COPY source #{source.inspect} is excluded from the Docker release context") unless included
  end
end
forbidden_context_entries = [
  '.env',
  'build/',
  'build_review/',
  'build_brpc/',
  'build_linux/',
  'certs/',
  'data/',
  'logs/',
  'secrets/',
  'tmp/',
  'temp/',
  'Testing/'
]
forbidden_context_entries.each do |entry|
  abort("Docker release context must not include #{entry}") if
    context_entries.any? { |candidate| candidate == entry.delete_suffix('/') || candidate.start_with?(entry) }
end
forbidden_context_suffixes = %w[.crt .csr .jks .key .p12 .pem]
context_entries.each do |entry|
  abort("Docker release context must not include sensitive file #{entry}") if
    forbidden_context_suffixes.any? { |suffix| entry.end_with?(suffix) } ||
    File.basename(entry).start_with?('.env.')
end

entrypoint = File.read('cedar-docker-compose/docker-entrypoint.sh')
abort('docker-entrypoint must dispatch cedar-metad') unless entrypoint.include?('cedar-metad')
abort('docker-entrypoint must dispatch cedar-storaged') unless entrypoint.include?('cedar-storaged')
abort('docker-entrypoint must dispatch cedar-graphd') unless entrypoint.include?('cedar-graphd')
abort('docker-entrypoint must validate mTLS client cert when CEDAR_GRPC_MTLS_ENABLED is set') unless
  entrypoint.include?('CEDAR_GRPC_MTLS_ENABLED') &&
  entrypoint.include?('CEDAR_GRPC_CLIENT_CERT') &&
  entrypoint.include?('CEDAR_GRPC_CLIENT_KEY')
abort('graphd-entrypoint must use health_port') unless File.read('cedar-docker-compose/scripts/graphd-entrypoint.sh').include?('--health_port "$HEALTH_PORT"')

storaged_tool = File.read('tools/storaged.cc')
abort('StorageD must map MetaD leader hints back to configured gRPC endpoints') unless
  storaged_tool.include?('SwitchToLeaderHint') &&
  storaged_tool.include?('AddressHost(meta_addrs_[i]) == leader_host')
abort('StorageD must not create a gRPC channel directly from MetaD leader_address because it may be a Raft 9559 address') if
  storaged_tool.include?('CreateChannel(response.leader_address') ||
  storaged_tool.include?('SwitchToMetaEndpoint')
graphd_tool = File.read('tools/graphd.cc')
abort('GraphD must apply CEDAR_GRPC_* environment variables to its runtime TLS config') unless
  graphd_tool.include?('ApplyTlsEnvOverrides(&config.tls)') &&
  graphd_tool.include?('CEDAR_GRPC_TLS_ENABLED') &&
  graphd_tool.include?('CEDAR_GRPC_SERVER_CERT')
abort('StorageD must apply CEDAR_GRPC_* environment variables to its runtime TLS config') unless
  storaged_tool.include?('ApplyTlsEnvOverrides(&config.tls)') &&
  storaged_tool.include?('CEDAR_GRPC_TLS_ENABLED') &&
  storaged_tool.include?('CEDAR_GRPC_SERVER_CERT')
{
  'src/client/connection_pool.cc' => ['ConnectionPool', 'config_.enable_tls || cedar::dtx::raft::TlsCredentialFactory::EnvTlsEnabled()'],
  'src/client/service_discovery.cc' => ['ServiceDiscovery', '!config.enable_tls && !cedar::dtx::raft::TlsCredentialFactory::EnvTlsEnabled()'],
  'src/client/graphd_load_balancer.cc' => ['GraphDLoadBalancer', '!config.enable_tls && !cedar::dtx::raft::TlsCredentialFactory::EnvTlsEnabled()']
}.each do |path, (name, guard)|
  text = File.read(path)
  abort("#{name} must honor CEDAR_GRPC_TLS_ENABLED before using insecure gRPC credentials") unless
    text.include?('EnvTlsEnabled()') &&
    text.include?('CreateClientCredentialsFromEnvStrict()') &&
    text.include?(guard)
end

admin = File.read('cedar-docker-compose/scripts/cedar-admin.sh')
root_cli = File.read('scripts/cedar-cli.sh')
compose_cli = File.read('cedar-docker-compose/scripts/cedar-cli.sh')
graphd_entrypoint = File.read('cedar-docker-compose/scripts/graphd-entrypoint.sh')
quick_start = File.read('cedar-docker-compose/scripts/quick-start.sh')
root_quick_start = File.read('scripts/quick-start.sh')
install_script = File.read('scripts/deploy/install.sh')
docker_runtime = File.read('scripts/preflight_docker_image_runtime.sh')
compose_smoke = File.read('scripts/preflight_compose_smoke.sh')
misleading_registration = /模拟成功|简化版：模拟成功|Registered:|注册完成|节点添加完成/
abort('cedar-admin must not claim simulated storage registration succeeded') if admin.match?(misleading_registration)
abort('graphd-entrypoint must not claim simulated storage registration succeeded') if graphd_entrypoint.match?(misleading_registration)
abort('cedar-admin show-hosts must fail closed when no real StorageD container is found') unless
  admin.include?('未发现真实 cedar-storaged 容器') &&
  admin.include?('exit 1')
{
  'scripts/cedar-cli.sh' => root_cli,
  'cedar-docker-compose/scripts/cedar-cli.sh' => compose_cli
}.each do |path, text|
  abort("#{path} SHOW HOSTS must fail closed when no real StorageD container is found") unless
    text.include?('未发现真实 cedar-storaged Docker 容器') &&
    text.include?('不能据此证明存储节点在线') &&
    text.include?('return 2')
  abort("#{path} SHOW HOSTS must derive host state from Docker, not hard-coded examples") unless
    text.include?('docker ps --filter "name=cedar-storaged"') &&
    text.include?('local state="ONLINE"') &&
    text.include?('state="OFFLINE"')
end
abort('cedar-admin auto-discover must be a reachability check unless it calls a real MetaD registration API') unless
  admin.include?('真实 StorageD 注册由 StorageD 进程通过 MetaD RPC 完成') &&
  admin.include?('可达节点')
abort('graphd-entrypoint storage discovery must be a reachability check unless it calls a real MetaD registration API') unless
  graphd_entrypoint.include?('真实 StorageD 注册由 StorageD 进程通过 MetaD RPC 完成') &&
  graphd_entrypoint.include?('Reachable:')
abort('quick-start readiness must check Docker health status, not only Running containers') unless
  quick_start.include?('unhealthy=') &&
  quick_start.include?('core_running') &&
  quick_start.include?('core=') &&
  quick_start.include?('unhealthy') &&
  root_quick_start.include?('unhealthy=') &&
  root_quick_start.include?('core_running') &&
  root_quick_start.include?('core=') &&
  root_quick_start.include?('unhealthy')
abort('quick-start must fail deployment initialization when GraphD status or StorageD reachability checks fail') unless
  quick_start.include?('GraphD 状态检查失败') &&
  quick_start.include?('StorageD 自动发现或连通性检查失败') &&
  quick_start.include?('init_cluster ||') &&
  root_quick_start.include?('GraphD 状态检查失败') &&
  root_quick_start.include?('StorageD 自动发现或连通性检查失败') &&
  root_quick_start.include?('init_cluster ||')
abort('quick-start must not downgrade failed storage discovery to a warning before printing deployment success') if
  quick_start.include?('warning "自动发现可能失败，请手动检查"') ||
  root_quick_start.include?('warning "自动发现可能失败，请手动检查"')
abort('quick-start --clean must require explicit opt-in and validate deletion targets') unless
  root_quick_start.include?('CEDAR_QUICKSTART_ALLOW_CLEAN') &&
  root_quick_start.include?('is_safe_clean_target') &&
  root_quick_start.include?('DATA_DIR:?') &&
  root_quick_start.include?('LOGS_DIR:?') &&
  quick_start.include?('CEDAR_QUICKSTART_ALLOW_CLEAN') &&
  quick_start.include?('is_safe_clean_target') &&
  quick_start.include?('DATA_DIR:?') &&
  quick_start.include?('LOGS_DIR:?')
abort('cedar-docker-compose quick-start must default to the verified immutable image tag') unless
  quick_start.include?('CEDAR_VERSION="k8s-fix-20260630"') &&
  quick_start.include?('默认: k8s-fix-20260630')
abort('root quick-start must default to the verified immutable image tag') unless
  root_quick_start.include?('CEDAR_VERSION="k8s-fix-20260630"') &&
  root_quick_start.include?('默认: k8s-fix-20260630')
abort('install.sh must refuse mutable latest releases') unless
  install_script.include?('Refusing to install the mutable') &&
  install_script.include?('VERSION="${1:-k8s-fix-20260630}"') &&
  !install_script.include?('/releases/latest/download')
abort('Docker image runtime preflight must validate the pinned CedarGraph runtime image') unless
  File.executable?('scripts/preflight_docker_image_runtime.sh') &&
  docker_runtime.include?('cedargraph/cedar:k8s-fix-20260630') &&
  docker_runtime.include?('image_user') &&
  docker_runtime.include?('non-root cedar user') &&
  docker_runtime.include?('docker-entrypoint.sh') &&
  docker_runtime.include?('Healthcheck.Test') &&
  docker_runtime.include?('id -u') &&
  docker_runtime.include?('sh -n /usr/local/bin/docker-entrypoint.sh') &&
  docker_runtime.include?('cedar-cli') &&
  docker_runtime.include?('cedar-admin') &&
  docker_runtime.include?('cedar-metad') &&
  docker_runtime.include?('cedar-storaged') &&
  docker_runtime.include?('cedar-graphd') &&
  docker_runtime.include?('ldd') &&
  docker_runtime.include?('not found')
abort('Release gate must include Docker image runtime preflight') unless
  File.read('scripts/preflight_release_gate.sh').include?('preflight_docker_image_runtime.sh')
abort('Production Compose smoke must run the core topology in temporary data/log directories') unless
  File.executable?('scripts/preflight_compose_smoke.sh') &&
  compose_smoke.include?('compose-smoke') &&
  compose_smoke.include?('DATA_DIR="${BASE_DIR}/data"') &&
  compose_smoke.include?('LOG_DIR="${BASE_DIR}/logs"') &&
  compose_smoke.include?('CEDAR_GRPC_ALLOW_INSECURE=1') &&
  compose_smoke.include?('metad0 metad1 metad2 storaged0 storaged1 storaged2 graphd') &&
  compose_smoke.include?('down --remove-orphans') &&
  compose_smoke.include?('LOCK_DIR') &&
  compose_smoke.include?('Another production Compose smoke appears to be running')
abort('Production Compose smoke must support a real TLS credential path') unless
  compose_smoke.include?('CEDAR_COMPOSE_SMOKE_TLS') &&
  compose_smoke.include?('openssl req -x509') &&
  compose_smoke.include?('CEDAR_GRPC_TLS_ENABLED=1') &&
  compose_smoke.include?('CEDAR_GRPC_ALLOW_INSECURE=0') &&
  compose_smoke.include?('TLS Compose smoke passed')
abort('Production Compose smoke must not treat recoverable GraphD bootstrap NotFound as a severe log failure') if
  compose_smoke.include?('not found|ERROR') ||
  compose_smoke.include?('NotFound')
abort('Release gate must include production Compose smoke') unless
  File.read('scripts/preflight_release_gate.sh').include?('preflight_compose_smoke.sh')
abort('Release gate must include production Compose TLS smoke') unless
  File.read('scripts/preflight_release_gate.sh').include?('CEDAR_COMPOSE_SMOKE_TLS=1')

puts 'Dockerfile static preflight passed'
RUBY
