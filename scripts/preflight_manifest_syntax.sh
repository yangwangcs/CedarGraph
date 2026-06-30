#!/bin/bash
# =============================================================================
# CedarGraph Manifest and Script Syntax Preflight
# =============================================================================
# Fast offline syntax checks for deployment scripts and operator-facing
# manifests. This intentionally avoids Helm templates because raw templates are
# not valid YAML until rendered.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."

cd "${PROJECT_ROOT}"

while IFS= read -r file; do
  bash -n "${file}"
  if head -n 1 "${file}" | grep -Eq '^#!.*(bash|sh)'; then
    [[ -x "${file}" ]] || {
      echo "script has a shell shebang but is not executable: ${file}" >&2
      exit 1
    }
  fi
done < <(find scripts cedar-docker-compose -type f -name '*.sh' | sort)

"${PROJECT_ROOT}/scripts/preflight_release_gate.sh" --self-test

ruby - <<'RUBY'
require 'json'
require 'yaml'

yaml_files = %w[
  .github/workflows/build.yml
  .github/workflows/docker-release.yml
  .github/workflows/snyk-scan.yml
  .github/workflows/trivy-scan.yml
  cedar-docker-compose/.github/workflows/docker-release.yml
  cedar-docker-compose/docker-compose.yml
  config/alertmanager.yml
  config/cedar.yaml
  config/cedar_alerts.yml
  config/docker-compose.monitoring.yml
  config/grafana/dashboards/cedargraph-overview.yml
  config/grafana/datasources/prometheus.yml
  config/graphd.yaml
  config/partition.yaml
  config/prometheus.yml
  config/storaged_node0.yaml
  config/storaged_node1.yaml
  config/storaged_node2.yaml
  docker-compose.production.yml
  helm-chart/cedargraph/Chart.yaml
  helm-chart/cedargraph/values.yaml
  k8s/graphd-config.yaml
  k8s/graphd-secrets.example.yaml
  k8s/graphd.yaml
  k8s/kustomization.yaml
  k8s/metad.yaml
  k8s/namespace.yaml
  k8s/network-policy.yaml
  k8s/pod-disruption-budget.yaml
  k8s/queryd.yaml
  k8s/storaged-config.yaml
  k8s/storaged.yaml
  tools/cedargraph/config.example.yaml
  tools/cedargraph/config/cedar.yaml
]

json_files = %w[
  config/grafana/dashboards/cedargraph-overview.json
]

yaml_files.each do |path|
  abort("missing YAML file: #{path}") unless File.file?(path)
  YAML.load_stream(File.read(path))
end

json_files.each do |path|
  abort("missing JSON file: #{path}") unless File.file?(path)
  JSON.parse(File.read(path))
end

script_files = Dir.glob('{scripts,cedar-docker-compose}/**/*.sh')
developer_repo_path = ['', 'Users', 'wangyang', 'Desktop', 'CedarGraph-Core'].join('/')
script_files.each do |path|
  text = File.read(path)
  abort("#{path} must not hard-code a developer-specific repository path") if
    text.include?(developer_repo_path)
  abort("#{path} must not globally pkill CedarGraph service processes; track and stop owned PIDs instead") if
    text.match?(/pkill\s+-f\s+["']?cedar-(metad|storaged|graphd)\b/)
end

%w[
  scripts/cedar-cli.sh
  cedar-docker-compose/scripts/cedar-cli.sh
].each do |path|
  text = File.read(path)
  %w[CREATE\ SPACE SHOW\ SPACES SHOW\ ZONES DESCRIBE\ SPACE CREATE\ TAG CREATE\ EDGE SHOW\ EDGES INSERT\ VERTEX INSERT\ EDGE MATCH].each do |command|
    pattern = Regexp.escape(command.gsub('\\ ', ' '))
    abort("#{path} must not advertise #{command.gsub('\\ ', ' ')} as an implemented cedar-cli script command") if
      text.match?(/^\s*#{pattern}(\s{2,}|$|<)/)
  end
  abort("#{path} must not hard-code a production graph space in cedar-cli output") if
    text.match?(/^\s*(echo|printf).*production/)
  abort("#{path} must not claim CREATE SPACE succeeded without a real GraphD or MetaD API call") if
    text.include?('Space created successfully')
  abort("#{path} must not print simulated ONLINE storaged hosts") if
    text.match?(/storaged-[0-9].*ONLINE/)
end

production_guide = File.read('docs/PRODUCTION_DEPLOYMENT_GUIDE.md')
%w[SHOW\ SPACES SHOW\ TAGS SHOW\ EDGES CREATE\ SPACE CREATE\ TAG CREATE\ EDGE INSERT\ VERTEX INSERT\ EDGE MATCH].each do |command|
  abort("Production guide must not advertise cedar-cli #{command.gsub('\\ ', ' ')} as an implemented script command") if
    production_guide.match?(/cedar-cli\s+-e\s+["']#{command}/)
end

deployment_readme = File.read('docs/deployment/README.md')
%w[
  cedargraph/metad:latest
  cedargraph/storaged:latest
  cedargraph/graphd:latest
  cedargraph:latest
].each do |obsolete_image|
  abort("docs/deployment/README.md must not advertise obsolete or mutable image #{obsolete_image}") if
    deployment_readme.include?(obsolete_image)
end
abort('docs/deployment/README.md production examples must use the unified cedargraph/cedar image') unless
  deployment_readme.include?('cedargraph/cedar:k8s-fix-20260630')

operator_docs = %w[
  docs/deployment/README.md
  docs/PRODUCTION_DEPLOYMENT_GUIDE.md
  helm-chart/README.md
  helm-chart/cedargraph/templates/NOTES.txt
]
operator_docs.each do |path|
  text = File.read(path)
  abort("#{path} must not provide copy-paste PVC deletion commands; require backup, namespace, release-label audit, and explicit operator approval") if
    text.match?(/kubectl\s+delete\s+pvc/i)
end

%w[
  cedar-docker-compose/README.md
  cedar-docker-compose/DOCKERHUB_README.md
  docs/deployment/README.md
].each do |path|
  text = File.read(path)
  abort("#{path} must not document a bare quick-start --clean command; require CEDAR_QUICKSTART_ALLOW_CLEAN=1 and a backup note") if
    text.match?(%r{(?<!CEDAR_QUICKSTART_ALLOW_CLEAN=1 )\./scripts/quick-start\.sh --clean})
end

%w[
  cedar-docker-compose/README.md
  cedar-docker-compose/IMPLEMENTATION_SUMMARY.md
].each do |path|
  text = File.read(path)
  abort("#{path} must not advertise cedar-cli SHOW SPACES as an implemented script command") if
    text.match?(/cedar-cli(?:\.sh)?\s+-e\s+["']SHOW SPACES/)
  abort("#{path} must not show simulated ONLINE storaged hosts") if
    text.match?(/storaged-[0-9].*ONLINE/)
end
compose_summary = File.read('cedar-docker-compose/IMPLEMENTATION_SUMMARY.md')
abort('Docker compose implementation summary must explain that root docker-release.yml is the executed GitHub Actions workflow') unless
  compose_summary.include?('GitHub Actions 只会自动执行仓库根目录 `.github/workflows/docker-release.yml`')
abort('Docker compose implementation summary must document docker-release workflow synchronization rule') unless
  compose_summary.include?('两份文件必须保持完全一致') &&
  compose_summary.include?('scripts/preflight_manifest_syntax.sh')

Dir.glob('{README.md,docs/**/*.md,config/**/*,tools/cedargraph/**/*.yaml}').each do |path|
  next unless File.file?(path)
  abort("#{path} must not contain developer-specific absolute paths") if
    File.read(path).include?(developer_repo_path)
end

dockerignore = File.readlines('.dockerignore', chomp: true).map(&:strip).reject(&:empty?)
%w[
  .git
  .env
  .env.*
  .preflight-backups
  build
  build_review
  build_brpc
  build_linux
  certs
  cmake-build-*
  data
  dataset
  logs
  out
  secrets
  temp
  tmp
  Testing
  test_results
  .superpowers
  .worktrees
  academic-research-skills-codex-main
  superpowers
  web
  *.db
  *.log
  *.sst
  *.sock
  *.sock.lock
  *.crt
  *.csr
  *.jks
  *.key
  *.p12
  *.pem
  WAL
].each do |pattern|
  abort(".dockerignore must exclude #{pattern} from release build context") unless dockerignore.include?(pattern)
end

root_docker_release_path = '.github/workflows/docker-release.yml'
compose_docker_release_path = 'cedar-docker-compose/.github/workflows/docker-release.yml'
abort('Root GitHub Actions Docker release workflow must exist') unless File.file?(root_docker_release_path)
abort('cedar-docker-compose Docker release workflow copy must stay synchronized with root workflow') unless
  File.read(root_docker_release_path) == File.read(compose_docker_release_path)
docker_release = YAML.load_file(root_docker_release_path)
jobs = docker_release.fetch('jobs')

release_gate = File.read('scripts/preflight_release_gate.sh')
abort('Release preflight gate must require Kubernetes API validation by default') unless
  release_gate.include?('CEDAR_RELEASE_SKIP_K8S_API') &&
  release_gate.include?('kubectl is required for the release preflight gate') &&
  release_gate.include?('preflight_k8s_server_dry_run.sh')
abort('Release preflight gate must run disposable Kubernetes recovery drill by default') unless
  release_gate.include?('CEDAR_RELEASE_K8S_RECOVERY_DRILL') &&
  release_gate.include?('preflight_k8s_recovery_drill.sh') &&
  release_gate.include?('Kubernetes recovery drill') &&
  release_gate.include?('CEDAR_DRILL_MIN_POD_AGE_SECONDS') &&
  release_gate.include?('CEDAR_DRILL_NAMESPACE="${CEDAR_DRILL_NAMESPACE:-cedargraph-recovery-drill-$(date +%s)}"')
abort('Release preflight gate must validate boolean switches fail-closed') unless
  release_gate.include?('validate_bool()') &&
  release_gate.include?('CEDAR_RELEASE_K8S_RECOVERY_DRILL "${K8S_RECOVERY_DRILL}"') &&
  release_gate.include?('invalid release bool should fail')
abort('Release preflight gate must validate soak durations fail-closed') unless
  release_gate.include?('validate_positive_int()') &&
  release_gate.include?('CEDAR_RELEASE_SOAK_SECONDS "${SOAK_SECONDS}"') &&
  release_gate.include?('CEDAR_RELEASE_SOAK_POLL_SECONDS "${SOAK_POLL_SECONDS}"') &&
  release_gate.include?('zero soak seconds should fail') &&
  release_gate.include?('negative soak poll seconds should fail')
abort('Release preflight gate help must describe enforced switch and duration constraints') unless
  release_gate.include?('All switch values must be 0 or 1') &&
  release_gate.include?('All duration values must be positive integers') &&
  release_gate.include?('Kubernetes API dry-run') &&
  release_gate.include?('disposable Kubernetes recovery drill') &&
  release_gate.include?('cedargraph-recovery-drill-<timestamp>')
abort('Release preflight gate must not silently skip Kubernetes API guards when kubectl is missing') if
  release_gate.match?(/if\s+command -v kubectl[^;]+then/m)

soak_gate = File.read('scripts/preflight_soak.sh')
abort('Soak preflight must reject zero or invalid durations') unless
  soak_gate.include?('validate_positive_int()') &&
  soak_gate.include?('CEDAR_SOAK_SECONDS "${SOAK_SECONDS}"') &&
  soak_gate.include?('CEDAR_SOAK_POLL_SECONDS "${POLL_SECONDS}"')
abort('Soak preflight must validate minimum cluster size') unless
  soak_gate.include?('validate_min_int CEDAR_METAD_COUNT "${CEDAR_METAD_COUNT}" 1') &&
  soak_gate.include?('validate_min_int CEDAR_STORAGED_COUNT "${CEDAR_STORAGED_COUNT}" 1')

compose_smoke = File.read('scripts/preflight_compose_smoke.sh')
abort('Compose smoke preflight must validate TLS switch and wait loop durations') unless
  compose_smoke.include?('validate_bool CEDAR_COMPOSE_SMOKE_TLS "${TLS_SMOKE}"') &&
  compose_smoke.include?('validate_positive_int CEDAR_COMPOSE_SMOKE_ATTEMPTS "${ATTEMPTS}"') &&
  compose_smoke.include?('validate_positive_int CEDAR_COMPOSE_SMOKE_POLL_SECONDS "${POLL_SECONDS}"') &&
  compose_smoke.include?('for i in $(seq 1 "${ATTEMPTS}")') &&
  compose_smoke.include?('sleep "${POLL_SECONDS}"')

storage_failover = File.read('scripts/preflight_failover_smoke.sh')
graphd_failover = File.read('scripts/preflight_graphd_failover_smoke.sh')
abort('StorageD failover smoke must validate heartbeat wait duration') unless
  storage_failover.include?('validate_positive_int CEDAR_FAILOVER_WAIT_SECONDS "${WAIT_SECONDS}"')
abort('StorageD failover smoke must require enough nodes to kill the middle storage node and verify survivors') unless
  storage_failover.include?('validate_min_int CEDAR_METAD_COUNT "${CEDAR_METAD_COUNT}" 3') &&
  storage_failover.include?('validate_min_int CEDAR_STORAGED_COUNT "${CEDAR_STORAGED_COUNT}" 3')
abort('GraphD failover smoke must validate cleanup wait duration') unless
  graphd_failover.include?('validate_positive_int CEDAR_GRAPHD_FAILOVER_WAIT_SECONDS "${WAIT_SECONDS}"')
abort('GraphD failover smoke must validate overridden base ports before starting services') unless
  %w[
    CEDAR_GRAPHD_FAILOVER_META_RAFT_PORT
    CEDAR_GRAPHD_FAILOVER_META_GRPC_PORT
    CEDAR_GRAPHD_FAILOVER_GRAPHD_BASE_PORT
    CEDAR_GRAPHD_FAILOVER_HEALTH_BASE_PORT
    CEDAR_GRAPHD_FAILOVER_METRICS_BASE_PORT
  ].all? { |name| graphd_failover.include?("validate_port #{name}") }

local_smoke = File.read('scripts/preflight_local_smoke.sh')
abort('Local smoke must validate all overridden ports before starting services') unless
  %w[
    CEDAR_PREFLIGHT_META_RAFT_PORT
    CEDAR_PREFLIGHT_META_GRPC_PORT
    CEDAR_PREFLIGHT_STORAGE_PORT
    CEDAR_PREFLIGHT_STORAGE_HEALTH_PORT
    CEDAR_PREFLIGHT_STORAGE_METRICS_PORT
    CEDAR_PREFLIGHT_GRAPH_PORT
    CEDAR_PREFLIGHT_GRAPH_HEALTH_PORT
    CEDAR_PREFLIGHT_GRAPH_METRICS_PORT
  ].all? { |name| local_smoke.include?("validate_port #{name}") }

tls_smoke = File.read('scripts/preflight_tls_smoke.sh')
abort('TLS smoke must validate mTLS switch and minimum cluster size') unless
  tls_smoke.include?('validate_bool CEDAR_TLS_SMOKE_MTLS "${CEDAR_TLS_SMOKE_MTLS}"') &&
  tls_smoke.include?('validate_min_int CEDAR_METAD_COUNT "${CEDAR_METAD_COUNT}" 1') &&
  tls_smoke.include?('validate_min_int CEDAR_STORAGED_COUNT "${CEDAR_STORAGED_COUNT}" 1')
abort('TLS smoke must serialize runs that use the default port set') unless
  tls_smoke.include?('CEDAR_TLS_SMOKE_LOCK_DIR') &&
  tls_smoke.include?('Another TLS smoke appears to be running') &&
  tls_smoke.include?('rmdir "${LOCK_DIR}"')

distributed_smoke = File.read('scripts/preflight_distributed_smoke.sh')
abort('Distributed smoke must validate minimum cluster size') unless
  distributed_smoke.include?('validate_min_int CEDAR_METAD_COUNT "${CEDAR_METAD_COUNT}" 1') &&
  distributed_smoke.include?('validate_min_int CEDAR_STORAGED_COUNT "${CEDAR_STORAGED_COUNT}" 1')

non_test_raft_smoke = File.read('scripts/preflight_non_test_raft_smoke.sh')
abort('Non-test-mode Raft smoke must validate minimum cluster size') unless
  non_test_raft_smoke.include?('validate_min_int CEDAR_METAD_COUNT "${CEDAR_METAD_COUNT}" 1') &&
  non_test_raft_smoke.include?('validate_min_int CEDAR_STORAGED_COUNT "${CEDAR_STORAGED_COUNT}" 1')

networkpolicy_gate = File.read('scripts/preflight_k8s_networkpolicy.sh')
abort('Kubernetes NetworkPolicy preflight must fail closed when kubectl cannot read policies') unless
  networkpolicy_gate.include?('Failed to read CedarGraph NetworkPolicy objects') &&
  networkpolicy_gate.include?('if ! policies="$(kubectl get networkpolicy')
abort('Kubernetes NetworkPolicy preflight must not hide policy list API failures behind "|| true"') if
  networkpolicy_gate.match?(/policies="\$\(kubectl get networkpolicy.*\|\| true/m)

recovery_drill = File.read('scripts/preflight_k8s_recovery_drill.sh')
abort('Kubernetes recovery drill must validate timeout and TLS duration overrides fail-closed') unless
  recovery_drill.include?('validate_k8s_duration CEDAR_DRILL_HELM_TIMEOUT "${HELM_TIMEOUT}"') &&
  recovery_drill.include?('validate_k8s_duration CEDAR_DRILL_WAIT_TIMEOUT "${WAIT_TIMEOUT}"') &&
  recovery_drill.include?('validate_positive_int CEDAR_DRILL_TLS_DAYS "${CEDAR_DRILL_TLS_DAYS:-365}"') &&
  recovery_drill.include?('zero Helm timeout should fail') &&
  recovery_drill.include?('zero TLS days should fail')

def assert_docker_release_preflight_tools(steps, job_name)
  install_step = steps.find { |step| step['name'] == 'Install preflight tools' }
  abort("#{job_name} must install preflight tools before running static checks") unless install_step
  install_run = install_step.fetch('run')
  %w[ripgrep ruby].each do |package|
    abort("#{job_name} preflight tool install is missing #{package}") unless install_run.include?(package)
  end
  verify_step = steps.find { |step| step['name'] == 'Verify preflight tools' }
  abort("#{job_name} must verify preflight tools before running static checks") unless verify_step
  verify_run = verify_step.fetch('run')
  %w[rg\ --version ruby\ --version docker\ compose\ version].each do |command|
    abort("#{job_name} preflight tool verification is missing #{command.gsub('\\ ', ' ')}") unless
      verify_run.include?(command.gsub('\\ ', ' '))
  end
end

build_steps = jobs.fetch('build-and-test').fetch('steps')
assert_docker_release_preflight_tools(build_steps, 'Docker release build-and-test job')
docker_preflight_step = build_steps.find { |step| step['name'] == 'Docker release static preflight' }
abort('Docker release workflow must run Docker release static preflight before image build') unless docker_preflight_step
docker_preflight_run = docker_preflight_step.fetch('run')
%w[
  ./scripts/preflight_manifest_syntax.sh
  ./scripts/preflight_docker_static.sh
].each do |command|
  abort("Docker release static preflight is missing #{command}") unless docker_preflight_run.include?(command)
end
build_step = build_steps.find { |step| step['name'] == 'Build Docker image' }
abort('Docker release workflow must build the root context') unless build_step.dig('with', 'context') == '.'
abort('Docker release workflow must use cedar-docker-compose/Dockerfile') unless
  build_step.dig('with', 'file') == 'cedar-docker-compose/Dockerfile'
test_step = build_steps.find { |step| step['name'] == 'Test Docker image' }
abort('Docker release workflow must not test obsolete cedar-server binary') if
  test_step.fetch('run').include?('cedar-server')
abort('Docker release workflow must reuse the Docker image runtime preflight for image tests') unless
  test_step.fetch('run').include?('CEDAR_DOCKER_IMAGE="${{ env.IMAGE_NAME }}:test"') &&
  test_step.fetch('run').include?('./scripts/preflight_docker_image_runtime.sh')
abort('Docker release workflow must not duplicate a hand-written docker run image test') if
  test_step.fetch('run').include?('docker run --rm --entrypoint sh')

release_steps = jobs.fetch('release').fetch('steps')
assert_docker_release_preflight_tools(release_steps, 'Docker release job')
release_preflight_step = release_steps.find { |step| step['name'] == 'Docker release final preflight' }
abort('Docker release job must run final static preflight before login/push') unless release_preflight_step
release_preflight_run = release_preflight_step.fetch('run')
%w[
  ./scripts/preflight_manifest_syntax.sh
  ./scripts/preflight_docker_static.sh
].each do |command|
  abort("Docker release final preflight is missing #{command}") unless release_preflight_run.include?(command)
end
login_index = release_steps.index { |step| step['name'] == 'Log in to Docker Hub' }
final_preflight_index = release_steps.index(release_preflight_step)
abort('Docker release final preflight must run before Docker Hub login') unless
  final_preflight_index && login_index && final_preflight_index < login_index
push_step = release_steps.find { |step| step['name'] == 'Build and push Docker image' }
abort('Docker release push must use cedar-docker-compose/Dockerfile') unless
  push_step.dig('with', 'file') == 'cedar-docker-compose/Dockerfile'
metadata_step = release_steps.find { |step| step['name'] == 'Extract metadata' }
metadata_tags = metadata_step.dig('with', 'tags')
abort('Docker release metadata must publish latest for tag releases') unless
  metadata_tags.include?('type=raw,value=latest')
abort('Docker release latest tag must not depend on is_default_branch because releases run on tag refs') if
  metadata_tags.include?('enable={{is_default_branch}}')
description_step = release_steps.find { |step| step['name'] == 'Update Docker Hub description' }
abort('Docker Hub description must use the repo-root relative Docker Hub README path') unless
  description_step.dig('with', 'readme-filepath') == './cedar-docker-compose/DOCKERHUB_README.md'
dockerhub_readme = File.read('cedar-docker-compose/DOCKERHUB_README.md')
abort('Docker Hub README must document latest as the stable release tag') unless
  dockerhub_readme.include?('`latest` - 最新稳定版')
abort('Docker Hub README must document semver release tags') unless
  dockerhub_readme.include?('`v0.1.0`, `v0.1`, `v0` - 版本标签')
abort('Docker Hub README must not advertise an unpublished dev image tag') if
  dockerhub_readme.include?('`dev`')
studio_job = jobs.fetch('release-studio')
unless studio_job.fetch('if').include?("hashFiles('studio/**') != ''")
  abort('Studio image release must be skipped when the studio source tree is absent')
end
studio_steps = studio_job.fetch('steps')
studio_metadata = studio_steps.find { |step| step['name'] == 'Extract metadata' }
studio_metadata_tags = studio_metadata.dig('with', 'tags')
abort('Studio release metadata must publish latest for tag releases') unless
  studio_metadata_tags.include?('type=raw,value=latest')
abort('Studio latest tag must not depend on is_default_branch because releases run on tag refs') if
  studio_metadata_tags.include?('enable={{is_default_branch}}')

ci_workflow = YAML.load_file('.github/workflows/build.yml')
ci_steps = ci_workflow.fetch('jobs').fetch('build-and-test').fetch('steps')
linux_install = ci_steps.find { |step| step['name'] == 'Install dependencies on Linux' }.fetch('run')
%w[
  cmake
  pkg-config
  libcurl4-openssl-dev
  liblz4-dev
  libzstd-dev
  libleveldb-dev
  libgrpc++-dev
  protobuf-compiler-grpc
  libprotobuf-dev
  protobuf-compiler
  libprotoc-dev
  libssl-dev
  libyaml-cpp-dev
  libgflags-dev
  libgoogle-glog-dev
  nlohmann-json3-dev
  ripgrep
  libgtest-dev
].each do |package|
  abort("CI Linux dependency install is missing #{package}") unless linux_install.include?(package)
end

macos_install = ci_steps.find { |step| step['name'] == 'Install dependencies on macOS' }.fetch('run')
%w[
  cmake
  pkg-config
  lz4
  zstd
  grpc
  protobuf
  openssl
  yaml-cpp
  gflags
  glog
  leveldb
  nlohmann-json
  ripgrep
  googletest
].each do |package|
  abort("CI macOS dependency install is missing #{package}") unless macos_install.include?(package)
end
helm_step = ci_steps.find { |step| step['uses'] == 'azure/setup-helm@v4' }
abort('CI must install Helm with a pinned setup action') unless helm_step
abort('CI Helm version must be pinned to v4.2.2') unless helm_step.dig('with', 'version') == 'v4.2.2'
kubectl_step = ci_steps.find { |step| step['uses'] == 'azure/setup-kubectl@v4' }
abort('CI must install kubectl with a pinned setup action') unless kubectl_step
abort('CI kubectl version must be pinned to v1.35.0') unless kubectl_step.dig('with', 'version') == 'v1.35.0'
verify_tools = ci_steps.find { |step| step['name'] == 'Verify preflight tools' }
abort('CI must verify preflight tools before running release checks') unless verify_tools
verify_tools_run = verify_tools.fetch('run')
%w[rg\ --version helm\ version kubectl\ version ruby\ --version].each do |command|
  abort("CI preflight tool verification is missing #{command.gsub('\\ ', ' ')}") unless
    verify_tools_run.include?(command.gsub('\\ ', ' '))
end

ci_build = ci_steps.find { |step| step['name'] == 'Build' }.fetch('run')
abort('CI build step must tee build logs for diagnostic scanning') unless
  ci_build.include?('tee "${{ runner.temp }}/cedar-build.log"')
ci_scan = ci_steps.find { |step| step['name'] == 'Scan build diagnostics' }
abort('CI must scan build diagnostics after compilation') unless ci_scan
ci_scan_run = ci_scan.fetch('run')
%w[warning: deprecated macro\ redefined duplicate\ librar /opt/homebrew/include/butil error:].each do |pattern|
  abort("CI build diagnostic scan is missing #{pattern.gsub('\\ ', ' ')}") unless
    ci_scan_run.include?(pattern.gsub('\\ ', ' '))
end
preflight_step = ci_steps.find { |step| step['name'] == 'Release manifest preflight' }
abort('CI must run release manifest preflight checks') unless preflight_step
preflight_run = preflight_step.fetch('run')
%w[
  ./scripts/preflight_manifest_syntax.sh
  ./scripts/preflight_deployment_static.sh
  ./scripts/preflight_helm_static.sh
  ./scripts/preflight_k8s_static.sh
].each do |command|
  abort("CI release manifest preflight is missing #{command}") unless preflight_run.include?(command)
end
docker_static_step = ci_steps.find { |step| step['name'] == 'Docker static preflight' }
abort('CI must run Docker static preflight as a separate step') unless docker_static_step
abort('CI Docker static preflight must run on Linux where docker compose is available') unless
  docker_static_step.fetch('if') == "runner.os == 'Linux'"
abort('CI Docker static preflight must verify docker compose before rendering compose files') unless
  docker_static_step.fetch('run').include?('docker compose version')
abort('CI Docker static preflight must run preflight_docker_static.sh') unless
  docker_static_step.fetch('run').include?('./scripts/preflight_docker_static.sh')

snyk_workflow = YAML.load_file('.github/workflows/snyk-scan.yml')
snyk_workflow.fetch('jobs').each do |job_name, job|
  steps = job.fetch('steps')
  snyk_step = steps.find { |step| step.fetch('name', '').start_with?('Run Snyk') }
  next unless snyk_step
  abort("#{job_name} Snyk scan step must be gated on SNYK_TOKEN") unless
    snyk_step.fetch('if').include?('secrets.SNYK_TOKEN')
  build_step = steps.find { |step| step.fetch('name', '') == 'Build Docker image' }
  if build_step
    abort("#{job_name} Docker image build must be gated on SNYK_TOKEN because the only consumer is the Snyk container scan") unless
      build_step.fetch('if').include?('secrets.SNYK_TOKEN')
    abort("#{job_name} Docker image build must use cedar-docker-compose/Dockerfile") unless
      build_step.fetch('run').include?('docker build -f cedar-docker-compose/Dockerfile')
  end
  upload_step = steps.find { |step| step.fetch('uses', '') == 'github/codeql-action/upload-sarif@v3' }
  abort("#{job_name} Snyk upload step must be gated on SNYK_TOKEN") unless
    upload_step.fetch('if').include?('secrets.SNYK_TOKEN')
end

trivy_workflow = YAML.load_file('.github/workflows/trivy-scan.yml')
docker_scan = trivy_workflow.fetch('jobs').fetch('docker-scan')
trivy_step = docker_scan.fetch('steps').find { |step| step['name'] == 'Run Trivy on Docker build context' }
abort('Trivy Docker scan must scan the release build context, not only a single Dockerfile') unless
  trivy_step.dig('with', 'scan-ref') == '.'
abort('Trivy Docker scan must include vuln, secret, and config scanners') unless
  trivy_step.dig('with', 'scanners').to_s.include?('vuln,secret,config')
abort('Trivy Docker scan should use the repository ignore file for audited false positives') unless
  trivy_step.dig('with', 'trivyignores') == '.trivyignore'
abort('Trivy ignore file referenced by workflow must exist') unless File.file?('.trivyignore')
abort('Trivy workflow must still mention cedar-docker-compose/Dockerfile as the release Dockerfile path') unless
  File.read('.github/workflows/trivy-scan.yml').include?('cedar-docker-compose/Dockerfile') ||
  File.read('scripts/preflight_manifest_syntax.sh').include?('cedar-docker-compose/Dockerfile')
RUBY

echo "Manifest and script syntax preflight passed"
