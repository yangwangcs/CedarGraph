#!/bin/bash
# =============================================================================
# CedarGraph Deployment Manifest Static Guard
# =============================================================================
# Catches deployment regressions that unit tests and native smoke tests cannot
# see: wrong container entrypoint arguments, MetaD Raft/gRPC port mixups, and
# HTTP health checks pointed at gRPC ports.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

fail() {
    log_error "$1"
    exit 1
}

require_command() {
    local cmd="$1"
    command -v "${cmd}" >/dev/null 2>&1 || fail "Required command not found: ${cmd}"
}

assert_no_match() {
    local description="$1"
    local pattern="$2"
    shift 2
    if rg -n -- "${pattern}" "$@"; then
        fail "${description}"
    fi
}

assert_match() {
    local description="$1"
    local pattern="$2"
    shift 2
    if ! rg -n -- "${pattern}" "$@" >/dev/null; then
        fail "${description}"
    fi
}

cd "${PROJECT_ROOT}"

require_command rg

DEPLOYMENT_FILES=(
    docker-compose.production.yml
    cedar-docker-compose/docker-compose.yml
    cedar-docker-compose/scripts/graphd-entrypoint.sh
    cedar-docker-compose/Dockerfile
    cedar-docker-compose/Dockerfile.cn
    cedar-docker-compose/Dockerfile.build
    k8s/metad.yaml
    k8s/storaged.yaml
    k8s/graphd.yaml
    k8s/kustomization.yaml
    k8s/storaged-config.yaml
    k8s/graphd-config.yaml
    helm-chart/cedargraph/templates/_helpers.tpl
    helm-chart/cedargraph/templates/metad-statefulset.yaml
    helm-chart/cedargraph/templates/storaged-statefulset.yaml
    helm-chart/cedargraph/templates/graphd-deployment.yaml
    helm-chart/cedargraph/values.yaml
    k8s/queryd.yaml
    scripts/start_local.sh
    scripts/start_cluster.sh
    scripts/start_full_cluster.sh
    scripts/test_cluster.sh
    scripts/cedar_health_check.sh
    scripts/deploy_auto_recovery.sh
    scripts/deploy/install.sh
    scripts/deploy/health_check.sh
    config/storaged.service
    tools/cedargraph/config/cedar.yaml
    tools/cedargraph/config.example.yaml
    config/cedar.yaml
    config/storaged_node0.yaml
    config/storaged_node1.yaml
    config/storaged_node2.yaml
    config/prometheus.yml
    config/cluster/metad_node1.conf
    config/cluster/metad_node2.conf
    config/cluster/metad_node3.conf
    config/cluster/storaged_node1.conf
    config/cluster/storaged_node2.conf
    config/cluster/storaged_node3.conf
    config/storaged_auto_recovery.conf
)

assert_no_match \
    "Deployment manifests must use cedar-graphd/cedar-storaged --meta, not obsolete --meta_servers." \
    "--meta_servers|--query_port|--http_port" \
    "${DEPLOYMENT_FILES[@]}"

assert_no_match \
    "Deployment manifests must not deploy the removed standalone cedar-queryd binary." \
    "cedar-queryd|NODE_ROLE=queryd|containerPort: 9889|targetPort: 9889" \
    docker-compose.production.yml \
    cedar-docker-compose/docker-compose.yml \
    cedar-docker-compose/Dockerfile \
    cedar-docker-compose/Dockerfile.cn \
    cedar-docker-compose/Dockerfile.build \
    k8s/kustomization.yaml \
    k8s/queryd.yaml \
    helm-chart/cedargraph/templates/queryd-deployment.yaml

assert_no_match \
    "GraphD/StorageD and service clients must connect to MetaD gRPC port 10559, not MetaD Raft port 9559." \
    "CEDAR_METAD_ENDPOINT=.*9559|META_SERVERS=.*9559|metad:9559" \
    docker-compose.production.yml \
    cedar-docker-compose/docker-compose.yml \
    cedar-docker-compose/scripts/graphd-entrypoint.sh \
    k8s/storaged.yaml \
    k8s/graphd.yaml \
    k8s/queryd.yaml \
    scripts/start_local.sh \
    scripts/start_full_cluster.sh \
    scripts/deploy_auto_recovery.sh \
    scripts/deploy/health_check.sh \
    config/cedar.yaml \
    config/storaged_node0.yaml \
    config/storaged_node1.yaml \
    config/storaged_node2.yaml \
    helm-chart/cedargraph/templates/storaged-statefulset.yaml \
    helm-chart/cedargraph/templates/graphd-deployment.yaml

assert_no_match \
    "Generated systemd services must use installed cedar-* binaries." \
    "\\$\\{BIN_DIR\\}/(metad|storaged|graphd) --config" \
    scripts/deploy/install.sh

assert_no_match \
    "Generated service configs must not point GraphD/StorageD clients at MetaD Raft port 9559." \
    "metad_endpoints=.*9559|servers = \\[\"127\\.0\\.0\\.1:9559\"\\]" \
    scripts/deploy/install.sh

assert_match \
    "C++ CedarClientConfig default MetaD port must use the MetaD gRPC client API port 10559." \
    "int metad_port = 10559;" \
    include/cedar/client/cedar_client.h

assert_no_match \
    "GCN CoordinatorClient default must use MetaD gRPC port 10559, not MetaD Raft port 9559." \
    'DEFINE_string\(gcn_coordinator, "127\.0\.0\.1:9559"' \
    src/gcn/gcn_node.cc

assert_no_match \
    "GraphD/StorageD must use strict TLS env fallback; implicit insecure fallback is not production-safe." \
    "Create(Server|Client)CredentialsFromEnv\\(\\)" \
    tools/graphd.cc tools/storaged.cc

assert_no_match \
    "Production service-to-service clients must not use implicit insecure TLS env fallback." \
    "TlsCredentialFactory::Create(Server|Client)CredentialsFromEnv\\(\\)" \
    src/service/graph_service_router.cc \
    src/service/graphd_registrar.cc \
    src/dtx/unified_meta_client.cc \
    src/dtx/storage/storage_server_with_grpc.cc \
    src/dtx/storage_impl/storage_server.cc \
    src/dtx/storage_impl/storage_client.cc \
    src/dtx/storage_impl/meta_service_client.cc

assert_no_match \
    "Client SDK TLS-enabled paths must not use implicit insecure TLS env fallback." \
    "TlsCredentialFactory::CreateClientCredentialsFromEnv\\(\\)" \
    src/client/connection_pool.cc \
    src/client/graphd_load_balancer.cc \
    src/client/service_discovery.cc

assert_no_match \
    "Client sample config must use MetaD gRPC port 10559, not Raft port 9559." \
    "^port = 9559$" \
    config/client.conf

assert_match \
    "Client sample config must point MetaD clients at gRPC port 10559." \
    "^port = 10559$" \
    config/client.conf

assert_match \
    "GraphD/StorageD must allow insecure credentials only through explicit strict fallback policy." \
    "Create(Server|Client)CredentialsFromEnvStrict\\(\\)" \
    tools/graphd.cc tools/storaged.cc

assert_match \
    "GCN CoordinatorClient default must point at MetaD gRPC port 10559." \
    'DEFINE_string\(gcn_coordinator, "127\.0\.0\.1:10559"' \
    src/gcn/gcn_node.cc

assert_match \
    "CedarGraph CLI MetaD config must keep Raft/listen port at 9559." \
    "port: 9559" \
    tools/cedargraph/config/cedar.yaml tools/cedargraph/config.example.yaml

assert_match \
    "CedarGraph CLI MetaD config must expose gRPC client API port 10559 separately." \
    "grpc_port: 10559" \
    tools/cedargraph/config/cedar.yaml tools/cedargraph/config.example.yaml

assert_match \
    "CedarGraph CLI must start MetaD with --listen, not the StorageD/GraphD --bind flag." \
    '"--listen"' \
    tools/cedargraph/internal/cluster/manager.go

assert_match \
    "CedarGraph CLI must pass MetaD --grpc_port when starting MetaD." \
    '"--grpc_port"' \
    tools/cedargraph/internal/cluster/manager.go

assert_no_match \
    "CedarGraph CLI must not pass host:port to StorageD/GraphD --bind; --port must be separate." \
    '"--bind", fmt\.Sprintf\("%s:%d"' \
    tools/cedargraph/internal/cluster/manager.go

assert_match \
    "CedarGraph CLI must pass StorageD/GraphD --port separately from --bind." \
    '"--port"' \
    tools/cedargraph/internal/cluster/manager.go

assert_no_match \
    "CedarGraph CLI default configs must not pass root config/cedar.yaml; service --config is loaded after flags and would override CLI ports/data dirs." \
    "config_file:" \
    tools/cedargraph/config/cedar.yaml tools/cedargraph/config.example.yaml

if rg -n "meta_servers:" scripts/deploy/install.sh >/dev/null; then
    if rg -n -U "meta_servers:\\n[[:space:]]+- \"127\\.0\\.0\\.1:9559\"" scripts/deploy/install.sh; then
        fail "Generated service configs must use MetaD gRPC port 10559 in meta_servers."
    fi
fi

assert_no_match \
    "install.sh must generate YAML service configs that current cedar-* --config parsers understand, not legacy INI/key=value files." \
    "^\\[(node|raft|heartbeat|logging|meta|query)\\]|^(node_id|bind_address|data_dir|metad_endpoints|grpc_threads|log_level|log_file)=" \
    scripts/deploy/install.sh

assert_no_match \
    "Production deployment scripts must not call non-existent backup/restore binaries." \
    "(^|[^[:alnum:]_-])cedar-(backup|restore)([[:space:]]|$)" \
    scripts/deploy.sh

assert_no_match \
    "Client ClusterBackup must not shell out to non-existent cedar-backup/cedar-restore binaries." \
    "(^|[^[:alnum:]_-])cedar-(backup|restore)([[:space:]]|$)" \
    src/client/cluster_backup.cc include/cedar/client/cluster_backup.h

assert_match \
    "Client ClusterBackup S3 upload must require a completed verified local backup." \
    "backup.status != BackupStatus::COMPLETED" \
    src/client/cluster_backup.cc

assert_match \
    "Client ClusterBackup S3 download must fail closed until remote backup metadata registration exists." \
    "Remote backup metadata registration is not implemented" \
    src/client/cluster_backup.cc

assert_match \
    "Client ClusterBackup must track initialization state for fail-closed operations." \
    "initialized_" \
    include/cedar/client/cluster_backup.h src/client/cluster_backup.cc

assert_match \
    "Client ClusterBackup CreateBackup must return a FAILED status before Initialize succeeds." \
    "ClusterBackup is not initialized" \
    src/client/cluster_backup.cc

assert_no_match \
    "Client LoadBalancer must not route to the first node when all nodes are unhealthy." \
    "return nodes_\\[0\\]" \
    src/client/load_balancer.cc

assert_match \
    "Client LoadBalancer must have a regression test for all-unhealthy routing." \
    "LoadBalancerDoesNotRouteToUnhealthyNodes" \
    tests/test_client.cc

assert_match \
    "GraphDLoadBalancer must fail closed when no ONLINE GraphD node exists." \
    "No ONLINE GraphD nodes available" \
    src/client/graphd_load_balancer.cc

assert_no_match \
    "GraphDLoadBalancer must not fall back to routing across OFFLINE/BUSY nodes." \
    "If no ONLINE nodes, use all nodes|available\\.reserve\\(nodes_\\.size\\(\\)\\)" \
    src/client/graphd_load_balancer.cc

assert_match \
    "GraphDLoadBalancer must test no-ONLINE-node failure." \
    "SelectNodeFailsWhenNoOnlineNodesExist" \
    tests/client/test_graphd_load_balancer.cc

assert_match \
    "K8sManager readiness must fail closed when no pods are returned." \
    "pods\\.empty\\(\\)" \
    src/client/k8s_manager.cc

assert_match \
    "K8sManager must test that an empty pod list is not ready." \
    "EmptyPodListIsNotReady" \
    tests/test_client.cc

assert_match \
    "ClusterMonitor unsupported Prometheus queries must return NaN instead of fake zero metrics." \
    "quiet_NaN" \
    src/client/cluster_monitor.cc

assert_match \
    "ClusterMonitor must test unimplemented integrations fail closed." \
    "UnimplementedIntegrationsFailClosed" \
    tests/test_client.cc

ruby - <<'RUBY'
path = 'src/client/cluster_monitor.cc'
text = File.read(path)
body = text[/bool ClusterMonitor::CreateGrafanaDashboard\(const std::string& dashboard_json\) \{.*?\n\}/m]
abort("#{path}: ClusterMonitor must define CreateGrafanaDashboard") unless body
abort("#{path}: CreateGrafanaDashboard must not report success before Grafana API support exists") if body.include?('return true;')
RUBY

assert_match \
    "deploy.sh backups must use the documented cedargraph-backup prefix." \
    "cedargraph-backup-" \
    scripts/deploy.sh

assert_match \
    "deploy.sh restore must validate tar archives before extraction." \
    "validate_backup_archive" \
    scripts/deploy.sh

assert_match \
    "deploy.sh restore must inspect tar member names before extraction." \
    "tar -tzf" \
    scripts/deploy.sh

assert_match \
    "deploy.sh restore must reject absolute or parent-traversal paths." \
    "危险路径" \
    scripts/deploy.sh

assert_match \
    "deploy.sh restore must reject symlinks and hardlinks in backup archives." \
    "符号链接或硬链接" \
    scripts/deploy.sh

assert_match \
    "deploy.sh backup must fail closed unless DATA_DIR/LOG_DIR map to stable data/logs archive roots." \
    "require_local_backup_layout" \
    scripts/deploy.sh

assert_match \
    "deploy.sh .env loader must not override explicitly provided process environment variables." \
    '\$\{!key\+x\}' \
    scripts/deploy.sh

assert_match \
    "deploy.sh backup must archive stable data and logs roots." \
    "data logs" \
    scripts/deploy.sh

assert_match \
    "deploy.sh tar backup/restore must reject custom DATA_DIR layouts that would not restore to the intended path." \
    "只支持 DATA_DIR=\\./data 或 data" \
    scripts/deploy.sh

assert_no_match \
    "deploy.sh cleanup must not hard-code data/logs deletion paths." \
    "rm -rf data/\\* logs/\\*" \
    scripts/deploy.sh

ruby - scripts/deploy.sh <<'RUBY'
path = ARGV.fetch(0)
text = File.read(path)
body = text[/cleanup\(\) \{(?<body>.*?)\n\}/m, :body]
abort("#{path}: cleanup function not found") unless body
abort("#{path}: cleanup must reuse require_local_backup_layout before deleting DATA_DIR/LOG_DIR") unless
  body.include?('require_local_backup_layout')
abort("#{path}: cleanup must require CEDAR_DEPLOY_ALLOW_CLEANUP=1") unless
  body.include?('CEDAR_DEPLOY_ALLOW_CLEANUP')
allow_idx = body.index('CEDAR_DEPLOY_ALLOW_CLEANUP') || body.length
stop_idx = body.index('stop_cluster') || body.length
abort("#{path}: cleanup must check CEDAR_DEPLOY_ALLOW_CLEANUP before stopping services") unless
  allow_idx < stop_idx
RUBY

assert_no_match \
    "Operator-facing deployment docs must not instruct users to call non-existent backup/restore binaries." \
    "(^|[^[:alnum:]_-])cedar-(backup|restore)([[:space:]]|$)" \
    README.md \
    docs/PRODUCTION_DEPLOYMENT_GUIDE.md \
    docs/deployment/README.md \
    cedar-docker-compose/README.md \
    cedar-docker-compose/DOCKERHUB_README.md \
    helm-chart/README.md

assert_no_match \
    "install.sh generated service config sections must not include fields ignored by current cedar-* parsers." \
    "^[[:space:]]+enabled: true|^[[:space:]]+raft:" \
    scripts/deploy/install.sh

assert_match \
    "install.sh must generate a metad YAML section for cedar-metad --config." \
    "metad:" \
    scripts/deploy/install.sh

assert_match \
    "install.sh generated main config must keep MetaD Raft listen address at 0.0.0.0:9559." \
    'listen_address: "0\.0\.0\.0:9559"' \
    scripts/deploy/install.sh

assert_match \
    "install.sh generated main and service configs must include MetaD gRPC port 10559." \
    "grpc_port: 10559" \
    scripts/deploy/install.sh

assert_match \
    "install.sh must generate storaged.meta_server for cedar-storaged --config." \
    "meta_server: \"127\\.0\\.0\\.1:10559\"" \
    scripts/deploy/install.sh

assert_match \
    "install.sh must generate graphd.meta_server for cedar-graphd --config." \
    "graphd:" \
    scripts/deploy/install.sh

assert_match \
    "install.sh systemd services must load the shared Cedar service env for TLS/auth settings." \
    "EnvironmentFile=\\$\\{SERVICE_ENV_FILE\\}" \
    scripts/deploy/install.sh

assert_match \
    "deploy.sh must validate GraphD mTLS client certificate files before production compose startup." \
    "CEDAR_GRPC_MTLS_ENABLED" \
    scripts/deploy.sh

assert_match \
    "deploy.sh must validate GraphD mTLS client certificate path." \
    "CEDAR_GRPC_CLIENT_CERT" \
    scripts/deploy.sh

assert_match \
    "deploy.sh must validate GraphD mTLS client key path." \
    "CEDAR_GRPC_CLIENT_KEY" \
    scripts/deploy.sh

assert_no_match \
    "install.sh must not leave GraphD-only env wiring after MetaD/StorageD also need TLS environment." \
    "GRAPHD_ENV_FILE|__CEDAR_GRAPHD_ENVIRONMENT_FILE__" \
    scripts/deploy/install.sh

assert_no_match \
    "Legacy cluster scripts must use current CMake targets and cedar-* binary names." \
    'metad_server|build/storaged|bin/storaged|/usr/local/bin/storaged|\$INSTALL_DIR/bin/storaged' \
    scripts/start_cluster.sh scripts/start_full_cluster.sh scripts/test_cluster.sh scripts/deploy_auto_recovery.sh config/storaged.service

assert_no_match \
    "Executable deployment scripts must not generate legacy key=value service configs for cedar-* --config." \
    "^(node_id|bind_address|data_dir|metad_endpoints|io_threads|worker_threads|grpc_threads|log_level|log_file)=" \
    scripts/start_full_cluster.sh scripts/deploy_auto_recovery.sh scripts/deploy/install.sh

assert_no_match \
    "Cluster example configs must use YAML understood by current cedar-* --config parsers." \
    "^\\[(node|raft|heartbeat|logging|meta|query)\\]|^(node_id|bind_address|data_dir|metad_endpoints|grpc_threads|log_level|log_file)=" \
    config/cluster/metad_node1.conf \
    config/cluster/metad_node2.conf \
    config/cluster/metad_node3.conf \
    config/cluster/storaged_node1.conf \
    config/cluster/storaged_node2.conf \
    config/cluster/storaged_node3.conf \
    config/storaged_auto_recovery.conf

assert_no_match \
    "Cluster test script must not reference removed metad_server HTTP example APIs." \
    "metad_server|/raft/status|/space/create|/space/get|curl .*2379" \
    scripts/test_cluster.sh

assert_match \
    "start_full_cluster.sh must route StorageD to MetaD gRPC ports, not Raft ports." \
    "meta_server: \"127\\.0\\.0\\.1:6101,127\\.0\\.0\\.1:6102,127\\.0\\.0\\.1:6103\"" \
    scripts/start_full_cluster.sh

assert_no_match \
    "Compose healthchecks must not run HTTP cedar_health_check.sh against gRPC service ports." \
    "cedar_health_check\\.sh localhost:(9559|9669|9779)" \
    docker-compose.production.yml cedar-docker-compose/docker-compose.yml

assert_match \
    "deploy.sh start must wait for Docker Compose core services before reporting success." \
    "wait_for_compose_ready" \
    scripts/deploy.sh

assert_match \
    "deploy.sh start must run the post-start health check before reporting success." \
    "run_post_start_health_check" \
    scripts/deploy.sh

assert_match \
    "deploy.sh readiness must reject unhealthy compose containers." \
    "unhealthy" \
    scripts/deploy.sh

assert_match \
    "deploy.sh readiness must fail closed when Docker Compose status cannot be read." \
    "无法读取 Docker Compose 服务状态" \
    scripts/deploy.sh

assert_no_match \
    "deploy.sh readiness must not hide Docker Compose ps failures behind an empty status snapshot." \
    "ps --format .*\\|\\| true" \
    scripts/deploy.sh

assert_match \
    "deploy.sh readiness must require all seven core containers." \
    "core=\\$\\{running_core\\}/7" \
    scripts/deploy.sh

assert_match \
    "deploy.sh post-start health check must use MetaD gRPC 10559." \
    "METAD_ENDPOINTS=\"127\\.0\\.0\\.1:\\$\\{META_GRPC_PORT:-10559\\}\"" \
    scripts/deploy.sh

assert_no_match \
    "Deployment health check must not probe GraphD/StorageD query or gRPC ports as HTTP /health endpoints." \
    'check_http_endpoint "\$endpoint"|check_process_running "(metad|storaged|graphd)"' \
    scripts/deploy/health_check.sh

assert_no_match \
    "Deployment health check must not report service readiness from local process-name existence." \
    'check_process_running "cedar-(metad|storaged|graphd)"' \
    scripts/deploy/health_check.sh

assert_match \
    "Deployment health check must use GraphD HTTP health port 9668." \
    "GRAPHD_HEALTH_ENDPOINTS=.*9668" \
    scripts/deploy/health_check.sh

assert_match \
    "Deployment health check must use StorageD HTTP health port 7000." \
    "STORAGED_HEALTH_ENDPOINTS=.*7000" \
    scripts/deploy/health_check.sh

assert_match \
    "Deployment health check must document StorageD TCP-only fallback as a limited post-start signal." \
    "registration with MetaD must still be verified" \
    scripts/deploy/health_check.sh

assert_match \
    "Deployment health check must support StorageD TCP-only mode when no HTTP endpoint is configured." \
    "HEALTHY:tcp_reachable" \
    scripts/deploy/health_check.sh

assert_no_match \
    "Deployment scripts must not use post-increment arithmetic that returns 1 under set -e." \
    "\\(\\((idx|attempts|current)\\+\\+\\)\\)" \
    scripts/deploy/health_check.sh scripts/deploy/rolling_update.sh

assert_match \
    "Rolling update must back up and install current cedar-* binary names." \
    "CEDAR_BINARIES=\\(cedar-metad cedar-storaged cedar-graphd\\)" \
    scripts/deploy/rolling_update.sh

assert_no_match \
    "Rolling update must not iterate obsolete short binary names for production installs." \
    "for bin in metad storaged graphd" \
    scripts/deploy/rolling_update.sh

assert_match \
    "Rolling update must refuse to continue without a binary rollback point." \
    "refusing to continue without a rollback point" \
    scripts/deploy/rolling_update.sh

assert_match \
    "Rolling update must fail closed when a required release binary is missing." \
    "Required binary not found in release archive" \
    scripts/deploy/rolling_update.sh

assert_match \
    "install.sh must fail closed when a required release binary is missing." \
    "Required binary not found in archive" \
    scripts/deploy/install.sh

assert_match \
    "install.sh must fail closed when systemd cannot start a CedarGraph service." \
    "die \"Failed to start cedar-(metad|storaged|graphd)" \
    scripts/deploy/install.sh

assert_match \
    "Health monitor must default to the installed cedar-storaged systemd service." \
    "STORAGED_SERVICE_NAME=.*cedar-storaged" \
    scripts/cedar_health_monitor.sh

assert_no_match \
    "Health monitor must not hard-code the obsolete storaged systemd unit." \
    "systemctl (is-active|restart).* storaged|journalctl -u storaged" \
    scripts/cedar_health_monitor.sh

assert_match \
    "Health monitor must not abort all checks when free is unavailable." \
    "free command not available" \
    scripts/cedar_health_monitor.sh

assert_match \
    "Health monitor must not abort all checks when top output is unsupported." \
    "unsupported top output" \
    scripts/cedar_health_monitor.sh

assert_match \
    "Health monitor must parse current YAML storaged config keys." \
    "config_value" \
    scripts/cedar_health_monitor.sh

assert_match \
    "Health monitor network check must read storaged.meta_server from YAML configs." \
    "meta_server" \
    scripts/cedar_health_monitor.sh

assert_match \
    "Health monitor network check must probe MetaD/peer TCP endpoints instead of pinging only host IPs." \
    "check_tcp_endpoint" \
    scripts/cedar_health_monitor.sh

assert_no_match \
    "Health monitor must not rely solely on legacy INI data_dir or peer_addresses grep parsing." \
    "grep \"\\^(data_dir|peer_addresses)=\"" \
    scripts/cedar_health_monitor.sh

assert_match \
    "Health monitor must be sourceable for function-level preflight tests without running main." \
    "BASH_SOURCE\\[0\\]" \
    scripts/cedar_health_monitor.sh

ruby - <<'RUBY'
path = 'scripts/deploy/health_check.sh'
File.readlines(path, chomp: true).each_with_index do |line, idx|
  if line.match?(/^\s*result=\$\(check_(metad|storaged|graphd)_node\b/)
    abort("#{path}:#{idx + 1}: Deployment health check must not let set -e abort before recording unhealthy node results")
  end
end
RUBY

assert_no_match \
    "Legacy cedar_health_check.sh must not default to removed 9090/9091/9092 health ports." \
    "localhost:9090|localhost:9091|localhost:9092|/ready" \
    scripts/cedar_health_check.sh

assert_match \
    "Legacy cedar_health_check.sh must check StorageD health on 7000." \
    "STORAGED_HEALTH_ADDR=.*7000" \
    scripts/cedar_health_check.sh

assert_match \
    "Legacy cedar_health_check.sh must check GraphD health on 9668." \
    "GRAPHD_HEALTH_ADDR=.*9668" \
    scripts/cedar_health_check.sh

assert_match \
    "Legacy cedar_health_check.sh must check GraphD metrics on 9667." \
    "GRAPHD_METRICS_ADDR=.*9667" \
    scripts/cedar_health_check.sh

assert_no_match \
    "Auto recovery StorageD configs must not use old health/metrics port split." \
    "health_port: 7001|metrics_port: 7002" \
    scripts/deploy_auto_recovery.sh config/storaged_auto_recovery.conf

assert_match \
    "Auto recovery deployment success logger must use the GREEN color variable without invalid array syntax." \
    "\\$\\{GREEN\\}\\[SUCCESS\\]" \
    scripts/deploy_auto_recovery.sh

assert_no_match \
    "Auto recovery deployment success logger must not use invalid GREEN[SUCCESS] expansion." \
    "GREEN\\[SUCCESS\\]" \
    scripts/deploy_auto_recovery.sh

assert_no_match \
    "Deployment docs must not direct operators to scrape StorageD metrics from the gRPC port." \
    "localhost:9779/metrics|metrics_port: 9090" \
    docs/deployment/README.md

assert_no_match \
    "Deployment docs must not list StoragePartitionManager-only fields in current standalone cedar-storaged config examples." \
    "storage_mode:|max_open_partitions:|per_partition_memtable_mb:|enable_lru_eviction:" \
    docs/deployment/README.md

assert_no_match \
    "Deployment docs must not use legacy config keys that current cedar-* --config parsers ignore." \
    "^[[:space:]]+host:|metad_host:|metad_port:|snapshot_interval_sec:|max_concurrent_requests:|request_timeout_ms:|memtable_size_mb:|l0_max_files:|max_bytes_for_level_base_mb:|max_bytes_for_level_multiplier:|enable_wal:|sync_on_write:|sync_interval_ms:|enable_auto_compaction:|compaction_threads:|rate_limit_mb_per_sec:|block_cache_mb:|row_cache_mb:|max_query_timeout_ms:|max_result_size:" \
    docs/deployment/README.md

assert_match \
    "Compose must launch MetaD with explicit Raft listen port." \
    "--listen.*0\\.0\\.0\\.0:9559|--listen" \
    docker-compose.production.yml cedar-docker-compose/docker-compose.yml

assert_match \
    "Compose must launch MetaD with explicit gRPC port 10559." \
    "--grpc_port.*10559|10559" \
    docker-compose.production.yml cedar-docker-compose/docker-compose.yml

assert_match \
    "Docker images must expose MetaD gRPC port 10559." \
    "EXPOSE .*10559" \
    cedar-docker-compose/Dockerfile cedar-docker-compose/Dockerfile.cn cedar-docker-compose/Dockerfile.build

assert_match \
    "K8s MetaD manifest must expose both Raft and gRPC ports." \
    "containerPort: 10559|port: 10559" \
    k8s/metad.yaml

ruby - <<'RUBY'
checks = {
  'k8s/graphd.yaml' => {
    'livenessProbe' => /livenessProbe:\n(?:[^\n]*\n){0,8}?[[:space:]]+tcpSocket:\n[[:space:]]+port: health/,
    'readinessProbe' => /readinessProbe:\n(?:[^\n]*\n){0,8}?[[:space:]]+tcpSocket:\n[[:space:]]+port: health/
  },
  'k8s/storaged.yaml' => {
    'livenessProbe' => /livenessProbe:\n(?:[^\n]*\n){0,8}?[[:space:]]+tcpSocket:\n[[:space:]]+port: health/,
    'readinessProbe' => /readinessProbe:\n(?:[^\n]*\n){0,8}?[[:space:]]+tcpSocket:\n[[:space:]]+port: health/
  },
  'helm-chart/cedargraph/templates/graphd-deployment.yaml' => {
    'livenessProbe' => /livenessProbe:\n(?:[^\n]*\n){0,8}?[[:space:]]+tcpSocket:\n[[:space:]]+port: health/,
    'readinessProbe' => /readinessProbe:\n(?:[^\n]*\n){0,8}?[[:space:]]+tcpSocket:\n[[:space:]]+port: health/
  },
  'helm-chart/cedargraph/templates/storaged-statefulset.yaml' => {
    'livenessProbe' => /livenessProbe:\n(?:[^\n]*\n){0,8}?[[:space:]]+tcpSocket:\n[[:space:]]+port: \{\{ \.Values\.storaged\.service\.ports\.health \}\}/,
    'readinessProbe' => /readinessProbe:\n(?:[^\n]*\n){0,8}?[[:space:]]+tcpSocket:\n[[:space:]]+port: \{\{ \.Values\.storaged\.service\.ports\.health \}\}/
  }
}

checks.each do |path, probes|
  text = File.read(path)
  probes.each do |probe, pattern|
    abort("#{path} #{probe} must probe the dedicated health port, not the service gRPC/query port") unless
      text.match?(pattern)
  end
end
RUBY

assert_no_match \
    "K8s kustomization must use current labels syntax, not deprecated commonLabels." \
    "^commonLabels:" \
    k8s/kustomization.yaml

assert_no_match \
    "K8s service ConfigMaps must use YAML understood by current cedar-* --config parsers." \
    "node_type=|query_port=|http_port=|cluster_id=|election_timeout=|heartbeat_interval=|metad_endpoints=|meta_servers:" \
    k8s/metad.yaml k8s/storaged-config.yaml k8s/graphd-config.yaml scripts/deploy/install.sh

assert_no_match \
    "Root config/cedar.yaml MetaD section must use fields understood by current cedar-metad --config parser." \
    "election_timeout_min_ms|election_timeout_max_ms|rpc_timeout_ms" \
    config/cedar.yaml

assert_match \
    "Root config/cedar.yaml MetaD section must define listen_address for Raft." \
    'listen_address: "0\.0\.0\.0:9559"' \
    config/cedar.yaml

assert_match \
    "Root config/cedar.yaml MetaD section must define grpc_port 10559." \
    "grpc_port: 10559" \
    config/cedar.yaml

assert_match \
    "Helm values must define metad.service.grpcPort." \
    "grpcPort: 10559" \
    helm-chart/cedargraph/values.yaml

TLS_SECRET_REFERENCE_FILES=(
    k8s/graphd.yaml
    helm-chart/cedargraph/templates/graphd-deployment.yaml
)

TLS_SECRET_SOURCE_FILES=(
    k8s/graphd-secrets.example.yaml
    docs/deployment/README.md
    docs/PRODUCTION_DEPLOYMENT_GUIDE.md
    helm-chart/README.md
    helm-chart/cedargraph/templates/NOTES.txt
)

assert_match \
    "ClusterConfig default image must be pinned to the verified release tag, not latest." \
    "cedargraph/cedar:k8s-fix-20260630" \
    include/cedar/client/cluster_types.h
assert_no_match \
    "ClusterConfig default image must not use the mutable latest tag." \
    "cedargraph/cedar:latest" \
    include/cedar/client/cluster_types.h

for tls_file in tls.crt tls.key ca.crt client.crt client.key; do
    assert_match \
        "GraphD deployment references TLS file ${tls_file}; keep mounted-secret expectations explicit." \
        "${tls_file}" \
        "${TLS_SECRET_REFERENCE_FILES[@]}"
    assert_match \
        "GraphD TLS Secret examples and operator docs must include ${tls_file}." \
        "${tls_file}" \
        "${TLS_SECRET_SOURCE_FILES[@]}"
done

if [ -x "$(command -v ruby)" ]; then
    ruby -e 'require "yaml"; ARGV.each { |f| YAML.load_stream(File.read(f)) }' \
        docker-compose.production.yml \
        cedar-docker-compose/docker-compose.yml \
        config/docker-compose.monitoring.yml \
        config/prometheus.yml \
        config/cedar_alerts.yml \
        k8s/metad.yaml \
        k8s/storaged.yaml \
        k8s/graphd.yaml \
        k8s/network-policy.yaml \
        helm-chart/cedargraph/values.yaml

    ruby - config/cedar.yaml <<'RUBY'
require 'yaml'

root = YAML.load_file(ARGV.fetch(0))
metad = root.fetch('metad')
bad_keys = %w[bind_address port election_timeout_min_ms election_timeout_max_ms rpc_timeout_ms]
present_bad_keys = bad_keys.select { |key| metad.key?(key) }
abort("Root config/cedar.yaml metad uses unsupported current parser keys: #{present_bad_keys.inspect}") unless
  present_bad_keys.empty?
abort('Root config/cedar.yaml metad.listen_address must be 0.0.0.0:9559') unless
  metad['listen_address'] == '0.0.0.0:9559'
abort('Root config/cedar.yaml metad.grpc_port must be 10559') unless
  metad['grpc_port'] == 10559

tls = root.fetch('tls')
expected_tls = {
  'enabled' => true,
  'ca_cert' => '/etc/cedar/tls/ca.crt',
  'server_cert' => '/etc/cedar/tls/tls.crt',
  'server_key' => '/etc/cedar/tls/tls.key',
  'client_cert' => '/etc/cedar/tls/client.crt',
  'client_key' => '/etc/cedar/tls/client.key'
}
expected_tls.each do |key, expected|
  actual = tls[key]
  abort("Root config/cedar.yaml tls.#{key} must be #{expected.inspect}, got #{actual.inspect}") unless
    actual == expected
end

storaged = root.fetch('storaged')
allowed_storaged_keys = %w[
  node_id
  port
  bind_address
  advertise_address
  data_dir
  meta_server
  heartbeat_interval_sec
  health_port
  metrics_port
]
unsupported_storaged_keys = storaged.keys - allowed_storaged_keys
abort("Root config/cedar.yaml storaged has fields ignored by current cedar-storaged --config parser: #{unsupported_storaged_keys.inspect}") unless
  unsupported_storaged_keys.empty?

graphd = root.fetch('graphd')
allowed_graphd_keys = %w[
  port
  bind_address
  meta_server
  gcn_server
  health_port
  metrics_port
]
unsupported_graphd_keys = graphd.keys - allowed_graphd_keys
abort("Root config/cedar.yaml graphd has fields ignored by current cedar-graphd --config parser: #{unsupported_graphd_keys.inspect}") unless
  unsupported_graphd_keys.empty?

abort('Root config/cedar.yaml must not include standalone queryd section; QueryD execution is merged into GraphD') if
  root.key?('queryd')
RUBY

    ruby - config/docker-compose.monitoring.yml <<'RUBY'
require 'yaml'

services = YAML.load_file(ARGV.fetch(0)).fetch('services')
expected_images = {
  'prometheus' => 'prom/prometheus:v2.55.1',
  'grafana' => 'grafana/grafana:11.4.0',
  'alertmanager' => 'prom/alertmanager:v0.27.0',
  'node-exporter' => 'prom/node-exporter:v1.8.2'
}
expected_images.each do |service, expected|
  actual = services.fetch(service).fetch('image')
  abort("Monitoring service #{service} must use pinned image #{expected}, got #{actual}") unless
    actual == expected
end
latest_images = services.values.map { |service| service['image'] }.compact.grep(/:latest\z/)
abort("Monitoring compose must not contain :latest images: #{latest_images.inspect}") unless
  latest_images.empty?
RUBY

    ruby - config/prometheus.yml <<'RUBY'
require 'yaml'

prom = YAML.load_file(ARGV.fetch(0))
jobs = prom.fetch('scrape_configs').to_h { |job| [job.fetch('job_name'), job] }
graphd = jobs.fetch('cedar-graphd')
graphd_relabels = graphd.fetch('relabel_configs', [])
graphd_static_targets = graphd.fetch('static_configs', []).flat_map { |cfg| cfg.fetch('targets', []) }
has_graphd_static_metrics = graphd_static_targets.include?('graphd:9667')
has_graphd_port_rewrite = graphd_relabels.any? do |rule|
  rule['source_labels'] == ['__address__', '__meta_kubernetes_pod_annotation_prometheus_io_port'] &&
    rule['target_label'] == '__address__' &&
    rule['action'] == 'replace'
end
abort('Prometheus cedar-graphd job must scrape GraphD metrics port 9667') unless
  has_graphd_static_metrics || has_graphd_port_rewrite

storage = jobs.fetch('cedar-storage')
storage_relabels = storage.fetch('relabel_configs', [])
storage_static_targets = storage.fetch('static_configs', []).flat_map { |cfg| cfg.fetch('targets', []) }
has_storage_static_metrics = %w[storaged0:7001 storaged1:7001 storaged2:7001].all? do |target|
  storage_static_targets.include?(target)
end
has_storage_metrics_service = storage_relabels.any? do |rule|
  rule['source_labels'] == ['__meta_kubernetes_service_name'] &&
    rule['action'] == 'keep' &&
    rule['regex'] == 'storaged-metrics'
end
has_storage_metrics_port = storage_relabels.any? do |rule|
  rule['source_labels'] == ['__meta_kubernetes_endpoint_port_name'] &&
    rule['action'] == 'keep' &&
    rule['regex'] == 'metrics'
end
abort('Prometheus cedar-storage job must scrape StorageD metrics port 7001') unless
  has_storage_static_metrics || (has_storage_metrics_service && has_storage_metrics_port)
RUBY

    ruby - config/cedar_alerts.yml <<'RUBY'
require 'yaml'

alerts_path = ARGV.fetch(0)
alerts = YAML.load_file(alerts_path)
require 'set'

# Metrics currently exported by StorageD/GraphD metrics endpoints. Keep this
# list tied to MetricsRegistry registrations and histogram expansions.
exported_metrics = %w[
  cedar_cpu_cores_total
  cedar_disk_free_bytes
  cedar_disk_total_bytes
  cedar_process_memory_bytes
  cedar_storage_delete_ops_total
  cedar_storage_get_latency_seconds_bucket
  cedar_storage_get_latency_seconds_count
  cedar_storage_get_latency_seconds_sum
  cedar_storage_get_ops_total
  cedar_storage_keys_total
  cedar_storage_put_latency_seconds_bucket
  cedar_storage_put_latency_seconds_count
  cedar_storage_put_latency_seconds_sum
  cedar_storage_put_ops_total
  cedar_storage_size_bytes
].to_set

refs = []
alerts.fetch('groups').each do |group|
  group.fetch('rules').each do |rule|
    expr = rule.fetch('expr')
    refs.concat(expr.scan(/\bcedar_[a-zA-Z0-9_:]+/))
  end
end

missing = refs.uniq.reject { |metric| exported_metrics.include?(metric) }
abort("Alert rules reference non-exported Cedar metrics: #{missing.inspect}") unless missing.empty?
RUBY

    ruby - tools/storaged.cc tools/graphd.cc <<'RUBY'
require 'set'

exported_metrics = %w[
  cedar_disk_free_bytes
  cedar_process_memory_bytes
  cedar_storage_put_latency_seconds
].to_set

refs = ARGV.flat_map do |path|
  File.read(path).scan(/condition_metric\s*=\s*"([^"]+)"/).flatten
end

missing = refs.uniq.reject { |metric| exported_metrics.include?(metric) }
abort("Built-in AlertManager rules reference non-exported metrics: #{missing.inspect}") unless missing.empty?
RUBY
fi

log_info "Deployment manifest static guard passed"
