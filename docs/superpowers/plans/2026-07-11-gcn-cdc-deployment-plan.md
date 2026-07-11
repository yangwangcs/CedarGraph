# GCN Build And Deployment Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Package and deploy a persistent, health-checked GCN service in local install, Docker Compose, Kubernetes, and Helm.

**Architecture:** `graphcomputenode` becomes a first-class production binary and role. Every deployment surface supplies stable identity, MetaD discovery, persistent checkpoint/TMV storage, probes, bounded resources, TLS material, and required network paths; static gates validate resources structurally.

**Tech Stack:** CMake install, Bash/systemd, multi-stage Docker, Docker Compose, Kubernetes/Kustomize, Helm, Ruby/YAML preflight scripts.

## Global Constraints

- Do not expose GCN port 9780 from the GraphD container.
- GCN data directories require persistent storage in production manifests.
- Probes call real GCN health/readiness endpoints, not a port owned by another process.
- GraphD discovers GCN through MetaD; deployment templates do not reintroduce a static GCN singleton dependency.
- TLS secrets and non-root security context follow existing production conventions.

---

### Task 1: Install And Run The Fourth Production Binary

**Files:**
- Modify: `CMakeLists.txt`
- Modify: `scripts/deploy/install.sh`
- Create: `config/graphcomputenode.service`
- Create: `config/gcn.yaml`
- Modify: `scripts/preflight_deployment_static.sh`

**Interfaces:**
- Consumes: graphcomputenode flags/config from the consumer and routing plans.
- Produces: installable binary, config, data directory, and systemd unit.

- [ ] **Step 1: Add failing static assertions**

Add assertions that fail unless `install(TARGETS ...)` contains `graphcomputenode`, install.sh installs it, the systemd unit uses `NODE_ROLE=gcn` semantics/config, and the unit has restart, limits, non-root user, and writable persistent data directory.

```ruby
abort('CMake install must include graphcomputenode') unless
  cmake.match?(/install\(TARGETS[^\)]*graphcomputenode/m)
abort('GCN systemd unit missing') unless File.exist?('config/graphcomputenode.service')
```

- [ ] **Step 2: Run and verify failure**

Run: `./scripts/preflight_deployment_static.sh`

Expected: FAIL with `CMake install must include graphcomputenode`.

- [ ] **Step 3: Add install target, config, and unit**

Install `graphcomputenode`, copy `gcn.yaml`, create `/var/lib/cedar/gcn` owned by the Cedar user, and enable a service that passes node id, advertise endpoint, MetaD endpoints, data directory, TLS, CDC limits, and readiness port.

- [ ] **Step 4: Run static and install dry-run checks**

Run: `./scripts/preflight_deployment_static.sh && cmake --install build --prefix "$(mktemp -d)"`

Expected: exits `0`; the temporary prefix contains `bin/graphcomputenode`, `etc/cedar/gcn.yaml`, and the service asset.

- [ ] **Step 5: Commit**

```bash
git add CMakeLists.txt scripts/deploy/install.sh config/graphcomputenode.service config/gcn.yaml scripts/preflight_deployment_static.sh
git commit -m "feat(deploy): install the GCN service"
```

### Task 2: Package And Run GCN In Docker Compose

**Files:**
- Modify: `cedar-docker-compose/Dockerfile`
- Modify: `cedar-docker-compose/Dockerfile.build`
- Modify: `cedar-docker-compose/Dockerfile.cn`
- Modify: `cedar-docker-compose/docker-entrypoint.sh`
- Modify: `cedar-docker-compose/docker-compose.yml`
- Modify: `docker-compose.production.yml`
- Modify: `scripts/preflight_docker_static.sh`
- Modify: `scripts/preflight_docker_image_runtime.sh`
- Modify: `scripts/preflight_compose_smoke.sh`

**Interfaces:**
- Produces: image containing four binaries and Compose topologies with a persistent GCN service.

- [ ] **Step 1: Add failing Docker/Compose structural assertions**

```ruby
abort('runtime image must contain graphcomputenode') unless
  dockerfile.include?('/build/build/graphcomputenode')
abort('production compose must define gcn') unless services.key?('gcn')
abort('gcn requires persistent data volume') unless
  services['gcn'].fetch('volumes', []).any? { |v| v.to_s.include?('/data/gcn') }
```

Also assert GraphD does not expose 9780 and GCN does expose 9780 plus its health/metrics endpoints.

- [ ] **Step 2: Run and verify failure**

Run: `./scripts/preflight_docker_static.sh`

Expected: FAIL because the runtime image and Compose services omit GCN.

- [ ] **Step 3: Update images, entrypoint, and both Compose files**

Build/copy `graphcomputenode`; support `NODE_ROLE=gcn`; add a scalable `gcn` service without fixed `container_name`, with persistent volume, MetaD dependencies, TLS mounts, limits, probes, and `gcn` network alias. Do not configure GraphD with a static GCN endpoint.

- [ ] **Step 4: Validate image contents and real Compose startup**

Run: `./scripts/preflight_docker_static.sh && ./scripts/preflight_docker_image_runtime.sh`

Expected: image runtime test finds and invokes `--help` on all four binaries.

Run: `./scripts/preflight_compose_smoke.sh`

Expected: MetaD, StorageD, GCN, and GraphD become healthy; the smoke observes GCN registration through MetaD.

- [ ] **Step 5: Commit**

```bash
git add cedar-docker-compose/Dockerfile cedar-docker-compose/Dockerfile.build cedar-docker-compose/Dockerfile.cn cedar-docker-compose/docker-entrypoint.sh cedar-docker-compose/docker-compose.yml docker-compose.production.yml scripts/preflight_docker_static.sh scripts/preflight_docker_image_runtime.sh scripts/preflight_compose_smoke.sh
git commit -m "feat(docker): deploy persistent GCN workers"
```

### Task 3: Add Native Kubernetes GCN Resources

**Files:**
- Create: `k8s/gcn.yaml`
- Create: `k8s/gcn-config.yaml`
- Modify: `k8s/kustomization.yaml`
- Modify: `k8s/graphd.yaml`
- Modify: `k8s/network-policy.yaml`
- Modify: `k8s/pod-disruption-budget.yaml`
- Modify: `scripts/preflight_k8s_static.sh`
- Modify: `scripts/preflight_k8s_server_dry_run.sh`

**Interfaces:**
- Produces: GCN StatefulSet, headless/client Services, PVCs, probes, PDB, config, and allowed network paths.

- [ ] **Step 1: Replace the incorrect GraphD-port assertion with failing GCN resource assertions**

```ruby
gcn = resource('StatefulSet', 'gcn')
abort('GCN StatefulSet missing') unless gcn
abort('GraphD must not expose GCN port') if graphd_ports.key?('gcn')
abort('GCN PVC template missing') if gcn.dig('spec', 'volumeClaimTemplates').to_a.empty?
abort('GCN readiness probe missing') unless gcn.dig('spec','template','spec','containers',0,'readinessProbe')
```

- [ ] **Step 2: Run and verify failure**

Run: `./scripts/preflight_k8s_static.sh`

Expected: FAIL with `GCN StatefulSet missing`.

- [ ] **Step 3: Add resources and network policy**

Create a StatefulSet with stable node id derived from pod identity, PVC mounted at `/data/gcn`, config/TLS mounts, startup/readiness/liveness probes, security context, termination grace, and resource bounds. Add headless and client Services. Permit GCN egress to MetaD/StorageD and GraphD ingress to GCN; remove the bogus GraphD 9780 port.

- [ ] **Step 4: Render and server-dry-run**

Run: `kubectl kustomize k8s > /tmp/cedar-k8s.yaml && ./scripts/preflight_k8s_static.sh`

Expected: exits `0`; rendered YAML contains GCN StatefulSet, two Services, PVC template, PDB, probes, and NetworkPolicy rules.

Run: `./scripts/preflight_k8s_server_dry_run.sh`

Expected: API server dry-run accepts all rendered objects.

- [ ] **Step 5: Commit**

```bash
git add k8s/gcn.yaml k8s/gcn-config.yaml k8s/kustomization.yaml k8s/graphd.yaml k8s/network-policy.yaml k8s/pod-disruption-budget.yaml scripts/preflight_k8s_static.sh scripts/preflight_k8s_server_dry_run.sh
git commit -m "feat(k8s): deploy GCN StatefulSet"
```

### Task 4: Add Helm GCN Values And Templates

**Files:**
- Modify: `helm-chart/cedargraph/values.yaml`
- Modify: `helm-chart/cedargraph/templates/_helpers.tpl`
- Create: `helm-chart/cedargraph/templates/gcn-statefulset.yaml`
- Modify: `helm-chart/cedargraph/templates/networkpolicy.yaml`
- Modify: `helm-chart/cedargraph/templates/pdb.yaml`
- Modify: `helm-chart/cedargraph/templates/NOTES.txt`
- Modify: `scripts/preflight_helm_static.sh`

**Interfaces:**
- Produces: configurable Helm GCN deployment matching native K8s semantics.

- [ ] **Step 1: Add failing Helm assertions**

Assert default render contains a GCN StatefulSet, Service, PVC template, probes, resources, security context, PDB, and network policy; assert `--set gcn.enabled=false` removes GCN resources while leaving GraphD/StorageD valid.

- [ ] **Step 2: Run and verify failure**

Run: `./scripts/preflight_helm_static.sh`

Expected: FAIL because values and GCN templates are missing.

- [ ] **Step 3: Implement values and templates**

Add:

```yaml
gcn:
  enabled: true
  replicas: 1
  persistence:
    enabled: true
    size: 20Gi
  cdc:
    batchRecords: 1024
    batchBytes: 4194304
    maxAllowedLag: 0
  resources:
    requests: {cpu: 500m, memory: 1Gi}
    limits: {cpu: 2, memory: 4Gi}
```

Use the same identity, volume, probe, TLS, PDB, and NetworkPolicy semantics as native K8s.

- [ ] **Step 4: Lint and render enabled/disabled variants**

Run: `helm lint helm-chart/cedargraph && helm template cedar helm-chart/cedargraph > /tmp/cedar-helm.yaml && helm template cedar helm-chart/cedargraph --set gcn.enabled=false > /tmp/cedar-helm-no-gcn.yaml && ./scripts/preflight_helm_static.sh`

Expected: all commands exit `0`; enabled render contains GCN resources and disabled render contains none.

- [ ] **Step 5: Commit**

```bash
git add helm-chart/cedargraph/values.yaml helm-chart/cedargraph/templates/_helpers.tpl helm-chart/cedargraph/templates/gcn-statefulset.yaml helm-chart/cedargraph/templates/networkpolicy.yaml helm-chart/cedargraph/templates/pdb.yaml helm-chart/cedargraph/templates/NOTES.txt scripts/preflight_helm_static.sh
git commit -m "feat(helm): add configurable GCN workers"
```

### Task 5: Add Prometheus Scraping And Blocking Alerts

**Files:**
- Modify: `config/prometheus.yml`
- Modify: `config/cedar_alerts.yml`
- Modify: `config/grafana/dashboards/cedargraph-overview.yml`
- Modify: `config/docker-compose.monitoring.yml`
- Modify: `scripts/preflight_deployment_static.sh`

**Interfaces:**
- Consumes: CDC/GCN/GraphD metrics from the storage and routing plans.
- Produces: scrape targets, dashboard panels, and alerts for stalled consumption, retention risk, backfill failure, no healthy GCN, checksum errors, and lease churn.

- [ ] **Step 1: Add failing alert and scrape assertions**

Assert Prometheus discovers GCN metrics and every required alert references an existing metric with a nonzero `for` duration. Validate YAML and reject unbounded labels in dashboard queries.

- [ ] **Step 2: Run and verify failure**

Run: `./scripts/preflight_deployment_static.sh`

Expected: FAIL because GCN scrape and alert rules are absent.

- [ ] **Step 3: Add scrape, dashboard, and alert definitions**

Add rules named `CedarGcnConsumptionStalled`, `CedarGcnRetentionWindowRisk`, `CedarGcnBackfillFailed`, `CedarNoHealthyGcn`, `CedarCdcChecksumFailure`, and `CedarGcnLeaseChurn`. Use thresholds from explicit config values and route critical data-integrity alerts separately from availability warnings.

- [ ] **Step 4: Validate monitoring configuration**

Run: `./scripts/preflight_deployment_static.sh && docker compose -f config/docker-compose.monitoring.yml config >/dev/null`

Expected: exits `0`; Prometheus rules parse and all referenced metrics match registered names.

- [ ] **Step 5: Commit**

```bash
git add config/prometheus.yml config/cedar_alerts.yml config/grafana/dashboards/cedargraph-overview.yml config/docker-compose.monitoring.yml scripts/preflight_deployment_static.sh
git commit -m "feat(monitoring): alert on GCN CDC health"
```
