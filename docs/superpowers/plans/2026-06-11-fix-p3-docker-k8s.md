# P3 Fix: Docker Queryd Entrypoint + K8s Network Policy

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development.

**Goal:** Add `queryd` role to Docker entrypoint and fix K8s network policy to include storaged/queryd ports.

**Architecture:** Extend the bash case statement in docker-entrypoint.sh. Add missing TCP ports to the K8s NetworkPolicy manifest.

**Tech Stack:** Bash, Docker, Kubernetes YAML

---

## File Map

| File | Responsibility |
|------|----------------|
| `cedar-docker-compose/docker-entrypoint.sh` | Add queryd case + healthcheck port |
| `cedar-docker-compose/Dockerfile` | Add queryd HEALTHCHECK port |
| `k8s/network-policy.yaml` | Add ports 9779 (storaged) and 9889 (queryd) |
| `k8s/pod-disruption-budget.yaml` | Fix over-broad selector |

---

### Task 1: Add queryd to Docker Entrypoint

**Files:**
- Modify: `cedar-docker-compose/docker-entrypoint.sh`

- [ ] **Step 1: Add queryd case to the switch statement**

Replace the entire file content with:

```bash
#!/bin/bash
# CedarGraph Docker Entrypoint
# Handles signal forwarding (PID 1) and role-based binary dispatch.
set -e

# Map NODE_ROLE to binary and default port
case "${NODE_ROLE}" in
  metad)
    BINARY="/usr/local/bin/cedar-metad"
    HEALTH_PORT="${METAD_PORT:-9559}"
    ;;
  storaged)
    BINARY="/usr/local/bin/cedar-storaged"
    HEALTH_PORT="${STORAGED_PORT:-9779}"
    ;;
  graphd)
    BINARY="/usr/local/bin/cedar-graphd"
    HEALTH_PORT="${GRAPHD_PORT:-9669}"
    ;;
  queryd)
    BINARY="/usr/local/bin/cedar-queryd"
    HEALTH_PORT="${QUERYD_PORT:-9889}"
    ;;
  *)
    echo "ERROR: Unknown or missing NODE_ROLE='${NODE_ROLE}'."
    echo "Valid values: metad, storaged, graphd, queryd"
    exit 1
    ;;
esac

if [ ! -x "${BINARY}" ]; then
  echo "ERROR: Binary not found or not executable: ${BINARY}"
  exit 1
fi

# Pass through any extra arguments
exec "${BINARY}" "$@"
```

- [ ] **Step 2: Update Dockerfile HEALTHCHECK for queryd**

Read `cedar-docker-compose/Dockerfile` and find the HEALTHCHECK line. If it hardcodes a port list, add 9889. If it uses an env var, verify `QUERYD_PORT` is referenced.

- [ ] **Step 3: Commit**

```bash
cd <repo-root> && git add cedar-docker-compose/docker-entrypoint.sh cedar-docker-compose/Dockerfile && git commit -m "fix(docker): add queryd role to entrypoint and healthcheck"
```

---

### Task 2: Fix K8s Network Policy

**Files:**
- Modify: `k8s/network-policy.yaml`

- [ ] **Step 1: Add missing ports to ingress and egress**

Replace the file content with:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: cedargraph-network-policy
  namespace: cedargraph
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/part-of: cedargraph
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              name: cedargraph
      ports:
        - protocol: TCP
          port: 6000   # metad
        - protocol: TCP
          port: 7000   # internal
        - protocol: TCP
          port: 9090   # metrics
        - protocol: TCP
          port: 9559   # metad alt
        - protocol: TCP
          port: 9669   # graphd
        - protocol: TCP
          port: 9779   # storaged
        - protocol: TCP
          port: 9889   # queryd
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              name: cedargraph
      ports:
        - protocol: TCP
          port: 6000
        - protocol: TCP
          port: 7000
        - protocol: TCP
          port: 9090
        - protocol: TCP
          port: 9559
        - protocol: TCP
          port: 9669
        - protocol: TCP
          port: 9779
        - protocol: TCP
          port: 9889
```

- [ ] **Step 2: Commit**

```bash
cd <repo-root> && git add k8s/network-policy.yaml && git commit -m "fix(k8s): add storaged (9779) and queryd (9889) to network policy"
```

---

### Task 3: Fix K8s PodDisruptionBudget Selector

**Files:**
- Modify: `k8s/pod-disruption-budget.yaml`

- [ ] **Step 1: Read current PDB content**

- [ ] **Step 2: Replace over-broad selector with per-component PDBs**

Replace with separate PDBs for each component:

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: cedargraph-metad-pdb
  namespace: cedargraph
spec:
  minAvailable: 2
  selector:
    matchLabels:
      app.kubernetes.io/name: metad
      app.kubernetes.io/part-of: cedargraph
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: cedargraph-storaged-pdb
  namespace: cedargraph
spec:
  minAvailable: 2
  selector:
    matchLabels:
      app.kubernetes.io/name: storaged
      app.kubernetes.io/part-of: cedargraph
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: cedargraph-graphd-pdb
  namespace: cedargraph
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: graphd
      app.kubernetes.io/part-of: cedargraph
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: cedargraph-queryd-pdb
  namespace: cedargraph
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: queryd
      app.kubernetes.io/part-of: cedargraph
```

- [ ] **Step 3: Commit**

```bash
cd <repo-root> && git add k8s/pod-disruption-budget.yaml && git commit -m "fix(k8s): replace over-broad PDB with per-component PDBs"
```

---

### Task 4: Full Regression Test

- [ ] **Step 1: Run full test suite**

```bash
cd <repo-root>/build && ctest --output-on-failure -j$(sysctl -n hw.ncpu)
```
Expected: 1285/1285 passed, 0 failed.

Note: Docker/K8s changes do not affect C++ tests, but the suite ensures no regressions in the main codebase.
