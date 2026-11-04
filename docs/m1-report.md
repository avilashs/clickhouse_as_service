# M1 report: engine on platform

Date: 2026-11-04. Environment: one kind cell (Kubernetes 1.33.1; 1 control-plane, 3 `control` and 3 `shared` nodes, one of each per zone) in a 4-vCPU / 15 GiB Firecracker VM. Images: ClickHouse 26.8.34.13 LTS, Altinity operator 0.27.4, Envoy Gateway v1.5.0, Temporal 1.32.2 (dev server), PostgreSQL 16.10.

**Acceptance (prompt):** create and delete a shared cluster on kind through the API; a standard client connects over TLS; kill the workflow worker mid-create and confirm it resumes without duplicates.

## Results

**All M1 acceptance criteria pass.** `test/e2e/m1.sh` run 2: **32/32 checks passed** ([log](evidence/m1-e2e-run2.log)). Unit and integration tests: `go test ./...` passes (engine golden and invariant tests, secret store, store integration against PostgreSQL 16, and workflow tests in Temporal's test environment with the real store).

| Acceptance criterion | Evidence |
|---|---|
| Create and delete a shared cluster through the API | Both e2e clusters: `CREATING → ACTIVE` in 156 s and 238 s; delete in 78–84 s. Afterwards the namespace is gone, the reservation is `RELEASED`, credentials are deleted and the record is soft-deleted. |
| A standard client connects over TLS | Stock `clickhouse-client` 26.8 → shared gateway (SNI passthrough) → tenant pod, verifying the server cert against the CA from `GET /clusters/{id}` (`verificationMode strict`). HTTPS via curl `--cacert` also works. A client with a different CA is rejected. |
| Kill the worker mid-create; it resumes without duplicates | `kill -9` of the worker right after the create call, and again while waiting for convergence (worker down for 30 s, longer than the 20 s heartbeat timeout). After restart the cluster reached `ACTIVE`, with exactly one each of: cluster row, operation, `COMMITTED` reservation, desired-state row, Temporal workflow run, namespace, CHI and CHK, and credential set. Workflow history shows the wait activity retried (attempt 2). Replaying the create with the same `Idempotency-Key` returned the same cluster. |

Also checked by the suite, against the live cluster, as the tenant's `admin` user:
- an `insert_quorum = 2` insert through the gateway succeeds, and `system.replicas` shows 2/2 active replicas with 1000 rows;
- `ENGINE = MergeTree` is refused (`TABLE ENGINE ON MergeTree`);
- `url('http://169.254.169.254/…')` is refused (`READ ON URL`).

Workflow tests (`internal/workflows`):
- **Happy path:** reservation committed, cluster `ACTIVE`.
- **Convergence timeout:** compensation tombstones the cluster, waits for teardown, releases capacity, and marks cluster and operation `FAILED`.
- **No capacity:** the operation fails, and nothing is published or reserved.

**Unplanned resilience test.** The VM running this session restarted while the first cluster (`orders`) was being created. That killed Docker, the kind cell, Postgres, Temporal, the API and the worker at once. After restart:
1. Postgres and the Temporal dev server came back with their data.
2. The `CreateCluster` workflow resumed where it stopped.
3. The kind cell couldn't recover (limitation 2), so I recreated an empty cell.
4. The redeployed agent **rebuilt the cluster from the desired state alone**, with no API call.
5. The workflow completed with the operation `SUCCEEDED` (1,071 s end to end, including the outage).

This is the control-plane / data-plane split of design §3 working as intended. A real cell would have kept the cluster running instead of needing a rebuild.

Run 1 of the suite had 30/31 passing. The failure was in the test itself: it read both replicas with `clusterAllReplicas()`, which the tenant is correctly denied (`READ ON REMOTE`; see limitation 7). Run 2 checks replication through `system.replicas`. Between the runs I also fixed two things: cell bring-up now replaces the operator bundle's public default password with a random one, and the API's 202 response now carries stored timestamps.


## What was built

| Component | Notes |
|---|---|
| REST API (`cmd/basalt-api`, `api/openapi.yaml`) | Create, list, describe and delete clusters; get operations; initial credentials; sizes and versions. Every mutation returns `202` + `Operation` and needs an `Idempotency-Key`. Replay returns the original operation; a reused key with a different body returns `422`. Tenant bearer tokens are scoped to a project, and foreign projects return `404`. |
| Store (`internal/store`) | PostgreSQL schema per design §4: append-only specs, operations, idempotency keys, transactional capacity reservations (row-locked per pool), desired and observed state per cell. A partial unique index allows at most one unfinished operation per cluster. No secret values are stored. |
| Workflows (`internal/workflows`, `cmd/basalt-worker`) | `CreateCluster`: reserve → credentials → publish → wait for convergence (heartbeating) → commit. If anything after the reservation fails, compensation tombstones the cluster, waits for teardown, releases capacity and marks it `FAILED`. `DeleteCluster`: tombstone → wait until gone → release, delete credentials, soft-delete. The workflow ID `cluster/<id>` is the per-resource lock. A sweeper starts workflows for operations left `PENDING`. |
| Engine (`internal/engine/clickhouse`) | Pure `Render` and `Observe`, golden-tested. TLS everywhere, with plaintext ports removed: native 9440, HTTPS 8443, interserver HTTPS 9010, Keeper client 2281 and Keeper Raft over TLS. The operator verifies servers against the cell CA. Tenant limits live in the server default profile, and `TABLE ENGINE` grants allow only replicated engines. Also renders `customer_admin` and `admin` in replicated access storage, default-deny NetworkPolicies, a PDB, Pod Security `restricted`, placement rules R1 and R1b, and TLSRoutes on the shared gateway. |
| Cell agent (`cmd/basalt-agent`) | Runs in the cell and talks to the control plane outbound only. It applies desired state with server-side apply, and deletes only on an explicit tombstone, never because something is missing from the snapshot. It owns the cell CA (the key never leaves the cell) and issues per-cluster certificates. It runs the bootstrap SQL once all replicas see each other and Keeper, and probes every replica over SQL. |
| Cell (`hack/cell-up.sh`, `deploy/`) | kind nodes labelled by pool and zone, with a taint on the shared pool. Images are loaded per pool. The operator is installed from the unmodified upstream bundle plus patches, with a random operator password. Envoy Gateway runs with a TLS-passthrough Gateway. |

## Deviations from the design (and why)

| Design | M1 | Why / plan |
|---|---|---|
| cert-manager issues certificates | The cell agent issues them from a per-cell CA | cert-manager's images are on quay.io, which this environment can't reach. The agent-held CA also keeps the CA key inside the cell. Public client certificates (ACME) arrive with the cloud cell in M6. |
| gVisor `runtimeClassName` on shared pods | Not set | Needs `runsc` in the kind node image; this is the M2 sandbox spike. The renderer has the hook (`sandboxRuntime`). |
| Temporal with PostgreSQL persistence | Temporal dev server (SQLite file) | Local only. The production Helm deployment of Temporal comes with the cloud cell. |
| Cell agent over mTLS | Bearer token over HTTP on the kind bridge | Local only; mTLS with per-cell client certificates is planned. |
| `oapi-codegen` server stubs (ADR-0001) | Hand-written handlers against the spec | Faster for M1. A spec-vs-routes contract test is planned for M2. |
| Control pool tainted | Shared pool tainted, control pool only labelled | Every platform component needs tolerations first. This is M2 placement work. |

## Known limitations found in M1

1. **The operator can report `Completed` before every replica has the new `remote_servers`.** Seen on kind: replica 0-0 listed only itself for a few seconds. `Observe` therefore requires every replica to see all its peers, rather than trusting the CHI status.
2. **A Docker daemon restart breaks kind cells.** The control-plane node's IP changes and its kubelet keeps the old one. In practice this surfaced as a useful test (see Results): the session's VM restarted mid-create, the cell had to be recreated, and the agent rebuilt the cluster from desired state.
3. **Agent RBAC is cluster-wide** for Secrets and the kinds it manages. M3 narrows this to Basalt-managed namespaces.
4. **The operator processes CHI deletions in its shared reconcile queue.** A deletion can wait behind a long reconcile of another cluster: one delete took 3.5 minutes end to end while the operator reconciled a new cluster. That's acceptable for M1; M2 spike S1b measures it at scale.
7. **Tenants can't use `cluster()` / `clusterAllReplicas()`.** These need `READ ON REMOTE`, which `customer_admin` doesn't hold. Whether to grant it, scoped to the cluster's own hosts through `remote_url_allow_hosts` plus egress policy, is an M3 decision.
5. **Replicated access storage makes Keeper a hard startup dependency (new finding).** A ClickHouse server configured with `user_directories/replicated` exits at startup (exit code 231) when Keeper is unreachable. This happened on kind, where the first replica restarted until Keeper was up, and I reproduced it with the 26.8.34 binary against an unreachable Keeper. Consequence for design F-5: during Keeper quorum loss, running replicas stay read-only as planned, but **any replica that restarts cannot come back** until quorum returns. Candidate mitigations for M4: keep customer users in `local_directory` and have the agent replicate access DDL; protect Keeper with priority and PDB so restarts don't coincide with its outages; or find an upstream option for lazy initialisation.
6. **NetworkPolicy enforcement on kind** relies on kindnet's network-policy support. The M3 attack suite verifies enforcement per flow. M1 only shows that the allowed flows work.

## Next: M2 (placement)

* Dedicated tier: per-cluster Karpenter-style pools on kind (labelled/tainted nodes), exclusivity anti-affinity with `namespaceSelector: {}`.
* `ValidatingAdmissionPolicy` set from design §8.3, including the label-integrity rules, plus taints and tolerations on the control pool.
* `basalt-audit` CLI and metric for continuous placement checks.
* Sandbox spike S2: gVisor (`runsc`) in the kind node image; the ClickHouse compatibility checks from ADR-0003.
* Operator spikes S1 and S1b: rolling-restart gating, and deletion latency under load (limitation 4).
* Spec-vs-routes contract test for the OpenAPI document.
