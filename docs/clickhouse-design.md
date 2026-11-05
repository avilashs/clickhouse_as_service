# Managed ClickHouse® service: Phase 0 design

Status: **DRAFT rev 2, awaiting approval.** No implementation code exists yet.
Date: 2026-11-04. Rev 2 (same day) adds the AI query optimizer integration and the review fixes listed in
[review-2026-11-04.md](review-2026-11-04.md). Those fixes are backed by tests against ClickHouse 26.8.34 LTS
([evidence](evidence/2026-11-04-lab.md), [script](../test/lab/phase0_verify.sh)).

> Working name: **Basalt** (placeholder for `{{PROJECT}}`; see Open Question Q1).
> Basalt is "a managed service for ClickHouse". ClickHouse is a registered trademark of
> ClickHouse, Inc. Basalt is not affiliated with or endorsed by ClickHouse, Inc.

---

## 0. Starting state and what this document covers

* **The platform does not exist.** `avilashs/clickhouse_as_service` (this repo) and
  `avilashs/kafka_as_service` have no commits on their default branches. There's no API,
  workflow engine, cell agent or `Engine` interface to reuse. This design specifies the
  platform as well as the ClickHouse engine, and keeps the platform core engine-agnostic so
  a Kafka engine can plug in later.
* **The AI query optimizer** is `avilashs/query_optimizer`, branch
  `claude/blissful-tesla-01lxws` @ `0d86d10`. It's a Python multi-LLM advisor with an
  optional auto-apply ("god mode") that ships as a CronJob. I read it, ran its test suite
  (73 pass, 2 fail, 6 skipped; the failures come from a file missing from the repo), and ran it
  end-to-end against ClickHouse 26.8. It's integrated as a **per-cluster CronJob** (§17,
  [ADR-0008](adr/0008-query-optimizer-integration.md)).

Placeholders and the defaults I assumed (all reversible):

| Placeholder | Assumed for this draft |
|---|---|
| `{{PROJECT}}` | `Basalt` (working name) |
| `{{CLOUD}}` | AWS |
| `{{DOMAIN}}` | `<domain>` (written literally; examples use `db.example.com`) |
| `{{LICENSE}}` | Apache-2.0 |

Documents in this phase:

| Document | Topic |
|---|---|
| this file | Overall design, data model, placement, sizing, threat model, failure modes |
| [ADR-0001](adr/0001-platform-stack.md) | Platform stack (Go, PostgreSQL, Temporal, pull-based cell agent) |
| [ADR-0002](adr/0002-operator.md) | Kubernetes operator choice |
| [ADR-0003](adr/0003-sandbox.md) | Sandbox runtime for shared nodes |
| [ADR-0004](adr/0004-keeper-topology.md) | Keeper topology and cost |
| [ADR-0005](adr/0005-networking.md) | Endpoints, gateway, TLS, session affinity |
| [ADR-0006](adr/0006-backup.md) | Backup and restore mechanism |
| [ADR-0007](adr/0007-replication-safety.md) | Preventing non-replicated tables |
| [ADR-0008](adr/0008-query-optimizer-integration.md) | AI query optimizer integration (per-cluster CronJob) |
| [review-2026-11-04.md](review-2026-11-04.md) | Review of rev 1 and of the optimizer, with fixes |
| [evidence/2026-11-04-lab.md](evidence/2026-11-04-lab.md) | Test evidence from ClickHouse 26.8.34 |
| [dependencies.md](dependencies.md) | Dependencies and their licences |

---

## 1. Upstream verification: where the prompt's facts were checked

I checked the claims the prompt flagged as possibly stale. `clickhouse.com` is **not
reachable** from this session's network policy, so ClickHouse-documentation facts were
checked against search-engine snapshots of those pages and against GitHub source. Before
M1, I'd like `clickhouse.com` allowed in the environment's network settings (Q9).

| Prompt claim | Finding | Confidence |
|---|---|---|
| Altinity operator is Apache-2.0 with `ClickHouseInstallation`/`ClickHouseKeeperInstallation` | **Confirmed.** LICENSE is Apache-2.0. The latest tag is `release-0.27.4`, with the last commit on 2026-09-24. Since 0.27.0 a CHI can reference a CHK directly, which Altinity calls GA-level Keeper support. | High (source read) |
| ClickHouse Inc. has a newer operator | **Confirmed.** `ClickHouse/clickhouse-operator` is Apache-2.0 (copyright ClickHouse, Inc.). The API is **`v1alpha1`**, the tags run only from `v0.0.1` to `v0.0.8`, and the last commit was on 2026-11-03. The CRDs are `ClickHouseCluster` and `KeeperCluster`, with one KeeperCluster required per ClickHouseCluster. | High (source read) |
| SharedMergeTree is Cloud-only | Not contradicted; nothing I found shows it in the open-source build. | Medium |
| Zero-copy replication is off by default and not for production | **Confirmed.** It has been off by default since 22.8, and the docs say "not recommended for production use". | High |
| ingress-nginx is retiring | **Confirmed.** Kubernetes announced the retirement for March 2026 (best-effort maintenance until then, then no further fixes and read-only repos). A January 2026 Steering/SRC statement warns against staying on it. Do not use it. | High |
| Default to LTS releases | The current LTS is **26.8**, released 2026-08-27 and supported until about 2027-08. **26.3 LTS** is supported until about 2027-03. The cadence is two LTS releases a year (March and August), each supported for 1 year. | Medium-high |
| *(new)* Async inserts are now on by default | **Confirmed on 26.8.34:** `async_insert = 1` and `wait_for_async_insert = 1` by default. The ack comes after the buffer flush, so it isn't fire-and-forget, but batching and quorum semantics differ from synchronous inserts. We pin both explicitly per profile, and M5 tests `insert_quorum` together with async inserts. | **High (tested)** |
| *(new)* AVX2 is required on x86 from 26.6 | Affects instance families on AWS: every current Graviton and AVX2-capable Intel/AMD family is fine. The only source is a secondary one. | Low, verify in M1 |
| *(new)* 26.3 changed String serialization in nested types ("downgrades unsafe") and `NOT` operator precedence | Directly relevant to §13 (upgrade and downgrade). | Low, verify in M1 |
| *(new)* Grant model in 26.8 | **Tested:** source access is `READ/WRITE ON <source>` (`READ ON URL`, `READ ON S3`), with `access_control_improvements.enable_read_write_grants`. `executable()` is gated by `TABLE ENGINE ON Executable`. `table_engines_require_grant` lives under `access_control_improvements`, and at top level the server refuses to start. | **High (tested)** |

---

## 2. Goals, non-goals, product promises

Goals are as stated in the prompt. Here are the **promises the product makes, in plain words**.
They will appear in the docs exactly like this:

1. **Scaling is not instant.** Vertical scaling is a rolling restart, one replica per shard
   at a time. Adding replicas copies the full dataset over the network before the replica
   serves traffic, so the time is proportional to data size.
2. **Adding a shard does not move data.** Existing data stays on the old shards. New inserts
   are distributed by the `Distributed` table's sharding key and weights. We provide a
   rebalancing helper the customer runs explicitly (§13.4).
3. **Removing shards is not supported in v1** (§13.4).
4. **HA applies only to replicated tables.** We make the replicated path the default and
   fence off non-replicated engines (ADR-0007), but HA is a property of the table engine.
5. **Restore always goes to a new cluster,** to the state of a completed backup. This is
   *backup-point* restore, not arbitrary point-in-time recovery. ClickHouse has no WAL to
   replay. I'd rename "point-in-time-style restore" to "restore from backup point" (Q6).
6. **Durability of acknowledged inserts** depends on `insert_quorum` (§13.6). Our
   default profile uses a majority quorum, and the docs say what happens if a client lowers it.

Non-goals are as in the prompt (serverless, resharding, compute-storage separation, managed
ingestion, SQL console). **Removed from v1 because it isn't feasible:** remove-shard (see §13.4).

---

## 3. Architecture

```
             ┌──────────────────────── Control plane (region) ────────────────────────┐
  tenant ──► │  API (REST/OpenAPI) ──► PostgreSQL (desired state, operations, usage)  │
             │        │                     ▲                                         │
             │        ▼                     │                                         │
             │  Temporal (workflows) ──────►│  Secret manager (AWS Secrets Manager)   │
             │        ▲                                                               │
             │        │ signals: observed state                                       │
             │  Cell gateway API  ◄────────── outbound HTTPS/mTLS, long-poll ──────┐  │
             └──────────────────────────────────────────────────────────────────── │ ─┘
                                                                                    │
     ┌───────────────── Cell = one EKS cluster (data plane, size-limited) ───────────┴──┐
     │  cell-agent (pulls desired state, applies, reports status; no inbound)           │
     │  operator (Altinity) ── CHI / CHK per tenant cluster                             │
     │  placement admission (ValidatingAdmissionPolicy) + placement auditor             │
     │  Envoy Gateway (shared-tier SNI routing)   cert-manager   Prometheus             │
     │  LLM gateway (optimizer → approved LLM providers; injects keys, meters tokens)   │
     │                                                                                  │
     │  node pools: control (Keeper, system, optimizer jobs) │ shared (gVisor) │ dedicated-<cluster>… │
     │  namespace per tenant cluster: CH pods, PDBs, NetworkPolicies, Services, certs,  │
     │                                optimizer CronJob (if enabled)                    │
     └──────────────────────────────────────────────────────────────────────────────────┘
```

**Failure-domain separation.** The cell holds everything needed to keep serving: operator,
CRs, certs, secrets (synced into the cell as Kubernetes Secrets, encrypted with KMS
envelope encryption), gateway and Prometheus. If the control plane is down, nothing in the
cell depends on it. Only *changes* stop. The cell agent caches the last desired state
locally (in a ConfigMap and in CR annotations) and does not delete anything while it is
disconnected (§16, F-12).

**Pull model.** The agent long-polls `GET /cells/{id}/desired?since=<generation>` over
mTLS, applies the manifests with server-side apply under a field manager it owns, and
posts observed state back. Workflows never call into the cell. They write desired state,
then wait on a Temporal signal that the API raises when the agent reports
`observedGeneration >= target` with the expected conditions.

Stack details and rationale are in [ADR-0001](adr/0001-platform-stack.md).

---

## 4. Data model (PostgreSQL)

The authoritative desired state is in PostgreSQL. Secret *values* are never stored there,
only references to the secret manager.

```sql
-- Tenancy
organizations(id, name, created_at)
projects(id, org_id, name, created_at)

-- Cells and capacity
cells(id, region, cloud, k8s_version, state CHECK (state IN ('ACTIVE','DRAINING','SUSPENDED')),
      max_clusters, max_shared_vcpu, max_shared_mem_gib, max_dedicated_nodes,
      agent_last_seen_at, desired_generation, observed_generation)
cell_capacity(cell_id, pool, zone, vcpu_total, mem_gib_total, vcpu_reserved, mem_gib_reserved)
capacity_reservations(id, cell_id, cluster_id, operation_id, pool, zone,
      vcpu, mem_gib, nodes, state CHECK (state IN ('HELD','COMMITTED','RELEASED')),
      expires_at)                                 -- HELD reservations expire if never committed

-- Clusters
clusters(id, project_id, cell_id, name, tier CHECK (tier IN ('shared','dedicated')),
      state CHECK (state IN ('CREATING','ACTIVE','UPDATING','DEGRADED','FAILED','DELETING','DELETED')),
      state_reason, current_spec_version, desired_spec_version,
      version_channel, server_version, compatibility_version,
      created_at, deleted_at,
      UNIQUE (project_id, name) WHERE deleted_at IS NULL)
cluster_specs(cluster_id, version, size, shards, replicas_per_shard, storage_gib,
      storage_class, iops, throughput_mibps, zones jsonb, config_version, created_by_operation,
      PRIMARY KEY (cluster_id, version))          -- immutable, append-only
cluster_configs(cluster_id, version, settings jsonb, validated_against_allowlist_version,
      PRIMARY KEY (cluster_id, version))          -- immutable, append-only
cluster_endpoints(cluster_id, kind, host, port, protocol)
db_users(cluster_id, name, roles text[], secret_ref, created_at, rotated_at)

-- Operations (async API)
operations(id, cluster_id, kind, state CHECK (state IN ('PENDING','RUNNING','SUCCEEDED','FAILED','CANCELLED')),
      workflow_id, request jsonb, result jsonb, error jsonb, created_at, updated_at)
idempotency_keys(project_id, key, request_hash, operation_id, created_at,
      PRIMARY KEY (project_id, key))              -- same key + different hash => 422
resource_locks(resource_type, resource_id, operation_id, acquired_at,
      PRIMARY KEY (resource_type, resource_id))   -- belt-and-braces; Temporal workflow IDs are the primary lock

-- Backups
backup_schedules(cluster_id, cron, retention_days, enabled)
backups(id, cluster_id, kind CHECK (kind IN ('full','incremental')), base_backup_id,
      state, location_uri, size_bytes, started_at, finished_at, per_shard jsonb)
restore_verifications(id, backup_id, scratch_cluster_id, state, expected jsonb, observed jsonb, finished_at)

-- AI query optimizer (§17, ADR-0008)
optimizer_settings(cluster_id PRIMARY KEY, enabled, schedule, collection jsonb,
      auto_apply jsonb,                          -- {enabled, min_score, max_risk, max_per_day}
      llm_consent jsonb,                         -- {providers[], accepted_at, accepted_by}; required to enable
      monthly_token_budget, updated_at)
optimizer_state(cluster_id PRIMARY KEY, state_db bytea, updated_at)   -- the optimizer's SQLite de-dup state
recommendations(id, cluster_id, run_id, normalized_query_hash, query_shape_redacted,
      kind, summary, statements jsonb, rewritten_query, score, risk, confidence, rationale,
      status CHECK (status IN ('OPEN','APPLYING','APPLIED','REJECTED_BY_VALIDATOR','ROLLED_BACK','DISMISSED','SUPERSEDED')),
      apply_operation_id, verification jsonb, created_at,
      UNIQUE (cluster_id, normalized_query_hash, md5(statements::text)))  -- re-ingest is idempotent
                                                 -- raw query text is never stored

-- Metering and audit
usage_records(cluster_id, meter, window_start, window_end, quantity, unit,
      PRIMARY KEY (cluster_id, meter, window_start))  -- idempotent upsert
audit_log(id, at, actor, actor_type, action, resource, request_id, source_ip, details jsonb)
```

**Versioning.** Every mutation appends a new `cluster_specs` and/or `cluster_configs` row and
moves `desired_spec_version`. `current_spec_version` advances only when the workflow
confirms the cell reached it. Reverting means reapplying an earlier version, which goes
through validation again.

---

## 5. API (summary; the OpenAPI spec comes in M1)

```
POST   /v1/projects/{p}/clusters                       -> 202 Operation
GET    /v1/projects/{p}/clusters[/{c}]
DELETE /v1/projects/{p}/clusters/{c}                   -> 202 Operation
PATCH  /v1/projects/{p}/clusters/{c}                   -> 202 Operation  (size, replicas, shards, storage)
PUT    /v1/projects/{p}/clusters/{c}/config            -> 202 Operation  (allowlisted settings)
POST   /v1/projects/{p}/clusters/{c}:upgrade           -> 202 Operation
GET|POST|DELETE /v1/projects/{p}/clusters/{c}/users[/{u}]   (+ :rotatePassword)
GET|POST|DELETE /v1/projects/{p}/clusters/{c}/databases[/{d}]   (creates Replicated databases, ADR-0007)
GET|POST /v1/projects/{p}/clusters/{c}/backups         (POST = on-demand)
PUT    /v1/projects/{p}/clusters/{c}/backupSchedule
POST   /v1/projects/{p}/clusters:restore               -> 202 Operation  (source backup -> NEW cluster)
GET    /v1/projects/{p}/clusters/{c}/metrics?query=…   (scoped PromQL proxy)
PUT    /v1/projects/{p}/clusters/{c}/optimizer         (enable / schedule / autoApply / llmConsent, §17)
GET    /v1/projects/{p}/clusters/{c}/recommendations
POST   /v1/projects/{p}/clusters/{c}/recommendations/{id}:apply    -> 202 Operation
POST   /v1/projects/{p}/clusters/{c}/recommendations/{id}:dismiss
GET    /v1/operations/{id}
GET    /v1/versions   /v1/sizes
```

All mutations require an `Idempotency-Key`. Replaying a key with the same body returns the
original Operation. Replaying it with a different body returns `422`.

---

## 6. Engine interface

This interface is the platform core's contract. Kafka would be a second implementation.

```go
type Engine interface {
    Kind() string                                             // "clickhouse"
    Validate(ctx context.Context, spec ClusterSpec, cfg Config) []FieldError
    PlanPlacement(ctx context.Context, spec ClusterSpec, cell CellView) (PlacementPlan, error) // resources to reserve
    Render(ctx context.Context, in RenderInput) (Manifests, error) // pure: same input -> same bytes
    Observe(ctx context.Context, obs ObservedObjects) (Health, error) // pure: CR status + canary -> ACTIVE/DEGRADED/...
    PlanChange(ctx context.Context, from, to ClusterSpec) (ChangePlan, error) // ordered, gated steps
    Delete(ctx context.Context, c ClusterRef) (Manifests, error)  // what to remove, and the finalizers to wait on
}
```

`Render` and `Observe` are **pure functions**, which is what makes workflow activities
idempotent and testable with golden files. `PlanChange` returns steps, for example
`[AddReplica(s0), WaitCaughtUp(s0r2), ShiftTraffic, ...]`, each with a gate the workflow
waits on. The workflow engine runs the steps, so engines contain no orchestration logic.

---

## 7. Tiers and sizing

### 7.1 Shared tier sizes (requests = limits, memory never overcommitted)

| Size | vCPU | Mem GiB | Replicas | Disk GiB (default / max) | gp3 IOPS / MiB/s | `max_threads` | `max_concurrent_queries` | HA |
|---|---|---|---|---|---|---|---|---|
| `dev-1` | 1 | 4 | 1 | 50 / 200 | 3000 / 125 | 2 | 20 | **No** (labelled) |
| `s-2` | 2 | 8 | 2 | 100 / 1000 | 3000 / 125 | 4 | 50 | Yes |
| `s-4` | 4 | 16 | 2 | 200 / 2000 | 6000 / 250 | 6 | 100 | Yes |
| `s-8` | 8 | 32 | 2 or 3 | 500 / 4000 | 12000 / 500 | 12 | 150 | Yes |

The figures are initial guesses for the M4 and M6 benchmarks to tune. The memory-to-vCPU
ratio is 4:1, and shared tier is 1 shard. Above `s-8`, customers go dedicated.

### 7.2 Dedicated tier

The customer chooses shards × replicas (replicas ≥ 2 unless they accept a no-HA label) and
an instance family: general `m7g/m7i`, memory `r7g/r7i`, or local NVMe `i4i/i7ie`. Each
server pod gets the node's allocatable memory and CPU minus a **system reservation**
(kubelet, DaemonSets, and 5% headroom). This is computed per instance type from a table in
code, not from live allocatable, so renders stay deterministic.

### 7.3 Keeper cost

Per tenant cluster there are 3 Keeper pods on the provider-owned `control` pool, each
`0.5 vCPU / 1 GiB` (dev: `0.25 / 0.5`) with a 10 GiB gp3 volume. Assuming roughly
**$0.025 per vCPU-hour and $0.0034 per GiB-hour** (blended on-demand rates for m7g; to be
replaced with current pricing):

Assuming 730 hours a month (rev 2 corrects the arithmetic in rev 1):

| | Keeper $/month (compute + EBS) | Server compute $/month | Keeper as % of compute |
|---|---|---|---|
| `dev-1`, 3-node Keeper 3×(0.25, 0.5) | ≈ $17 + $2.4 | 1×(1 vCPU, 4 GiB) ≈ $28 | ≈ 70% |
| `dev-1`, **single-node** Keeper (recommended) | ≈ $6 + $0.8 | ≈ $28 | **≈ 24%** |
| `s-2` | ≈ $35 + $2.4 | 2×(2, 8) ≈ $113 | **≈ 33%** |
| `s-4` | ≈ $35 + $2.4 | 2×(4, 16) ≈ $225 | ≈ 17% |
| `s-8` | ≈ $35 + $2.4 | 2×(8, 32) ≈ $451 | ≈ 8% |
| dedicated 3×r7g.4xlarge (≈ $0.86/h each) | ≈ $37 | ≈ $1,880 | ≈ 2% |

**Per-tenant Keeper is expensive at the bottom of the range.** Options, in the order I'd
recommend (details in [ADR-0004](adr/0004-keeper-topology.md)):

1. **(Recommended)** Keep a per-tenant 3-node Keeper for every HA size, as the prompt
   requires, and price it in. For `dev-1` only, use a **single-node Keeper**: no HA was
   promised, and this costs about $7/month. Bin-pack Keeper pods densely on the control
   pool, with CPU requests of 0.25 and limits of 0.5 (CPU burst only; memory stays
   requests = limits). This is the one place I'd allow CPU burst.
2. Embedded Keeper inside the ClickHouse server pods (`keeper_server` in the server
   process). It's cheaper, but the server pods run tenant SQL, so a crashing query takes a
   Keeper voter with it. I'd reject this for HA sizes.
3. A shared, multi-tenant Keeper with per-tenant chroot and ACL. The prompt forbids this
   for v1, and I agree.

---

## 8. Placement (enforced, not just configured)

### 8.1 Pools and labels

| Pool | Label | Taint | Who schedules there |
|---|---|---|---|
| control | `basalt.io/pool=control` | `basalt.io/pool=control:NoSchedule` | Keeper, operator, gateway, agent, auditor |
| shared | `basalt.io/pool=shared` | `basalt.io/pool=shared:NoSchedule` | Shared-tier server pods (gVisor) |
| dedicated | `basalt.io/pool=dedicated`, `basalt.io/dedicated-cluster=<id>` | `basalt.io/dedicated-cluster=<id>:NoSchedule` | Server pods of cluster `<id>` only |

Every pool is tainted, so nothing lands anywhere by accident. Node-level DaemonSets (CNI,
CSI, log shipper, node exporter, gVisor installer) carry explicit tolerations and are
allowlisted in admission by **namespace + ServiceAccount**. Admission is never relaxed for
"all DaemonSets".

Node pools are **Karpenter NodePools** on AWS: one `shared` pool, and one per dedicated
cluster with `limits` equal to the node count plus surge. Consolidation is
`WhenEmpty` only, with disruption budgets, and drift replacement goes through our rotation
workflow (§13.5) instead of Karpenter's default. On `kind`, the "pools" are labelled and
tainted kind nodes.

### 8.2 Rules rendered into every server pod

**Shared tier:**

```yaml
runtimeClassName: gvisor                          # ADR-0003
nodeSelector: {basalt.io/pool: shared}
tolerations: [{key: basalt.io/pool, value: shared, effect: NoSchedule}]
affinity:
  podAntiAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:     # R1: replicas of one shard never share a node
    - labelSelector: {matchLabels: {basalt.io/cluster: <id>, basalt.io/shard: "0"}}
      topologyKey: kubernetes.io/hostname
    preferredDuringSchedulingIgnoredDuringExecution:    # "never two pods of one cluster where avoidable"
    - weight: 100
      podAffinityTerm:
        labelSelector: {matchLabels: {basalt.io/cluster: <id>}}
        topologyKey: kubernetes.io/hostname
topologySpreadConstraints:                              # R1b: zone spread (required for HA sizes)
- maxSkew: 1
  minDomains: 3          # forces Karpenter to provision a 3rd zone instead of settling for 2
  topologyKey: topology.kubernetes.io/zone
  whenUnsatisfiable: DoNotSchedule
  labelSelector: {matchLabels: {basalt.io/cluster: <id>, basalt.io/shard: "0"}}
```

A 2-replica shard in 3 zones uses 2 distinct zones. "Spread across 3 zones" means
*maxSkew 1 over 3 eligible zones*. With 3 or more replicas, every zone gets one.

**Dedicated tier** adds exclusivity. There is no `runtimeClassName`; it uses the runc
hardened baseline.

```yaml
nodeSelector: {basalt.io/pool: dedicated, basalt.io/dedicated-cluster: <id>}
tolerations: [{key: basalt.io/dedicated-cluster, value: <id>, effect: NoSchedule}]
affinity:
  podAntiAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
    - labelSelector: {matchLabels: {basalt.io/component: clickhouse-server}}   # R2: against ALL CH servers
      namespaceSelector: {}                                                    # all namespaces
      topologyKey: kubernetes.io/hostname
```

**R3 (shared pool exclusion)** follows from the taints: shared pods don't tolerate
`dedicated-cluster=*` and select `pool=shared`.

**R2b (node count equals server count).** The dedicated NodePool's `limits` and our
reconciler keep `nodes == servers + surge`. During rotation `surge = 1`. The auditor flags
idle dedicated nodes older than 30 minutes, since they cost money and suggest a stuck
rotation.

**M2 implementation notes** (`internal/engine/clickhouse/placement.go`):
- One pod template serves every shard, so per-shard rules (R1, R1b) select the cluster's pods
  and use `matchLabelKeys: [clickhouse.altinity.com/shard]` instead of a fixed shard value
  (Kubernetes ≥ 1.31).
- Dedicated pods carry both the per-shard term and the exclusivity term.
- On local cells, dedicated node pools are emulated by the agent: it claims pre-created
  `basalt.io/pool=free` nodes across zones, all or nothing, and returns them only once no
  tenant pods remain (`internal/agent/nodepool.go`). Cloud cells render Karpenter NodePools
  from the same `NodePoolRequest`.

### 8.3 Admission (R4)

A **ValidatingAdmissionPolicy** (CEL, in-process, so it doesn't need a webhook to stay
available) bound to pods with label `basalt.io/component=clickhouse-server`:

* tier `shared` ⇒ `spec.runtimeClassName == 'gvisor'`, `nodeSelector['basalt.io/pool'] == 'shared'`,
  and no toleration for `basalt.io/dedicated-cluster`;
* tier `dedicated` ⇒ `nodeSelector['basalt.io/dedicated-cluster'] == labels['basalt.io/cluster']`,
  the toleration value equals the cluster's own id, and a required anti-affinity term exists
  with `namespaceSelector == {}` and a selector on `component=clickhouse-server`;
* all ⇒ `automountServiceAccountToken == false`, `runAsNonRoot`, seccomp `RuntimeDefault`
  (or a profile of ours), no `hostNetwork/hostPID/hostIPC/hostPath`, no privileged, no
  added capabilities, and requests == limits for cpu and memory;
* the shard anti-affinity term exists for the pod's own `cluster`/`shard` labels.

Companion policies (rev 2 corrects #2: rev 1 keyed it on the operator's ServiceAccount, but
**pods are created by the StatefulSet controller, not by the operator**):

1. **Taint toleration is fenced.** Any pod that tolerates *any* `basalt.io/pool` or
   `basalt.io/dedicated-cluster` taint must be one of: a labelled CH server or Keeper of the
   matching cluster; the optimizer Job (`component=optimizer`, control pool only, §17);
   a platform component in `basalt-system`; or an allowlisted DaemonSet (namespace +
   ServiceAccount). This stops an unlabelled pod from reaching a shared or dedicated node.
2. **Labels are integrity-protected at the level that creates them.**
   * `StatefulSet`s carrying `basalt.io/*` labels may be created or updated only by the
     operator's ServiceAccount (`request.userInfo.username`).
   * Pods carrying `basalt.io/component in (clickhouse-server, keeper)` must be created by
     `system:serviceaccount:kube-system:statefulset-controller`, and carry an ownerReference
     to a StatefulSet. (Verify on EKS that kube-controller-manager uses per-controller
     ServiceAccounts; otherwise key on the ownerReference plus a
     `ValidatingAdmissionPolicy` on the StatefulSet template.)
   * The operator ServiceAccount itself may not create bare pods.

   Without this, a pod could evade the placement policy simply by omitting the label.
3. **No direct node binding (M2).** `spec.nodeName` set at creation skips the scheduler and
   therefore every taint. Only kubelets (static pods) and the volume provisioner may do it.
4. **Node-level infrastructure** (DaemonSets, and on local cells the local-path provisioner's
   helper pods) may tolerate every taint. Both the policy and the auditor allowlist exactly
   these.

Implemented in `deploy/cell/policies/placement.yaml` as four policies. The sandbox name is a
parameter. Every rule has a negative test in `test/e2e/m2.sh`.

Tests: one negative test per rule (a hand-crafted pod is rejected) and one positive test
(rendered pods are admitted).

### 8.4 Continuous audit (R5)

`basalt-placement-audit` reads pods and nodes (informers) and evaluates the *actual*
state, independent of what was rendered:

* two replicas of one shard on one node (V1); replicas of an HA cluster not spread to the
  required zone count (V1b);
* any pod on a dedicated node that isn't that cluster's server or an allowlisted
  DaemonSet (V2), and more than one CH server on a dedicated node (V2b);
* a shared CH pod without `runtimeClassName: gvisor`, or on a non-shared node (V3);
* dedicated node count ≠ servers + allowed surge for more than 30 minutes (V4).

It exports `basalt_placement_violations{rule,cluster,node}` and an alert fires on any value
above 0 for 2 minutes. The same code ships as a CLI, `basalt-audit --kubeconfig … -o json`,
which the M2 e2e tests call after each scenario.

### 8.5 Disruption

* **PDB per shard:** `maxUnavailable: 1`, selector `cluster=<id>,shard=<n>`. For
  single-replica (dev) clusters, the PDB is `maxUnavailable: 1` and the docs warn about it,
  because blocking node drains forever is worse than the outage we already told the
  customer about.
* **Keeper PDB:** `maxUnavailable: 1`.
* **Rolling gates.** Before taking the next replica down, the workflow requires, for every
  replica of the shard: `system.replicas.absolute_delay < 10s`, `queue_size` below a
  threshold, `is_readonly = 0`, and Keeper `mntr` showing a leader plus quorum followers in
  sync (`zk_synced_followers`). These gates run as workflow activities via the cell agent,
  which executes a read-only `provider_ops` query.

The Altinity operator does its own rolling restarts, and its built-in waits are not ours.
To control the pace we either (a) drive the restart one host at a time by flipping
per-host `templates` / `stop` fields, or (b) use the operator's reconcile-wait settings.
**Spike S1 in M2** decides which, and is part of the operator evaluation (ADR-0002).

---

## 9. Threat model

### 9.1 Assets and adversaries

* **Assets:** other tenants' data, cloud credentials (IAM role and instance metadata), the
  provider's `provider_ops` credentials, per-tenant Keeper, backups, the control plane,
  and node integrity.
* **Primary adversary:** an authenticated tenant with `customer_admin`, running **arbitrary
  SQL**, on a shared node next to other tenants. Assume they find a ClickHouse memory-safety
  bug and get **code execution as the server process**. The sandbox and the network policy
  are the walls that must hold *after* that happens.
* **Secondary adversaries:** an internet attacker on public endpoints; a compromised node
  DaemonSet; a malicious or compromised operator image (supply chain; images pinned by digest).

### 9.2 SQL attack surface and controls

| Surface | Examples | Control | Test (M3) |
|---|---|---|---|
| Outbound network from SQL | `url()`, `s3()`, `remote()`, `remoteSecure()`, `mysql()`, `postgresql()`, `mongodb()`, `redis()`, `azureBlobStorage()`, `hdfs()`, `iceberg*()`, `deltaLake()`, `hudi()`, dictionary `SOURCE(HTTP/MYSQL/…)`, table engines `URL/S3/Kafka/RabbitMQ/NATS/MySQL/PostgreSQL/…`, `input_format_*_schema` URLs. **Also `EXPLAIN` of any of these:** tested, EXPLAIN performs schema-inference requests. | 1) **Default-deny egress NetworkPolicy**: server pods reach only their Keeper, replicas, DNS, and the S3 VPC endpoint for backups. 2) `remote_url_allow_hosts` matches **no host**, plus customer opt-ins. (M3 changed this from "the cluster's own hosts": HTTP, MySQL, PostgreSQL, Redis, MongoDB, Cassandra and remote ClickHouse dictionary sources are not checked against `READ ON <source>` grants upstream, so the allowlist is their only SQL-level control, and nothing needed the own hosts.) 3) **No `READ/WRITE ON <source>` grants** (tested: `url()` → `READ ON URL` denied, `s3()` → `READ ON S3` denied), with `enable_read_write_grants = true`. 4) Separate deny rules for IMDS `169.254.169.254/32`, `fd00:ec2::254`, the link-local range (which includes the EKS Pod Identity agent at `169.254.170.23`), pod CIDR, service CIDR, and the VPC CIDR, layered under the allowlist so an opt-in can't re-open them. 5) IMDSv2 hop limit 1 on nodes; **no ambient cloud credentials in server pods at all** (§12). | SSRF to IMDS, the K8s API, another tenant's service, the control plane, the gateway's internal port, and DNS rebinding via `url()`; each must fail closed. |
| Opt-in egress | Customer wants `s3()` against their own bucket, or `postgresql()` against their DB | v1: per-cluster **egress allowlist** of FQDNs and ports, set via the API, enforced by **both** `remote_url_allow_hosts` and an L7 egress proxy (Envoy forward proxy, SNI/Host allowlist; NetworkPolicy can't do FQDNs). The opt-in grants specific `READ`/`WRITE ON <source>` (e.g. `READ ON S3`, `READ ON POSTGRES`). Private ranges stay blocked even if the FQDN resolves into them. | Opt-in to `a.example.com`, then try `b.example.com`, and a DNS name resolving to `10.x`; both must fail. |
| Filesystem from SQL | `file()`, `File` engine, `INTO OUTFILE` (client-side only), `user_files_path`, `format_schema_path`, `FROM INFILE` (client), `SYSTEM … ` reading paths, dictionaries `SOURCE(FILE)`, `filesystem*` functions | No `READ/WRITE ON FILE` grant. `user_files_path` points to an empty dir (tmpfs, no secrets); tested: `file('/etc/passwd')` → "is not inside user_files_path". Config, certs and secrets are mounted at paths outside it, readable only by the clickhouse uid (0400), and **not** under any path a function can address. Format schemas are API-managed and stored in a separate read-only dir. | `file('/etc/clickhouse-server/config.xml')`, `../` traversal, symlink tricks, `/proc/self/environ`, `/var/run/secrets/…` (no token is mounted anyway). |
| Code execution | `executable()` table function, `Executable`/`ExecutablePool` engines, executable dictionaries, executable UDFs (`user_defined_executable_functions_config`) | **`TABLE ENGINE ON Executable/ExecutablePool` not granted** (tested: `executable()` denied for that reason). Defence in depth: `user_scripts_path` points to an empty read-only dir, and `user_defined_executable_functions_config` is unset. Executable dictionary sources are disabled. **There's no upload path into the pod filesystem.** SQL UDFs (`CREATE FUNCTION f AS x -> …`) are allowed: they're expressions, not code. | `executable('sh', …)`, executable dictionary DDL, attempts to write a script via `INSERT INTO FUNCTION file(...)`. |
| Privilege escalation | `GRANT` beyond own grants, editing provider users, `ALTER USER provider_ops`, `SET` past limits, **a new user created without `customer_admin`** | See §9.3 and §9.4. Provider users live in **read-only `users_xml` storage** (tested: `ACCESS_STORAGE_READONLY`). Customer users live in `replicated` access storage. **Tenant limits live in the server `default` profile, not on the role.** Tested: role-attached constraints were escaped by a role-less user (`max_memory_usage=0` and `max_threads=64` accepted); default-profile constraints were not. | Try to `ALTER USER provider_ops`, `GRANT` beyond the role, create a role-less user or one with its own profile, then exceed limits. Already scripted in `test/lab/phase0_verify.sh`. |
| Cross-tenant | Any path to another tenant's cluster | There's no shared process, Keeper or storage. Separate namespaces, NetworkPolicy, per-cluster certs, and per-cluster IAM scope for backups. | From tenant A: connect to B's service IP, B's Keeper, B's backup prefix. |
| DoS of neighbours | Memory, CPU, disk I/O, inode exhaustion, `system.*` log growth | §10. | M4 and M6 noisy-neighbour suite. |
| ClickHouse server RCE (bug) | Arbitrary code in the server process | gVisor on shared nodes, non-root, seccomp, no SA token, read-only root filesystem except data and tmp volumes, egress deny. | Container escape probes under gVisor (published CVE classes), as a best effort. |

### 9.3 `customer_admin` grants (initial; M3 verifies each with a test)

M3 additions (details in [m3-report.md](m3-report.md)): the cluster has an operator-generated
interserver secret, so `Distributed` queries reach the other replicas as the issuing user
instead of `default`. The stock `default` user is an access-management admin in the image's
`users.xml`, and in-process logins (local ClickHouse dictionary sources) skip its address
check. Basalt removes its admin rights, limits it to localhost, and grants it only
`SELECT ON system.one`. `provider_ops` authenticates only with a client certificate signed
by the cell CA.

Server config (`access_control_improvements`, tested on 26.8.34): `table_engines_require_grant = true`,
`enable_read_write_grants = true`, `settings_constraints_replace_previous = true`,
`on_cluster_queries_require_cluster_grant = true`, `select_from_system_db_requires_grant = true`.

**Granted:** `SELECT ON *.*` **with partial revokes** (tested: without them a tenant reads
`system.zookeeper`): `REVOKE SELECT ON system.zookeeper, system.zookeeper_connection,
system.zookeeper_log, _basalt.*`. Also `INSERT, ALTER, CREATE DATABASE` (without an explicit
engine, see ADR-0007), `CREATE TABLE, CREATE VIEW, CREATE DICTIONARY, CREATE FUNCTION, DROP,
TRUNCATE, OPTIMIZE, SHOW`, `dictGet` (**not** `KILL QUERY`: M3 showed it lets its holder kill any
user's queries, the platform's included; every user can kill its own queries without it), `CLUSTER` (needed for
`ON CLUSTER` DDL in tenant-created Atomic databases; `remote_servers` contains only their own
cluster). `TABLE ENGINE ON` the replicated MergeTree family, `Distributed`, `MaterializedView`,
`View`, `Null`, `Memory`, `Set`, `Join`, `Buffer`, `Dictionary` and `Merge` (ADR-0007).
`introspection` is **denied** (`addressToSymbol` etc. leak memory layout). `ACCESS MANAGEMENT`
is limited to `CREATE/ALTER/DROP USER, ROLE, ROW POLICY, QUOTA` **with GRANT OPTION only for
the grants above**. Plus `SYSTEM FLUSH LOGS`, `SYSTEM SYNC REPLICA`, `SYSTEM DROP QUERY CACHE`,
`SYSTEM STOP/START FETCHES/MERGES/TTL MERGES/MOVES` **on their own tables**, `SYSTEM RESTART
REPLICA` on their own tables, and `SYSTEM WAIT LOADING PARTS`.

**Never granted:**
* `READ`/`WRITE ON <source>` (URL, S3, FILE, REMOTE, MYSQL, POSTGRES, …), except per opt-in.
* `TABLE ENGINE ON` `MergeTree` (non-replicated), `Log`-family, `Executable`,
  `ExecutablePool`, `File`, `URL`, `S3`, or any integration engine except per opt-in.
* `SYSTEM SHUTDOWN/KILL`, `SYSTEM RELOAD CONFIG/USERS/DICTIONARIES-config`,
  `SYSTEM DROP DNS/MARK/UNCOMPRESSED/MMAP CACHE` (they hurt their own performance, not
  others', but are noisy), `SYSTEM DROP REPLICA` (it can delete Keeper metadata of live
  replicas), `SYSTEM RESTORE REPLICA`, `SYSTEM UNFREEZE`, `SYSTEM FREEZE`/`ALTER … FREEZE`
  (shadow dir disk fill), `SYSTEM … DISTRIBUTED SENDS` (allowed later if needed),
  `SYSTEM ENABLE/DISABLE FAILPOINT`.
* `CREATE/ALTER SETTINGS PROFILE`. Profiles are API-managed. (Tested: a tenant profile can't
  set values outside the default-profile constraints anyway; this is tidiness, not the control.)
* `NAMED COLLECTION` / `NAMED COLLECTION ADMIN`. Tested: tenants can't use or list the
  provider's backup collection (§12).
* `BACKUP`. Backups are a platform operation, and `BACKUP TO Disk/S3` would otherwise be
  an exfil/SSRF path. Tested: denied.
* Anything on `_basalt.*` (the canary DB) or `system.zookeeper*` (Keeper paths): partial
  revokes as above.

The grants that were tested are listed in `test/lab/phase0_verify.sh`. The full list becomes
the M3 attack suite.

### 9.4 Settings constraints

Customers can't change server config. Changes to user-level settings go through the
**allowlist** (API-validated).

**Where constraints live (rev 2, tested).** Tenant limits and constraints are rendered into the
server's **`default` profile in `users.xml`** (the server's `default_profile`). That profile
applies to every user, including users the tenant creates via SQL **without** any role or
profile. Rev 1 attached them via `customer_admin`'s profile; testing showed a role-less user
created by the tenant escaped them completely. Provider users (`provider_ops`, `basalt_canary`,
`basalt_optimizer`) get their own `users.xml` profiles. These explicitly relax only what they
need with `changeable_in_readonly` (constraints from the default profile still apply to them
otherwise, as observed in the lab).

Constraints in the `default` profile (values per size, `tenant_<size>`):

* `max_memory_usage`, `max_memory_usage_for_user`: `<max>` per size; `<min>` > 0 so `0`
  (unlimited) isn't possible;
* `max_threads`, `max_execution_time`, `max_bytes_before_external_group_by`/`_sort`,
  `max_result_rows`, `max_partitions_per_insert_block`, `max_insert_threads`,
  `max_concurrent_queries_for_user`: bounded;
* `insert_quorum`: `<min>` = 0, `<max>` = replicas. Customers may *lower* durability for
  throughput; the docs say what that means;
* `readonly`, `allow_ddl`, `allow_introspection_functions`, `log_queries`,
  `log_query_threads`, `send_logs_level`, `allow_experimental_*`/`allow_suspicious_*`,
  `async_insert_*` bounds: `const` or `changeable_in_readonly` as appropriate;
* `settings_constraints_replace_previous = true` (server), so a *provider* profile can
  replace a default-profile constraint for provider users. Tenants can't use this to weaken
  limits: tested, creating a tenant profile with `max_threads = 64 WRITABLE` is rejected by
  the creator's own constraints.
* `database_replicated_allow_only_replicated_engine = 1` `const` (ADR-0007). Tested:
  `Replicated` database DDL uses the server default profile value even when a session
  overrides it.
* `async_insert`, `wait_for_async_insert`: defaults pinned explicitly (both 1 on 26.8);
  `wait_for_async_insert` `const` = 1 on shared tier.

**Proven in the lab** (`test/lab/phase0_verify.sh`): a tenant-created role-less user can't set
`max_threads = 64` or `max_memory_usage = 0`, and per-query `SETTINGS max_memory_usage = 0` is
rejected. M3 extends this to every constrained setting.

### 9.5 Baseline controls (all tiers)

* **TLS always.** Plain 8123/9000 aren't exposed: those ports are bound to localhost for
  probes, or disabled. Interserver traffic uses TLS 9010, with per-cluster mTLS certs from
  a per-cell cert-manager CA issuer, and Keeper uses TLS 9281 (raft over TLS as well).
* Non-root uid 101, `readOnlyRootFilesystem`, all capabilities dropped, seccomp
  `RuntimeDefault`, `automountServiceAccountToken: false`, no `hostPath`.
* Encryption at rest: EBS encrypted with a per-cell KMS key, and S3 SSE-KMS per tenant
  prefix. A per-cluster KMS key costs $1 per key per month, so I'd leave it as an option for
  dedicated, not the default (Q7).
* NetworkPolicy: default-deny ingress and egress per namespace, plus explicit allows.
  The CNI must enforce NetworkPolicy. On EKS that's VPC CNI's network policy agent, or
  Cilium. **Cilium is preferred** for FQDN egress and bandwidth management
  (ADR-0001), *but Cilium with gVisor needs testing* (Q10).
* Control-plane audit log for every API mutation and every cell-agent apply. EKS audit
  logs go to CloudWatch.

---

## 10. Noisy-neighbour and stability controls

| Area | Control | Verified by |
|---|---|---|
| Memory | Guaranteed QoS (requests = limits). **`max_server_memory_usage` as explicit bytes (85% of the pod)**: the M2 spike showed ClickHouse can't read its cgroup under gVisor and sizes itself to the node, so `max_server_memory_usage_to_ram_ratio` (the earlier plan) is unsafe there and is not used. `max_memory_usage` per query and `max_memory_usage_for_user` per size. `memory_worker` / page-cache settings checked. | M4: a runaway `GROUP BY` on high-cardinality data returns `MEMORY_LIMIT_EXCEEDED` (code 241); the pod's `restartCount` doesn't increase and there are no `OOMKilled` events. |
| CPU | Integer CPU limits; `max_threads` and `max_concurrent_queries` per size; background pool sizes scaled to vCPU. Dedicated: CPU manager `static` policy (exclusive cores). | M4/M6: CFS throttling (`container_cpu_cfs_throttled_periods_total`) vs. p99 latency at several `max_threads` values. |
| Disk I/O | gp3 with per-volume IOPS and throughput (isolated per volume, by EBS design). No local disks in shared. The instance's EBS bandwidth is a *shared* node-level resource, so we cap volumes per node by instance EBS bandwidth during placement (`PlanPlacement` accounts for it). ClickHouse **workload scheduling** (`CREATE RESOURCE … (READ DISK, WRITE DISK)`, `CREATE WORKLOAD`) is evaluated for prioritising foreground reads over merges *within* a tenant. | M6 noisy-neighbour benchmark. |
| Network | Per-pod egress bandwidth annotation (`kubernetes.io/egress-bandwidth`) via Cilium Bandwidth Manager. **Ingress shaping isn't solved by this.** Replication fetches into a pod are bounded by `max_replicated_fetches_network_bandwidth_for_server` / `max_replicated_sends_network_bandwidth_for_server`. | M6. |
| Background work | `background_pool_size`, `background_merges_mutations_concurrency_ratio`, `background_fetches_pool_size`, `background_schedule_pool_size` per size. Alerts: `max(parts per partition) > 0.7 × parts_to_throw_insert`, merge queue age, mutations stuck > 1h. | M4 insert storm: many tiny inserts produce `TOO_MANY_PARTS` errors or throttle via `parts_to_delay_insert`, and neighbours are unaffected. |
| Small inserts | **Async inserts are on by default in 26.8** (tested: `async_insert = 1`, `wait_for_async_insert = 1`). We pin both in the default profile, with `wait_for_async_insert` `const` on shared tier, so clients can't get an ack before the flush. We document `async_insert_busy_timeout_ms` and batching for small-insert clients. | M4: acks happen only after the flush. M5: durability with `insert_quorum` under async inserts. |
| Quotas | One **platform-owned quota** (`basalt_default`, M4) shared by all of a tenant's users (not keyed per user, so creating users does not multiply it), assigned `TO ALL EXCEPT` the platform's users: queries, errors, result/read rows, read bytes, execution time and written bytes per hour, scaled by the size. Tenants hold no `CREATE/ALTER/DROP QUOTA` (quotas cannot be scoped, so the right to alter one is the right to remove ours). Usage is counted **per server**, so a user can make up to one limit per replica. The values are backstops against runaway clients, not billing. Limits on table count (`max_table_num_to_throw`), database count (`max_database_num_to_throw`), partitions (`max_partitions_per_insert_block`, plus an alert on total partitions) and parts (`max_parts_in_total`). | M4. |
| System tables | Every `*_log` gets a TTL (query_log 14d; trace/metric/asynchronous_metric/text/processors_profile 3d; part_log and most others 7d; session_log, crash_log and backup_log 30d), as a `ttl` setting, or inside the `engine` for the four tables whose engine the operator or the stock config fixes (`query_log`, `part_log`, `trace_log`: the operator gives them a 30-day TTL; `opentelemetry_span_log`). The operator also removes `query_thread_log`. `session_log` is enabled. `max_size_rows` and `reserved_size_rows` bound the in-memory flush buffer, not the table; the TTL is the size bound. `trace_log` and the query profiler are sampled down on shared. The list of log tables is generated from `system.tables WHERE database='system' AND name LIKE '%_log'` at version-qualification time, so new log tables in a release fail CI if they lack a TTL. | M4: 24h soak, size bounded. |
| Startup | The operator's liveness probe kills a server that needs more than ~90 s to open its port; a loaded node, gVisor or many tables exceed that, and the kill restarts the whole load (M4: observed twice). The server container has a 10-minute **startup probe** (5 s x 120) and a `wait-for-keeper` init container (F-5). | M4: restart a replica during a Keeper outage: no crash loop, serving 75 s after quorum returns. |
| Disk full | Alerts at 70% (warn) and 85% (page). **Auto-expand** in 25% steps up to the size's max. Note: an **EBS volume can only be modified about once every 6 hours**, so auto-expand must start early and can't be relied on in a fast fill. Reserved space: `keep_free_space_bytes` on the default disk (5% or ≥ 2 GiB) plus a **2 GiB balloon file** operators delete in an emergency. Runbook covers stopping merges, dropping detached and shadow dirs, expanding, and `SYSTEM START MERGES`. | M4 and M6 fill test. |

---

## 11. Replication, Keeper, sharding

### 11.1 Engine

`ReplicatedMergeTree` family with Keeper, with each replica keeping its own copy of the
data. **No zero-copy replication.** Optional cold tier: an S3 disk in a storage policy
(`TTL … TO VOLUME 'cold'`) where **each replica writes its own objects**
(`allow_remote_fs_zero_copy_replication = 0`, enforced as a non-overridable server
setting). This doubles cold storage cost by design.

### 11.2 Preventing silent loss of HA

See [ADR-0007](adr/0007-replication-safety.md). Tested on 26.8.34:

* **The trap is real.** With stock settings, `ENGINE = MergeTree` inside a `Replicated`
  database creates a plain, unreplicated table: no conversion, no warning.
* **Primary fence: `TABLE ENGINE` grants.** A tenant without `TABLE ENGINE ON MergeTree` gets
  `ACCESS_DENIED` in any database type; `ReplicatedMergeTree` works.
* **Second layer:** `database_replicated_allow_only_replicated_engine = 1` (`const`) in the
  server default profile. `Replicated`-database DDL then rejects non-replicated engines even
  if a session overrides the setting.
* **Databases.** Tenant `CREATE DATABASE` (no engine) produces **Atomic**. Database engines
  can't be granted via `TABLE ENGINE`, so tenants can't create `Replicated` databases
  themselves. The provider creates `default` as `Replicated`, and the API offers
  `POST …/databases` for more. In Atomic databases tenants use `ON CLUSTER` DDL with
  `ReplicatedMergeTree`.

Plain `MergeTree` attempts are **rejected**, not silently converted. For tables that existed before the policy (restores from external
backups, say), the auditor reports `basalt_nonreplicated_tables` and the cluster shows a
warning condition.

### 11.3 Keeper quorum loss

Losing 2 of 3 Keepers makes all replicated tables read-only. `INSERT` fails with
`TABLE_IS_READ_ONLY`, while `SELECT` keeps working. Detection: `system.replicas.is_readonly`
on all replicas, plus the Keeper `mntr` state. Cluster state becomes **`DEGRADED`** with
reason `KEEPER_QUORUM_LOST`. Readiness must **not** fail on read-only status, or reads go
down too (§11.5). M4/M6 tests: kill 2 Keeper pods and assert inserts fail, selects work,
the state is DEGRADED, and inserts recover after the quorum returns.

### 11.4 `ON CLUSTER` DDL

The Altinity operator renders a `remote_servers` cluster named after the CHI. We render a
stable alias `default` so `ON CLUSTER default` works. Inside `Replicated` databases, DDL
is replicated by the database itself and `ON CLUSTER` is unnecessary; the docs explain both.
The `distributed_ddl` queue path is per-tenant Keeper, so there's no cross-tenant
interference.

### 11.5 Readiness vs. replica lag

The `/replicas_status` endpoint returns an error when a replica is lagging or read-only. If
readiness failed on that alone, Keeper loss would take *every* replica out of the Service
and turn a write outage into a full outage. So readiness = `/ping` **and** a small sidecar
check that fails *only* when `absolute_delay > threshold` **and** at least one other replica of
the same shard is healthy. A replica that's still catching up after being added uses a separate
gate (§13.3).

---

## 12. Backup and restore

See [ADR-0006](adr/0006-backup.md). Summary:

* **Native `BACKUP … TO S3(…)`**, driven by our workflow (not `clickhouse-backup`). Full
  backups weekly, incremental daily (`base_backup`), and on-demand. Retention is enforced by
  the control plane, which deletes whole backup chains when no other backup depends on them.
  S3 lifecycle rules are only a safety net.
* **Layout:** `s3://basalt-backups-<cell>/<org>/<cluster>/<backup_id>/`.
* **Credentials (rev 2).** Rev 1 attached an IAM role to the server pods via EKS Pod
  Identity. That contradicted the baseline: Pod Identity needs a projected ServiceAccount
  token and the link-local agent at `169.254.170.23`, both of which §9.5 and the egress
  policy forbid. It would also have put cloud credentials within reach of tenant SQL. Instead:
  **server pods have no ambient cloud credentials.** For each backup or restore, the workflow:
  1. mints STS credentials (`AssumeRole`, duration ≈ backup timeout, **session policy scoped
     to that cluster's prefix**);
  2. writes them into a provider-only named collection
     (`CREATE OR REPLACE NAMED COLLECTION basalt_backup AS url=…, access_key_id=…, secret_access_key=…, session_token=…`);
  3. runs `BACKUP … TO S3(basalt_backup, '<id>')`;
  4. drops the collection.

  Tested on 26.8: the session token is sent (`x-amz-security-token`). Tenants can't use the
  collection (`NAMED COLLECTION ON basalt_backup` denied), can't list it, and can't `BACKUP`.
  Secrets in `S3(...)` arguments appear as `[HIDDEN]` in `query_log`. The positional
  `S3(url, key, secret)` form has no session-token slot, hence the named collection. The
  bucket policy only allows the VPC endpoint, and SSE-KMS is on. This resolves rev 1's Q8.
* **Multi-shard consistency (honest):** `BACKUP … ON CLUSTER` snapshots each shard at
  slightly different moments, and inserts in flight may appear on one shard and not
  another. There's no cross-shard snapshot. The docs say so, and for strict consistency
  customers pause writes (we offer `pauseIngest` via a quota) during the backup window.
* **Verification:** a daily Temporal schedule restores a sampled backup (each cluster at
  least monthly; dedicated at least weekly) into a scratch cluster on the shared pool,
  compares per-table `count()` and `sum(cityHash64(*))` on a partition sample against
  values recorded at backup time, records `restore_verifications`, and deletes the scratch
  cluster. A backup isn't marked `VERIFIED` until that passes. Failures page.

---

## 13. Lifecycle operations

All of these are workflows built from `PlanChange` steps with gates.

1. **Vertical scale / upgrade.** Rolling, one replica per shard, with the gates from §8.5.
   Upgrades: first pin `compatibility = <old version>` in the profile, roll the binary,
   run canary queries (provider canary plus a sample of the tenant's own recent
   `query_log` SELECTs, normalised, with `max_execution_time` and `readonly=1`, at low
   rate, opt-out allowed), soak for 24h, then raise `compatibility`. **Downgrade is
   possible until new-format data is written**: practically, until the first merge after
   the new binary runs or `compatibility` is raised. Some releases (26.3 String
   serialization, if confirmed) are one-way earlier. The API shows `downgradeable: false`
   once that point passes.
2. **Version channels:** `lts` (default; 26.8 now), `stable` (latest monthly, minus 2
   weeks of soak), `pinned`. Each image is qualified in CI against the security suite
   (M3) and the system-log TTL check before it's offered.
3. **Add replica.** Create the replica. It gets the `basalt.io/serving=false` label, so the
   client Service excludes it. Wait until `system.replicas.queue_size == 0`,
   `absolute_delay < 10s` and part counts match the leader, *then* set `serving=true`.
4. **Add shard.** Create the shard and add it to `remote_servers`. `Distributed` tables pick
   it up; weights are customer-set. Existing data doesn't move. The **rebalance helper**
   (customer-run, throttled) does `INSERT INTO dist SELECT … FROM old_shard_local WHERE
   partition IN (…)` followed by `ALTER TABLE … DROP PARTITION` on the source. That's
   per partition, idempotent by partition ID, and **not atomic**: queries during a move can
   double-count, and the docs say so. **Remove shard** is a non-goal: there's no safe
   automatic method, because data has to be drained with the same helper and every
   `Distributed` table and MV has to stop referencing the shard. The helper makes it
   *possible* manually; we don't offer it as an API operation in v1.
5. **Node rotation.** Cordon, then `surge` a node, move one replica (EBS: detach and
   reattach in the same AZ, which is fast; NVMe: new replica, full sync, then remove old),
   and gate on caught-up before the next node. Karpenter drift is disabled for these pools
   and the workflow handles rotation instead.
6. **Insert durability (for M5).** The no-acknowledged-insert-lost guarantee holds with
   `insert_quorum = 'auto'` (majority), `insert_quorum_parallel = 1`, and
   `insert_quorum_timeout` large enough, with `async_insert = 0` or `wait_for_async_insert = 1`.
   With the ClickHouse default `insert_quorum = 0`, an insert acked by one replica can be
   lost if that replica's disk is lost before the fetch. The verifier uses quorum inserts
   and checks every acked batch ID.
7. **Tier migration (shared → dedicated).** Add replicas on dedicated nodes (runc) to the
   same shard, sync, flip `serving` labels, then remove the shared (gVisor) replicas. Data
   format is identical across runtimes; the difference is only in the kernel interface. Risks:
   the interserver path has to cross pools (the NetworkPolicy allows it), and the cluster
   temporarily spans both tiers. The admission policy needs a `migrating` exception that
   expires automatically. **Spike S4 in M5**; the fallback is restore-to-new-cluster plus
   DNS cutover with a write pause.

---

## 14. Networking

See [ADR-0005](adr/0005-networking.md). Summary:

* **Ports:** HTTPS 8443 and native TLS 9440. MySQL and PostgreSQL wire protocols are off,
  as are plain 8123/9000 externally.
* **Endpoint:** `<clusterId>.<cell>.<domain>`, with per-replica endpoints
  `<clusterId>-s<N>r<M>.<cell>.<domain>` for debugging.
* **Shared tier:** one Envoy Gateway per cell, with **TLS passthrough** (`TLSRoute`) routing
  by SNI to the per-cluster Service, so tenant TLS terminates in the tenant's pod with the
  tenant's own cert, and the gateway never sees plaintext. **Dedicated tier:** a per-cluster
  NLB, or PrivateLink endpoint service.
* **Affinity:** passthrough is L4, so affinity is per TCP connection. Native protocol
  sessions and temporary tables are per-connection, which works. HTTP `session_id` needs
  the same connection (keep-alive) or the per-replica endpoint; the docs say so. If that's
  not acceptable, ADR-0005 lists the L7 alternative.
* **Lag-aware health:** handled by readiness (§11.5).

---

## 15. Observability, canary, metering

* **Metrics:** the ClickHouse built-in Prometheus endpoint (port 9363, scraped in-cell)
  plus Keeper metrics, kube-state-metrics, and the auditor. A per-cell Prometheus with
  remote-write to a regional long-term store, and every series labelled `cluster_id`.
* **Tenant access:** `GET /metrics?query=` is a PromQL proxy that injects
  `cluster_id="<c>"` server-side (label enforcement via a prom-label-proxy-style
  rewriter). `system.query_log` is queried directly; a row policy hides provider users'
  rows (`user NOT IN ('provider_ops','basalt_canary')`).
* **Canary:** every 30s per cluster, `basalt_canary` inserts a row (with quorum) into
  `_basalt.canary` and reads it back from every replica. It records insert latency, read
  latency and replication delay. Three consecutive failures turn the cluster `DEGRADED`;
  recovery is hysteretic (5 successes). This drives the SLOs (availability = canary
  success ratio).
* **Provider alerts, each with a runbook in `docs/runbooks/`:** replication lag, readonly
  replicas, parts near `parts_to_throw_insert`, merge backlog, mutations stuck, Keeper
  leader loss / outstanding requests / fsync latency, disk 70/85%, memory near cap,
  OOMKilled pods, certificate expiry, placement violations, non-replicated tables, backup
  failures and verification failures, cell agent disconnected.
* **Metering:** the cell agent emits hourly samples (size-hours per server, provisioned
  storage GB-hours, backup bytes from S3 inventory, egress bytes from the gateway and NLB
  metrics, and optimizer LLM tokens from the LLM gateway). Records are upserted with key `(cluster_id, meter, window_start)`, so
  re-sending is idempotent. Late data inside 48h is reconciled; after that it's frozen.

---

## 16. Failure modes

| # | Failure | Detection | Behaviour / mitigation | Test |
|---|---|---|---|---|
| F-1 | Server pod killed | Pod restarts; canary on that replica fails | Clients reconnect to other replicas via Service; the replica catches up from Keeper log | M6 |
| F-2 | Node terminated | Node NotReady | EBS reattaches in the same AZ (minutes); other replicas serve; PDB unaffected (involuntary) | M6 |
| F-3 | Zone outage | Several nodes NotReady in one AZ | 2-replica shards keep 1 replica; Keeper keeps 2/3 if spread across 3 AZs (required); EBS is zonal, so the pods stay Pending until the zone returns, and **no automatic cross-zone rebuild in v1** (runbook to add a replica in another zone) | M6 (simulated by tainting and killing the AZ's nodes) |
| F-4 | Disk full | Alerts at 70/85% | Inserts fail with `NOT_ENOUGH_SPACE`; reads OK; auto-expand; balloon release | M4, M6 |
| F-5 | Keeper quorum loss | `is_readonly`, `mntr` | Read-only, `DEGRADED` (M4: the worker derives it from the agents' probes within ~10 s and clears it on recovery). **M1 finding:** with replicated access storage a server cannot *start* without Keeper (exit 231). **M4 mitigation:** a `wait-for-keeper` init container holds a restarting replica until the Keeper Service has a ready endpoint (no crash loop), and a 10-minute startup probe stops the operator's liveness probe from killing it while it starts. Tested: with 2 of 3 Keeper nodes down, inserts are refused and selects work; a replica deleted during the outage waits with zero restarts and serves 75 s after quorum returns. Still open: Keeper PodDisruptionBudget and priority class (M6), and a replica that restarts during the outage stays down until quorum returns | M4, M6 |
| F-6 | Single Keeper node loss | `mntr` | Quorum intact; replace pod; PVC reattaches | M6 |
| F-7 | Too many parts | Parts alert | `parts_to_delay_insert` slows, `parts_to_throw_insert` rejects; customer guidance (batching / async) | M4 |
| F-8 | Runaway query | Memory errors | Query fails, pod survives | M4 |
| F-9 | Workflow worker crash mid-op | Temporal | Resumes from the last completed activity; activities idempotent (SSA, upserts keyed by operation) | M1 |
| F-10 | Postgres down | API 5xx | API read and write unavailable; data plane unaffected | M6 |
| F-11 | Temporal down | Workflow lag | Operations queue; data plane unaffected | M6 |
| F-12 | Control plane fully down / cell partitioned | Agent `last_seen` | Agent keeps the last state, **never deletes on disconnect**, canaries continue locally, alerts fire in-cell | M6 |
| F-13 | Operator down | Deployment unavailable | Running clusters unaffected (operator isn't in the data path); changes stall | M6 |
| F-14 | Bad config pushed | Canary / health regressions | Config changes roll per replica with gates; automatic rollback to the previous `cluster_configs` version | M5 |
| F-15 | Upgrade regression | Canary queries diverge or fail | Halt rollout; roll back if still downgradeable | M5 |
| F-16 | Cert expiry | Alert at 21 days | cert-manager renews; ClickHouse reloads certs without restart (verify) | M3 |
| F-17 | Merge backlog / mutation stuck | Alerts | Runbook; never auto-kill mutations | M4 |
| F-18 | Cell capacity exhausted | Reservation failure | Create returns `FAILED` with `CAPACITY` before anything is provisioned; scheduler picks another cell | M1 |
| F-19 | Sandbox runtime incompatibility on new release | Qualification CI | Release not offered on shared until it passes | CI |
| F-20 | Optimizer Job fails, LLM gateway down, or LLM vendor errors | Job exit code, gateway metrics | No data-plane effect; the run is reported failed; provider-side alert only | M-opt |
| F-21 | Applied skip index doesn't help or materialisation runs away | `ApplyRecommendation` verification / timeout | `KILL MUTATION` → `DROP INDEX`; recommendation `ROLLED_BACK` (ADR-0008 §8) | M-opt |
| F-22 | Optimizer EXPLAIN does real work (scalar subqueries) | Query log of `basalt_optimizer` | Bounded by its profile (60 s, 1 GiB, 2 threads, `MAX`-constrained) | lab (tested) |
| F-23 | Bad rendered server config (unknown element) | Server fails to start; this happens for a misplaced element (observed) | CI boots every rendered config variant against the target version before release; rollout gates catch the rest | M1 CI |

---

## 17. Query optimizer integration

Full design: [ADR-0008](adr/0008-query-optimizer-integration.md). Summary:

* **What it is.** `avilashs/query_optimizer@0d86d10`: Python, reads `system.query_log` and
  the catalogs, EXPLAINs the top statements, asks several LLMs for skip-index DDL and
  rewrites, has a judge LLM rank them, and optionally auto-applies ("god mode").
* **How it runs: a CronJob per opted-in cluster**, not an operator. It's rendered by the
  engine, applied by the cell agent, lives in the tenant namespace, schedules on the control
  pool, and the agent suspends it during lifecycle operations. An operator would duplicate
  the agent's reconciliation, and a cell-wide controller would put every tenant's
  credentials and query text in one process.
* **Read-only analysis.** It uses a least-privilege `basalt_optimizer` user (`users_xml`,
  `readonly=2`, `MAX`-constrained limits, **no source grants**), always with god mode off.
  It's driven by a thin wrapper that pre-checks cluster health, generates the config
  (customer databases only, literals redacted by force), round-trips the de-dup state
  through the cell agent (no PVC per cluster), and posts the report to the agent.
* **Applying is a platform workflow.** `ApplyRecommendation` holds the same per-cluster lock
  as upgrades. It re-validates the DDL with our own allow-list, gates on disk and merges,
  tracks the `MATERIALIZE` mutation to completion, verifies the marks reduction, and rolls
  back with `KILL MUTATION` then `DROP INDEX`. Tenants can opt into auto-apply with
  thresholds.
* **Why not the optimizer's own god mode** (tested on 26.8): `MATERIALIZE INDEX` returns
  before the index exists, so its verification measured nothing (`2443 → 2443`, marked
  "applied"); `DROP INDEX` is blocked while the mutation runs, so its rollback can't work;
  and EXPLAIN of an LLM rewrite using `url()` made outbound requests and scored as cost 0.0
  when run with a privileged account.
* **LLM access** goes through a provider-run per-cell **LLM gateway** (the optimizer's
  `openai_compatible` provider plus a per-cluster token). Keys stay in the gateway, budgets
  are per tenant, and tokens are metered. It's off by default and needs recorded consent
  naming the providers. Raw query text is never stored, because the optimizer's report
  contains it even with redaction on.
* **Not inline.** No query-path rewriting proxy, for the same reasons as rev 1 (latency, a
  new failure domain, parsing hostile SQL in a shared component).

---

## 18. Repository layout and milestone plan (M1 preview)

```
cmd/            api, worker, cell-agent, placement-audit
optimizer/      basalt-optimizer wrapper (Python; consumes query_optimizer@<pinned commit>), Dockerfile
internal/       platform/{api,store,workflows,capacity,cells,idempotency,secrets}
                engine/clickhouse/{render,observe,plan,validate,config,security,optimizer}
                placement/{rules,admission,audit}
api/openapi.yaml
deploy/         kind/, helm/, policies/ (VAP, NetworkPolicy, PSA), cell/
test/           lab/ (phase0_verify.sh), e2e/ (kind), attack/ (M3 suite), stability/, lifecycle/ (verifier)
docs/           design, adr/, runbooks/, security-guide.md, operator-guide.md
```

**M1 (local, kind):** Postgres, Temporal dev server, API, worker, cell agent on kind with
the Altinity operator, cert-manager and Envoy Gateway; create, connect, delete a shared
`s-2` cluster. Acceptance: `clickhouse-client --secure` connects via SNI through the
gateway, plus a test that kills the worker mid-create and checks for no duplicate
resources, operations or reservations. **Caveat:** gVisor on kind needs `runsc` installed
in the kind node image, which I'll attempt. If it doesn't work there, M1 runs shared pods
under runc with the admission rule in audit mode, and M2 enables enforcement on a node
image with runsc.

**M-opt (AI query optimizer), after M4, can run in parallel with M5.** It needs M3's grant
model and M4's stability gates. Scope: the wrapper and image, CronJob render plus the agent's
suspend logic, the LLM gateway (evaluation and deployment), report ingest and API, and the
`ApplyRecommendation` workflow with our validator. Acceptance:
1. With a mock LLM in kind, a recommended skip index is applied, the mutation is tracked to
   completion, and the marks reduction is verified.
2. A recommendation that doesn't reduce marks is rolled back, including a rollback while the
   mutation is still running.
3. An LLM rewrite using `url()`/`s3()`/`file()` causes no outbound request and no file read.
4. No raw literal appears in Postgres or the API.
5. The Job holds no provider LLM key.
6. The CronJob is suspended during an upgrade.

---

## 19. Assumptions

* A1: AWS / EKS, Kubernetes ≥ 1.33 (ValidatingAdmissionPolicy GA since 1.30; sidecar containers GA).
* A2: Karpenter for node provisioning; EBS CSI; Cilium as CNI (pending Q10).
* A3: ClickHouse **26.8 LTS** as the first supported version; 26.3 LTS also offered until EOL.
* A4: The Altinity operator (ADR-0002), pinned to `release-0.27.x`.
* A5: Temporal (MIT) self-hosted with PostgreSQL persistence; Go SDK.
* A6: Per-tenant secrets in AWS Secrets Manager, synced to cells by the agent (not External Secrets Operator: one fewer cross-plane dependency).
* A7: Pricing numbers in §7.3 are rough on-demand figures from memory and must be refreshed.
* A8: Tenants use standard clients (`clickhouse-client`, JDBC/ODBC, `clickhouse-go`, `clickhouse-connect`), all of which send SNI over TLS. **Tested for `clickhouse-client` 26.8 (native TLS).** The others are still to verify; a client without SNI can't use the shared tier.
* A9: The optimizer is consumed unmodified at a pinned commit. Upstream fixes (ADR-0008 U1–U8) are welcome but not required.

## 20. Open questions

* **Q1 – Name.** Is `Basalt` acceptable, or what is `{{PROJECT}}`? What's `{{DOMAIN}}`?
* **Q2 – Query optimizer.** *Resolved in rev 2* (found, tested, integrated as a CronJob).
  Follow-ups:
  * **Q2a:** Which LLM providers may receive tenant schema, query shapes and plans? This
    determines the gateway's upstreams and the consent text.
  * **Q2b:** Add a LICENSE file (MIT is declared but no file exists).
  * **Q2c:** Do you want me to make the upstream fixes U1–U8 in `query_optimizer`? That
    needs push access to that repo; I only have read access now.
  * **Q2d:** Pricing: is the optimizer a metered add-on (LLM tokens) or included?
* **Q3 – Kafka platform.** Do you still plan a Kafka engine on the same core? If yes, I'll keep
  the platform packages engine-neutral as designed; if no, I can simplify.
* **Q4 – dev-1 single-node Keeper** (ADR-0004 option 1). OK?
* **Q5 – Sandbox.** If gVisor benchmarks badly for ClickHouse I/O, is Kata on `*.metal`
  instances for the shared pool acceptable? It's a much larger node and has a cost floor.
* **Q6 – Restore wording.** "Backup-point restore" instead of "point-in-time-style"?
* **Q7 – Per-cluster KMS keys** for dedicated only?
* **Q8 – S3 credential exposure.** *Resolved in rev 2*: no ambient credentials in server
  pods; per-backup STS credentials go in a provider-only named collection (tested).
* **Q9 – Network.** Please allow `clickhouse.com` (docs) and container registries in the
  environment's network policy for M1.
* **Q10 – CNI.** Is Cilium acceptable as a dependency (Apache-2.0)? It interacts with gVisor
  in ways I need to test.
* **Q11 – HTTP session affinity.** Is the L4 per-connection behaviour (§14) acceptable for v1?
* **Q12 – Cost of the cloud test environment** for M6: zone outage tests and noisy-neighbour
  benchmarks need a real EKS account. Do you have one I should target?

## 21. Things in the prompt I'd change

1. "Point-in-time-style restore" → backup-point restore (no PITR in ClickHouse).
2. "Spread across 3 zones" for 2-replica shards → "maxSkew 1 across 3 eligible zones";
   2 replicas can only use 2 zones.
3. "Document async inserts": they **are on by default in 26.8** (tested), so pin and test
   them rather than just document them.
4. Single-replica dev sizes should get a single-node Keeper. A 3-node quorum protecting a
   1-replica server is wasted money.
5. Add an explicit **label-integrity** admission rule (§8.3). Otherwise placement rules
   keyed on labels can be bypassed by omitting the label.
6. Add **readiness must not fail on read-only** (§11.5). The naive "health-check against
   replica lag" turns Keeper loss into a full outage.
7. Mention the **EBS modification rate limit**: auto-expand can't respond quickly to a fast fill.
8. "Use settings profile `<constraints>`" must say **where**: in the server `default`
   profile. Constraints on a role's profile are escaped by any user the tenant creates
   without that role (tested).
9. "A default `Replicated` database engine" alone doesn't prevent plain `MergeTree`: on 26.8
   a `MergeTree` inside a `Replicated` database is silently unreplicated (tested). The fence
   is `TABLE ENGINE` grants.
10. The AI optimizer's own auto-apply must not be used against ClickHouse as it stands:
    verification and rollback don't account for asynchronous mutations (tested). Apply
    through the platform workflow instead.
