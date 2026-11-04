# ADR-0008: AI query optimizer integration

Status: Proposed (revised 2026-11-04 after the optimizer code was located and tested). Date: 2026-11-04

## Context

The optimizer is `avilashs/query_optimizer`, branch `claude/blissful-tesla-01lxws`, commit `0d86d10`. It's a Python 3.10+ package
(`qopt` CLI and library) that:

1. reads schema from system catalogs (`system.tables/columns/data_skipping_indices/projections` on ClickHouse);
2. picks the top N statements by CPU, time and memory from `system.query_log`;
3. runs `EXPLAIN indexes = 1` and `EXPLAIN ESTIMATE` (marks are used as a cost proxy);
4. asks several LLMs in parallel for a recommendation (strict JSON: rewrite, DDL, risk, confidence);
5. has a judge LLM rank the anonymised candidates, with a heuristic fallback;
6. in **god mode**, applies the winning DDL if it passes an allow-list (for ClickHouse: `ADD INDEX` skip indexes and `MATERIALIZE INDEX`), then re-measures the cost and rolls back on regression. Otherwise it reports everything as `pending_approval`;
7. notifies via console, file, webhook, Slack, Teams or email, and keeps de-duplication and an audit log in a SQLite state file.

It ships a Kubernetes **CronJob** manifest and a systemd timer. Its licence is declared MIT in `pyproject.toml`, but **the repo has no LICENSE file**.

The question was whether to run it as an **operator** or a **CronJob** on the platform.

## What testing showed (ClickHouse 26.8.34 LTS, `test/lab/phase0_verify.sh` plus `docs/evidence/2026-11-04-lab.md`)

| # | Finding | Consequence for integration |
|---|---|---|
| E1 | `ALTER … MATERIALIZE INDEX` returns in about 70 ms, while the mutation keeps running. `EXPLAIN ESTIMATE` right after reports 2,444 marks; once the mutation finishes it's 233. | The optimizer's god-mode verification on ClickHouse measures **before the index exists**. An end-to-end god-mode run reported `applied; plan cost 2443.0 -> 2443.0` for both statements, so the verification checked nothing. |
| E2 | `DROP INDEX` fails with "affected by mutation … not finished" while `MATERIALIZE` is running. | Its auto-rollback can't work during materialisation. A rollback has to `KILL MUTATION` first. |
| E3 | `EXPLAIN`, `EXPLAIN indexes=1` and `EXPLAIN ESTIMATE` of `SELECT … FROM url(...)` **make outbound HTTP requests** (schema inference). | EXPLAIN is not side-effect free. In an end-to-end run with a privileged account, an LLM-proposed rewrite using `url()` was EXPLAINed (5 requests to the target) and marked "validated with EXPLAIN". |
| E4 | `EXPLAIN ESTIMATE` of a non-MergeTree source returns no rows, so the optimizer computes cost 0.0. | That `url()` rewrite was presented to the judge as costing **0.0 against 354** for the original: a strong but false signal. |
| E5 | `EXPLAIN` executes scalar subqueries: an EXPLAIN containing `sleepEachRow` took 3 s. | EXPLAIN can do real work. The optimizer account needs hard execution, memory and thread limits. |
| E6 | A least-privilege optimizer user (SELECT on catalog tables and customer DBs, no `READ ON <source>` grants, profile `readonly=2`) can EXPLAIN normally and gets `ACCESS_DENIED` for `url()`; it can't `ALTER`. | E3 is closed on our platform by the grant model, without code changes. |
| E7 | With `privacy.redact_literals: true`, literals are kept out of LLM prompts **but are still in the report JSON** sent to notifiers. | The report must be scrubbed on ingest; we don't store raw query text. |
| E8 | The committed repo is missing `query_optimizer/templates/config.yaml` (excluded by `.gitignore: config.yaml`). `qopt init` fails, and 2 of 75 tests fail. | We don't use `qopt init`; we generate config at runtime. It's still an upstream bug. |
| E9 | The `openai_compatible` provider takes any endpoint and a bearer token. | We can point it at our own LLM gateway without code changes. |

## Decision

### 1. Kubernetes CronJob per opted-in cluster, not an operator

Analysis runs as a **CronJob in the tenant cluster's namespace**. It's rendered by the ClickHouse engine (`Render` adds it when `spec.optimizer.enabled`) and applied by the cell agent like every other object, so it's created, updated, suspended and deleted with the cluster.

Why not an operator:

* The tool is a batch program with exit codes. CronJob already gives us scheduling, `concurrencyPolicy: Forbid`, deadlines and history. Our cell agent is already the reconciler for desired state, and a second controller would duplicate it.
* A cell-wide operator or controller would run **every tenant's analysis in one process**: every tenant's credentials and query text in one memory space, and one failure domain. A per-cluster Job keeps the blast radius to one cluster, with one credential and one token.
* A CRD (`ClickHouseOptimizer`) would only be worth it if we sold the optimizer outside the platform. On the platform, the API is the interface.

### 2. Analysis is read-only. Applying changes is a platform workflow, not optimizer god mode

On the platform, the CronJob always runs with **god mode off and the reader account**, so it physically can't change anything. This follows the optimizer's own deployment guide (separate reader and writer).

Applying a recommendation goes through a Temporal workflow `ApplyRecommendation`. It's triggered by the tenant (`POST …/recommendations/{id}:apply`) or by **auto-apply** (the tenant's opt-in replacement for god mode). Reasons:

* E1 and E2: correct verification and rollback on ClickHouse need mutation tracking and kill-then-drop, which is long-running, durable work.
* It takes the **same per-cluster lock** (workflow ID `cluster/<id>`) as upgrades, scaling and backups, so an index build never collides with a rolling restart.
* It applies the stability gates that only the platform knows about (merge backlog, disk headroom, cluster state).

The optimizer's own god mode stays available for standalone users. We'd consider using it on the platform once the upstream fixes U2–U4 (below) land and are tested.

### 3. Rendered CronJob (spec)

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: basalt-optimizer
  namespace: ch-<clusterId>
  labels: {basalt.io/cluster: <clusterId>, basalt.io/component: optimizer}
spec:
  schedule: "<m> */6 * * *"         # m = hash(clusterId) % 60: spreads LLM and EXPLAIN load. Tenant may choose 1h..7d.
  concurrencyPolicy: Forbid
  startingDeadlineSeconds: 600
  successfulJobsHistoryLimit: 1
  failedJobsHistoryLimit: 3
  suspend: false                     # the cell agent sets true while any lifecycle operation runs on the cluster
  jobTemplate:
    spec:
      backoffLimit: 0
      activeDeadlineSeconds: 1800
      ttlSecondsAfterFinished: 3600
      template:
        metadata:
          labels: {basalt.io/cluster: <clusterId>, basalt.io/component: optimizer}
        spec:
          restartPolicy: Never
          automountServiceAccountToken: false
          nodeSelector: {basalt.io/pool: control}
          tolerations: [{key: basalt.io/pool, value: control, effect: NoSchedule}]
          securityContext: {runAsNonRoot: true, runAsUser: 10001, seccompProfile: {type: RuntimeDefault}}
          containers:
          - name: optimizer
            image: <registry>/basalt-optimizer@sha256:<digest>   # query_optimizer@<pinned commit> + basalt wrapper
            args: ["--cluster", "<clusterId>"]
            resources:                                            # requests = limits (Guaranteed)
              requests: {cpu: 250m, memory: 512Mi}
              limits:   {cpu: 250m, memory: 512Mi}
            securityContext: {allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: {drop: [ALL]}}
            env:
            - {name: QO_GOD_MODE, value: "false"}                 # never true on the platform (decision 2)
            envFrom: [{secretRef: {name: basalt-optimizer}}]     # reader password and LLM-gateway token for THIS cluster only
            volumeMounts:
            - {name: settings, mountPath: /etc/basalt-optimizer, readOnly: true}
            - {name: ca, mountPath: /etc/basalt-ca, readOnly: true}
            - {name: work, mountPath: /data}
          volumes:
          - {name: settings, configMap: {name: basalt-optimizer}} # tenant settings validated by the API
          - {name: ca, secret: {secretName: ch-<clusterId>-ca}}
          - {name: work, emptyDir: {sizeLimit: 256Mi}}
```

Placement: the control pool, not the shared ClickHouse nodes. LLM client work doesn't take CPU from tenant compute, and the pod never shares a node's sandbox pool with ClickHouse servers. The admission policy (design §8.3) gets one more rule: pods with `component=optimizer` must match the spec above (control pool, no SA token, non-root, `QO_GOD_MODE=false`).

### 4. The wrapper (`basalt-optimizer` entrypoint, about 200 lines of Python, in this repo)

1. **Pre-flight.** It calls the cell agent's local read-only endpoint `GET /v1/clusters/<id>/status` using the Job's per-cluster token. It exits 0 and does nothing if the cluster isn't `ACTIVE`, an operation is in progress, disk is above 70%, a merge-backlog or parts alert is firing, or the tenant's monthly LLM budget is spent.
2. **Config generation.** It renders `config.yaml` in `/data`:
   * `database`: `clickhouses://basalt_optimizer@ch-<id>.ch-<id>.svc:8443` with the CA, and `statement_timeout_s: 60`.
   * `database.schemas`: computed live as every database except `system`, `INFORMATION_SCHEMA`, `information_schema` and `_basalt`. Provider canary queries are never analysed.
   * `llms`: one or more `provider: openai_compatible` entries with `endpoint: http://llm-gateway.basalt-system.svc:8080/v1`, `api_key: <per-cluster gateway token>`, and model names from the cell's provider-approved list.
   * `ranker.strategy: llm`, with the judge model set by the provider.
   * `privacy.redact_literals: true`, **forced**; tenants can't turn it off.
   * `collection` (`top_n`, `min_calls`, `lookback_hours`, `exclude_patterns`) from tenant settings, within bounds.
   * `apply.god_mode: false`; `apply.allowed_actions: [add_skip_index, materialize]` (used only for labelling).
   * `state.path: /data/state.db`; `notifiers: [{type: webhook, options: {url: http://basalt-agent.basalt-system.svc:8080/v1/clusters/<id>/optimizer-reports, headers: {Authorization: Bearer <token>}}}]`.
3. **State round-trip.** Before the run, `GET …/optimizer-state` gives the previous `state.db` (de-duplication through `skip_recent_hours`, which keeps LLM spend down). After the run, `PUT` uploads the new one. If the state can't be fetched, the run is skipped rather than re-analysing everything. This replaces the upstream manifest's per-cluster PVC: a thousand clusters with hourly attach and detach of a 1 GiB EBS volume would mean EBS attach churn and API throttling for a file of a few MB.
4. **Run.** `QueryOptimizer.from_config(...).run()`, then exit with the optimizer's exit code. Non-zero becomes a provider-side metric (`basalt_optimizer_runs_total{result}`), not a tenant page.

### 5. ClickHouse user for the optimizer

`basalt_optimizer` is defined in **read-only `users_xml` storage**, so tenants can't alter it. Its password is per cluster and rotated by the platform.

```sql
-- profile (users.xml): readonly = 2, max_execution_time = 60 (MAX 60), max_memory_usage = 1 GiB (MAX), max_threads = 2 (MAX),
--                      max_concurrent_queries_for_user = 2, max_result_rows = 10000
GRANT SELECT ON system.query_log, system.tables, system.columns,
               system.data_skipping_indices, system.projections, system.parts TO basalt_optimizer;
GRANT SELECT ON <each customer database>.* TO basalt_optimizer;   -- EXPLAIN needs it; maintained by the agent as databases appear
-- deliberately NOT granted: READ/WRITE ON <any source> (E3/E6), TABLE ENGINE, ALTER, CREATE, BACKUP, NAMED COLLECTION
```

The `system.query_log` row policy that hides provider users from tenants also includes `basalt_optimizer`.

### 6. LLM gateway (per cell, provider-operated)

The Job's only external dependency is `llm-gateway.basalt-system:8080`, an OpenAI-compatible proxy. The candidate is **Envoy AI Gateway** (Apache-2.0, built on the Envoy Gateway we already run), with LiteLLM (MIT core) as the alternative; to be evaluated in the milestone that ships this. Its job:

* map provider-approved model names to upstreams (Azure AI Foundry, Anthropic, OpenAI and others, or a self-hosted vLLM for tenants who don't allow third-party processing), and inject the real API keys, so **provider keys never sit in tenant-scoped pods**;
* authenticate the per-cluster token and enforce per-tenant token budgets and rate limits;
* emit token usage per cluster for metering (`usage_records.meter = 'optimizer_llm_tokens'`).

### 7. NetworkPolicy

* **Optimizer pods:** egress only to the cluster's ClickHouse Service on 8443, `llm-gateway` and `basalt-agent` in `basalt-system`, and DNS. No ingress.
* **ClickHouse server pods:** ingress on 8443 is also allowed from `component=optimizer` pods in the same namespace.
* **LLM gateway:** egress only to the allow-listed upstream FQDNs, through the L7 egress proxy (design §9.2).

### 8. Ingest, API and apply workflow

* **Ingest.** The agent forwards reports to the control plane. On ingest we **drop `query_text`** (E7), keep `normalized_query_hash`, a redacted query shape, the statements, the rewrite (LLM output, already literal-free because the prompts were redacted), scores, risk and rationale, and upsert into `recommendations`.
* **API.**
  * `GET /v1/projects/{p}/clusters/{c}/recommendations`
  * `POST …/recommendations/{id}:apply`
  * `POST …/recommendations/{id}:dismiss`
  * `PUT …/optimizer` with body `{enabled, schedule, collection, autoApply: {enabled, minScore≥8, maxRisk: low, maxPerDay≤1}, llmConsent: {providers:[…], acceptedAt, acceptedBy}}`. Enabling requires recorded consent naming the LLM providers that will see schema, query shapes and plans.
* **`ApplyRecommendation` workflow** (holds lock `cluster/<id>`):
  1. **Re-validate** in our own Go validator, not trusting the optimizer's classifier. Only `ALTER TABLE <db>.<t> ADD INDEX qo_<name> <expr> TYPE {minmax | set(N≤1000) | bloom_filter[(p)] | ngrambf_v1(…) | tokenbf_v1(…)} GRANULARITY 1..16` followed by `MATERIALIZE INDEX qo_<name>` is accepted. The table must be `Replicated*`, the expression must reference existing columns only, and `ON CLUSTER` is not allowed (ALTERs on Replicated tables replicate through Keeper anyway).
  2. **Gates:** cluster `ACTIVE`; disk below 70% with estimated index size below 5% of free space; no mutation pending on the table; merge backlog below threshold.
  3. **Baseline:** `EXPLAIN ESTIMATE` marks for the target query shapes, run **as `basalt_optimizer`** (no source grants, so E3 can't happen).
  4. Run `ADD INDEX`, then `MATERIALIZE INDEX`. Poll `system.mutations` on every replica until `is_done` (heartbeating activity, bounded by a timeout).
  5. **Verify:** marks must drop by at least 20% (configurable) on the target shapes. If not: `DROP INDEX`.
  6. **Abort or timeout:** `KILL MUTATION`, then `DROP INDEX` (E2).
  7. **After 7 days:** compare `read_rows` per query hash from `query_log` before and after. If there's no real-world gain, mark the recommendation as "consider dropping". We don't auto-drop in v1.

## Security analysis (additions to the threat model)

| Threat | Control |
|---|---|
| Prompt injection via tenant query text or comments steers the LLM | It only affects that tenant's own recommendations; output is re-validated by our allow-list and applied only through gated workflows |
| LLM-proposed SQL used for SSRF or file read during validation (E3) | The optimizer user has no `READ ON <source>` grants (E6 verified). The ClickHouse pod's egress is default-deny anyway |
| EXPLAIN does real work (E5) | `readonly=2`, `max_execution_time` 60, 1 GiB memory and 2 threads, all `MAX`-constrained |
| Tenant data leaving to LLM vendors | Off by default; explicit per-cluster consent naming the providers; literals redacted (forced); self-hosted model option; raw query text never stored by us (E7) |
| Cross-tenant | One Job per cluster with per-cluster credentials and gateway token, no shared process; the gateway sees all prompts, so it's provider-operated, logs no prompt bodies by default, and has its own threat review |
| Provider LLM keys | Only in the gateway, never in tenant-scoped pods |

## Upstream changes recommended in `query_optimizer` (not blockers, since the integration works around them)

* **U1:** commit `query_optimizer/templates/config.yaml` and fix the `.gitignore` rule (E8).
* **U2:** ClickHouse: run `MATERIALIZE INDEX` with `mutations_sync = 2`, or poll `system.mutations`, bounded by `ddl_timeout_s`, before re-measuring cost (E1). Implement `ddl_session` for ClickHouse.
* **U3:** ClickHouse rollback: `KILL MUTATION` before `DROP INDEX`, and map a regression found after `MATERIALIZE` back to dropping the index (E2).
* **U4:** reject rewrites and collected statements that use table functions or external engines (`url`, `s3`, `remote*`, `file`, `mysql`, `postgresql`, `hdfs`, `azureBlobStorage`, `executable`, …) before EXPLAIN. Document that the optimizer account must hold no source grants (E3).
* **U5:** treat an empty `EXPLAIN ESTIMATE` as unknown cost (`None`), not 0 (E4).
* **U6:** apply `redact_literals` to reports and notifier payloads too, not only prompts (E7).
* **U7:** add a LICENSE file matching the declared MIT licence.
* **U8:** note in the docs that ClickHouse `EXPLAIN` can execute scalar subqueries (E5).

## Consequences

* Optimizer runs are fully decoupled from the data plane. If the Job, the gateway or the LLM vendors fail, tenant queries are unaffected.
* LLM spend becomes a metered, budgeted add-on per cluster.
* We maintain a thin wrapper plus a Go validator for skip-index DDL. The optimizer itself is consumed unmodified at a pinned commit.
