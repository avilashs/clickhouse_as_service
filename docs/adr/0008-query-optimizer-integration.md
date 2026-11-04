# ADR-0008: Query optimizer integration

Status: **Proposed, blocked on locating the optimizer** (design Q2). Date: 2026-11-04

## Context
The request says to integrate "the query optimizer that we built". The repository `avilashs/query_optimizer` exists but has **no commits**, and nothing in this repo or `kafka_as_service` references an optimizer. Its inputs, outputs, language and licence are unknown, so this ADR defines an integration boundary that's safe for a multi-tenant service and can host whatever the optimizer turns out to be.

## Decision (proposed)
Integrate it as an **out-of-band advisor** behind a stable interface:

```go
// Implemented by an adapter around the existing optimizer (in-process library, sidecar, or RPC service).
type Advisor interface {
    // Analyze returns recommendations; must be side-effect free.
    Analyze(ctx context.Context, in AdvisorInput) ([]Recommendation, error)
}

type AdvisorInput struct {
    ClusterID      string
    ServerVersion  string
    Schemas        []TableSchema    // from system.tables / system.columns / SHOW CREATE
    PartsStats     []PartsStat      // rows, bytes, parts, partitions per table
    Workload       []QuerySample    // normalized_query_hash, query text (opt-in), read_rows, read_bytes, duration, memory, count
    Settings       map[string]string
}

type Recommendation struct {
    Kind           string // ORDER_BY | SKIP_INDEX | PROJECTION | MATERIALIZED_VIEW | CODEC | TYPE | QUERY_REWRITE | SETTING
    Target         string // db.table or normalized query hash
    Rationale      string
    EstimatedGain  Gain   // bytes read / latency / storage, with confidence
    DDL            []string // empty for QUERY_REWRITE
    Risk           string // LOW (additive, e.g. ADD INDEX) | MEDIUM | HIGH (table rebuild)
}
```

Flow:
1. A per-cell `optimizer-worker` (deployment on the control pool) collects `AdvisorInput` from opted-in clusters with a provider read-only user (`provider_advisor`, SELECT on `system.*` plus `SHOW` on customer DBs, no data SELECT). Query text is included only if the tenant opts in, because it can contain literals or PII. Otherwise only normalised query hashes and shapes are used.
2. It calls `Advisor.Analyze`, stores results in `recommendations(cluster_id, id, kind, target, payload, created_at, status)`, and exposes them via `GET /v1/.../recommendations`.
3. `POST …/recommendations/{id}:apply` runs low-risk DDL through a normal gated workflow, for example `ALTER TABLE … ADD INDEX …` then `MATERIALIZE INDEX` throttled by merge pool limits. HIGH-risk recommendations, such as an ORDER BY change that needs a rebuild, are advisory only.

## Why not inline
An inline rewriting proxy would:
* sit on every tenant's query path, adding latency and a new failure domain, which contradicts "data plane keeps serving";
* parse attacker-controlled SQL in a shared component, which would be a new cross-tenant attack surface;
* change query semantics without the user seeing the change, and break the native protocol (9440) unless it reimplements that protocol.

If the optimizer is fundamentally a rewriter, v2 can offer an **opt-in, per-cluster sidecar** in the tenant's own pod (same sandbox), on HTTP only, which fails open. Even then, it should return rewrite *suggestions* rather than altering queries by default.

## Needed from you
The repo location or code, a short description of what it does, its language and runtime, and its licence (must be compatible with Apache-2.0 and hosted use). Then I'll write the adapter and replace this ADR's "proposed" interface with one checked against the real code.
