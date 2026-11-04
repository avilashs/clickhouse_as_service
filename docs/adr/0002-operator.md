# ADR-0002: Kubernetes operator for ClickHouse

Status: Proposed (pending spike S1 in M2). Date: 2026-11-04

## Context
We need an operator that manages ClickHouse servers and Keeper per tenant cluster, gives us full pod-template control (placement, `runtimeClassName`, security context), performs safe rolling changes, and scales to hundreds or thousands of clusters per cell.

## Evidence gathered (2026-11-04, by reading each repository directly)
| | Altinity `clickhouse-operator` | ClickHouse Inc. `clickhouse-operator` |
|---|---|---|
| License | Apache-2.0 (LICENSE file read) | Apache-2.0, "Copyright 2016-2026 ClickHouse, Inc." (LICENSE file read) |
| Release history | Tags up to `release-0.27.4`, last commit 2026-09-24; dozens of releases over many years | Tags `v0.0.1` … `v0.0.8`; last commit 2026-11-03 |
| API maturity | `clickhouse.altinity.com/v1` (CHI), `clickhouse-keeper.altinity.com/v1` (CHK); CHI→CHK direct reference since 0.27.0 ("GA-level Keeper support") | **`v1alpha1`** `ClickHouseCluster` / `KeeperCluster` |
| Keeper | CHK resource; per-CHI reference | `KeeperCluster` with replicas ∈ {1,3,5,…}; exactly one of `keeperClusterRef`/`externalKeeper`; each ClickHouseCluster needs its own KeeperCluster |
| Pod template | Full `podTemplates` (arbitrary PodSpec, incl. `runtimeClassName`, affinity, tolerations, topology spread) | `podTemplate` with affinity, tolerations, `runtimeClassName`, `securityContext`, volumes, init containers; plus convenience fields: `topologyZoneKey` (required zone spread for replicas of one shard), `topologyMinDomains`, and `nodeHostnameKey` (required anti-affinity across **all pods of the cluster**, i.e. one pod per node) |
| Rolling behaviour | Host-by-host reconcile, with configurable waits; must check whether it can gate on our own replication/Keeper criteria (S1) | Not yet assessed in depth |
| Scale across many CRs | Single operator watching all namespaces; known production use at large CR counts by multiple vendors (to be confirmed by load test S1b) | Unknown at this maturity |

Note: the ClickHouse Inc. operator's `nodeHostnameKey` is anti-affinity *within one cluster*. Our dedicated rule (R2) needs anti-affinity against *all* ClickHouse server pods in *all* namespaces (`namespaceSelector: {}`), so we would render that term into `podTemplate.affinity` ourselves with either operator.

## Decision
**Use the Altinity operator**, pinned to `release-0.27.x`, by image digest.

Why: a stable `v1` API and a long release history, compared with a `v1alpha1` API at `v0.0.8` whose schema may change incompatibly. Both meet our pod-template needs; maturity decides it.

To keep the option of switching later, the engine's `Render` produces an **internal model** (shards, replicas, pod spec, config files, Keeper). An operator-specific adapter turns it into CRs. We review the ClickHouse Inc. operator again at each milestone; migrating would mean writing the second adapter and running a per-cluster cutover. The data on disk is the same either way.

## Spikes before this becomes "Accepted"
* **S1:** Can we gate each host's restart on our own checks (replication delay, Keeper quorum), or must we drive it host by host ourselves (e.g. per-host template switch or `stop`)? We measure what the operator does under continuous insert load.
* **S1b:** One operator with 500 CHIs on kind (minimal resources). We measure reconcile latency and operator memory.
* **S1c:** Confirm the operator doesn't overwrite our `runtimeClassName`, tolerations or `namespaceSelector` terms, and that its generated Services can carry our `serving` label selector.

## Consequences
We depend on Altinity's release cadence. The operator runs with cluster-wide RBAC, so we restrict it to namespaces labelled `basalt.io/managed=true`, and treat its ServiceAccount as highly privileged (§8.3 label integrity).
