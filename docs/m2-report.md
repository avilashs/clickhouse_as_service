# M2 report: placement

Date: 2026-11-04. Environment: one kind cell (Kubernetes 1.33.1) with **10 nodes**: control-plane, 3 `control`, 3 `shared` (gVisor), 3 `free`, one of each pool per zone. It runs in a 4-vCPU / 15 GiB Firecracker VM. Images: ClickHouse 26.8.34.13 LTS, Altinity operator 0.27.4, Envoy Gateway v1.5.0, gVisor `go` branch @ `e0105ccf`.

**Acceptance (prompt):** both tiers with all enforcement rules; tests prove each invariant; the admission policy rejects bad pods, including one missing the sandbox.

## Results

**All M2 acceptance criteria pass.** `test/e2e/m2.sh` run 2: **51/51 checks passed** ([log](evidence/m2-e2e-run2.log)). `go test ./...` passes; the new tests are engine invariants and goldens for both tiers, node-pool claim and release, every auditor rule, and the OpenAPI-vs-routes contract.

**M1 regression under M2:** `test/e2e/m1.sh` passes **31/31** on the M2 cell, so shared clusters now run under gVisor with admission enforced ([log](evidence/m2-m1-regression.log)). That covers TLS through the gateway, killing the worker twice mid-create with no duplicates, idempotent replay, and delete. (While counting these I found the M1 report had said 32/32; the M1 suite has 31 checks. It's corrected there.)

| Invariant (design §8) | Rendered by | Admission rule | Live check in `m2.sh` | Audit rule |
|---|---|---|---|---|
| R1: replicas of a shard never share a node | required anti-affinity, `matchLabelKeys: [shard]` | rejects a pod without it | shared and dedicated: 2 replicas on 2 nodes | V1 |
| R1b: replicas spread over zones | zone spread, maxSkew 1, minDomains 3 | rejects a pod without it | both tiers: replicas in different zones | V1b |
| R2: dedicated pods only on their own pool, one server per node | nodeSelector + unique taint + anti-affinity against all servers with `namespaceSelector: {}` | rejects missing exclusivity, exclusivity scoped to one namespace, another cluster's pool or toleration | pool nodes carry only the cluster's taint; one server per node; no other workload pods there | V2, V2b, V3 |
| R2b: node count = server count | `NodePoolRequest{Nodes: servers}` | — | 2 nodes for 2 servers; nodes returned to the free pool on delete | V4 (30 min grace) |
| R3: shared pods only on the shared pool, never on dedicated nodes | nodeSelector + toleration + dedicated taints | rejects a shared pod tolerating a dedicated taint; the fence rejects any non-ClickHouse pod tolerating dedicated or shared taints | servers only on shared nodes; a pod without the toleration stays `Unschedulable` on a dedicated node | V3 |
| Sandbox on shared nodes | `runtimeClassName: gvisor` (cell parameter) | **rejects a shared server without the sandbox**, and one with another RuntimeClass | `uname -r` inside the pod is the gVisor kernel | V3 |
| Keeper on the control pool, one per node | nodeSelector + toleration + anti-affinity | the fence allows the control pool only for Keeper and platform pods | 3 Keeper replicas on 3 different control nodes | V5 |
| Label integrity | — | ClickHouse and Keeper pods only from `statefulset-controller` with a StatefulSet owner; their StatefulSets only from the operator | a correct pod created by cluster-admin is rejected; a rogue StatefulSet is rejected | — |
| No node binding that bypasses taints | — | `spec.nodeName` at create only for kubelets and the volume provisioner | rejected | — |
| Baseline | non-root, no SA token, requests = limits, no privilege escalation | rejects an SA token mount, requests ≠ limits, privilege escalation, hostPath, tolerate-all | — | — |

Each negative admission test starts from the **live StatefulSet's pod template**, breaks exactly one rule, and is submitted as the StatefulSet controller (`--as=system:serviceaccount:kube-system:statefulset-controller`). That makes each denial attributable to exactly that rule. The unmodified template is admitted for both tiers.

**Audit:**
- `basalt-audit --once` reports 0 violations with both clusters running, and again after cleanup.
- Two violations were injected by relabelling nodes, which is the kind of hand-edit admission can't see. The CLI reported V4 and V2.
- The in-cell service exported `basalt_placement_violations_count > 0` within 90 s. It showed only V2, because the service applies the 30-minute V4 grace, as intended.
- Both cleared once the labels were restored.

**Both tiers serve TLS queries.** The shared tier goes through the SNI gateway and runs under gVisor. The dedicated tier goes through its own Service; a quorum insert reached both replicas.

Timings (run 2):

| Operation | Shared | Dedicated |
|---|---|---|
| Create | 178 s | 295 s, including claiming 2 nodes |
| Delete | 349 s | 349 s, including returning 2 nodes |

## What was built

| Component | Notes |
|---|---|
| Dedicated tier (`internal/engine/clickhouse`) | Sizes `dt-1` (kind) and `d-m7g-4xl` (m7g.4xlarge minus reservation). Tenant-chosen shards and replicas, one PDB per shard, a `LoadBalancer` Service with NLB annotations, and a `NodePoolRequest` sized to the server count. Placement rules live in `placement.go`. |
| Shared tier changes | `runtimeClassName` from the cell's sandbox parameter. `skip_binary_checksum_checks` only for sandboxed servers. `max_server_memory_usage` rendered as explicit bytes for every tier. |
| Node pools (`internal/agent/nodepool.go`) | Local provider: claims `free` nodes all or nothing, spreading over zones. It swaps the `free` taint for the cluster's own taint, and returns nodes only when no tenant pods remain. A delete counts as gone only after the nodes are back. Cloud provider: Karpenter NodePool (M6). |
| Admission (`deploy/cell/policies/placement.yaml`) | Four ValidatingAdmissionPolicies (CEL, in-process, `failurePolicy: Fail`): server placement, workload identity, StatefulSet owner, toleration fence. |
| Auditor (`cmd/basalt-audit`, `internal/audit`) | Rules V1–V5. Runs as a CLI (`--once -o json`, non-zero exit on violations) or as a service exporting `basalt_placement_violations{rule,cluster,node}`. Deployed in `basalt-system`. Alert rules are in `deploy/cell/monitoring/placement-alerts.yaml`; runbook in `docs/runbooks/placement-violation.md`. |
| Cell v2 (`deploy/kind`, `hack/`) | All three pools tainted, with tolerations for every platform component: operator, gateway and its certgen job, Envoy, agent, auditor, Keeper. gVisor RuntimeClass. kubelet on the cgroupfs driver. `hack/build-gvisor.sh`. inotify limits raised. |
| Spike S2 (`test/spike/gvisor-bench.sh`) | gVisor compatibility and performance; see ADR-0003. |

## Findings

1. **gVisor in this environment** (details in ADR-0003, "M2 spike S2 results"):
   - ClickHouse needs `skip_binary_checksum_checks` under gVisor; without it the startup self-check fails with code 246.
   - It can't see its cgroup memory or CPU limits, so memory and threads must be explicit.
   - It is **2.5–7.7x slower than runc here**, far outside ADR-0003's thresholds. This VM has no KVM and nests systrap inside Firecracker, so the numbers don't transfer to EC2. **The sandbox decision is re-benchmarked on cloud nodes in M6.**
2. **runsc with the systemd cgroup driver on cgroup v1 silently drops pod limits.** The sandbox ran outside the pod cgroup and the benchmark came out *faster* than runc. Caught by checking the pod cgroup's usage directly. Fixed locally with the cgroupfs driver. Cloud cells (cgroup v2) must run the same check: the spike script does it.
3. **The local-path provisioner's helper pods tolerate every taint.** The first version of the toleration fence blocked all volume provisioning. The fence and the auditor now allowlist exactly that ServiceAccount in `local-path-storage`. Cloud cells use the EBS CSI driver instead.
4. **`spec.nodeName` bypasses taints.** Admission now restricts who can set it at creation.
5. **Partial node claims would strand nodes.** Caught by the unit test; claims are now all or nothing.
6. **kind needs inotify limits raised beyond 7 nodes**, and the cell uses about 19 GB of disk for 10 nodes; images are now loaded per pool.

## Not done in M2 (and why)

| Item | Status |
|---|---|
| Operator spike S1 (gate rolling restarts on our own checks) | Moved to **M5**, where rolling upgrades and scaling are built and can be tested under load. |
| Operator spike S1b (500 CHIs) | Not feasible in this VM: each CHI needs Keeper pods and memory. M2 did observe deletions queuing behind other reconciles, about 6 minutes per delete with 2 clusters. To re-measure with minimal CHIs on cloud nodes in M6. |
| Cloud node pools (Karpenter) | M6, with the cloud cell. |
| Keeper pod rules in admission | Keeper is covered by the identity rule and the control-pool fence; its own anti-affinity and resource rules are audited (V5) but not admission-checked. Planned for M3. |

## Limitations

- The fence's list of platform namespaces and the sandbox name live in policy and ConfigMap; a new platform namespace needs a policy change.
- Auditor V4 grace timers are in memory and reset when the auditor restarts. A restart delays an alert by up to the grace period but never suppresses it.
- Dedicated `LoadBalancer` Services stay `<pending>` on kind (no LB controller); tests use a port-forward.
- The free pool is fixed at 3 nodes, so at most 3 dedicated servers at a time locally. Capacity reservation (`dedicated` pool = 3 nodes) enforces this before provisioning.
