# M3 report: security

Date: 2026-11-04. Environment: the M2 kind cell (Kubernetes 1.33.1, 10 nodes, gVisor on the shared pool) in a 4-vCPU / 15 GiB VM. Images: ClickHouse 26.8.34.13 LTS, Altinity operator 0.27.4, Envoy Gateway v1.5.0.

**Acceptance (prompt):** an automated attack suite runs as a tenant and every attempt fails: SSRF to metadata and internal services, reading server files, executable functions, privilege escalation, raising limits past constraints, and cross-tenant access.

## Status: not accepted yet. One layer is open, and closing it needs a decision

The suite, `test/attack/run.sh`, runs 9 tests with 99 subtests (plus 3 tests that are a single check each) against two live tenant clusters:

- **86 subtests and the 3 single-check tests pass.** They cover every SQL-level attack, identity attack, limit attack and cross-tenant attack ([run 6 log](evidence/m3-attack-run6.log)).
- **13 subtests fail, all in `TestNetworkIsolation`.** That test models an attacker who already has code execution inside tenant A's server process. From there, TCP connections reach everything the NetworkPolicies are meant to block:
  - tenant B's server and Keeper
  - the in-cell canary
  - the Kubernetes API and the control plane on the host network
  - `169.254.169.254` and `169.254.170.23`
  - the gateway, the operator's metrics endpoint, and the internet

The policies are rendered and applied, but **the kind CNI (kindnet) does not enforce NetworkPolicy**. Its node agent has no policy controller and there are no nftables rules. No SQL path reached any of these targets: the grant, allowlist and file controls held every time, and the canary saw no SQL-originated connection.

The local cell needs a policy enforcer for this layer, and either option needs your go-ahead (see "Decision needed").

## Results by area (run 6)

| Test | Checks | Result | What it shows |
|---|---|---|---|
| `TestControls` | 1 | pass | Normal tenant work succeeds: replicated table, quorum insert, `Distributed` across replicas, SQL UDF, a new user, reading `query_log`. Shows the denials aren't vacuous. |
| `TestTenantAttacks` | 70 | pass | S1–S20 plus S6g–S17g SSRF, F1–F8 filesystem, E1–E8 code execution, P1–P15 privilege escalation, L1–L10 limits; then the server still serves and the canary saw nothing. Each attack names the error codes that count as "blocked by the right control". A syntax error or any other code fails the attack. |
| `TestTenantCreatedUserCannotEscapeLimits` | 1 | pass | A user created with no role or profile still hits every constraint (452). |
| `TestProviderAccountsUnreachableFromTenantSide` | 5 | pass | Through the tenant gateway: `provider_ops` without a certificate or with a self-signed one, `clickhouse_operator` with its real password, and `default` with its real password are all refused. The control shows the agent's CA-signed certificate works. |
| `TestNoSecretsVisibleToTenant` | 1 | pass | No platform activity in `query_log` or `processes`. No secret appears in `system.users`, `query_log`, `text_log`, `server_settings` or `named_collections`. |
| `TestCrossTenant` | 3 | pass | Tenant A's credentials fail on B. A's certificate doesn't cover B's name. A's API token gets 404 on B's resources, and 401 on the cell-agent API. |
| `TestDictionaryCannotBorrowPlatformUsers` | 5 | pass | A local ClickHouse dictionary can't gain privileges by logging in as `default` (with or without its password), `provider_ops` or `clickhouse_operator`. The control shows the tenant's own user works. |
| `TestProviderQueriesCannotBeKilledByTenant` | 1 | pass | `KILL QUERY ON CLUSTER` is refused, and repeated plain kills don't touch the platform's query. The control: the same kills do kill the tenant's own query. |
| `TestNetworkIsolation` | 15 | **2 pass, 13 fail** | Both controls pass (own replica, own Keeper); all 13 blocked-target checks fail, as described above. |

`go test ./...` passes, including the new render invariant `TestProviderUserUsesCertificateOnly` and updated goldens for both tiers.

## Findings and fixes

The suite found ten problems. Nine are fixed, each with a test that failed before its fix and passes after it. The network gap stays open.

1. **`provider_ops` was reachable through the tenant gateway with a password.** The gateway's pod IP is in 10/8, so an IP allowlist meant nothing.
   - Fix: `provider_ops` now authenticates **only** with a client certificate signed by the cell CA (CN `basalt-agent`). The agent mints its own certificate, and the provider password no longer exists.
   - Test: `TestProviderAccountsUnreachableFromTenantSide`.
2. **Tenants could ALTER, DROP or TRUNCATE `system` tables and manage row policies on them**, including dropping the policy that hides platform activity.
   - Fix: partial revokes on `system.*`.
   - Tests: P9–P13.
3. **HTTP dictionary sources ignore source grants.** I confirmed this in the upstream source: `HTTPDictionarySource` checks only `remote_url_allow_hosts`, and the MySQL, PostgreSQL, Redis, MongoDB, Cassandra and remote ClickHouse dictionary sources work the same way. With the allowlist set to "this cluster's own hosts", a tenant made an HTTP request to its own replica (S17g, run 2).
   - Fix: `remote_url_allow_hosts` now matches **no host** (one entry, `basalt-no-egress.invalid`). Nothing needed the own hosts: `Distributed` doesn't consult the allowlist, and `remote()` is denied by grants.
   - Run 2 also showed the grant layer blocks `remote`, `remoteSecure`, `mysql`, `postgresql`, `mongodb` and `redis` on its own (497) when the host is allowed.
4. **`Distributed` tables were broken.** With no interserver secret, replicas logged in to each other as `default` with an empty password, which failed with code 516. The security guide had promised `Distributed` works.
   - Fix: an operator-generated cluster secret (`secret.auto`). `Distributed` queries now run on the other replicas as the issuing user.
   - Test: the `TestControls` `Distributed` check.
5. **The stock `default` user is an admin, and dictionary logins skip its address check.**
   - The image's `users.xml` gives `default` `access_management` and `named_collection_control`.
   - The operator widens its `networks` to every pod of the cluster.
   - A local ClickHouse dictionary source logs in in-process over the LOCAL interface. That login passed `default`'s address restriction (run 3: with the real password, the dictionary ran as `default`).
   - Fix: a `users.d` override (named to merge last) removes both admin keys and resets `networks` to localhost. `default` is granted only `SELECT ON system.one`; I verified the merged config locally with `clickhouse extract-from-config`. Nothing logs in as `default` any more.
   - What remains: a dictionary given `default`'s real password still logs in, but can read nothing (497, `TestDictionaryCannotBorrowPlatformUsers`). That password exists only in the platform secret store; the cell holds just its SHA-256.
6. **`KILL QUERY` lets its holder kill *any* user's queries, the platform's included.** I confirmed this upstream in `InterpreterKillQueryQuery`: the grant isn't scoped to users, and killing your own queries needs no grant. `customer_admin` held it, and in run 5 the tenant killed a `provider_ops` query.
   - Fix: `REVOKE KILL QUERY ON *.* FROM customer_admin`. It's explicit, so it also applies to clusters bootstrapped earlier.
   - Cost: a tenant admin can no longer kill queries of users it created. This is noted in the security guide.
7. **The kill test was vacuous at first.** The gateway balances connections across replicas, so the kill could land on the replica that wasn't running the query. That's how finding 6 got through runs 1–2. The test now repeats the kills until the tenant's own sleeping query (the control) is gone, and also checks that `ON CLUSTER` is refused.
8. **Two PodDisruptionBudgets covered every server pod, so no pod could be evicted at all (node drains blocked).** The operator's cluster-wide PDB and Basalt's per-shard PDBs both matched each pod.
   - Fix: `pdbManaged: "no"`.
9. **Deleted clusters could hang forever.** The M2 admission policy `basalt-statefulset-owner` denied the garbage collector's removal of the `foregroundDeletion` finalizer, so StatefulSets deleted in the foreground stayed in Terminating.
   - Fix: UPDATEs that leave `spec` unchanged are allowed. I checked by hand that a `nodeSelector` patch is still denied and an annotation is allowed.
   - This one was found while recycling test clusters, not by an attack. `m2.sh` now checks it (see Regression).
10. **NetworkPolicy isn't enforced in local cells.** This is open; see the status section.

Other M3 changes, each covered by a test above:
- The operator now reconciles Keeper installations in parallel (`basalt-overrides` ClickHouseOperatorConfiguration). Before, one stuck cluster delayed every other Keeper.
- Two new admission policies (`deploy/cell/policies/security.yaml`):
  - Keeper pods must be on the control pool, one per node, non-root, with no ServiceAccount token, and with memory requests equal to limits.
  - A write fence for the cell agent: it may write only in `ch-*` namespaces and to the cell CA Secret. Tested: denied in `kube-system`, allowed in a tenant namespace.
- `user_scripts_path` points to a directory that does not exist.
- Introspection is pinned off.
- `max_memory_usage_for_user` is constrained.

## Expectation corrections (not weakened controls)

In run 1 some attacks were blocked by a different control than the one I had predicted. I accepted the additional codes only where that control is a legitimate, documented block:
- **491 `UNACCEPTABLE_URL`** (host allowlist) for S6–S11 and S17. The grant layer is shown separately by the S6g–S11g variants.
- **481 `PATH_ACCESS_DENIED`** for the file dictionary (F8).
- **482 `DICTIONARY_ACCESS_DENIED`** for the executable dictionary (E4).
- **194** for `default` through the gateway. ClickHouse returns it when the user exists but the address isn't allowed.

I also fixed three test bugs:
- `query_id` has to be an HTTP parameter, not a SETTING.
- The role-less user is granted `SELECT ON default.*`, because `customer_admin` can't grant `*.*` past its own partial revokes.
- The kill test lacked a control (finding 7).

## Regression

- **M1 (`test/e2e/m1.sh`) on the M3 cell: 31/31** ([log](evidence/m3-m1-regression.log)). The first run had 30/31. The failed check counted three credential files per cluster, but M3 removed the provider password on purpose. The check now names the two expected secrets (`admin`, `default`).
- **M2 (`test/e2e/m2.sh`): 53/53** ([log](evidence/m3-m2-regression.log)): the 51 M2 checks plus two new ones for finding 9. Impersonating the garbage collector, a metadata-only StatefulSet update is admitted and a spec change is denied.

## Environment incidents (local only)

- **A VM restart scrambled the kind nodes' IP addresses.** Docker reassigns them in start order, and the result breaks node addresses, DNS, volume binding and Envoy's xDS stream. I re-pinned each node to a static address on the `kind` network and restarted Envoy Gateway. `hack/cell-up.sh` now pins addresses when it creates a cell. The manual procedure was verified; the script path hasn't run yet, because it only runs on cell creation.
- **The inotify limits reset on reboot.** `cell-up.sh` already raises them; rerun it, or the sysctl, after a VM restart.

## Limitations and follow-ups

- **No way to roll a config change out to existing clusters.** A render change reaches only clusters created afterwards, so I recreated the test clusters four times. Re-rendering and republishing existing clusters (with the operator's rolling restart) belongs in M5 lifecycle.
- **Tenant admins can't kill their own users' queries** (finding 6). A platform endpoint could do it on their behalf.
- **Opt-in egress** (design §9.2) will add hosts to `remote_url_allow_hosts`. That makes HTTP and other dictionary sources reachable for those hosts regardless of grants; the egress proxy and NetworkPolicy must carry that case.

## Decision needed: enforcing NetworkPolicy in the local cell

| Option | What | Licence | Cost |
|---|---|---|---|
| **A (recommended)** | Run [kube-network-policies](https://github.com/kubernetes-sigs/kube-network-policies) v1.1.3 (kubernetes-sigs, the reference NetworkPolicy implementation for kindnet-style CNIs) as a DaemonSet. Its image is on registry.k8s.io, which this environment can't reach, so the binary has to be **built from its source here** (`go build ./cmd/kube-network-policies/standard`). | Apache-2.0 | Small; keeps the cell as is. Building external code was blocked by this session's permission classifier, so it needs your explicit approval. |
| B | Recreate the cell with `disableDefaultCNI` and Calico (images from Docker Hub via mirror.gcr.io) | Apache-2.0 | The cell must be rebuilt, M1 and M2 re-run, and Docker Hub rate limits may get in the way. |

Option A is prepared: `hack/build-netpol.sh` builds only the tagged v1.1.3 commit (`d3fc697c`) from a clean upstream clone and loads it as `basalt-netpol:v1.1.3`. `deploy/cell/netpol/kube-network-policies.yaml` is the upstream install manifest, adapted to use that image, and `hack/cell-up.sh` applies it when the image is present. The build step itself was blocked twice by the session's permission classifier ("code from external"), and still is.

Once enforcement is on, watch for kubelet probes. The tenant policies admit no host-network traffic, and how kube-network-policies treats traffic from the node has not been tested here.

With either option, M3 acceptance is: re-run `test/attack/run.sh` and `TestNetworkIsolation` must pass with its controls still connecting.
