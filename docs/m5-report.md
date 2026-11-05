# M5 report: lifecycle and backup

Date: 2026-11-05. Environment: the M4 kind cell (Kubernetes 1.33.1, 10 nodes, gVisor on the shared pool, kube-router enforcing NetworkPolicy) in a 4-vCPU / 15 GiB VM. ClickHouse 26.8.34.13 and 26.8.36.3 LTS, Altinity operator 0.27.4. The VM was heavily loaded during every run below (load average 20 to 45 with three replicas, all under gVisor), so durations are upper bounds for real hardware, not benchmarks.

**Acceptance as set:** scale, upgrade and add replica, each under continuous insert and query load, with a verifier confirming no acknowledged insert was lost (and stating which `insert_quorum` setting the guarantee depends on); scheduled restores verify correctly.

## Results

| Check | Result | Evidence |
|---|---|---|
| Add replica, upgrade 26.8.34.13 → 26.8.36.3, vertical scale, back to back on one cluster under insert and query load | **pass** | [lifecycle-final.log](evidence/m5/lifecycle-final.log) |
| Verifier: every acknowledged batch present, complete and not duplicated on **every** replica, including the added one | **1141 of 1141 batches, 3 of 3 replicas, 0 missing / 0 partial / 0 duplicated / 0 unacknowledged-but-present** | same |
| Backup (full), incremental backup naming its base | pass | [backup-restore-final.log](evidence/m5/backup-restore-final.log) |
| Restore of the incremental backup into a new cluster: rows and content hash equal the source on both replicas, restored cluster has its own credentials | pass | same |
| Tenant cannot reach the store or the credentials (7 statements refused, no named collection visible during a running job, none left behind, bucket refuses unsigned requests) | pass | same |
| Deleting a backup others are built on is refused (409); deleting newest-first removes every object (73 → 0) | pass | same |
| Restore verification of a good backup | **PASSED** (2 tables, both exact) | [verify-final.log](evidence/m5/verify-final.log) |
| Restore verification of a backup with a damaged object in the store | **FAILED, as it must** (restored 8000 rows, source held 20000); backup not marked verified; scratch cluster deleted | same |
| **Scheduled** backup and **scheduled** restore verification, nobody asking | **pass** (scheduler took the backup, restored it into a scratch cluster, compared, deleted it) | [scheduled-backup-verify.log](evidence/m5/scheduled-backup-verify.log) |
| `go test ./...` | pass | – |
| Regression: M3 attack suite on the M5 render (backup egress rule, backup host in `remote_url_allow_hosts`) | **99/99 subtests, 9/9 tests** | [attack-regression.log](evidence/m5/attack-regression.log) |
| Regression: M1 | **31/31** | [m1-regression.log](evidence/m5/m1-regression.log) |
| Regression: M2 | **53/53** (second run; see below) | [m2-regression.log](evidence/m5/m2-regression.log) |

## Which `insert_quorum` the guarantee depends on

The verifier's writers use **`insert_quorum = 'auto'`** (a majority of the replicas that registered the table), **`insert_quorum_parallel = 1`**, **`insert_quorum_timeout = 120000` ms** (longer than a replica restart), `async_insert = 0`. A write is acknowledged only after a majority has the part, and a failed write is retried as the identical block (the table deduplicates it). Under that setting no acknowledged insert was lost through any of the three operations.

- With 2 replicas `auto` means both, so writes **pause while one replica restarts** and clients must retry. With 3 replicas a single restart does not stop writes. The upgrade and scale steps ran with 3.
- **ClickHouse's default is `insert_quorum = 0`.** I did not run the comparison: I have no measurement of what the verifier finds with `LIFE_QUORUM=0`. The risk (an insert acknowledged by one replica lost if that replica's disk is lost before the others fetch it) is from the design and ClickHouse's documentation. A graceful rolling restart does not lose disks, so I would not expect the verifier to find losses in this environment either way, and it could not have shown the difference.
- **The platform does not set `insert_quorum` for tenants.** It is a client-side setting. This needs your decision, see "Decisions".

## What the lifecycle run showed

One cluster (`t-2`, 2 replicas), the verifier running from before the first change to after the last (3 writers, 2 readers, batches of 2000 rows).

| Step | Took | During it: acknowledged batches / failed write attempts / failed reads / p99 read | Longest pause in acknowledgements while it ran |
|---|---|---|---|
| Baseline (60 s) | – | 36 / 0 / 0 of 71 / 1.9 s | 2.6 s |
| Add replica (`t-2` → `t-3r`, 2 → 3 replicas) | 4 m 31 s | 112 / 0 / 0 of 227 / 6.1 s | 7.6 s |
| Upgrade 26.8.34.13 → 26.8.36.3, rolling | 23 m 54 s | 354 / 11 / 4 of 795 / 8.4 s | 48.7 s |
| Vertical scale (`t-3r` → `t-4r`, 1 → 2 CPU, 2 → 3 GiB) | 23 m 18 s | 521 / 4 / 7 of 615 / 9.8 s | 10.6 s |
| Whole run | 59 min | 1141 acknowledged of 1141 attempted; 15 failed attempts; 11 failed reads of 1871; 0 torn | – |

- Every failed write attempt was `http: server closed idle connection`/EOF (a connection to a replica that was restarting, 5) or `QUERY_WAS_CANCELLED` (394, 10, a query on a server being shut down). Each was retried as the same block until it was acknowledged, which is why the ledger is complete. A client that does not retry loses those writes; it does not lose acknowledged ones.
- Reads returned a failure for about 0.6% of queries during the changes. No read ever saw a partial batch (a count that is not a multiple of the batch size).
- The 48.7 s pause during the upgrade is the one that matters for a user: with a quorum of 2 of 3, writes stop for about the time a replica takes to come back.
- After each step the cluster was `ACTIVE` and every pod ran the new image, size and version (checked through `SELECT version()` and the pod spec); rollback was not triggered.
- **The added replica received the tenant's table from the cell agent**, not from the operator (log line "schema copied to a new replica", 14:51). See finding 2.

## Backup and restore, in detail

Design in [ADR-0006](adr/0006-backup.md) (revision 3). Native `BACKUP`/`RESTORE` to an S3-compatible store, run by the cell agent as a job, with per-job credentials in a named collection only the platform's user can see; incremental backups; retention by the control plane; restore always into a new cluster; verification by restore into a scratch cluster; a scheduler for backups and verifications.

- A full backup of a cluster with 60 000 rows in two tables and a view took 20 to 40 s. Incremental backups took 25 s to 1 m 40 s. Sizes were 317 KB (full) and 374 KB (incremental). **The incremental backup was not smaller**: background merges rewrote the parts between the two backups, so the base's files no longer matched. The incremental mechanism is proven by the restore (the incremental backup restored correctly using its base, which needed the chain fix in finding 4); a size saving is not shown.
- Restore of the incremental backup into a new cluster: **the whole operation took 6 to 10 minutes**, almost all of it creating the cluster. The restored cluster's two replicas hold the same rows and the same content hash as the source for all three objects (tables `bk_events`, `bk_small`, view `bk_view`).
- Restore verification: a good backup took 4 to 6 minutes to verify; a damaged one was caught. The comparison rule: a table nobody wrote to during the backup must match exactly (rows and hash); one that was written to must be within the row counts taken before and after. The scheduled run saw exactly that case (the table held 4000 rows before the backup and 8000 after; the restore had a count in between and passed as "bounded").
- Retention and chains: a backup that something is built on cannot be deleted; objects are removed before the backup is marked deleted; backups of a deleted cluster age out. The scheduler also backed up a restored cluster on its own (its objects stay in the store until retention, because the cluster was deleted).

## Findings and fixes

1. **The deployed cell agent was a stale binary.** `Dockerfile.cell` copies a prebuilt `bin/basalt-agent`; rebuilding the image did not rebuild the binary, so for the first two lifecycle runs and the first backup attempts the cluster ran an agent without the schema sync and without backup jobs. The jobs sat `PENDING`; the lifecycle runs measured the old behaviour. The evidence below says which run used which agent. A CGO build also does not start in the `scratch` image (`no such file or directory`); the agent must be built with `CGO_ENABLED=0`. *Not fixed in the tooling beyond noting it here; a `make agent-image` that builds then packages would remove the trap.*
2. **A new replica did not get the tenant's tables.** The operator logs that it migrated the schema but the tables a tenant created in `default` (database engine Atomic, not Replicated) were missing on the new replica, so reads and writes that landed on it failed with `UNKNOWN_TABLE`. First lifecycle run: during the upgrade, **1008 of 1247 reads and 133 write attempts failed with code 60**, writes paused 4 m 36 s, because the third replica had no table and the quorum needed it. Fixed by the cell agent copying the schema (databases, tables, views, functions that every other healthy replica has) to a replica that has just started (`internal/agent/schemasync.go`, unit-tested planner). With it: the same step had 0 failed attempts and the added replica held every batch. *Side effect seen once:* a tenant `CREATE TABLE … ON CLUSTER` straight after a `DROP … ON CLUSTER SYNC` on a replica younger than an hour collided with the sync (`TABLE_ALREADY_EXISTS`); the window is one hour after a replica starts and only for objects every other replica still has.
3. **Updates can no longer be reported done while pods run the old spec.** The operator's status carries no generation and says `Completed` for a while after a change. `Observe` now compares the pod facts (image, CPU, memory, Ready) and each replica's `SELECT version()` with the desired spec, so an update finishes when the pods actually run it. Covered by stale-`Completed` unit tests.
4. **An incremental backup can only be restored if the restore knows its chain.** The backup records its base by the *name* of a named collection; the restore failed with `There is no named collection …_base`. Fixed with a deterministic collection per base (`basalt_base_<id>`) and a restore job that creates one for every ancestor. Overriding the URL of one collection is not possible for the platform's own user without the grant to see its secrets.
5. **`CREATE OR REPLACE NAMED COLLECTION` is not valid** on 26.8; the job drops and creates.
6. **The scratch project name `_basalt-verify` is not a valid Kubernetes label value**, so the first scheduled verification could never create its namespace (agent log: `Invalid value … a valid label must be an empty string or consist of alphanumeric characters`). Renamed `basalt-verify`. Found by the scheduler firing on its own.
7. **The fixture store signs for one region.** ClickHouse signs requests for `us-east-1`; Garage returned 400 to any other region. The fixture is set to `us-east-1`; a `region` key in the named collection did not change what ClickHouse signed.
8. **Image housekeeping on the control nodes broke the cell once.** `crictl rmi --prune`, which I ran to recover disk, removed unused images the control nodes would need after a VM restart (local-path helper, Envoy, operator); pods were `ImagePullBackOff` or volumes unbound until I reloaded or rescheduled them. Registry pulls were rate-limited (HTTP 429) at the time. Local environment only.
9. **A gVisor sandbox can hang**: on `bk1` replica 0, `kubectl exec` failed with `runsc did not terminate successfully`, the pod stuck `Terminating` for hours and blocked the namespace deletion (and its capacity release) until a forced delete. Related, seen once on another run: a replica returned `Too many open files` (limit 20 000) to an insert after many hours of test churn. Neither is understood; I did not reproduce them on demand. They are kind/gVisor findings to carry into M6, not changes to the product.
10. **Test hygiene:** backups of a cluster that is deleted stay in the store until retention (now expired by `BackupsToExpire`; before the fix, the newest backup of a deleted cluster was never expired).

## Not verified, or only partly

- **STS credential minting.** The session-policy builder is unit-tested; `AssumeRole` was never called against a real STS (the Garage fixture has none, and I had no AWS account). The local run uses one static key for the whole store, stored in the secret store and not scoped per cluster. **The "credentials scoped to one cluster's prefix" property is therefore designed and unit-tested but not demonstrated.**
- **Object-store features of a real cell:** SSE-KMS, the VPC-endpoint-only bucket policy, the S3 endpoint allow-list for egress by CIDR. Local fixture only; M6.
- **Multi-shard backups** and their cross-shard consistency. Clusters are single-shard so far.
- **Restoring the backups of a deleted cluster** through the API. They are kept until retention, but the endpoints require the cluster to exist.
- **Rollback of a failed update under load.** The rollback path is unit-tested (workflow with a fake agent) and a capacity-refused update was exercised live in earlier work, but I did not make a real update fail under the verifier.
- **`insert_quorum = 0` comparison** (above), and a **lifecycle run with the quorum disabled** to show the verifier detects a loss: not run.
- **A downgrade, a shard change, a storage growth on a local cell** are refused by validation (unit-tested); not exercised live.
- **The M4 stability suite was not re-run** on the M5 render. The M5 render changes are the NetworkPolicy egress rule and `remote_url_allow_hosts` for the backup target; the attack suite (below) covers both.
- **Restore verification at scale.** The scratch cluster is size `t-1` (one replica); sampling of large tables by partition, and verification load on the cell, are not tested.
- The promtool alert-threshold tests from M4 are still waiting on your approval to install promtool.

## Decisions I need from you

1. **Should the platform make `insert_quorum = 'auto'` the default for tenants?** Pros: no acknowledged insert is lost across a single-replica failure, which is the guarantee the product is sold on. Cons: with 2 replicas every write waits for both and writes pause during any restart (the lifecycle run shows 7 to 49 s pauses); clients must retry. It can be done by pinning it in the default profile as a default (not a constraint), so tenants can still lower it. Until you decide, the platform default is ClickHouse's `0` and the guarantee is opt-in per client.
2. **Garage** (AGPL-3.0, test fixture only) was deployed with your approval; it is recorded in `docs/dependencies.md`. For real cells the first cloud decision is which object store and region to use.
3. Still open from Phase 0: product name and domain, LLM providers, optimizer licence, AWS account.

## Environment incidents (local only)

- The VM restarted several times during M5; `hack/vm-recover.sh` brought the stack back each time. The disk filled repeatedly (Go build caches in `/tmp` and `~/.cache`, containerd layers of the agent image on the control nodes, image copies). Go builds now use `GOCACHE`/`GOTMPDIR` on `/dev/shm`; unused images on the control nodes were pruned (see finding 8).
- The port-forward from the host to the test object store dies when the store pod restarts, which stalled a backup's `FinishBackup` step for several minutes until it was restarted. The workflow resumed by itself (the activity retries); the production path (a VPC endpoint) has no such hop.

## Regression

- **M3 attack suite: 99/99 subtests, 9/9 tests**, on clusters rendered with the backup target configured (so the server pods have the egress rule to the object store and `remote_url_allow_hosts` names its host). The tenant-facing statements to reach the store are also in the M5 backup test (7 refused).
- **M1: 31/31.**
- **M2: 53/53 on the second run.** The first run failed 5 checks (all about the dedicated cluster) because the ClickHouse server image was missing from the three free-pool nodes, the result of my earlier image clean-up: the pods sat in `ImagePullBackOff` ([first run, kept](evidence/m5/m2-regression-first-run-missing-image.log)). I copied the image to those nodes, deleted the leftover clusters, and the rerun passed everything. A product note from this: a dedicated cluster whose image cannot be pulled shows `CREATING` until the convergence timeout, with the reason in `stateReason`; that worked as designed.
- `go build ./...`, `go vet ./...` and `go test ./...` pass; the OpenAPI contract test covers the 8 new routes.
- Not re-run: the M4 stability suite (see "Not verified").
