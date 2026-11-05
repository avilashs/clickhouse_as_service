# ADR-0006: Backup and restore mechanism

Status: Accepted, implemented in M5 (revision 3, 2026-11-05). Date: 2026-11-04

## Options
| | Native `BACKUP`/`RESTORE` | `clickhouse-backup` (Altinity, MIT) |
|---|---|---|
| Incremental | `SETTINGS base_backup = S3(...)` | Yes (`--diff-from`) |
| Multi-shard | `ON CLUSTER`; per-shard snapshot at different instants | Per-host invocation; same consistency limit |
| Object storage | `S3(...)` destination, or a backup disk | S3, GCS, Azure, etc. |
| Orchestration | SQL from our workflow; async with `ASYNC` + `system.backups` polling | Sidecar with REST API, own scheduling and retention |
| Retention | Ours | Built in |
| Extra component in pod | None | Sidecar per server pod, with access to data dir and credentials |
| Restore to new cluster | `RESTORE … FROM S3(...)` on the new cluster (`ON CLUSTER`) | Yes |

## Decision
**Native `BACKUP`/`RESTORE`**, orchestrated by Temporal:
* There's no extra sidecar with filesystem and credential access inside a pod that runs hostile SQL.
* Scheduling, retention and verification belong to the control plane anyway, so that we can meter them and show them in the API. `clickhouse-backup`'s built-in scheduler would duplicate that.
* It's upstream and versioned with the server, which keeps compatibility simple.

Mechanics: `BACKUP DATABASE … ON CLUSTER default TO S3('<prefix>/<id>', …) SETTINGS base_backup = …, async = 1`, then poll `system.backups` on every host. Weekly full, daily incremental, and on-demand. A chain is deleted only when no live backup references it.

Isolation: per-cell bucket, per-cluster prefix, bucket policy that allows access only via the VPC endpoint, SSE-KMS. Customers don't get `BACKUP`/`RESTORE` grants (tested: denied).

Credentials (rev 2): **no ambient cloud credentials in server pods.** Rev 1's Pod Identity role conflicted with "no SA token" and with the link-local egress deny, and would have exposed credentials to tenant SQL. Per operation, the workflow mints STS credentials with a session policy scoped to the cluster's prefix. It writes them into a provider-only named collection `basalt_backup` (`url, access_key_id, secret_access_key, session_token`), runs `BACKUP … TO S3(basalt_backup, '<id>')`, then drops the collection. Tested on 26.8.34: the session token is sent as `x-amz-security-token`; tenants can't use or list the collection; secrets are shown as `[HIDDEN]` in `query_log`. The positional `S3(url, key, secret)` form accepts only 1 or 3 arguments, so a session token needs the named collection.

Consistency: documented as per-shard snapshots taken at slightly different moments; no cross-shard atomicity.

Restore: always into a **new** cluster (`POST …/clusters:restore`), restoring the backup point.

Verification: daily schedule; restore samples into scratch clusters; compare counts and hashes recorded at backup time; a backup counts as `VERIFIED` only after that passes.

## Revisit if
Native incremental backups across many small parts prove too slow or expensive in M5, or if `system.backups` doesn't give enough progress information to drive the workflow reliably.

## Implementation notes (M5, what the code does and what was found)

* **Who runs it.** The control plane never moves backup data. A workflow (`BackupCluster`, `RestoreCluster`, `VerifyBackup`) writes a *job* row; the cell agent, which already reaches every server with the provider's client certificate, picks it up from the same poll that carries the desired state, runs the SQL on the cluster's first replica and reports the result. Jobs are not part of the desired-state hash. An agent that restarts mid-job reports it failed rather than starting a second backup into a half-written prefix.
* **Statements.** `BACKUP DATABASE d1, DATABASE d2 TO S3(basalt_job_<id>) [SETTINGS base_backup = S3(basalt_base_<base id>)] ASYNC`, then poll `system.backups`. Not `ON CLUSTER`: the tables are replicated, so one replica's snapshot is the table; before it the agent runs `SYSTEM SYNC REPLICA` so the backup holds what the cluster had acknowledged. Users, roles and grants are not backed up (Basalt owns them); databases only.
* **Credentials.** One named collection per job (`CREATE NAMED COLLECTION`; `CREATE OR REPLACE` is not valid for named collections), created and dropped by the provider user, the only one with `named_collection_control`. Tested: `customer_admin` sees no named collection while a backup runs (polled every 150 ms through the job), cannot create one, cannot `BACKUP`/`RESTORE` to S3 or a disk, cannot use `s3()`/`url()` against the store.
* **An incremental backup records the *name* of its base's collection.** A first version used one collection and overrode its `url` for the base, which needs the grant to see the collection's secrets (`SHOW NAMED COLLECTIONS SECRETS`); a restore then failed with `There is no named collection basalt_job_..._base`. Now each base has a collection named from its id (`basalt_base_<id>`), and a restore job creates one for every ancestor in the chain. Unit-neutral but important: **a backup cannot be restored by anything that does not know its chain.**
* **Credential scope.** `backup.ScopePolicy` builds the session policy (write under the backup's own prefix; read under its chain; list only those prefixes). `backup.STS` calls AssumeRole with it. **Only the policy builder is tested. No STS endpoint was available (the fixture store has none), so credential minting against a real STS is unexercised.** The local fixture uses one static key kept in the secret store and resolved when the job is served; it is not scoped.
* **Retention** is by the control plane: a backup is deleted when it is older than the retention period, nothing is built on it, and it is not the newest completed backup of a live cluster; its objects are removed by prefix and only then is it marked deleted. A chain unwinds from its newest member. Backups of a deleted cluster age out too.
* **Network.** The server NetworkPolicy gains one egress rule to the cell's backup target (an in-cluster Service on the fixture; CIDRs of the S3 endpoint in the cloud) and `remote_url_allow_hosts` names that one host, instead of the deny-all placeholder. Keeper pods get no such rule. Tenants cannot use the door: they hold no grant on the S3 functions.
* **Verification** (`VerifyBackup`): restore into a scratch cluster in project `basalt-verify` (no tenant token maps to it), run `CHECK TABLE` on each table, compare rows and `sum(cityHash64(*))` with the fingerprints the backup job took just before and just after the backup, and delete the scratch cluster in every outcome. A table nobody wrote to during the backup must match exactly; one that was written to must be within the row counts before and after (inserts only add). The comparison is `CompareFingerprints`, unit-tested including corruption, missing and extra tables.
* **Findings about the store.** ClickHouse signs requests for region `us-east-1`; Garage answers 400 to any other region than the one it is configured with, so the fixture is configured with `us-east-1`. Passing `region` in the named collection did not change what ClickHouse signed in our test, so a cell whose store is in another region needs either a store that accepts it or more work here.
* **Not done:** cross-shard consistency (single-shard clusters only so far); SSE-KMS and the VPC-endpoint-only bucket policy (cloud, M6); restoring the backups of a *deleted* cluster through the API (they are kept until retention, but the endpoints require the cluster to exist).
