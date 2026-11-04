# ADR-0006: Backup and restore mechanism

Status: Proposed. Date: 2026-11-04

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
