# Runbook: a backup or a restore verification failed

**Signals:** `GET …/backups` shows a backup `FAILED` (with `error`), a backup with `verified: false` long after it completed, or `GET …/backups/<id>/verifications` shows `FAILED` with `problems`. The worker logs `backup failed` or `scheduled restore verification started` without a later pass.

## Backup FAILED

1. `error` carries the server's message, with the credentials removed. Common causes seen so far:
   - `S3_ERROR … HTTP response code: 400` on `.backup`: the store rejects the request's signature or region (ClickHouse signs for `us-east-1`). Check the store's configured region.
   - `ACCESS_DENIED … SHOW NAMED COLLECTIONS SECRETS`: the job tried to override a named collection's URL; collections must be created per prefix (`basalt_job_*`, `basalt_base_<id>`).
   - `NOT_ENOUGH_SPACE` on the source: see `disk-full.md`.
2. A failed backup leaves no usable data; the workflow deletes what it wrote. The scheduler tries again at the next interval. An agent that restarted while a job ran reports it failed; nothing is half-written to a backup that is later used.
3. If the control plane cannot reach the store (`FinishBackup` retries in the worker log), the backup stays `RUNNING` until it can; the workflow resumes by itself.

## Verification FAILED

1. `problems` says what differed: a table missing from the restore, `CHECK TABLE` errors, or a row count or hash that does not match the source. **Treat the backup as unusable** until proven otherwise: restore it yourself into a new cluster, and take a new backup meanwhile.
2. The scratch cluster is deleted whatever the outcome. If one is left (`kubectl get ns | grep ch-`, a cluster in project `basalt-verify`), delete it through the API.
3. Verification runs one at a time per cell. A verification that never finishes usually means the scratch cluster could not be scheduled (capacity).
