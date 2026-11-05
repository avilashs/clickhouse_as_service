# Runbook: BasaltDiskUsageHigh / BasaltDiskUsageCritical

**Alerts:** the data volume is more than 70% used for 10 minutes (ticket), or more than 85% for 5 minutes (page). Design §10 "Disk full", failure mode F-4.

## What the platform does by itself

- ClickHouse stops accepting writes **before** the volume is full: `keep_free_space_bytes` on the `default` disk is 5% of the volume (128 MiB to 2 GiB; 16 to 64 MiB in local cells). Writes then fail with `NOT_ENOUGH_SPACE` (code 243). **Reads keep working and the pod stays up.** (`TestDiskFull`)
- A **balloon file** of the same size, `/var/lib/clickhouse/.basalt-balloon`, is allocated on the volume. Deleting it hands that space back immediately.

## Steps

1. See where the space went (run in the pod; the commands need the cluster's admin password):

   ```sql
   SELECT database, table, formatReadableSize(sum(bytes_on_disk)) AS size, count() AS parts
   FROM system.parts WHERE active GROUP BY database, table ORDER BY sum(bytes_on_disk) DESC LIMIT 10;
   SELECT name, formatReadableSize(free_space) AS free, formatReadableSize(keep_free_space) AS reserve FROM system.disks;
   ```

2. Tables in `system` that are large mean a TTL is missing or merges are stuck. Check `docs/runbooks/stuck-mutation.md`, then `SELECT name, engine_full FROM system.tables WHERE database = 'system' AND name LIKE '%\_log'`; every row must carry a `TTL`.

3. **Expand the volume** (cloud cells): raise `storageGiB` through the API. An EBS volume can be modified about once every 6 hours, so start early; this is not a way out of a fast fill.

4. **If writes are already refused and the owner needs room now**, release the balloon, then free real space and put the balloon back:

   ```bash
   kubectl -n ch-<cluster> exec chi-<cluster>-default-0-0-0 -c clickhouse -- rm /var/lib/clickhouse/.basalt-balloon
   ```

   This gives back one reserve's worth of free space, which is enough for the owner to `DROP` or `TRUNCATE` data. Do not leave the cluster in this state: the next restart recreates the balloon only if the volume has room.

5. Never delete anything under `/var/lib/clickhouse/store` by hand. Detached parts (`/var/lib/clickhouse/data/*/*/detached`) and `shadow` directories can be removed once the owner agrees.

6. While the volume is almost full, `SYSTEM STOP MERGES` on the largest table stops merges from needing temporary space; run `SYSTEM START MERGES` afterwards.

## Notes for the local cell

On kind, ClickHouse reports the **host's** disk (hundreds of GB), not the 5 GiB volume, so these alerts do not fire there. `TestDiskFull` puts a replica's data directory on a small tmpfs to exercise the behaviour above.
