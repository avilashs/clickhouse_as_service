# Runbook: BasaltPartsApproachingLimit / BasaltInsertsDelayed / BasaltInsertsAboutToBeRejected

**Alerts:** the largest partition of any table holds more than 2100 parts (70% of the limit), inserts are being slowed (`DelayedInserts` above 0), or a partition holds more than 2700 parts. Failure mode F-7.

## Limits (every cluster)

| Setting | Value | Effect |
|---|---|---|
| `parts_to_delay_insert` | 1000 | inserts into a partition with this many active parts are delayed, up to `max_delay_to_insert` (1 s) each, growing with the count |
| `parts_to_throw_insert` | 3000 | inserts are refused with `TOO_MANY_PARTS` (code 252) |
| `max_parts_in_total` | 100000 | per table |

## Cause

Almost always many tiny inserts: each synchronous insert makes one part, and merges cannot keep up on a small server. In `TestInsertStormIsContainedAndNeighbourUnaffected`, 5000 single-row inserts with merges stopped slowed inserts to 2.5 s each (the delay is applied) and a partition reached 2699 parts.

## Steps

1. Find the table and partition:

   ```sql
   SELECT database, table, partition, count() AS parts FROM system.parts WHERE active
   GROUP BY database, table, partition ORDER BY parts DESC LIMIT 10;
   ```

2. Check merges are running: `SELECT count() FROM system.merges` and `SELECT value FROM system.metrics WHERE metric = 'BackgroundMergesAndMutationsPoolTask'`. If the owner ran `SYSTEM STOP MERGES`, ask them to run `SYSTEM START MERGES`.

3. Tell the owner to batch inserts (thousands of rows each) or to use asynchronous inserts. The platform pins `wait_for_async_insert = 1`, so an acknowledged async insert is durable; the `async_insert_busy_timeout_max_ms` setting controls how long a small insert waits to be batched.

4. Once the count falls below 1000, inserts are no longer delayed. Merges on a 1-vCPU server work through roughly 600 parts a minute; do not raise `parts_to_throw_insert`, it only moves the failure to a slower and larger one.
