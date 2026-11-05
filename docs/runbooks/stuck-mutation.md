# Runbook: BasaltMutationRunningLong / BasaltMergePoolSaturated / BasaltReplicationQueueBacklog

**Alerts:** a mutation has been running for over an hour, the merge/mutation pool has been full for 30 minutes, or a replica's replication queue holds more than 500 entries. Failure mode F-17.

**Never kill a mutation automatically.** A mutation that looks stuck is usually waiting for a part that is being merged, a replica that is behind, or a long-running query that holds the part.

## Steps

1. List unfinished mutations (the exporter does not expose them, so this query is the source of truth):

   ```sql
   SELECT database, table, mutation_id, command, create_time, parts_to_do_names, latest_fail_reason, latest_fail_time
   FROM system.mutations WHERE NOT is_done ORDER BY create_time;
   ```

   `latest_fail_reason` says why it is not progressing.

2. Check the replication queue and the pool:

   ```sql
   SELECT database, table, type, create_time, num_tries, last_exception FROM system.replication_queue ORDER BY create_time LIMIT 20;
   SELECT metric, value FROM system.metrics WHERE metric IN ('BackgroundMergesAndMutationsPoolTask', 'BackgroundMergesAndMutationsPoolSize');
   ```

   A pool that is full for 30 minutes on a small server (the pool is 2 to 4 tasks on `t-2`/`s-2`) means merges are CPU-bound; this is capacity, not a fault.

3. If a replica is behind (`ReplicasMaxAbsoluteDelay`), see `docs/runbooks/keeper-quorum-loss.md`.

4. Only with the owner's agreement: `KILL MUTATION WHERE mutation_id = '...'`. The platform's own optimizer rolls back a skip-index mutation this way (ADR-0008); nothing else does it automatically.
