# Runbook: BasaltReplicaReadonly / BasaltNoKeeperSession / BasaltReplicaLagging

**Alerts:** a replica has read-only tables (`ReadonlyReplica` above 0) or no Keeper session for 2 minutes (page), or a replica is more than 5 minutes behind (ticket). The API shows the cluster as `DEGRADED` with the reason (for example `chi-…-0-0 cannot reach Keeper`). Failure mode F-5.

## What to expect (`TestKeeperQuorumLoss`, two of three Keeper nodes stopped)

| What | Behaviour |
|---|---|
| `SELECT` | works on every running replica |
| `INSERT` into a replicated table | **not acknowledged**: refused with `TABLE_IS_READ_ONLY` (242) or `KEEPER_EXCEPTION` (999). With ClickHouse's default retries the client waits and normally hits its own timeout instead of an error; nothing is ever acknowledged without quorum |
| DDL, `CREATE USER` | fails (access storage and DDL both live in Keeper) |
| A server pod that **restarts** during the outage | cannot start: with replicated access storage the server exits at startup when Keeper is unreachable (upstream gives it two attempts). The pod's `wait-for-keeper` init container holds it in `Init:0/2` with no restarts and no crash loop, and the server container starts when the Keeper Service has a ready endpoint |
| After quorum returns | in the test, all three Keeper nodes were ready after 11 s and the waiting replica was serving 1 m 15 s later |
| The API | `DEGRADED` within one 10-second health sync of the agent reporting it; `ACTIVE` again after recovery |

## Steps

1. Which Keeper nodes are down, and why:

   ```bash
   kubectl -n ch-<cluster> get pods -l clickhouse-keeper.altinity.com/chk=<cluster> -o wide
   kubectl -n ch-<cluster> describe pod chk-<cluster>-keeper-0-<n>-0 | tail -20
   ```

   Keeper pods run on the control pool, one per node and zone. A cluster keeps quorum with 2 of 3.

2. Bring the missing Keeper pods back (node, volume or scheduling problem). Do **not** delete a Keeper's PVC: it holds the raft log. If a Keeper pod is merely Pending, check the control pool has room (`kubectl describe node -l basalt.io/pool=control`).

3. Do not restart the server pods. A running replica recovers by itself when its Keeper session comes back (`ReadonlyReplica` returns to 0); a restarted one waits in `wait-for-keeper`.

4. If a replica stays behind after quorum returns: `SYSTEM SYNC REPLICA <db>.<table>` and the queue query in `docs/runbooks/stuck-mutation.md`.

## Startup time

A server can legitimately need several minutes to start (loading tables, syncing with Keeper), and much longer under gVisor on a loaded node. The server container has a **startup probe** of 10 minutes (5 s x 120). Before it existed, the operator's liveness probe killed servers that needed more than about 90 seconds, in a loop (seen in M4, twice: after a VM restart and while recovering from this exact outage). If a server is killed for a failed `startupProbe`, it took more than 10 minutes: look at `system.text_log` and `kubectl logs --previous` rather than raising the limit.
