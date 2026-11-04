# ADR-0007: Preventing non-replicated tables

Status: Proposed; **M4 must prove the chosen mechanism by test**. Date: 2026-11-04

## Problem
`ENGINE = MergeTree` on a multi-replica cluster creates a table on one replica only. It looks fine until that replica is lost, and then the data is gone. Customers do this by accident all the time.

## Candidates (to test on 26.8)
1. **`Replicated` database engine for all customer databases.** The provider creates `default` as `Replicated('/clickhouse/databases/default', '{shard}', '{replica}')`. DDL replicates automatically. To check: (a) whether `ENGINE = MergeTree` inside a `Replicated` database is rejected, converted to `ReplicatedMergeTree`, or created locally, and which server setting controls this (e.g. `database_replicated_allow_only_replicated_engine`); (b) how to force `CREATE DATABASE` by customers to produce `Replicated` (a server default setting for new databases if one exists in 26.8; otherwise customers create databases via the API, or `CREATE DATABASE` is denied and the API provides it).
2. **`TABLE ENGINE` grants** (server setting `table_engines_require_grant = true`): grant only `ReplicatedMergeTree`, `ReplicatedReplacingMergeTree`, `ReplicatedAggregatingMergeTree`, `ReplicatedSummingMergeTree`, `ReplicatedCollapsingMergeTree`, `ReplicatedVersionedCollapsingMergeTree`, `Distributed`, `MaterializedView` targets, `View`, `Null`, `Memory`/`Set`/`Join` (with the documented caveat that they don't persist across restart), `Buffer` (caveat), `Dictionary`, `Merge`. Anything else is rejected with an access error.
3. **Engine conversion** (`convert_to_replicated` flag): an operator-side, one-off conversion of existing plain MergeTree tables. Useful as a repair tool, not as prevention.

## Proposed decision
Use **1 + 2 together**. The `Replicated` database gives customers replicated DDL, and engine grants are the hard fence. They should prefer an error over a silent conversion: conversion changes engine semantics behind their back. Candidate 3 goes in the runbook for repairs.

## Tables that bypass it anyway
Possible routes: a restore from an external backup, an engine we forgot, or a future engine. The auditor scans `system.tables` hourly for persistent non-replicated engines in customer databases and exports `basalt_nonreplicated_tables{cluster,db,table}`. The cluster gets a warning condition, which shows in the API and console as "Table X is not replicated: no HA". We don't escalate the cluster state to DEGRADED for this; it's a customer data-model issue, not a service fault. The dev size (1 replica) is exempt.

## Test (M4)
For each engine in the allowlist and a sample outside it: `CREATE TABLE` as `customer_admin` and assert accept or reject; insert on replica A, read on replica B; and `CREATE DATABASE` as a customer, then check its engine.
