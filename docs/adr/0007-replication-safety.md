# ADR-0007: Preventing non-replicated tables

Status: Proposed; mechanism tested in the Phase 0 lab, full proof in M4. Date: 2026-11-04

## Problem
`ENGINE = MergeTree` on a multi-replica cluster creates a table on one replica only. It looks fine until that replica is lost, and then the data is gone. Customers do this by accident all the time.

## What testing showed (26.8.34 LTS; `test/lab/phase0_verify.sh`, `docs/evidence/2026-11-04-lab.md`)
1. **`Replicated` database alone doesn't help.** With stock settings, `ENGINE = MergeTree` inside a `Replicated` database creates a plain `MergeTree`: unreplicated, no conversion, no warning.
2. **`database_replicated_allow_only_replicated_engine = 1`** makes such DDL fail ("Only tables with a Replicated engine or tables which do not store data on disk are allowed"); `Memory` is still allowed. It's a user-level setting. When it was set via a role, a role-less tenant user could set it back to 0. When it's set in the **server default profile**, `Replicated`-database DDL rejects plain `MergeTree` even if the session sets 0.
3. **`TABLE ENGINE` grants** (`access_control_improvements.table_engines_require_grant = true`) work in every database type. Without `TABLE ENGINE ON MergeTree` a tenant gets `ACCESS_DENIED`; with `TABLE ENGINE ON ReplicatedMergeTree` it can create replicated tables. The same mechanism also gates `executable()` (`TABLE ENGINE ON Executable`).
4. **Database engines can't be granted:** `GRANT TABLE ENGINE ON Replicated` returns "Unknown table engine Replicated", yet `CREATE DATABASE … ENGINE = Atomic` requires `TABLE ENGINE ON Atomic`. With engine grants on, tenants can only run `CREATE DATABASE x` with no engine, which gives Atomic. `default_database_engine` is obsolete.
5. Not yet tested: the `convert_to_replicated` flag for one-off repair of existing tables (M4).

## Decision
* **Primary fence: `TABLE ENGINE` grants.** `customer_admin` gets `TABLE ENGINE ON` `ReplicatedMergeTree`, `ReplicatedReplacingMergeTree`, `ReplicatedAggregatingMergeTree`, `ReplicatedSummingMergeTree`, `ReplicatedCollapsingMergeTree`, `ReplicatedVersionedCollapsingMergeTree`, `Distributed`, `MaterializedView`, `View`, `Null`, `Memory`, `Set`, `Join`, `Buffer` and `Dictionary`/`Merge`. The non-persistence of `Memory`/`Set`/`Join`/`Buffer` across restarts is documented. Anything else is rejected. We **reject rather than convert**, because conversion changes engine semantics behind the customer's back.
* **Second layer:** `database_replicated_allow_only_replicated_engine = 1` with `const` in the server default profile.
* **Databases:** the provider creates `default` as `Replicated`. Further `Replicated` databases come from `POST /v1/…/clusters/{c}/databases`, since tenants can't create that engine themselves. Tenant-created databases are Atomic, and tenants use `ReplicatedMergeTree` with `ON CLUSTER` DDL there (they hold the `CLUSTER` grant for their own cluster only). The docs explain both.
* **Repair:** a runbook for one-off conversion of legacy tables (`convert_to_replicated` flag, to be tested in M4).

## Tables that bypass it anyway
Possible routes: a restore from an external backup, an engine we forgot, or a future engine. The auditor scans `system.tables` hourly for persistent non-replicated engines in customer databases and exports `basalt_nonreplicated_tables{cluster,db,table}`. The cluster gets a warning condition, which shows in the API as "Table X is not replicated: no HA". We don't escalate the cluster state to DEGRADED for this; it's a customer data-model issue, not a service fault. The dev size (1 replica) is exempt.

## Test (M4)
The lab checks above move into the M4 suite. In addition: every engine in the allowlist (accept), a sample outside it (reject), insert on replica A and read on replica B, and the auditor detecting a table planted by the provider.
