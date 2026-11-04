# ADR-0004: Keeper topology and cost

Status: Proposed. Date: 2026-11-04

## Decision
* Each tenant cluster has its own `ClickHouseKeeperInstallation` on the provider-owned `control` pool. Tenants can't run SQL there; Keeper ports accept connections only from that cluster's server pods (NetworkPolicy plus mTLS).
* **HA sizes:** 3 Keeper replicas across 3 zones (required anti-affinity by hostname, required spread by zone), PDB `maxUnavailable: 1`.
* **`dev-1` (no-HA size):** 1 Keeper replica. Its server is single-replica, so a 3-node quorum would add no availability.
* Resources: `memory` requests = limits (1 GiB; dev 0.5 GiB); `cpu` request 0.25 and limit 0.5. The control pool is the only place CPU burst is allowed: Keeper is latency-sensitive but light on average.
* Keeper settings: `snapshot_distance`, `snapshots_to_keep`, `reserved_log_items` tuned for small disks; `four_letter_word_white_list` = `mntr,ruok,srvr,stat,conf,isro` for health checks over the provider network only.
* Not shared between tenants in v1. Keeper isn't embedded in server pods for HA sizes (a hostile query crashing the server would take a voter with it).

## Cost
See design §7.3. At about $37/month per HA tenant, Keeper is about **33%** of the `s-2` compute cost, 17% of `s-4` and 8% of `s-8`. A single-node dev Keeper is about 24% of `dev-1`. Pricing for small sizes must include it. (Rev 1 said 50% and 12%; that was an arithmetic error, now corrected.)

## Revisit triggers
If the share of tenants on `dev-1`/`s-2` makes Keeper a dominant cost line, evaluate (in order): denser Keeper pods (0.1 vCPU requests), then a shared Keeper with per-tenant chroot, ACLs and per-tenant request rate limits. The latter needs a fresh threat-model review.
