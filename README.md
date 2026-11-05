# Basalt (working name): a managed service for ClickHouse®

Open-source, self-hostable platform: tenants request ClickHouse clusters through an API and connect with standard ClickHouse clients.

**Status: M5 (lifecycle and backup) complete, running locally on kind.** Reports with test results and known limitations: [M1](docs/m1-report.md) (engine on platform), [M2](docs/m2-report.md) (shared and dedicated tiers, admission, audit, gVisor), [M3](docs/m3-report.md) (tenant attack suite, NetworkPolicy enforcement), [M4](docs/m4-report.md) (stability controls, Keeper loss, disk full, insert storms), [M5](docs/m5-report.md) (scale, upgrade, add replica under load; backup, restore, restore verification). What tenants can and cannot do: [security guide](docs/security-guide.md).

* [Design](docs/clickhouse-design.md) · [ADRs](docs/adr/) · [Dependencies](docs/dependencies.md) · [API (OpenAPI)](api/openapi.yaml)
* [Phase 0 review](docs/review-2026-11-04.md) and [lab evidence](docs/evidence/2026-11-04-lab.md)

## Quick start (local cell on kind)

Requirements: Linux with Docker, 4+ CPUs, 16 GB RAM, about 30 GB free disk, root (the cell raises inotify limits). Install `kind` (v0.29), `kubectl` (1.33), `helm` (v3), the `temporal` CLI (v1.10), `clickhouse` (the official static binary, for the client), Go 1.26 and gcc. The cell builds gVisor from source on first run (`hack/build-gvisor.sh`).

Bring up the cell: 10 kind nodes in 3 zones (control, shared under gVisor, and free nodes for dedicated clusters), the Altinity operator, Envoy Gateway and the placement admission policies.

```bash
hack/cell-up.sh
```

Start the control plane (Postgres, Temporal, API, worker) and deploy the cell agent:

```bash
hack/dev-up.sh
```

Load the generated tokens into your shell:

```bash
source /var/lib/basalt/env
```

Create a cluster:

```bash
curl -s -X POST http://127.0.0.1:8080/v1/projects/demo/clusters -H "Authorization: Bearer $BASALT_TENANT_TOKEN" -H "Idempotency-Key: $(uuidgen)" -H 'Content-Type: application/json' -d '{"name":"orders","size":"t-2"}'
```

Poll the cluster until `state` is `ACTIVE` (about 3 minutes on kind). Use the `clusterId` from the response above:

```bash
curl -s http://127.0.0.1:8080/v1/projects/demo/clusters/<clusterId> -H "Authorization: Bearer $BASALT_TENANT_TOKEN"
```

The response's `endpoint.host` is the hostname to connect to, and `endpoint.caCertificate` is the CA to verify it against. `GET …/initialCredentials` returns the `admin` user's password. To connect on kind, follow what `test/e2e/m1.sh` does: port-forward the shared gateway and resolve the cluster hostname locally.

A dedicated cluster (one server per node, on nodes owned by the cluster) takes `tier`, `shards` and `replicasPerShard`:

```bash
curl -s -X POST http://127.0.0.1:8080/v1/projects/demo/clusters -H "Authorization: Bearer $BASALT_TENANT_TOKEN" -H "Idempotency-Key: $(uuidgen)" -H 'Content-Type: application/json' -d '{"name":"analytics","size":"dt-1","tier":"dedicated","shards":1,"replicasPerShard":2}'
```

Check placement invariants at any time:

```bash
bin/basalt-audit --once
```

Run the acceptance suites:

```bash
test/e2e/m1.sh
```

```bash
test/e2e/m2.sh
```

Run the M3 tenant attack suite (it needs two ACTIVE clusters in projects `demo` and `rival`):

```bash
test/attack/run.sh <cluster-in-demo> <cluster-in-rival>
```

Run the unit and integration tests (the store tests need the Postgres started by `dev-up.sh`):

```bash
BASALT_TEST_PG_DSN="postgres://basalt:basalt@127.0.0.1:5432/basalt?sslmode=disable" go test ./...
```

## Layout

| Path | What |
|---|---|
| `cmd/basalt-api` | Tenant REST API and the cell-agent API |
| `cmd/basalt-worker` | Temporal worker: cluster lifecycle workflows, pending-operation sweeper |
| `cmd/basalt-agent` | Cell agent (runs in the cell; pulls desired state, applies, reports, manages dedicated node pools) |
| `cmd/basalt-audit` | Placement auditor: CLI and in-cell service exporting `basalt_placement_violations` |
| `internal/engine/clickhouse` | Render and Observe for ClickHouse on the Altinity operator |
| `internal/store` | PostgreSQL desired state, operations, capacity, desired/observed cell state |
| `deploy/` | kind config, operator and gateway installs, admission policies, agent and audit manifests, alerts |
| `test/e2e`, `test/lab`, `test/spike` | Acceptance suites; Phase 0 ClickHouse behaviour checks; gVisor benchmark |

ClickHouse is a registered trademark of ClickHouse, Inc. This project is not affiliated with or endorsed by ClickHouse, Inc.
