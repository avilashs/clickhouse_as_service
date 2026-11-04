# Basalt (working name): a managed service for ClickHouse®

Open-source, self-hostable platform: tenants request ClickHouse clusters through an API and connect with standard ClickHouse clients.

**Status: M1 (engine on platform), running locally on kind.** See [docs/m1-report.md](docs/m1-report.md) for what works, the test results and the known limitations.

* [Design](docs/clickhouse-design.md) · [ADRs](docs/adr/) · [Dependencies](docs/dependencies.md) · [API (OpenAPI)](api/openapi.yaml)
* [Phase 0 review](docs/review-2026-11-04.md) and [lab evidence](docs/evidence/2026-11-04-lab.md)

## Quick start (local cell on kind)

Requirements: Linux with Docker, 4+ CPUs, 16 GB RAM, about 25 GB free disk. Install `kind` (v0.29), `kubectl` (1.33), `helm` (v3), the `temporal` CLI (v1.10) and `clickhouse` (the official static binary, for the client), plus Go 1.26.

Bring up the cell: 7 kind nodes in 3 zones, the Altinity operator, and Envoy Gateway.

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

Run the M1 acceptance suite:

```bash
test/e2e/m1.sh
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
| `cmd/basalt-agent` | Cell agent (runs in the cell; pulls desired state, applies, reports) |
| `internal/engine/clickhouse` | Render and Observe for ClickHouse on the Altinity operator |
| `internal/store` | PostgreSQL desired state, operations, capacity, desired/observed cell state |
| `deploy/` | kind config, operator and gateway installs, agent manifests |
| `test/e2e`, `test/lab` | Acceptance suite; Phase 0 ClickHouse behaviour checks |

ClickHouse is a registered trademark of ClickHouse, Inc. This project is not affiliated with or endorsed by ClickHouse, Inc.
