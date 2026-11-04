# ADR-0001: Platform stack

Status: Proposed. Date: 2026-11-04

## Context
No platform exists in this repo (or in `kafka_as_service`). We need an API, an authoritative store, a durable workflow engine, cells with a pull-based agent, and an `Engine` interface that a future Kafka engine can also implement.

## Decision
| Concern | Choice | Why |
|---|---|---|
| Language | **Go** | Kubernetes client libraries, operator CRD types, Temporal Go SDK, single static binaries for the agent. |
| API | REST plus an OpenAPI 3.1 spec, written first; server stubs generated with `oapi-codegen` | The spec is the contract; generated types prevent drift. |
| Store | **PostgreSQL** (≥ 16) | Transactions for capacity reservation (`SELECT … FOR UPDATE` on `cell_capacity`) and idempotency keys. |
| Workflows | **Temporal** (MIT), self-hosted, using Postgres persistence | Durable execution, deterministic replay, activity retries with retryable vs. non-retryable errors, the saga/compensation pattern, schedules (backups, restore verification). The workflow ID `cluster/<id>` is the per-resource lock: `WorkflowIDReusePolicy` and `WorkflowIDConflictPolicy=Fail` mean only one mutation runs per cluster. |
| Secrets | AWS Secrets Manager (control plane) → Kubernetes Secrets in the cell (KMS envelope encryption) | No runtime dependency from the data plane on the control plane. |
| Cell agent | Outbound-only mTLS long-poll; server-side apply under field manager `basalt-agent` | No inbound access to cells; idempotent apply. |
| Local dev | kind + `temporal server start-dev` + Postgres container | M1 acceptance runs fully locally. |

### Alternatives considered
* **Argo Workflows / plain controller reconcile loops** for orchestration. Reconcilers are good at convergence but bad at long, gated, ordered sequences with compensation (rolling with per-step gates, restore verification). Temporal fits better. We still use reconcilers *in-cell* (the operator).
* **Push from control plane to cell (kubeconfig in the control plane)**: rejected. It's an inbound attack path, and it couples availability.
* **Crossplane / Cluster API for cells**: possible later for cell provisioning; out of scope for M1. Cells are provisioned with Terraform/OpenTofu (MPL-2.0), run by operators, not by tenants.

## Consequences
* Temporal is a significant operational dependency (its own DB schema and history service). If Temporal is down, mutations stall but data planes keep serving (F-11).
* Workflow code must be deterministic. All I/O goes in activities, and activities are idempotent (SSA, upserts keyed by operation ID).
