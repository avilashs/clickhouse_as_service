# Dependencies

Every third-party dependency must have a licence compatible with Apache-2.0 **and** usable by a hosted service. Licences listed below were checked from the project's LICENSE file where marked (✓). The rest are from my knowledge and must be re-checked before each is first pulled in.

| Dependency | Purpose | Licence | Checked | Notes |
|---|---|---|---|---|
| ClickHouse (server, Keeper), stock upstream images | Database | Apache-2.0 | – | No fork. LTS 26.8 first. Trademark: we describe ourselves as "for ClickHouse". |
| Altinity clickhouse-operator `release-0.27.x` | Operator | Apache-2.0 | ✓ (2026-11-04) | ADR-0002 |
| ClickHouse Inc. clickhouse-operator | Evaluated, not used | Apache-2.0 | ✓ (2026-11-04) | `v1alpha1`; revisit |
| Kubernetes / EKS | Orchestration | Apache-2.0 | – | |
| Karpenter | Node provisioning | Apache-2.0 | – | |
| Cilium | CNI, NetworkPolicy, FQDN egress, bandwidth | Apache-2.0 (eBPF parts dual GPL/BSD) | – | Pending Q10 |
| gVisor | Sandbox | Apache-2.0 | – | ADR-0003 |
| Kata Containers | Sandbox alternative | Apache-2.0 | – | ADR-0003 |
| Envoy Gateway / Envoy | Shared gateway, egress proxy | Apache-2.0 | – | ADR-0005 |
| Gateway API CRDs | Routing API | Apache-2.0 | – | |
| cert-manager | Certificates | Apache-2.0 | – | |
| Temporal server + Go SDK | Workflows | MIT | – | ADR-0001 |
| PostgreSQL | Desired state | PostgreSQL License | – | |
| Prometheus, Alertmanager | Metrics | Apache-2.0 | – | |
| Grafana | Provider dashboards | **AGPL-3.0** | – | Used internally only, not modified and not offered to tenants. If tenant-facing dashboards come later, re-evaluate (Perses, Apache-2.0, is an alternative). |
| prom-label-proxy | Tenant metric scoping | Apache-2.0 | – | Or our own equivalent |
| oapi-codegen | API codegen | Apache-2.0 | – | |
| pgx | Postgres driver | MIT | – | |
| clickhouse-go | ClickHouse client (agent, canary, verifier) | Apache-2.0 | – | |
| kind | Local clusters | Apache-2.0 | – | |
| OpenTofu | Cell provisioning | MPL-2.0 | – | Tool only, not linked |
| clickhouse-backup | Evaluated, not used | MIT | – | ADR-0006 |
| query optimizer (yours) | Advisor | **unknown** | – | ADR-0008, Q2 |

Excluded on licence grounds: none so far. Licences that would be rejected for linked or bundled code: SSPL, BSL/BUSL, Elastic License, Commons Clause.
