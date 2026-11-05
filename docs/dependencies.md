# Dependencies

Every third-party dependency must have a licence compatible with Apache-2.0 **and** usable by a hosted service. Licences listed below were checked from the project's LICENSE file where marked (✓). The rest are from my knowledge and must be re-checked before each is first pulled in.

| Dependency | Purpose | Licence | Checked | Notes |
|---|---|---|---|---|
| ClickHouse (server, Keeper), stock upstream images | Database | Apache-2.0 | – | No fork. LTS 26.8 first. Trademark: we describe ourselves as "for ClickHouse". |
| Altinity clickhouse-operator `release-0.27.x` | Operator | Apache-2.0 | ✓ (2026-11-04) | ADR-0002 |
| ClickHouse Inc. clickhouse-operator | Evaluated, not used | Apache-2.0 | ✓ (2026-11-04) | `v1alpha1`; revisit |
| Kubernetes / EKS | Orchestration | Apache-2.0 | – | |
| kube-router v2.11.2 (cloudnativelabs), firewall-only mode | NetworkPolicy enforcement in local kind cells only (kindnet ignores NetworkPolicy) | Apache-2.0 | ✓ (2026-11-04) | Image pulled by digest via mirror.gcr.io and slimmed by `hack/build-kube-router.sh`. kube-network-policies was tried first, but needs `CONFIG_NFT_QUEUE`, which the M3 VM kernel lacks. Not used in cloud cells. |
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
| minio-go (`github.com/minio/minio-go/v7`) | Object-store client in the control plane: STS AssumeRole for per-backup credentials, listing and deleting a backup's objects (retention). Backup data never passes through it. | Apache-2.0 | – | ADR-0006. The library is used, not the MinIO server. The STS path is unit-tested for the session policy it builds but **not exercised against a live STS** (the fixture store has none). |
| Garage (`dxflrs/garage:v2.5.0`) | S3-compatible object store, **test fixture only** on the local kind cell, run unmodified as a separate service | **AGPL-3.0** | – | Not linked, not modified, not shipped to tenants. Chosen because MinIO's images are no longer published and SeaweedFS's image is 1 GB. A real cell uses its cloud's object store. |
| query_optimizer (`avilashs/query_optimizer@0d86d10`) | AI query advisor, run as a CronJob | MIT (declared in `pyproject.toml`; **no LICENSE file**, Q2b) | – | ADR-0008 |
| pydantic, PyYAML | optimizer dependencies | MIT | – | |
| requests | optimizer dependency | Apache-2.0 | – | |
| clickhouse-connect | optimizer ClickHouse driver | Apache-2.0 | – | |
| Envoy AI Gateway | LLM gateway (candidate) | Apache-2.0 | – | ADR-0008 §6; to evaluate in M-opt |
| LiteLLM proxy | LLM gateway (alternative) | MIT core; enterprise features under a separate licence | – | Only the MIT core would be usable |
| LLM providers (Azure AI Foundry, Anthropic, OpenAI, …) | External services, not libraries | Commercial terms and DPAs | – | Q2a: which may process tenant data |

Excluded on licence grounds: none so far. Licences that would be rejected for linked or bundled code: SSPL, BSL/BUSL, Elastic License, Commons Clause.
