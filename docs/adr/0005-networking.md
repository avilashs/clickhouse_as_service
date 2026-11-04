# ADR-0005: Endpoints, gateway, TLS

Status: Proposed. Date: 2026-11-04

## Context
Clients need one endpoint per cluster (unlike Kafka). The shared tier needs many clusters behind one entry point. ingress-nginx is retired (announcement for March 2026) and must not be used.

## Decision
* **DNS:** `*.<cell>.<domain>` wildcard record → the cell's shared gateway NLB. Dedicated clusters get explicit records → their own NLB. Per-replica names: `<clusterId>-s<N>r<M>.<cell>.<domain>`.
* **Certificates:** per-cluster certs from cert-manager (Apache-2.0) with SANs `<clusterId>.<cell>.<domain>` and `*.<clusterId>…`-style per-replica names. A public ACME issuer (DNS-01) for client-facing certs; a per-cell private CA for interserver and Keeper mTLS.
* **Shared tier:** **Envoy Gateway** (Apache-2.0), Gateway API `TLSRoute` with **TLS passthrough**, routing on SNI for both 8443 and 9440 (two listeners). TLS terminates in the ClickHouse pod. The gateway never holds tenant keys or sees plaintext SQL, which matters because it's shared across tenants.
  * Check during M1: `TLSRoute`'s channel (experimental vs. standard) in the current Gateway API release and Envoy Gateway's support for it. **SNI tested** for `clickhouse-client` 26.8 over native TLS (it sends the hostname); JDBC, `clickhouse-go` and `clickhouse-connect` are still to test.
  * Per-cluster connection limits and rate limits at the gateway (Envoy Gateway `BackendTrafficPolicy` / `ClientTrafficPolicy`), so one tenant can't exhaust gateway connections.
* **Dedicated tier:** per-cluster NLB (`Service type=LoadBalancer`, AWS Load Balancer Controller), or a PrivateLink endpoint service for private access.
* **Ports:** 8443 (HTTPS) and 9440 (native TLS). MySQL (9004) and PostgreSQL (9005) are disabled by default; enabling them is a per-cluster opt-in, dedicated tier only in v1 (they don't carry SNI the same way, since the MySQL protocol starts in plaintext with STARTTLS).
* **Affinity:** L4 passthrough, so affinity is per TCP connection. Native-protocol sessions and temp tables are per-connection and work. HTTP `session_id` requires the client to reuse the connection or use a per-replica endpoint.
* **Health:** the Service includes only pods that are Ready and labelled `basalt.io/serving=true`. Readiness is lag-aware but never fails solely because of read-only status (design §11.5).

## Alternative (if L4 affinity isn't acceptable)
Terminate HTTPS at the gateway with a per-cell wildcard cert, re-encrypt to the pod, and use consistent hashing on the `session_id` query parameter or the `X-ClickHouse-Session-Id`-style header (if Envoy Gateway exposes query-parameter hashing; to verify). The cost: the shared gateway sees plaintext tenant SQL and credentials, which widens its blast radius. Not recommended for v1.
