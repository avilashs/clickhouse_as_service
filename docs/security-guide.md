# Security guide: what tenants can and cannot do

Audience: tenants and operators of Basalt. Every "cannot" below is enforced by at least one control and is checked by the automated attack suite (`test/attack`, results in [m3-report.md](m3-report.md)). The design rationale is in [clickhouse-design.md §9](clickhouse-design.md).

## Your account

Each cluster starts with one user, `admin`, holding the role `customer_admin`. Its password comes from `GET /v1/projects/{p}/clusters/{c}/initialCredentials`; rotate it after first use.

**You can:**
- Create and drop databases, tables, views, materialized views and dictionaries (with ClickHouse sources), and SQL functions (`CREATE FUNCTION f AS x -> …`).
- Use the replicated MergeTree family, `Distributed`, `Memory`, `Set`, `Join`, `Buffer`, `Null`, `Merge` and `View` engines. `Memory`, `Set`, `Join` and `Buffer` do not survive a restart.
- Insert, select, alter, optimize and truncate your tables.
- Create users, roles, row policies and quotas, and grant them any privilege you hold yourself.
- Read the `system` tables, including your own `query_log`, `processes`, `parts` and `metrics`.
- Run `SYSTEM FLUSH LOGS`, `SYSTEM SYNC REPLICA`, `SYSTEM STOP/START MERGES|FETCHES|MOVES|TTL MERGES` and `SYSTEM RESTART REPLICA` on your tables.
- Kill your own queries.
- Connect with any standard client over TLS: native protocol on port 9440, HTTPS on 8443.

**You cannot:**

| Capability | Why | Enforced by |
|---|---|---|
| Reach the network from SQL: `url`, `s3`, `remote`/`remoteSecure`, `mysql`, `postgresql`, `mongodb`, `redis`, `azureBlobStorage`, `hdfs`, `Kafka`/`URL`/`S3`/`MySQL` engines, HTTP dictionaries | Prevents SSRF against cloud metadata, internal services and other tenants | No `READ`/`WRITE ON <source>` grants. `remote_url_allow_hosts` limited to your own cluster. Default-deny egress NetworkPolicy, so even code running inside your server can reach only its own replicas, its Keeper and DNS. |
| Read or write server files: `file()`, `File` engine, file dictionaries | Protects configuration, certificates and keys | No `FILE` grant; `user_files_path` confinement |
| Run programs: `executable()`, `Executable`/`ExecutablePool` engines, executable dictionaries, executable UDFs | Code execution | No `TABLE ENGINE ON Executable*` grant; no user-scripts directory exists |
| Use introspection functions (`addressToSymbol`, …) | They leak memory layout | `allow_introspection_functions` pinned to 0 |
| Create plain (unreplicated) `MergeTree` tables | They silently lose high availability | `TABLE ENGINE` grants; `database_replicated_allow_only_replicated_engine` pinned to 1 |
| Raise limits: `max_memory_usage` (0 or above the size), `max_memory_usage_for_user`, `max_threads`, `max_concurrent_queries_for_user`, `wait_for_async_insert = 0`, `allow_introspection_functions` | Protects you and other tenants on shared nodes | Constraints in the server's default profile, which also cover users you create without any role |
| Change the platform's users (`provider_ops`, `default`, `clickhouse_operator`) or log in as them | Platform integrity | Read-only `users_xml` storage. `provider_ops` accepts only a client certificate signed by the cell CA, `default` only connections from the server itself, and `clickhouse_operator` only connections from the operator's pod IP. |
| See platform activity in `query_log`, `processes` and similar tables | Platform queries include the access bootstrap | Restrictive row policies for every user except the platform's |
| Truncate, drop or alter `system` tables, or drop the platform's row policies | They underpin monitoring and isolation | Partial revokes on `system.*` |
| `BACKUP`/`RESTORE`, named collections, `SYSTEM SHUTDOWN`/`RELOAD CONFIG`/`DROP REPLICA`, `EXECUTE AS` other users | Platform operations, or paths to secrets | Not granted. Backups are a platform operation (`/v1/…/backups`, M5). |
| Read Keeper contents (`system.zookeeper*`) | Coordination metadata | Partial revoke |
| Reach another tenant in any way | Tenant isolation | Separate processes, Keeper, credentials and certificates per cluster; NetworkPolicy; per-project API tokens |

## Data in transit and at rest

- **TLS everywhere.** Client connections, replication between replicas, ClickHouse to Keeper, Keeper to Keeper, and the operator to ClickHouse. Plaintext ports are removed from the server configuration. Each cluster has its own certificate, issued by the cell's CA; verify it against `endpoint.caCertificate`.
- **Shared tier.** Your TLS session ends inside your own server pod. The shared gateway routes by SNI and never sees plaintext.
- **At rest.** Encrypted volumes (EBS with KMS) in cloud cells. Local kind cells have no encryption at rest.

## Shared tier vs dedicated tier

- **Shared tier.** Your servers run on nodes shared with other tenants, inside a gVisor sandbox. A process escape from ClickHouse lands in the sandbox, not on the node.
- **Dedicated tier.** Your servers run on nodes reserved for your cluster, one server per node, and nothing else of any tenant is scheduled there. No sandbox is used.

## Reporting

Report a suspected vulnerability privately to the platform operators; do not test against clusters you don't own.
