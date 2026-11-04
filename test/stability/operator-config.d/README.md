These five files are what Altinity clickhouse-operator 0.27.4 (Apache-2.0) writes into every
ClickHouse pod's `config.d` before our settings (copied from a running cluster's
`chi-<id>-common-configd` ConfigMap). `render_boot.py` loads them first, because they change the
system log tables: the operator gives `query_log`, `part_log` and `trace_log` their own engine with
a fixed 30-day TTL and removes `query_thread_log`. Refresh them when the operator version changes.
