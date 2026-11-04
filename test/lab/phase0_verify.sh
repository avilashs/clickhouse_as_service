#!/usr/bin/env bash
# Phase 0 lab: checks every ClickHouse behaviour the design depends on,
# against a throwaway single-node server (embedded Keeper) on 127.0.0.1.
#
# Usage: test/lab/phase0_verify.sh /path/to/clickhouse [workdir]
#   The binary is the official static build, e.g. clickhouse-common-static-26.8.x-amd64.tgz
#   from the ClickHouse GitHub releases. Nothing is installed system-wide.
#
# Each check prints PASS or FAIL against the expectation the design relies on.
# Exit code = number of failed checks.
set -uo pipefail

CH=${1:?usage: $0 /path/to/clickhouse [workdir]}
WD=${2:-$(mktemp -d)}
PORT=19500; HTTP=19523; ISRV=19509; KPORT=19581; RAFT=19534; HOOK=19599
mkdir -p "$WD"/{data,logs,uf,cfg}
FAILS=0

cat > "$WD/cfg/config.xml" <<EOF
<clickhouse>
  <logger><level>warning</level><log>$WD/logs/server.log</log><errorlog>$WD/logs/err.log</errorlog></logger>
  <path>$WD/data/</path><tmp_path>$WD/data/tmp/</tmp_path><user_files_path>$WD/uf/</user_files_path>
  <http_port>$HTTP</http_port><tcp_port>$PORT</tcp_port><interserver_http_port>$ISRV</interserver_http_port><listen_host>127.0.0.1</listen_host>
  <users_config>users.xml</users_config>
  <user_directories>
    <users_xml><path>users.xml</path></users_xml>
    <replicated><zookeeper_path>/clickhouse/access/</zookeeper_path></replicated>
  </user_directories>
  <access_control_improvements>
    <settings_constraints_replace_previous>true</settings_constraints_replace_previous>
    <table_engines_require_grant>true</table_engines_require_grant>
    <enable_read_write_grants>true</enable_read_write_grants>
    <on_cluster_queries_require_cluster_grant>true</on_cluster_queries_require_cluster_grant>
  </access_control_improvements>
  <keeper_server>
    <tcp_port>$KPORT</tcp_port><server_id>1</server_id>
    <log_storage_path>$WD/data/coordination/log</log_storage_path><snapshot_storage_path>$WD/data/coordination/snap</snapshot_storage_path>
    <raft_configuration><server><id>1</id><hostname>127.0.0.1</hostname><port>$RAFT</port></server></raft_configuration>
  </keeper_server>
  <zookeeper><node><host>127.0.0.1</host><port>$KPORT</port></node></zookeeper>
  <macros><shard>01</shard><replica>r1</replica></macros>
  <remote_servers><default><shard><replica><host>127.0.0.1</host><port>$PORT</port></replica></shard></default></remote_servers>
  <query_log><database>system</database><table>query_log</table><flush_interval_milliseconds>1000</flush_interval_milliseconds></query_log>
</clickhouse>
EOF

# Tenant constraints live in the server *default* profile (the design's fix for the role-escape finding).
cat > "$WD/cfg/users.xml" <<'EOF'
<clickhouse>
  <profiles>
    <default>
      <max_memory_usage>4000000000</max_memory_usage>
      <max_threads>4</max_threads>
      <database_replicated_allow_only_replicated_engine>1</database_replicated_allow_only_replicated_engine>
      <constraints>
        <max_memory_usage><min>1</min><max>4000000000</max></max_memory_usage>
        <max_threads><max>4</max></max_threads>
        <database_replicated_allow_only_replicated_engine><const/></database_replicated_allow_only_replicated_engine>
      </constraints>
    </default>
    <provider><max_memory_usage>0</max_memory_usage><max_threads>16</max_threads>
      <constraints><max_memory_usage><changeable_in_readonly/></max_memory_usage><max_threads><changeable_in_readonly/></max_threads>
        <database_replicated_allow_only_replicated_engine><changeable_in_readonly/></database_replicated_allow_only_replicated_engine></constraints>
    </provider>
  </profiles>
  <users>
    <provider_ops><password></password><networks><ip>127.0.0.1</ip></networks><profile>provider</profile><quota>default</quota>
      <access_management>1</access_management><named_collection_control>1</named_collection_control></provider_ops>
  </users>
  <quotas><default/></quotas>
</clickhouse>
EOF

"$CH" server --config-file="$WD/cfg/config.xml" >"$WD/logs/stdout.log" 2>&1 &
SERVER_PID=$!
cleanup() { kill "$SERVER_PID" 2>/dev/null; [[ -n "${HOOK_PID:-}" ]] && kill "$HOOK_PID" 2>/dev/null; }
trap cleanup EXIT
for _ in $(seq 1 60); do "$CH" client --port $PORT --user provider_ops -q "SELECT 1" >/dev/null 2>&1 && break; sleep 1; done

P() { "$CH" client --port $PORT --user provider_ops "$@" 2>&1; }
T() { "$CH" client --port $PORT --user tenant --password tpw "$@" 2>&1; }

# expect <name> <regex> <command...>: PASS when the combined output matches regex
expect() {
  local name=$1 re=$2; shift 2
  local out; out=$("$@" 2>&1 | tr '\n' ' ')
  if [[ $out =~ $re ]]; then echo "PASS  $name"; else echo "FAIL  $name"; echo "      got: ${out:0:300}"; FAILS=$((FAILS+1)); fi
}

echo "== server: $(P -q 'SELECT version()')"

# ---- defaults the design pins explicitly
expect "async_insert is ON by default (design pins it)"        '^(1|true)[[:space:]]+(1|true) $' P -q "SELECT getSetting('async_insert'), getSetting('wait_for_async_insert')"
expect "insert_quorum defaults to 0 (M5 must set it)"          '^0 $'   P -q "SELECT getSetting('insert_quorum')"

# ---- tenant role (as the design grants it)
P -mn -q "
  CREATE DATABASE app ENGINE = Replicated('/clickhouse/databases/app', '{shard}', '{replica}');
  CREATE ROLE customer_admin;
  GRANT SELECT, INSERT, ALTER, CREATE TABLE, CREATE DATABASE, DROP TABLE, CREATE USER, ALTER USER, DROP USER ON *.* TO customer_admin;
  REVOKE SELECT ON system.zookeeper FROM customer_admin;
  GRANT TABLE ENGINE ON ReplicatedMergeTree TO customer_admin;
  GRANT TABLE ENGINE ON Memory TO customer_admin;
  CREATE USER tenant IDENTIFIED WITH plaintext_password BY 'tpw' DEFAULT ROLE customer_admin;
  GRANT customer_admin TO tenant;" >/dev/null

# ---- replication safety (ADR-0007)
# Stock default is 0, and then a plain MergeTree is silently created inside a Replicated DB (observed
# manually on 26.8.34, see docs/evidence). With 1 in the server default profile, Replicated-DB DDL
# is rejected even when the session overrides it, because the DDL runs with the server default profile.
expect "session override can't bypass allow_only_replicated in Replicated DB DDL" 'Only tables with a Replicated engine' \
  P --database_replicated_allow_only_replicated_engine=0 -q "CREATE TABLE app.plain (id UInt64) ENGINE = MergeTree ORDER BY id"
expect "allow_only_replicated_engine=1 rejects MergeTree"       'Only tables with a Replicated engine' \
  P --database_replicated_allow_only_replicated_engine=1 -q "CREATE TABLE app.plain2 (id UInt64) ENGINE = MergeTree ORDER BY id"
expect "tenant without TABLE ENGINE ON MergeTree is denied"     'TABLE ENGINE ON MergeTree' \
  T -q "CREATE TABLE app.t1 (id UInt64) ENGINE = MergeTree ORDER BY id"
expect "tenant can create ReplicatedMergeTree"                  'OK|^ ?$' \
  T -q "CREATE TABLE app.t2 (id UInt64) ENGINE = ReplicatedMergeTree ORDER BY id"
expect "tenant can't disable allow_only_replicated (CONST in default profile)" 'SETTING_CONSTRAINT_VIOLATION|should not be changed' \
  T -q "SELECT 1 SETTINGS database_replicated_allow_only_replicated_engine = 0"
expect "tenant CREATE DATABASE without engine yields Atomic"    'Atomic' \
  bash -c "\"$CH\" client --port $PORT --user tenant --password tpw -q 'CREATE DATABASE tdb' 2>&1; \"$CH\" client --port $PORT --user provider_ops -q \"SELECT engine FROM system.databases WHERE name='tdb'\""
expect "database engines can't be granted via TABLE ENGINE"     'Unknown table engine Replicated' \
  P -q "GRANT TABLE ENGINE ON Replicated TO customer_admin"

# ---- SQL attack surface (design 9.2/9.3)
expect "url() requires READ ON URL"                             'READ ON URL' T -q "SELECT * FROM url('http://169.254.169.254/latest/meta-data/', 'LineAsString')"
expect "s3() requires READ ON S3"                               'READ ON S3'  T -q "SELECT * FROM s3('http://169.254.169.254/some-bucket/key', 'LineAsString')"
expect "file() confined to user_files_path"                     'is not inside|READ ON FILE' T -q "SELECT * FROM file('/etc/passwd', 'LineAsString')"
expect "executable() requires TABLE ENGINE ON Executable"       'TABLE ENGINE ON Executable' T -q "SELECT * FROM executable('id', 'TSV', 'x String')"
expect "system.zookeeper revoked"                               'ACCESS_DENIED' T -q "SELECT count() FROM system.zookeeper WHERE path = '/'"
expect "users_xml provider users are read-only to tenant"       'readonly' T -q "ALTER USER provider_ops IDENTIFIED WITH plaintext_password BY 'x'"
expect "per-query max_memory_usage=0 rejected"                  'SETTING_CONSTRAINT_VIOLATION' T -q "SELECT 1 SETTINGS max_memory_usage = 0"

# ---- constraint escape via role-less user (the finding that moved constraints into the default profile)
T -q "CREATE USER esc IDENTIFIED WITH plaintext_password BY 'e'" >/dev/null
expect "role-less tenant-created user still constrained (max_threads)" 'SETTING_CONSTRAINT_VIOLATION' \
  "$CH" client --port $PORT --user esc --password e -q "SELECT 1 SETTINGS max_threads = 64"
expect "role-less tenant-created user still constrained (max_memory_usage)" 'SETTING_CONSTRAINT_VIOLATION' \
  "$CH" client --port $PORT --user esc --password e -q "SELECT 1 SETTINGS max_memory_usage = 0"

# ---- backup credentials (ADR-0006): STS session token via provider-only named collection
python3 -I - "$HOOK" "$WD/hook.log" <<'PY' &
import http.server, sys
class H(http.server.BaseHTTPRequestHandler):
    def _h(self):
        open(sys.argv[2], "a").write(f"{self.command} {self.path} token={self.headers.get('x-amz-security-token')}\n")
        if self.command == "GET": self.send_response(200); self.end_headers(); self.wfile.write(b"a,b\n1,2\n")
        else: self.send_response(404); self.end_headers()
    do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = _h
    def log_message(self, *a): pass
http.server.HTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
PY
HOOK_PID=$!; sleep 1
P -q "CREATE NAMED COLLECTION bk AS url = 'http://127.0.0.1:$HOOK/backups-bucket/c1/', access_key_id = 'A', secret_access_key = 'S', session_token = 'STS-TOKEN'" >/dev/null
P -q "CREATE TABLE app.bk_src (id UInt64) ENGINE = ReplicatedMergeTree ORDER BY id" >/dev/null
P -q "BACKUP TABLE app.bk_src TO S3(bk, 'b1') SETTINGS async = 0" >/dev/null
expect "BACKUP sends STS session token from named collection"   'token=STS-TOKEN' cat "$WD/hook.log"
expect "tenant can't use provider named collection"             'NAMED COLLECTION ON bk' T -q "SELECT * FROM s3(bk, filename = 'x', format = 'CSV')"
expect "tenant can't BACKUP"                                    'BACKUP ON' T -q "BACKUP TABLE app.t2 TO S3(bk, 'stolen')"
expect "BACKUP S3() positional form has no session-token slot"  'requires 1 or 3 arguments' \
  P -q "BACKUP TABLE app.bk_src TO S3('http://127.0.0.1:$HOOK/backups-bucket/x', 'A', 'S', 'T')"

# ---- query optimizer integration (ADR-0008)
P -mn -q "
  CREATE TABLE app.events (ts DateTime, user_id UInt64, url String) ENGINE = ReplicatedMergeTree ORDER BY ts;
  INSERT INTO app.events SELECT toDateTime('2026-01-01') + number, number % 100000, concat('/p/', toString(number % 5000)) FROM numbers(20000000);" >/dev/null
Q="SELECT count() FROM app.events WHERE user_id = 4242"
marks() { local m; m=$(P -q "EXPLAIN ESTIMATE $Q" | awk '{s+=$5} END {print s+0}'); echo "${m:-0}"; }
BEFORE=$(marks)
P -q "ALTER TABLE app.events ADD INDEX qo_user user_id TYPE bloom_filter GRANULARITY 1" >/dev/null
P -q "ALTER TABLE app.events MATERIALIZE INDEX qo_user" >/dev/null
DURING=$(marks)
DROP_DURING=$(P -q "ALTER TABLE app.events DROP INDEX qo_user")
for _ in $(seq 1 120); do [[ $(P -q "SELECT min(is_done) FROM system.mutations WHERE table = 'events'") == 1 ]] && break; sleep 1; done
AFTER=$(marks)
echo "      marks: before=$BEFORE right-after-MATERIALIZE=$DURING after-mutation=$AFTER"
expect "MATERIALIZE INDEX returns before the index is usable"   'true' bash -c "[ $DURING -gt $(( AFTER * 2 )) ] && echo true"
expect "skip index visible to EXPLAIN ESTIMATE once materialized" 'true' bash -c "[ $AFTER -lt $(( BEFORE / 2 )) ] && echo true"
expect "DROP INDEX blocked while its mutation runs"             'affected by mutation|^$' echo "$DROP_DURING"

: > "$WD/hook.log"
P -q "EXPLAIN SELECT * FROM url('http://127.0.0.1:$HOOK/ssrf-via-explain', 'CSV')" >/dev/null
expect "EXPLAIN makes outbound requests for url()"              'ssrf-via-explain' cat "$WD/hook.log"
expect "EXPLAIN ESTIMATE of a non-MergeTree source is empty (cost reads as 0)" '^ ?$' \
  P -q "EXPLAIN ESTIMATE SELECT * FROM url('http://127.0.0.1:$HOOK/e', 'CSV')"
START=$(date +%s)
P -q "EXPLAIN SELECT (SELECT sum(sleepEachRow(1)) FROM numbers(2))" >/dev/null
expect "EXPLAIN executes scalar subqueries"                     'true' bash -c "[ $(( $(date +%s) - START )) -ge 2 ] && echo true"

P -mn -q "
  CREATE SETTINGS PROFILE optimizer SETTINGS readonly = 2, max_execution_time = 60 MAX 60, max_memory_usage = 1000000000 MAX 1000000000;
  CREATE USER basalt_optimizer IDENTIFIED WITH plaintext_password BY 'o' SETTINGS PROFILE 'optimizer';
  GRANT SELECT ON app.* TO basalt_optimizer;
  GRANT SELECT ON system.query_log TO basalt_optimizer; GRANT SELECT ON system.tables TO basalt_optimizer;
  GRANT SELECT ON system.columns TO basalt_optimizer;" >/dev/null
O() { "$CH" client --port $PORT --user basalt_optimizer --password o "$@" 2>&1; }
expect "optimizer user: EXPLAIN works under readonly=2"         'Expression|ReadFrom' O -q "EXPLAIN $Q"
expect "optimizer user: EXPLAIN url() denied (no SSRF)"         'READ ON URL' O -q "EXPLAIN SELECT * FROM url('http://127.0.0.1:$HOOK/x', 'CSV')"
expect "optimizer user: can't ALTER"                            'ACCESS_DENIED|readonly' O -q "ALTER TABLE app.events DROP INDEX qo_user"

echo "== $FAILS failed"
exit $FAILS
