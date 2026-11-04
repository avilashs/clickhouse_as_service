#!/usr/bin/env bash
# M1 acceptance on the local kind cell (hack/cell-up.sh + hack/dev-up.sh must have run).
#
#   1. Create a shared cluster through the API; a stock clickhouse-client connects over TLS
#      through the shared gateway (SNI passthrough) and verifies the server certificate
#      against the CA published by the API.
#   2. Create a second cluster and kill -9 the workflow worker twice mid-create (right after
#      the API call, and while the workflow waits for convergence). After restarting the
#      worker the create completes, with no duplicate rows, reservations, workflow runs,
#      Kubernetes objects or secrets.
#   3. Delete both clusters; namespaces, reservations and credentials are gone.
#
# Usage: test/e2e/m1.sh [--keep]   (--keep skips the deletes)
set -uo pipefail
cd "$(dirname "$0")/../.."
BASALT_HOME=${BASALT_HOME:-/var/lib/basalt}
# shellcheck disable=SC1091
source "$BASALT_HOME/env"
API=http://127.0.0.1:8080
P=demo
AUTH="Authorization: Bearer $BASALT_TENANT_TOKEN"
WORK=$(mktemp -d)
FAILS=0
KEEP=${1:-}
trap 'pkill -f "port-forward.*19440" 2>/dev/null; rm -rf "$WORK"' EXIT

pass() { echo "PASS  $*"; }
fail() { echo "FAIL  $*"; FAILS=$((FAILS+1)); }
check() { local name=$1; shift; if "$@"; then pass "$name"; else fail "$name"; fi; }
api() { curl -s --noproxy '*' -H "$AUTH" -H 'Content-Type: application/json' "$@"; }
json() { python3 -c "import json,sys; d=json.load(sys.stdin); print(eval(sys.argv[1]))" "$1"; }
sql() { docker exec basalt-pg psql -U basalt -tAc "$1"; }

wait_state() { # cluster want timeout_s
  local c=$1 want=$2 deadline=$(( $(date +%s) + $3 )) st=""
  while [ "$(date +%s)" -lt "$deadline" ]; do
    st=$(api "$API/v1/projects/$P/clusters/$c" | json "d.get('state','GONE')" 2>/dev/null || echo GONE)
    [ "$st" = "$want" ] && return 0
    [ "$st" = FAILED ] && [ "$want" != FAILED ] && return 1
    sleep 5
  done
  echo "      timed out in state $st" >&2
  return 1
}

create() { # name key -> prints cluster id
  api -X POST "$API/v1/projects/$P/clusters" -H "Idempotency-Key: $2" -d "{\"name\":\"$1\",\"size\":\"t-2\"}" | json "d['clusterId']"
}

echo "== 1. create and connect over TLS"
NAME1=e2e-a-$RANDOM
C1=$(create "$NAME1" "k-$NAME1")
echo "      cluster $C1"
check "cluster $C1 becomes ACTIVE" wait_state "$C1" ACTIVE 900

api "$API/v1/projects/$P/clusters/$C1" > "$WORK/c1.json"
HOST=$(json "d['endpoint']['host']" < "$WORK/c1.json")
json "d['endpoint']['caCertificate']" < "$WORK/c1.json" > "$WORK/ca.pem"
api "$API/v1/projects/$P/clusters/$C1/initialCredentials" > "$WORK/cred.json"
USER=$(json "d['username']" < "$WORK/cred.json"); PASS=$(json "d['password']" < "$WORK/cred.json")
cat > "$WORK/client.xml" <<EOF
<clickhouse><openSSL><client>
  <caConfig>$WORK/ca.pem</caConfig><loadDefaultCAFile>false</loadDefaultCAFile><verificationMode>strict</verificationMode>
</client></openSSL></clickhouse>
EOF
# kind has no load balancer: reach the shared gateway through a port-forward, and resolve the
# cluster hostname to it (DNS does this in a real cell).
SVC=$(kubectl -n envoy-gateway-system get svc -l gateway.envoyproxy.io/owning-gateway-name=shared -o jsonpath='{.items[0].metadata.name}')
kubectl -n envoy-gateway-system port-forward "svc/$SVC" 19440:9440 18443:8443 >/dev/null 2>&1 &
sleep 3
grep -q " $HOST\$" /etc/hosts || echo "127.0.0.1 $HOST" >> /etc/hosts
CH() { clickhouse client --config-file "$WORK/client.xml" --secure --host "$HOST" --port 19440 --user "$USER" --password "$PASS" "$@"; }
check "stock clickhouse-client connects over TLS with CA verification" \
  bash -c "$(declare -f CH); WORK=$WORK HOST=$HOST USER=$USER PASS='$PASS'; CH -q 'SELECT 1' | grep -qx 1"
CH -q "CREATE TABLE default.t ON CLUSTER default (id UInt64) ENGINE = ReplicatedMergeTree ORDER BY id" >/dev/null 2>&1
# insert_quorum = 2 only acknowledges once both replicas have the part
check "quorum insert (insert_quorum=2) through the gateway succeeds" \
  bash -c "$(declare -f CH); WORK=$WORK HOST=$HOST USER=$USER PASS='$PASS'; CH -q 'INSERT INTO default.t SELECT number FROM numbers(1000) SETTINGS insert_quorum = 2'"
check "table is replicated with 2/2 active replicas and 1000 rows" \
  test "$(CH -q "SELECT total_replicas, active_replicas, (SELECT count() FROM default.t) FROM system.replicas WHERE database = 'default' AND table = 't'" 2>&1 | tr '\t' ' ')" = "2 2 1000"
check "plain MergeTree is refused" bash -c "$(declare -f CH); WORK=$WORK HOST=$HOST USER=$USER PASS='$PASS'; CH -q 'CREATE TABLE default.p (id UInt64) ENGINE = MergeTree ORDER BY id' 2>&1 | grep -q 'TABLE ENGINE ON MergeTree'"
check "url() to cloud metadata is refused" bash -c "$(declare -f CH); WORK=$WORK HOST=$HOST USER=$USER PASS='$PASS'; CH -q \"SELECT * FROM url('http://169.254.169.254/latest/meta-data/', 'LineAsString')\" 2>&1 | grep -q 'READ ON URL'"
check "wrong CA is rejected by the client" bash -c "openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -keyout $WORK/x.key -out $WORK/ca.pem.bad -days 1 -subj /CN=x 2>/dev/null; sed 's#$WORK/ca.pem#$WORK/ca.pem.bad#' $WORK/client.xml > $WORK/bad.xml; ! clickhouse client --config-file $WORK/bad.xml --secure --host $HOST --port 19440 --user $USER --password '$PASS' -q 'SELECT 1' >/dev/null 2>&1"
check "HTTPS endpoint verifies against the published CA" \
  bash -c "curl -sf --noproxy '*' --cacert $WORK/ca.pem --resolve $HOST:18443:127.0.0.1 -u '$USER:$PASS' 'https://$HOST:18443/?query=SELECT+42' | grep -qx 42"

echo "== 2. kill the worker mid-create"
NAME2=e2e-b-$RANDOM
KEY2="k-$NAME2"
C2=$(create "$NAME2" "$KEY2")
echo "      cluster $C2"
pkill -9 -f bin/basalt-worker && pass "worker killed immediately after the create call" || fail "kill worker #1"
sleep 3
hack/restart.sh worker >/dev/null
# second kill while the workflow is waiting for convergence (desired state already published)
for _ in $(seq 1 60); do [ "$(sql "SELECT count(*) FROM desired_clusters WHERE cluster_id='$C2'")" = 1 ] && break; sleep 2; done
sleep 10
pkill -9 -f bin/basalt-worker && pass "worker killed while waiting for convergence" || fail "kill worker #2"
sleep 30 # longer than the 20 s heartbeat timeout: the in-flight activity must be retried elsewhere
hack/restart.sh worker >/dev/null
check "replaying the create with the same key returns the same operation" \
  test "$(create "$NAME2" "$KEY2")" = "$C2"
check "cluster $C2 becomes ACTIVE after the worker restarts" wait_state "$C2" ACTIVE 900

check "exactly one cluster row"         test "$(sql "SELECT count(*) FROM clusters WHERE name='$NAME2'")" = 1
check "exactly one operation"           test "$(sql "SELECT count(*) FROM operations WHERE cluster_id='$C2'")" = 1
check "exactly one capacity reservation (COMMITTED)" test "$(sql "SELECT count(*) || ':' || min(state) FROM capacity_reservations WHERE cluster_id='$C2'")" = "1:COMMITTED"
check "exactly one desired-state row"   test "$(sql "SELECT count(*) FROM desired_clusters WHERE cluster_id='$C2'")" = 1
check "exactly one workflow run"        test "$(temporal workflow list --query "WorkflowId='cluster/$C2'" -o json | python3 -c 'import json,sys; print(len(json.load(sys.stdin)))')" = 1
check "exactly one namespace"           test "$(kubectl get ns -l basalt.io/cluster=$C2 --no-headers | wc -l)" = 1
check "exactly one CHI and one CHK"     test "$(kubectl -n ch-$C2 get chi,chk --no-headers | wc -l)" = 2
check "exactly one set of credentials"  test "$(find "$BASALT_SECRETS_DIR/clusters/$C2" -name '*.sec' | wc -l)" = 3
RETRIED=$(temporal workflow show --workflow-id "cluster/$C2" -o json | python3 -c '
import json,sys
h=json.load(sys.stdin)
print(max([e.get("activityTaskStartedEventAttributes",{}).get("attempt",1) for e in h["events"] if "activityTaskStartedEventAttributes" in e] or [1]))')
check "an activity was retried after the kill (max attempt $RETRIED > 1)" test "$RETRIED" -gt 1

if [ "$KEEP" != --keep ]; then
  echo "== 3. delete"
  for c in "$C1" "$C2"; do
    api -X DELETE "$API/v1/projects/$P/clusters/$c" -H "Idempotency-Key: del-$c" >/dev/null
  done
  for c in "$C1" "$C2"; do
    check "cluster $c deleted" wait_state "$c" GONE 600
    check "namespace ch-$c removed"      bash -c "! kubectl get ns ch-$c >/dev/null 2>&1"
    check "reservation released"         test "$(sql "SELECT string_agg(DISTINCT state, ',') FROM capacity_reservations WHERE cluster_id='$c'")" = RELEASED
    check "credentials removed"          test ! -e "$BASALT_SECRETS_DIR/clusters/$c"
    check "cluster record soft-deleted"  test "$(sql "SELECT state FROM clusters WHERE id='$c'")" = DELETED
  done
  sed -i "/ $HOST\$/d" /etc/hosts
fi
echo "== $FAILS failed"
exit "$FAILS"
