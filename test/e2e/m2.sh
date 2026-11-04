#!/usr/bin/env bash
# M2 acceptance (placement) on the local kind cell (hack/cell-up.sh + hack/dev-up.sh).
#
#   1. A shared and a dedicated cluster are created through the API and become ACTIVE.
#   2. The live state satisfies every placement invariant (R1, R1b, R2, R2b, R3, sandbox,
#      Keeper on the control pool) and both clusters serve TLS queries.
#   3. The scheduler and admission enforce the rules: a pod without the dedicated toleration
#      cannot land on a dedicated node; the admission policies reject one crafted pod per rule
#      (including a shared server without the sandbox), admit the real pod spec, and reject
#      pods, StatefulSets and node bindings from the wrong creator.
#   4. basalt-audit reports zero violations, detects injected violations (CLI and metric), and
#      clears once they are reverted.
#   5. Deleting the dedicated cluster returns its nodes to the free pool.
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
STS_AS=(--as=system:serviceaccount:kube-system:statefulset-controller --as-group=system:serviceaccounts --as-group=system:serviceaccounts:kube-system)
trap 'kubectl delete runtimeclass basalt-e2e-runc --ignore-not-found >/dev/null 2>&1; pkill -f "port-forward.*1944[01]" 2>/dev/null; pkill -f "port-forward.*19102" 2>/dev/null; kubectl delete pod -n default e2e-taint-probe --ignore-not-found --wait=false >/dev/null 2>&1; rm -rf "$WORK"' EXIT

pass() { echo "PASS  $*"; }
fail() { echo "FAIL  $*"; FAILS=$((FAILS+1)); }
check() { local name=$1; shift; if "$@"; then pass "$name"; else fail "$name"; fi; }
api() { curl -s --noproxy '*' -H "$AUTH" -H 'Content-Type: application/json' "$@"; }
json() { python3 -c "import json,sys; d=json.load(sys.stdin); print(eval(sys.argv[1]))" "$1"; }
wait_state() {
  local c=$1 want=$2 deadline=$(( $(date +%s) + $3 )) st=""
  while [ "$(date +%s)" -lt "$deadline" ]; do
    st=$(api "$API/v1/projects/$P/clusters/$c" | json "d.get('state','GONE')" 2>/dev/null || echo GONE)
    [ "$st" = "$want" ] && return 0
    [ "$st" = FAILED ] && [ "$want" != FAILED ] && return 1
    sleep 5
  done
  echo "      timed out in state $st ($(api "$API/v1/projects/$P/clusters/$c" | json "d.get('stateReason','')" 2>/dev/null))" >&2
  return 1
}
create() { api -X POST "$API/v1/projects/$P/clusters" -H "Idempotency-Key: k-$1" -d "$2" | json "d['clusterId']"; }
nodes_of() { kubectl get nodes -l "$1" -o jsonpath='{.items[*].metadata.name}'; }
pod_nodes() { kubectl -n "ch-$1" get pods -l basalt.io/component=clickhouse-server -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}'; }
zone_of() { kubectl get node "$1" -o jsonpath='{.metadata.labels.topology\.kubernetes\.io/zone}'; }

echo "== 1. create a shared and a dedicated cluster"
S=$(create "shared-$RANDOM" '{"name":"e2e-shared-'$RANDOM'","size":"t-2"}')
D=$(create "ded-$RANDOM" '{"name":"e2e-ded-'$RANDOM'","size":"dt-1","tier":"dedicated","shards":1,"replicasPerShard":2}')
echo "      shared $S, dedicated $D"
check "shared cluster becomes ACTIVE" wait_state "$S" ACTIVE 1200
check "dedicated cluster becomes ACTIVE" wait_state "$D" ACTIVE 1200

echo "== 2. live invariants"
mapfile -t SN < <(pod_nodes "$S")
mapfile -t DN < <(pod_nodes "$D")
check "R1 shared: 2 replicas on 2 different nodes" test "${#SN[@]}" = 2 -a "${SN[0]}" != "${SN[1]:-}"
check "R1b shared: replicas in different zones" test "$(zone_of "${SN[0]}")" != "$(zone_of "${SN[1]}")"
check "R3 shared: servers only on shared-pool nodes" \
  bash -c "for n in ${SN[*]}; do [ \"\$(kubectl get node \$n -o jsonpath='{.metadata.labels.basalt\.io/pool}')\" = shared ] || exit 1; done"
check "sandbox: shared servers use RuntimeClass gvisor" \
  test "$(kubectl -n ch-$S get pods -l basalt.io/component=clickhouse-server -o jsonpath='{range .items[*]}{.spec.runtimeClassName}{" "}{end}')" = "gvisor gvisor "
check "sandbox: the shared server runs on the gVisor kernel" \
  bash -c "kubectl -n ch-$S exec chi-$S-default-0-0-0 -- uname -r | grep -q gvisor"
check "R2 dedicated: pool has exactly 2 nodes for 2 servers (R2b)" test "$(nodes_of basalt.io/dedicated-cluster=$D | wc -w)" = 2
check "R2 dedicated: each server on its own pool node, one per node" \
  bash -c "a='${DN[0]}' b='${DN[1]:-}'; [ \"\$a\" != \"\$b\" ] && for n in \$a \$b; do [ \"\$(kubectl get node \$n -o jsonpath='{.metadata.labels.basalt\.io/dedicated-cluster}')\" = $D ] || exit 1; done"
check "R2 dedicated: pool nodes carry only the cluster's own taint" \
  bash -c "for n in $(nodes_of basalt.io/dedicated-cluster=$D); do [ \"\$(kubectl get node \$n -o jsonpath='{range .spec.taints[*]}{.key}={.value}:{.effect} {end}')\" = 'basalt.io/dedicated-cluster=$D:NoSchedule ' ] || exit 1; done"
check "R2 dedicated: no other workload pods on the pool nodes" \
  bash -c "for n in $(nodes_of basalt.io/dedicated-cluster=$D); do kubectl get pods -A --field-selector spec.nodeName=\$n -o json | python3 -c 'import json,sys; ps=json.load(sys.stdin)[\"items\"]; bad=[p[\"metadata\"][\"name\"] for p in ps if not any(o[\"kind\"]==\"DaemonSet\" for o in p[\"metadata\"].get(\"ownerReferences\",[])) and p[\"metadata\"][\"labels\"].get(\"basalt.io/cluster\")!=\"$D\"]; sys.exit(1 if bad else 0)' || exit 1; done"
check "R1b dedicated: replicas in different zones" test "$(zone_of "${DN[0]}")" != "$(zone_of "${DN[1]}")"
check "dedicated servers run without the sandbox" \
  test -z "$(kubectl -n ch-$D get pods -l basalt.io/component=clickhouse-server -o jsonpath='{.items[*].spec.runtimeClassName}')"
check "Keeper: 3 replicas each on its own control node" \
  bash -c "for c in $S $D; do ns=\$(kubectl -n ch-\$c get pods -l basalt.io/component=keeper -o jsonpath='{range .items[*]}{.spec.nodeName}{\"\\n\"}{end}' | sort -u); [ \$(echo \"\$ns\" | wc -l) = 3 ] || exit 1; for n in \$ns; do [ \"\$(kubectl get node \$n -o jsonpath='{.metadata.labels.basalt\.io/pool}')\" = control ] || exit 1; done; done"
check "dedicated cluster has its own LoadBalancer Service" \
  test "$(kubectl -n ch-$D get svc clickhouse-$D -o jsonpath='{.spec.type}')" = LoadBalancer

# TLS queries: shared through the gateway (SNI), dedicated through its own Service
for c in "$S" "$D"; do
  api "$API/v1/projects/$P/clusters/$c" | json "d['endpoint']['caCertificate']" > "$WORK/ca-$c.pem"
  api "$API/v1/projects/$P/clusters/$c/initialCredentials" | json "d['password']" > "$WORK/pw-$c"
  printf '<clickhouse><openSSL><client><caConfig>%s</caConfig><loadDefaultCAFile>false</loadDefaultCAFile><verificationMode>strict</verificationMode></client></openSSL></clickhouse>' "$WORK/ca-$c.pem" > "$WORK/client-$c.xml"
  host=$(api "$API/v1/projects/$P/clusters/$c" | json "d['endpoint']['host']")
  grep -q " $host\$" /etc/hosts || echo "127.0.0.1 $host" >> /etc/hosts
  echo "$host" > "$WORK/host-$c"
done
GW=$(kubectl -n envoy-gateway-system get svc -l gateway.envoyproxy.io/owning-gateway-name=shared -o jsonpath='{.items[0].metadata.name}')
kubectl -n envoy-gateway-system port-forward "svc/$GW" 19440:9440 >/dev/null 2>&1 &
kubectl -n "ch-$D" port-forward "svc/clickhouse-$D" 19441:9440 >/dev/null 2>&1 &
sleep 3
q() { clickhouse client --config-file "$WORK/client-$1.xml" --secure --host "$(cat "$WORK/host-$1")" --port "$2" --user admin --password "$(cat "$WORK/pw-$1")" -q "$3"; }
check "shared cluster answers over TLS through the gateway (gVisor)" test "$(q "$S" 19440 'SELECT 1' 2>&1)" = 1
check "dedicated cluster answers over TLS through its own Service" test "$(q "$D" 19441 'SELECT 1' 2>&1)" = 1
check "dedicated: replicated insert reaches both replicas" bash -c "$(declare -f q); WORK=$WORK; q $D 19441 'CREATE TABLE default.t ON CLUSTER default (id UInt64) ENGINE = ReplicatedMergeTree ORDER BY id' >/dev/null && q $D 19441 'INSERT INTO default.t SELECT number FROM numbers(1000) SETTINGS insert_quorum = 2' && [ \"\$(q $D 19441 \"SELECT active_replicas FROM system.replicas WHERE table = 't'\")\" = 2 ]"

echo "== 3. enforcement"
# taints: a pod aimed at a dedicated node without the toleration never schedules
DNODE=$(nodes_of basalt.io/dedicated-cluster=$D | cut -d' ' -f1)
kubectl -n default run e2e-taint-probe --image=registry.k8s.io/pause:3.10 --restart=Never \
  --overrides="{\"spec\":{\"nodeSelector\":{\"kubernetes.io/hostname\":\"$DNODE\"}}}" >/dev/null 2>&1
sleep 8
check "taint: pod without the dedicated toleration stays unscheduled" \
  test "$(kubectl -n default get pod e2e-taint-probe -o jsonpath='{.status.conditions[?(@.type=="PodScheduled")].reason}')" = Unschedulable
kubectl -n default delete pod e2e-taint-probe --wait=false >/dev/null 2>&1

STS_S=$(kubectl -n "ch-$S" get sts -l basalt.io/cluster="$S" -o name | grep chi- | head -1)
STS_D=$(kubectl -n "ch-$D" get sts -l basalt.io/cluster="$D" -o name | grep chi- | head -1)
kubectl -n "ch-$S" get "$STS_S" -o json > "$WORK/sts-s.json"
kubectl -n "ch-$D" get "$STS_D" -o json > "$WORK/sts-d.json"
dry() { # sts-file variant arg expect-regex [creator]
  python3 test/e2e/podvariant.py "$2" "$3" < "$1" > "$WORK/pod.json"
  local who=("${STS_AS[@]}"); [ "${5:-}" = admin ] && who=()
  local out; out=$(kubectl create --dry-run=server "${who[@]}" -f "$WORK/pod.json" 2>&1)
  if [[ $out =~ $4 ]]; then return 0; fi
  echo "      got: ${out:0:300}" >&2; return 1
}
DENY="denied|forbidden|violat"
# a second, non-sandbox RuntimeClass, so "shared server with another runtime" reaches our policy
kubectl apply -f - >/dev/null <<'YAML'
apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata: {name: basalt-e2e-runc}
handler: runc
YAML
check "admission admits the real shared pod spec"     dry "$WORK/sts-s.json" good "" "created \(server dry run\)"
check "admission admits the real dedicated pod spec"  dry "$WORK/sts-d.json" good "" "created \(server dry run\)"
check "rejects shared server without the sandbox"     dry "$WORK/sts-s.json" no_sandbox "" "sandbox RuntimeClass gvisor"
check "rejects shared server with another runtime"    dry "$WORK/sts-s.json" other_runtime "" "sandbox RuntimeClass gvisor"
check "rejects shared server tolerating a dedicated taint" dry "$WORK/sts-s.json" shared_dedicated_toleration "$D" "must not tolerate dedicated-node taints"
check "rejects dedicated server without exclusivity"  dry "$WORK/sts-d.json" no_exclusivity "" "anti-affinity against all ClickHouse servers"
check "rejects exclusivity scoped to one namespace"   dry "$WORK/sts-d.json" scoped_exclusivity "" "anti-affinity against all ClickHouse servers"
check "rejects dedicated server selecting another pool" dry "$WORK/sts-d.json" other_pool "cother" "select their own cluster's node pool"
check "rejects dedicated server tolerating another cluster" dry "$WORK/sts-d.json" other_toleration "cother" "only tolerate their own cluster's taint"
check "rejects missing per-shard anti-affinity (R1)"  dry "$WORK/sts-s.json" no_shard_antiaffinity "" "R1: replicas of one shard"
check "rejects missing zone spread (R1b)"             dry "$WORK/sts-d.json" no_zone_spread "" "R1b"
check "rejects ServiceAccount token mount"            dry "$WORK/sts-s.json" sa_token "" "ServiceAccount token"
check "rejects requests != limits"                    dry "$WORK/sts-d.json" overcommit "" "requests equal to limits"
check "rejects privilege escalation"                  dry "$WORK/sts-d.json" privileged "" "$DENY"
check "rejects tolerating all taints"                 dry "$WORK/sts-s.json" tolerate_all "" "tolerate all taints|every taint"
check "rejects hostPath volumes"                      dry "$WORK/sts-d.json" host_path "" "$DENY"
check "rejects a correct pod not created by the StatefulSet controller" dry "$WORK/sts-s.json" good "" "only be created by the StatefulSet controller" admin
NP() { # name json-overrides expect
  local out; out=$(kubectl -n default run "$1" --image=registry.k8s.io/pause:3.10 --restart=Never --dry-run=server --overrides="$2" 2>&1)
  [[ $out =~ $3 ]] || { echo "      got: ${out:0:300}" >&2; return 1; }
}
check "rejects binding a pod straight to a node (spec.nodeName)" NP np1 "{\"spec\":{\"nodeName\":\"$DNODE\"}}" "spec.nodeName"
check "rejects a non-ClickHouse pod tolerating a dedicated taint" NP np2 "{\"spec\":{\"tolerations\":[{\"key\":\"basalt.io/dedicated-cluster\",\"operator\":\"Equal\",\"value\":\"$D\",\"effect\":\"NoSchedule\"}]}}" "only that cluster's ClickHouse servers"
check "rejects a non-ClickHouse pod tolerating the shared pool" NP np3 '{"spec":{"tolerations":[{"key":"basalt.io/pool","operator":"Equal","value":"shared","effect":"NoSchedule"}]}}' "shared or free pools"
check "rejects a tenant-namespace pod tolerating the control pool" NP np4 '{"spec":{"tolerations":[{"key":"basalt.io/pool","operator":"Equal","value":"control","effect":"NoSchedule"}]}}' "control pool"
check "rejects a ClickHouse StatefulSet not written by the operator" \
  bash -c "python3 -c 'import json,sys; s=json.load(open(\"$WORK/sts-d.json\")); m=s[\"metadata\"]; s[\"metadata\"]={\"name\":\"e2e-rogue\",\"namespace\":m[\"namespace\"]}; s.pop(\"status\",None); print(json.dumps(s))' > $WORK/rogue.json; kubectl create --dry-run=server -f $WORK/rogue.json 2>&1 | grep -q 'managed by the operator only'"
GC_AS=(--as=system:serviceaccount:kube-system:generic-garbage-collector --as-group=system:serviceaccounts --as-group=system:serviceaccounts:kube-system)
# M3 finding: denying the garbage collector's metadata-only updates (finalizer removal) left
# foreground-deleted StatefulSets, and with them whole clusters, stuck forever
check "admits a metadata-only StatefulSet update by the garbage collector" \
  bash -c "kubectl -n ch-$D annotate $STS_D basalt.io/e2e=1 --dry-run=server ${GC_AS[*]} 2>&1 | grep -q annotated"
check "rejects a StatefulSet spec change by the garbage collector" \
  bash -c "kubectl -n ch-$D patch $STS_D --type=merge -p '{\"spec\":{\"template\":{\"spec\":{\"nodeSelector\":{\"e2e\":\"x\"}}}}}' --dry-run=server ${GC_AS[*]} 2>&1 | grep -q 'managed by the operator only'"

echo "== 4. continuous audit"
check "basalt-audit: no violations with both clusters running" bash -c "bin/basalt-audit --once >/dev/null"
FREE=$(nodes_of basalt.io/pool=free | cut -d' ' -f1)
SNODE=${SN[0]}
kubectl label node "$FREE" basalt.io/dedicated-cluster="$D" --overwrite >/dev/null   # V4: pool larger than the cluster
kubectl label node "$SNODE" basalt.io/dedicated-cluster="$D" --overwrite >/dev/null  # V2: shared server on a node marked dedicated
bin/basalt-audit --once --grace=0 -o json > "$WORK/v.json"
check "audit detects V4 (dedicated node count != server count)" grep -q '"rule":"V4"' "$WORK/v.json"
check "audit detects V2 (foreign pod on a dedicated node)" grep -q '"rule":"V2"' "$WORK/v.json"
kubectl -n basalt-system port-forward svc/basalt-audit 19102:9102 >/dev/null 2>&1 &
sleep 3
metric() { curl -s --noproxy '*' localhost:19102/metrics | awk '/^basalt_placement_violations_count/ {print $2}'; }
deadline=$(( $(date +%s) + 90 )); m=0
while [ "$(date +%s)" -lt "$deadline" ]; do m=$(metric); [ "${m:-0}" -gt 0 ] && break; sleep 5; done
# the service applies the 30-minute grace to V4, so only V2 shows up here
check "in-cell audit service exports the violations as a metric (count=$m)" test "${m:-0}" -gt 0
kubectl label node "$FREE" basalt.io/dedicated-cluster- >/dev/null
kubectl label node "$SNODE" basalt.io/dedicated-cluster- >/dev/null
check "violations clear once the state is restored" bash -c "bin/basalt-audit --once --grace=0 >/dev/null"

echo "== 5. delete"
for c in "$S" "$D"; do api -X DELETE "$API/v1/projects/$P/clusters/$c" -H "Idempotency-Key: del-$c" >/dev/null; done
check "dedicated cluster deleted" wait_state "$D" GONE 900
check "shared cluster deleted" wait_state "$S" GONE 900
check "dedicated nodes returned to the free pool (3 free, none owned)" \
  test "$(nodes_of basalt.io/pool=free | wc -w)/$(nodes_of basalt.io/dedicated-cluster=$D | wc -w)" = "3/0"
check "returned nodes carry only the free-pool taint" \
  bash -c "for n in $(nodes_of basalt.io/pool=free); do [ \"\$(kubectl get node \$n -o jsonpath='{range .spec.taints[*]}{.key}={.value} {end}')\" = 'basalt.io/pool=free ' ] || exit 1; done"
check "basalt-audit: no violations after cleanup" bash -c "bin/basalt-audit --once >/dev/null"
for c in "$S" "$D"; do sed -i "/ $(cat "$WORK/host-$c")\$/d" /etc/hosts; done
echo "== $FAILS failed"
exit "$FAILS"
