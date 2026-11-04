#!/usr/bin/env bash
# ADR-0003 spike S2: ClickHouse 26.8 under gVisor (runsc, systrap) vs runc on the same pool,
# same pod resources (1 vCPU, 2 GiB, requests = limits) and the same explicit settings.
# Checks: the sandbox runs inside the pod cgroup (limits enforced), the memory cap holds
# (MEMORY_LIMIT_EXCEEDED, no OOM kill), then times CPU, insert, merge, scan and point queries.
# Usage: test/spike/gvisor-bench.sh [rounds]   (needs the local kind cell from hack/cell-up.sh)
set -uo pipefail
ROUNDS=${1:-3}
NS=spike-gvisor
kubectl create ns $NS --dry-run=client -o yaml | kubectl apply -f - >/dev/null
cat <<'YAML' | kubectl -n $NS apply -f - >/dev/null
apiVersion: v1
kind: ConfigMap
metadata: {name: ch-cfg}
data:
  basalt.xml: |
    <clickhouse>
      <!-- 85% of the 2 GiB pod limit, in bytes: gVisor hides the cgroup from ClickHouse -->
      <max_server_memory_usage>1825361100</max_server_memory_usage>
      <!-- gVisor systrap patches syscall instructions in memory; the binary self-check then fails -->
      <skip_binary_checksum_checks>1</skip_binary_checksum_checks>
      <profiles><default><max_threads>1</max_threads></default></profiles>
    </clickhouse>
YAML
for rt in runc gvisor; do
  rc=""; [ $rt = gvisor ] && rc="runtimeClassName: gvisor"
  cat <<YAML | kubectl -n $NS apply -f - >/dev/null
apiVersion: v1
kind: Pod
metadata: {name: ch-$rt, labels: {spike: gvisor}}
spec:
  $rc
  nodeSelector: {basalt.io/pool: shared}
  tolerations: [{key: basalt.io/pool, operator: Equal, value: shared, effect: NoSchedule}]
  securityContext: {runAsUser: 101, runAsGroup: 101, fsGroup: 101, runAsNonRoot: true}
  containers:
  - name: ch
    image: clickhouse/clickhouse-server:26.8.34.13
    imagePullPolicy: IfNotPresent
    resources: {requests: {cpu: "1", memory: 2Gi}, limits: {cpu: "1", memory: 2Gi}}
    volumeMounts: [{name: cfg, mountPath: /etc/clickhouse-server/config.d/basalt.xml, subPath: basalt.xml}]
  volumes: [{name: cfg, configMap: {name: ch-cfg}}]
YAML
done
kubectl -n $NS wait --for=condition=Ready pod -l spike=gvisor --timeout=300s >/dev/null || { kubectl -n $NS get pods; exit 1; }
sleep 10
q() { kubectl -n $NS exec "$1" -- clickhouse-client -n -q "$2" 2>&1; }
t() { kubectl -n $NS exec "$1" -- clickhouse-client --time -n -q "$2" 2>&1 >/dev/null | tail -1; }

echo "== sandbox placement (pod cgroup must contain the sandbox processes)"
for rt in runc gvisor; do
  node=$(kubectl -n $NS get pod ch-$rt -o jsonpath='{.spec.nodeName}'); uid=$(kubectl -n $NS get pod ch-$rt -o jsonpath='{.metadata.uid}')
  docker exec "$node" sh -c "d=\$(find /sys/fs/cgroup/memory/kubelet -maxdepth 4 -type d -name 'pod$uid' | head -1); echo \"$rt: kernel=\$(uname -r) pod_cgroup_mem_limit=\$(cat \$d/memory.limit_in_bytes) usage=\$(cat \$d/memory.usage_in_bytes) cpu_quota=\$(cat /sys/fs/cgroup/cpu/\${d#/sys/fs/cgroup/memory/}/cpu.cfs_quota_us)\""
  echo "   in-pod uname: $(kubectl -n $NS exec ch-$rt -- uname -r)"
done

echo "== memory cap (runaway query must fail with code 241, pod must not restart)"
for rt in runc gvisor; do
  r=$(q ch-$rt "SELECT length(groupArray(toString(number))) FROM numbers(2000000000)" | grep -oE "Code: [0-9]+" | head -1)
  echo "$rt: $r restarts=$(kubectl -n $NS get pod ch-$rt -o jsonpath='{.status.containerStatuses[0].restartCount}')"
done

echo "== benchmark (seconds, best of $ROUNDS)"
printf "%-7s %8s %10s %9s %9s %8s\n" runtime cpu insert30M optimize groupby point
for rt in runc gvisor; do
  best() { local b=999999; for _ in $(seq 1 "$ROUNDS"); do v=$(t ch-$rt "$1"); b=$(python3 -c "print(min($b, $v))"); done; echo "$b"; }
  q ch-$rt "DROP TABLE IF EXISTS b SYNC; CREATE TABLE b (k UInt64, s String, v Float64) ENGINE = MergeTree ORDER BY k" >/dev/null
  cpu=$(best "SELECT sum(cityHash64(number)) FROM numbers(100000000) FORMAT Null")
  ins=$(t ch-$rt "INSERT INTO b SELECT number, toString(number % 100000), rand() / 7 FROM numbers(30000000)")
  for i in 1 2 3 4 5; do q ch-$rt "INSERT INTO b SELECT number, toString(number), 1 FROM numbers(1000000)" >/dev/null; done
  opt=$(t ch-$rt "OPTIMIZE TABLE b FINAL")
  grp=$(best "SELECT s, count(), avg(v) FROM b GROUP BY s ORDER BY count() DESC LIMIT 5 FORMAT Null")
  pnt=$(best "SELECT count() FROM b WHERE k BETWEEN 1000 AND 2000 FORMAT Null")
  printf "%-7s %8s %10s %9s %9s %8s\n" $rt "$cpu" "$ins" "$opt" "$grp" "$pnt"
done
[ "${KEEP:-}" = 1 ] || kubectl delete ns $NS --wait=false >/dev/null
