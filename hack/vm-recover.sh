#!/usr/bin/env bash
# Bring the local stack back after a VM or Docker restart: dockerd, the kind nodes (their
# addresses are pinned by cell-up.sh), Postgres, host sysctls, Temporal, API and worker, then
# restart the pods that cache node or service addresses (Envoy Gateway's xDS, the operator).
set -euo pipefail
cd "$(dirname "$0")/.."
BASALT_HOME=${BASALT_HOME:-/var/lib/basalt}
CLUSTER=${KIND_CLUSTER:-basalt-cell1}
export PATH=/root/bin:$PATH

if ! docker info >/dev/null 2>&1; then
  nohup dockerd >> "$BASALT_HOME/logs/dockerd.log" 2>&1 &
  until docker info >/dev/null 2>&1; do sleep 1; done
fi
docker start $(kind get nodes --name "$CLUSTER") basalt-pg >/dev/null
sysctl -w fs.inotify.max_user_instances=1024 fs.inotify.max_user_watches=1048576 >/dev/null
until kubectl get nodes >/dev/null 2>&1; do sleep 2; done
kubectl wait --for=condition=Ready nodes --all --timeout=300s >/dev/null

if ! pgrep -f "temporal server start-dev" >/dev/null; then
  nohup temporal server start-dev --headless --db-filename "$BASALT_HOME/temporal.db" --port 7233 --ui-port 8233 \
    --log-level warn >> "$BASALT_HOME/logs/temporal.log" 2>&1 &
  until temporal operator cluster health >/dev/null 2>&1; do sleep 1; done
fi
hack/restart.sh api worker

kubectl -n envoy-gateway-system rollout restart deploy >/dev/null
kubectl -n kube-system rollout restart deploy/clickhouse-operator >/dev/null
kubectl -n envoy-gateway-system rollout status deploy --timeout=300s >/dev/null
echo "stack recovered"
