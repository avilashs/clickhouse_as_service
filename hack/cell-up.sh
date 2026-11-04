#!/usr/bin/env bash
# Bring up a local Basalt cell on kind: nodes, pool taints, images, Altinity operator, Envoy Gateway.
# Requires: docker, kind, kubectl, helm on PATH. Idempotent apart from cluster creation.
set -euo pipefail
cd "$(dirname "$0")/.."
CLUSTER=${KIND_CLUSTER:-basalt-cell1}
CH_VERSION=${CH_VERSION:-26.8.34.13}
EG_VERSION=v1.5.0
CONTROL_IMAGES=(
  altinity/clickhouse-operator:0.27.4 altinity/metrics-exporter:0.27.4
  clickhouse/clickhouse-keeper:$CH_VERSION
  envoyproxy/gateway:$EG_VERSION envoyproxy/envoy:distroless-v1.35.0
)
SHARED_IMAGES=(clickhouse/clickhouse-server:$CH_VERSION)

# 10 kind nodes need more inotify instances than the usual default of 128 (kubelet fails to
# start cAdvisor with "too many open files")
if [ "$(sysctl -n fs.inotify.max_user_instances)" -lt 1024 ]; then
  sysctl -w fs.inotify.max_user_instances=1024 fs.inotify.max_user_watches=1048576 >/dev/null
fi
[ -x /var/lib/basalt/gvisor/runsc ] || hack/build-gvisor.sh
[ -f /var/lib/basalt/gvisor/runsc.toml ] || cp deploy/kind/runsc.toml /var/lib/basalt/gvisor/runsc.toml

if ! kind get clusters | grep -qx "$CLUSTER"; then
  docker image inspect kindest/node:v1.33.1 >/dev/null 2>&1 || docker pull kindest/node:v1.33.1
  kind create cluster --name "$CLUSTER" --config deploy/kind/cell-kind.yaml --image kindest/node:v1.33.1 --wait 240s
  # Pin every node's address: Docker hands out IPs again in start order after a daemon or VM
  # restart, and the cell breaks (nodes, DNS, volume binding, Envoy xDS) when they move.
  # A static address on the kind network survives restarts.
  nodes=$(kind get nodes --name "$CLUSTER")
  declare -A ips
  for n in $nodes; do ips[$n]=$(docker inspect -f '{{(index .NetworkSettings.Networks "kind").IPAddress}}' "$n"); done
  docker stop $nodes >/dev/null
  for n in $nodes; do docker network disconnect kind "$n"; done
  for n in $nodes; do docker network connect --ip "${ips[$n]}" kind "$n"; done
  docker start $nodes >/dev/null
  until kubectl --context "kind-$CLUSTER" get nodes >/dev/null 2>&1; do sleep 2; done
  kubectl --context "kind-$CLUSTER" wait --for=condition=Ready nodes --all --timeout=240s >/dev/null
fi
kubectl config use-context "kind-$CLUSTER" >/dev/null

# Pool taints (design §8.1): every pool is tainted, so nothing lands anywhere by accident.
# Free nodes keep their taint until a dedicated cluster claims them (taint is then replaced by
# basalt.io/dedicated-cluster=<id>).
for pool in control shared free; do
  for n in $(kubectl get nodes -l basalt.io/pool=$pool -o name); do
    kubectl taint node "${n#node/}" basalt.io/pool=$pool:NoSchedule --overwrite >/dev/null
  done
done
kubectl apply -f deploy/cell/runtimeclass.yaml >/dev/null

for i in "${CONTROL_IMAGES[@]}" "${SHARED_IMAGES[@]}"; do
  docker image inspect "$i" >/dev/null 2>&1 || docker pull -q "$i" >/dev/null
done
# Each pool gets only the images it runs (a full copy per node costs several GB).
KIND_CLUSTER=$CLUSTER hack/kind-load.sh --pool control "${CONTROL_IMAGES[@]}"
KIND_CLUSTER=$CLUSTER hack/kind-load.sh --pool shared "${SHARED_IMAGES[@]}"
KIND_CLUSTER=$CLUSTER hack/kind-load.sh --pool free "${SHARED_IMAGES[@]}"   # dedicated servers

# operator credentials: random per cell, created once (never the bundle's public default)
if ! kubectl -n kube-system get secret clickhouse-operator >/dev/null 2>&1 || \
   [ "$(kubectl -n kube-system get secret clickhouse-operator -o jsonpath='{.data.password}' | base64 -d)" = clickhouse_operator_password ]; then
  kubectl -n kube-system create secret generic clickhouse-operator --from-literal=username=clickhouse_operator \
    --from-literal=password="$(openssl rand -hex 24)" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  kubectl -n kube-system rollout restart deploy/clickhouse-operator >/dev/null 2>&1 || true
fi
kubectl apply --server-side -k deploy/cell/altinity-operator >/dev/null
kubectl -n kube-system rollout status deploy/clickhouse-operator --timeout=180s

EG_CHART=${EG_CHART:-/tmp/gateway-helm-$EG_VERSION.tgz}
[ -f "$EG_CHART" ] || helm pull oci://docker.io/envoyproxy/gateway-helm --version "$EG_VERSION" -d /tmp
helm upgrade --install eg "$EG_CHART" -n envoy-gateway-system --create-namespace \
  -f deploy/cell/envoy-gateway/values.yaml --wait --timeout 5m >/dev/null
kubectl create namespace basalt-system --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl apply -f deploy/cell/envoy-gateway/gateway.yaml >/dev/null
kubectl -n basalt-system wait gateway/shared --for=condition=Programmed --timeout=180s
# placement admission (design §8.3); applied last so the platform components above are in place
kubectl apply -f deploy/cell/policies/placement.yaml -f deploy/cell/policies/security.yaml >/dev/null

# NetworkPolicy enforcement: kindnet ignores NetworkPolicy, so local cells run kube-router in
# firewall-only mode beside it (docs/m3-report.md)
[ -n "$(docker exec "$CLUSTER-worker" crictl images -q basalt-kube-router:v2.11.2 2>/dev/null)" ] || hack/build-kube-router.sh
kubectl apply -f deploy/cell/netpol/kube-router.yaml >/dev/null
kubectl -n kube-system rollout status ds/kube-router --timeout=180s >/dev/null
echo "cell $CLUSTER ready"
