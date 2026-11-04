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

if ! kind get clusters | grep -qx "$CLUSTER"; then
  docker image inspect kindest/node:v1.33.1 >/dev/null 2>&1 || docker pull kindest/node:v1.33.1
  kind create cluster --name "$CLUSTER" --config deploy/kind/cell-kind.yaml --image kindest/node:v1.33.1 --wait 240s
fi
kubectl config use-context "kind-$CLUSTER" >/dev/null

# Pool taints: shared nodes only accept shared-tier ClickHouse pods (design §8.1).
# The control pool stays untainted until M2 adds tolerations to every platform component.
for n in $(kubectl get nodes -l basalt.io/pool=shared -o name); do
  kubectl taint node "${n#node/}" basalt.io/pool=shared:NoSchedule --overwrite >/dev/null
done

for i in "${CONTROL_IMAGES[@]}" "${SHARED_IMAGES[@]}"; do
  docker image inspect "$i" >/dev/null 2>&1 || docker pull -q "$i" >/dev/null
done
# Each pool gets only the images it runs (a full copy per node costs several GB).
KIND_CLUSTER=$CLUSTER hack/kind-load.sh --pool control "${CONTROL_IMAGES[@]}"
KIND_CLUSTER=$CLUSTER hack/kind-load.sh --pool shared "${SHARED_IMAGES[@]}"

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
echo "cell $CLUSTER ready"
