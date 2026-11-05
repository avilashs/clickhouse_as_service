#!/usr/bin/env bash
# Run the M5 lifecycle suite against one live shared-tier cluster of the local cell (size t-2).
# Usage: test/lifecycle/run.sh <cluster id (project demo)> [steps]   e.g. add-replica,upgrade,scale
# LIFE_QUORUM overrides the insert_quorum the verifier uses (default auto).
set -uo pipefail
cd "$(dirname "$0")/../.."
BASALT_HOME=${BASALT_HOME:-/var/lib/basalt}
# shellcheck disable=SC1091
source "$BASALT_HOME/env"
A=${1:?cluster (project demo)}; STEPS=${2:-add-replica,upgrade,scale}
GW=$(kubectl -n envoy-gateway-system get svc -l gateway.envoyproxy.io/owning-gateway-name=shared -o jsonpath='{.items[0].metadata.name}')
kubectl -n envoy-gateway-system port-forward "svc/$GW" 29440:9440 28443:8443 >/dev/null 2>&1 &
PF=$!; trap 'kill $PF 2>/dev/null' EXIT
sleep 3
export LIFE_API=http://127.0.0.1:8080 LIFE_GATEWAY_HTTPS=127.0.0.1:28443 LIFE_STEPS=$STEPS \
  LIFE_PROJECT_A=demo LIFE_TOKEN_A=$BASALT_TENANT_TOKEN LIFE_CLUSTER_A=$A \
  LIFE_PROJECT_B=rival LIFE_TOKEN_B=$BASALT_TENANT2_TOKEN LIFE_CLUSTER_B=$A
LOG=${LOG:-$BASALT_HOME/logs/lifecycle-$(date +%Y%m%d-%H%M%S).log}
go test -tags lifecycle -count=1 -v -timeout 150m -run TestLifecycleUnderLoad ./test/lifecycle/ 2>&1 | tee "$LOG"
