#!/usr/bin/env bash
# Run the M4 stability suite against two live shared-tier clusters of the local cell.
# Usage: test/stability/run.sh <cluster A (project demo)> <cluster B (project rival)> [go test -run pattern]
# Needs hack/dev-up.sh and clusters of size t-2 created after the M4 render. The suite creates and
# removes its own tables and users; TestDiskFull restarts replica 0 of cluster A.
set -uo pipefail
cd "$(dirname "$0")/../.."
BASALT_HOME=${BASALT_HOME:-/var/lib/basalt}
# shellcheck disable=SC1091
source "$BASALT_HOME/env"
A=${1:?cluster A}; B=${2:?cluster B}; RUN=${3:-.}
GW=$(kubectl -n envoy-gateway-system get svc -l gateway.envoyproxy.io/owning-gateway-name=shared -o jsonpath='{.items[0].metadata.name}')
kubectl -n envoy-gateway-system port-forward "svc/$GW" 29440:9440 28443:8443 >/dev/null 2>&1 &
PF=$!; trap 'kill $PF 2>/dev/null' EXIT
sleep 3
export STAB_API=http://127.0.0.1:8080 STAB_GATEWAY_HTTPS=127.0.0.1:28443 \
  STAB_PROJECT_A=demo STAB_TOKEN_A=$BASALT_TENANT_TOKEN STAB_CLUSTER_A=$A \
  STAB_PROJECT_B=rival STAB_TOKEN_B=$BASALT_TENANT2_TOKEN STAB_CLUSTER_B=$B
LOG=${LOG:-$BASALT_HOME/logs/stability-$(date +%Y%m%d-%H%M%S).log}
go test -tags stability -count=1 -v -timeout 60m -run "$RUN" ./test/stability/ 2>&1 | tee "$LOG"
echo; echo "== summary ($LOG)"
grep -E "^(--- |ok|FAIL|PASS)" "$LOG" | sed -E 's/ \([0-9.]+s\)//'
