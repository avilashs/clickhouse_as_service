#!/usr/bin/env bash
# Run the M3 attack suite against two live tenants of the local cell.
# Usage: test/attack/run.sh <tenant-A cluster id (project demo)> <tenant-B cluster id (project rival)>
# Needs hack/dev-up.sh (control plane) and deploy/test/attack-canary.yaml applied.
set -uo pipefail
cd "$(dirname "$0")/../.."
BASALT_HOME=${BASALT_HOME:-/var/lib/basalt}
# shellcheck disable=SC1091
source "$BASALT_HOME/env"
A=${1:?tenant A cluster id}; B=${2:?tenant B cluster id}
kubectl apply -f deploy/test/attack-canary.yaml >/dev/null
kubectl -n basalt-system rollout status deploy/attack-canary --timeout=120s >/dev/null
GW=$(kubectl -n envoy-gateway-system get svc -l gateway.envoyproxy.io/owning-gateway-name=shared -o jsonpath='{.items[0].metadata.name}')
kubectl -n envoy-gateway-system port-forward "svc/$GW" 29440:9440 28443:8443 >/dev/null 2>&1 &
PF=$!; trap 'kill $PF 2>/dev/null' EXIT
sleep 3
export ATTACK_API=http://127.0.0.1:8080 ATTACK_GATEWAY_HTTPS=127.0.0.1:28443 ATTACK_GATEWAY_NATIVE=127.0.0.1:29440 \
  ATTACK_PROJECT_A=demo ATTACK_TOKEN_A=$BASALT_TENANT_TOKEN ATTACK_CLUSTER_A=$A \
  ATTACK_PROJECT_B=rival ATTACK_TOKEN_B=$BASALT_TENANT2_TOKEN ATTACK_CLUSTER_B=$B \
  ATTACK_SECRETS_DIR=$BASALT_SECRETS_DIR
LOG=${LOG:-$BASALT_HOME/logs/attack-$(date +%Y%m%d-%H%M%S).log}
go test -tags attack -count=1 -v -timeout 30m ./test/attack/ 2>&1 | tee "$LOG"
echo
echo "== summary ($LOG)"
grep -E "^\s+--- (PASS|FAIL)" "$LOG" | sed -E 's/ \([0-9.]+s\)//' | sort | uniq -c | awk '{print $2, $3}' | sort | uniq -c
grep -cE "^\s+--- PASS" "$LOG" | xargs echo "passed subtests:"
grep -cE "^\s+--- FAIL" "$LOG" | xargs echo "failed subtests:"
