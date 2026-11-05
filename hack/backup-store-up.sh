#!/usr/bin/env bash
# Deploy the test object store (deploy/test/backup-store.yaml), create the bucket and key, put the
# key in the control plane's secret store, and print the environment the API and worker need.
# Usage: hack/backup-store-up.sh   (idempotent)
set -euo pipefail
cd "$(dirname "$0")/.."
BASALT_HOME=${BASALT_HOME:-/var/lib/basalt}
BUCKET=basalt-backups
kubectl apply -f deploy/test/backup-store.yaml >/dev/null
kubectl -n basalt-system rollout status deploy/backup-store --timeout=180s >/dev/null
G() { kubectl -n basalt-system exec deploy/backup-store -- /garage -c /etc/garage/garage.toml "$@"; }
for i in $(seq 1 30); do G status >/dev/null 2>&1 && break; sleep 2; done
if ! G layout show 2>/dev/null | grep -q "dc1"; then
  NODE=$(G node id 2>/dev/null | head -1 | cut -d@ -f1)
  G layout assign -z dc1 -c 1G "$NODE" >/dev/null
  G layout apply --version 1 >/dev/null
  sleep 3
fi
G bucket info "$BUCKET" >/dev/null 2>&1 || G bucket create "$BUCKET" >/dev/null
if ! G key info basalt-backup >/dev/null 2>&1; then
  G key create basalt-backup >/dev/null
fi
G bucket allow --read --write --owner "$BUCKET" --key basalt-backup >/dev/null
KEY=$(G key info basalt-backup --show-secret)
AK=$(echo "$KEY" | awk '/Key ID:/{print $3}')
SK=$(echo "$KEY" | awk '/Secret key:/{print $3}')
# the key, sealed in the control plane's secret store (needs bin/basalt-secret: go build -o bin/basalt-secret ./cmd/basalt-secret)
# shellcheck disable=SC1091
( source "$BASALT_HOME/env"
  printf %s "$AK" | bin/basalt-secret set backup/access-key
  printf %s "$SK" | bin/basalt-secret set backup/secret-key )
echo "# add to $BASALT_HOME/env (API and worker):"
echo "export BASALT_BACKUP_URL=http://backup-store.basalt-system.svc.cluster.local:3900/$BUCKET/"
echo "export BASALT_BACKUP_EGRESS_NAMESPACE=basalt-system"
echo "export BASALT_BACKUP_EGRESS_POD_LABELS=app=backup-store"
echo "export BASALT_BACKUP_CONTROL_ENDPOINT=127.0.0.1:23900   # kubectl -n basalt-system port-forward svc/backup-store 23900:3900"
echo "export BASALT_BACKUP_ACCESS_KEY=$AK"
echo "export BASALT_BACKUP_SECRET_KEY=$SK"
