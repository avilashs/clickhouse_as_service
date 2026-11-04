#!/usr/bin/env bash
# Start the local control plane (Postgres, Temporal dev server, API, worker) on the host and
# deploy the cell agent into the kind cell. Run hack/cell-up.sh first.
# State lives in $BASALT_HOME (default /var/lib/basalt): env file, logs, secrets, Temporal DB.
set -euo pipefail
cd "$(dirname "$0")/.."
export BASALT_HOME=${BASALT_HOME:-/var/lib/basalt}
mkdir -p "$BASALT_HOME/logs" bin
ENVF=$BASALT_HOME/env
if [ ! -f "$ENVF" ]; then
  cat > "$ENVF" <<EOT
export BASALT_PG_DSN=postgres://basalt:basalt@127.0.0.1:5432/basalt?sslmode=disable
export BASALT_TEMPORAL=127.0.0.1:7233
export BASALT_SECRETS_DIR=$BASALT_HOME/secrets
export BASALT_LISTEN=0.0.0.0:8080
export BASALT_CELL_ID=cell1
export BASALT_DOMAIN=db.example.com
export BASALT_CELL_PROFILE=local
export BASALT_CELL_AGENT_TOKEN=$(openssl rand -hex 24)
export BASALT_TENANT_TOKEN=$(openssl rand -hex 24)
export BASALT_API_TOKENS=demo:\$BASALT_TENANT_TOKEN
EOT
  chmod 600 "$ENVF"
fi
# shellcheck disable=SC1090
source "$ENVF"

# Postgres
if docker ps -a --format '{{.Names}}' | grep -qx basalt-pg; then
  docker start basalt-pg >/dev/null   # keep existing data
else
  docker run -d --name basalt-pg -e POSTGRES_PASSWORD=basalt -e POSTGRES_USER=basalt -e POSTGRES_DB=basalt \
    -p 127.0.0.1:5432:5432 postgres:16.10 >/dev/null
fi
until docker exec basalt-pg pg_isready -U basalt >/dev/null 2>&1; do sleep 1; done
# Temporal dev server (persistent SQLite so workflows survive a server restart)
if ! pgrep -f "temporal server start-dev" >/dev/null; then
  nohup temporal server start-dev --headless --db-filename "$BASALT_HOME/temporal.db" --port 7233 --ui-port 8233 \
    --log-level warn > "$BASALT_HOME/logs/temporal.log" 2>&1 &
  until temporal operator cluster health >/dev/null 2>&1; do sleep 1; done
fi

CGO_ENABLED=0 go build -trimpath -o bin/ ./cmd/...

hack/restart.sh api worker

# Cell agent: image, token secret, deployment. The agent reaches the host-side control plane
# through the kind network gateway (outbound from the cell, as in production).
GW=$(docker network inspect kind -f '{{(index .IPAM.Config 0).Gateway}}')
docker build -q -f Dockerfile.cell -t basalt-cell:dev . >/dev/null
hack/kind-load.sh --pool control basalt-cell:dev
kubectl -n basalt-system create secret generic basalt-agent \
  --from-literal=token="$BASALT_CELL_AGENT_TOKEN" --from-literal=controlPlaneUrl="http://$GW:8080" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl apply -f deploy/cell/agent/agent.yaml -f deploy/cell/audit/audit.yaml >/dev/null
kubectl -n basalt-system rollout restart deploy/basalt-agent deploy/basalt-audit >/dev/null
kubectl -n basalt-system rollout status deploy/basalt-agent --timeout=120s
kubectl -n basalt-system rollout status deploy/basalt-audit --timeout=120s
echo "control plane: http://127.0.0.1:8080   tenant token: source $ENVF; echo \$BASALT_TENANT_TOKEN"
