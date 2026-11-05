#!/usr/bin/env bash
# (Re)start host-side control-plane processes: hack/restart.sh api worker
set -euo pipefail
cd "$(dirname "$0")/.."
BASALT_HOME=${BASALT_HOME:-/var/lib/basalt}
# shellcheck disable=SC1091
source "$BASALT_HOME/env"
for p in "$@"; do
  # exact process name: `pkill -f` also matched the calling shell whenever its command line mentioned bin/basalt-$p
  pkill -x "basalt-$p" 2>/dev/null || true
  sleep 1
  nohup "bin/basalt-$p" >> "$BASALT_HOME/logs/$p.log" 2>&1 &
  echo "started basalt-$p (pid $!)"
done
sleep 2
