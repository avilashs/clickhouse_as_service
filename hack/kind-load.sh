#!/usr/bin/env bash
# Load images into the nodes of a kind cluster, optionally only nodes of one Basalt pool.
# Works around `kind load` failing on multi-platform images with Docker's containerd image
# store ("content digest ... not found"), and keeps disk use down by loading per pool.
# Usage: hack/kind-load.sh [--pool control|shared] image...
set -euo pipefail
CLUSTER=${KIND_CLUSTER:-basalt-cell1}
POOL=""
if [ "${1:-}" = "--pool" ]; then POOL=$2; shift 2; fi
if [ -n "$POOL" ]; then
  NODES=$(kubectl --context "kind-$CLUSTER" get nodes -l "basalt.io/pool=$POOL" -o jsonpath='{.items[*].metadata.name}')
else
  NODES=$(kind get nodes --name "$CLUSTER")
fi
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
for img in "$@"; do
  f="$TMP/image.tar"
  docker save --platform linux/amd64 -o "$f" "$img"
  for node in $NODES; do
    docker exec -i "$node" ctr -n k8s.io images import --platform linux/amd64 - < "$f" >/dev/null
  done
  rm -f "$f"
  echo "loaded $img -> ${POOL:-all nodes}"
done
