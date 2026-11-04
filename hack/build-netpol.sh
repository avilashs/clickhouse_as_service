#!/usr/bin/env bash
# Build kube-network-policies (kubernetes-sigs, Apache-2.0) from source and load it into the kind
# cell as basalt-netpol:v1.1.3. kindnet does not enforce NetworkPolicy, and the upstream image
# lives on registry.k8s.io, which some build environments cannot reach (docs/m3-report.md).
# Usage: hack/build-netpol.sh <path to a clone of github.com/kubernetes-sigs/kube-network-policies>
set -euo pipefail
cd "$(dirname "$0")/.."
SRC=${1:?usage: hack/build-netpol.sh <kube-network-policies clone>}
TAG=v1.1.3
COMMIT=d3fc697c1acaa0c8872c262ff3e04d8299eac6b3
IMAGE=basalt-netpol:$TAG
OUT=${BASALT_HOME:-/var/lib/basalt}/knp

# kube-network-policies hands new connections to userspace with the nftables `queue` statement:
# the host kernel needs CONFIG_NFT_QUEUE (the M3 build VM's kernel lacks it; see docs/m3-report.md)
if zcat /proc/config.gz 2>/dev/null | grep -q '^# CONFIG_NFT_QUEUE is not set'; then
  echo "this kernel has no CONFIG_NFT_QUEUE: kube-network-policies cannot enforce anything here" >&2
  exit 1
fi

# build only the exact reviewed release: tagged commit, clean tree, upstream origin
[ "$(git -C "$SRC" rev-parse HEAD)" = "$COMMIT" ] || { echo "$SRC is not at $TAG ($COMMIT)" >&2; exit 1; }
[ -z "$(git -C "$SRC" status --porcelain)" ] || { echo "$SRC has local changes" >&2; exit 1; }
git -C "$SRC" remote get-url origin | grep -q 'github.com/kubernetes-sigs/kube-network-policies' ||
  { echo "$SRC origin is not kubernetes-sigs/kube-network-policies" >&2; exit 1; }

mkdir -p "$OUT"
(cd "$SRC" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$OUT/netpol" ./cmd/kube-network-policies/standard)
cat > "$OUT/Dockerfile" <<'EOF'
FROM scratch
COPY netpol /bin/netpol
ENTRYPOINT ["/bin/netpol"]
EOF
docker build -q -t "$IMAGE" "$OUT" >/dev/null
hack/kind-load.sh "$IMAGE"
echo "loaded $IMAGE into every node"
