#!/usr/bin/env bash
# Load kube-router (cloudnativelabs, Apache-2.0) into the kind cell for NetworkPolicy
# enforcement beside kindnet, which ignores NetworkPolicy (docs/m3-report.md). The image is
# pulled by digest and slimmed to what firewall-only mode uses: the upstream image also carries
# CNI plugins and gobgp (~120 MB), and every node stores its own copy.
# Usage: hack/build-kube-router.sh
set -euo pipefail
cd "$(dirname "$0")/.."
TAG=v2.11.2
DIGEST=sha256:3e80cdbe0681f5c1cfc2e3caa8397a2b34dfe29a6de3e0baf1539ebaf8f339b5
UPSTREAM=${KUBE_ROUTER_REGISTRY:-mirror.gcr.io}/cloudnativelabs/kube-router@$DIGEST
IMAGE=basalt-kube-router:$TAG
WORK=$(mktemp -d); trap 'rm -rf "$WORK"' EXIT

docker pull -q "$UPSTREAM" >/dev/null
cat > "$WORK/Dockerfile" <<EOF
FROM $UPSTREAM AS upstream
RUN rm -rf /usr/libexec/cni /usr/local/bin/gobgp /usr/local/bin/cni-install
FROM scratch
COPY --from=upstream / /
ENTRYPOINT ["/usr/local/bin/kube-router"]
EOF
docker build -q -t "$IMAGE" "$WORK" >/dev/null
for pool in control shared free; do hack/kind-load.sh --pool "$pool" "$IMAGE"; done
docker rmi "$IMAGE" "$UPSTREAM" >/dev/null 2>&1 || true
echo "loaded $IMAGE into every pool node"
