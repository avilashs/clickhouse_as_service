#!/usr/bin/env bash
# Build gVisor runsc + containerd shim for local kind cells into /var/lib/basalt/gvisor.
# Upstream release binaries were not downloadable from the build environment, so this builds
# gVisor's Go-buildable "go" branch at a pinned commit and applies local-only patches
# (hack/gvisor-local.patch). Production cells install upstream releases.
set -euo pipefail
cd "$(dirname "$0")/.."
COMMIT=${GVISOR_COMMIT:-e0105ccf200aa879bb21c212a6b8e6d91ff97378}
OUT=${OUT:-/var/lib/basalt/gvisor}
SRC=$(mktemp -d); trap 'rm -rf "$SRC"' EXIT
git -C "$SRC" init -q && git -C "$SRC" remote add origin https://github.com/google/gvisor
git -C "$SRC" fetch -q --depth 1 origin "$COMMIT" && git -C "$SRC" checkout -q FETCH_HEAD
patch -s -d "$SRC" -p1 < hack/gvisor-local.patch
mkdir -p "$OUT"
mkdir -p "$OUT/gvisor-bin"
(cd "$SRC" && CGO_ENABLED=0 go build -trimpath -o "$OUT/runsc" ./runsc \
  && CGO_ENABLED=0 go build -trimpath -o "$OUT/containerd-shim-runsc-v1" ./shim \
  && CGO_ENABLED=0 go build -trimpath -o "$OUT/gvisor-bin/gvisor_sentry" ./runsc/cmd/sentry \
  && CGO_ENABLED=0 go build -trimpath -o "$OUT/gvisor-bin/runsc-metric-server" ./runsc/cmd/metricserver \
  && CGO_ENABLED=0 go build -trimpath -o "$OUT/gvisor-bin/checkpointgofer" ./runsc/checkpointgofer)
# C sidecars (static, so they run inside any node image)
for c in prewarmer:gvisor-sentry-prewarmer procumounter:gvisor-proc-umounter fdparking:runsc-fd-parking; do
  gcc -O2 -static -nostdlib -nostartfiles -ffreestanding -fno-stack-protector -fno-builtin -o "$OUT/gvisor-bin/${c#*:}" "$SRC/runsc/${c%%:*}/${c%%:*}.c"
done
chmod 755 "$OUT"/runsc "$OUT"/containerd-shim-runsc-v1 "$OUT"/gvisor-bin/*
echo "built gVisor $COMMIT into $OUT"
