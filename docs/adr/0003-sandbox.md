# ADR-0003: Sandbox runtime for shared-tier server pods

Status: **Proposed, provisional** (M2 spike done, see results below; decision at M6 on cloud hardware). Date: 2026-11-04

## Context
Shared nodes run hostile SQL inside a process with a large attack surface. Even with SQL-level controls, a memory-safety bug gives the attacker code execution, and that leaves only the kernel boundary between them and other tenants. The prompt requires a sandbox `RuntimeClass` on shared nodes.

## Options
| | gVisor (`runsc`) | Kata Containers |
|---|---|---|
| Isolation | User-space kernel (Sentry) intercepts syscalls; small host syscall surface | Lightweight VM per pod; guest kernel; hardware virtualisation boundary |
| AWS fit | Runs on any EC2 instance (systrap platform; no nested virtualisation) | Needs `/dev/kvm`: **bare-metal (`*.metal`) instances**, or nested virtualisation where offered. This must be verified for current EC2 families; I'm not relying on nested virtualisation without confirming it. |
| I/O | Historically significant overhead for syscall-heavy and file I/O workloads; directfs and recent improvements narrow it. ClickHouse does heavy `pread`, mmap and many small files during merges. **Risk.** | Near-native CPU; block I/O via virtio-blk/virtio-fs; EBS volumes attach to the host and pass through to the guest. Good I/O if block devices pass through rather than through a shared filesystem. |
| Memory | Pod memory is plain host memory, with small overhead | Per-VM overhead (tens to hundreds of MiB); memory hotplug complicates "requests = limits" sizing |
| Compatibility | Some syscalls and `/proc` and cgroup files are emulated: **must check** that ClickHouse reads its memory limit correctly, and that `io_uring` (if enabled), `perf_event_open` (query profiler) and `getrusage` per-thread work | Full Linux guest; high compatibility |
| Ops | DaemonSet installs `runsc` and containerd shim; works on standard EKS AMIs/Bottlerocket with care | Metal instances raise the per-node cost floor and shrink fleet granularity |

## Benchmark plan (M2 spike S2, published in M6)
On the same instance type, run runc vs gVisor (systrap, directfs on), and Kata on a metal instance:
1. ClickBench-style query set on a 100 GB `hits`-like dataset (cold and warm cache).
2. Insert throughput: `INSERT … SELECT` at 1M rows/s target plus concurrent small inserts (merge pressure).
3. Merge throughput: time to `OPTIMIZE FINAL` a fixed set of parts.
4. Replication fetch throughput between two pods.
5. Startup time with 10k parts.
6. Behaviour checks: memory limit detected (`system.server_settings`/startup log), `MEMORY_LIMIT_EXCEEDED` instead of OOM, the query profiler works or is cleanly disabled.

**Acceptance threshold (proposed):** p50 query latency ≤ 1.3× runc, and insert/merge throughput ≥ 0.7× runc.

## Provisional decision
**gVisor**, because it works on ordinary EC2 instances and keeps shared-pool economics. If it misses the threshold, the next candidate is **Kata on metal** for the shared pool, which needs your approval because of cost (Q5). Under no circumstances do we drop the sandbox requirement without asking you.

## Consequences
Release qualification must run under the chosen runtime (F-19). Tier migration crosses runtimes (shared gVisor → dedicated runc); the on-disk format doesn't change, so data is unaffected.

## M2 spike S2 results (2026-11-04, local kind cell)

Evidence: `test/spike/gvisor-bench.sh` with output in `docs/evidence/m2-gvisor-bench-run1.log`, plus the M2 e2e run with a full TLS-only shared cluster under gVisor.

**Build.** Upstream release downloads weren't reachable from this environment, so `hack/build-gvisor.sh` builds gVisor's Go-buildable `go` branch at a pinned commit. That covers `runsc`, the containerd shim, and the sidecars this version requires (`gvisor_sentry`, a C prewarmer, a proc umounter and FD parking, built statically). Two local-only patches were needed for kind inside a Firecracker VM:
1. Ignore the shim's failure to set `oom_score_adj`, which the VM forbids. This mirrors containerd's `restrict_oom_score_adj`.
2. Skip cgroup v1 submounts that don't contain the process. kind bind-mounts `/kubelet*` inside each controller hierarchy. **This looks like an upstream gVisor bug worth reporting.**

The kubelet must use the **cgroupfs** driver. With the systemd driver on cgroup v1, `runsc` put the sandbox in a cgroup it invented under `containerd.service`, so the pod's CPU and memory limits silently did not apply. The pod cgroup showed 0 usage, and the first benchmark ran up to 3x *faster* than runc. With cgroupfs, the sandbox runs inside the pod cgroup (verified by the pod cgroup's memory usage).

**Compatibility (ClickHouse 26.8.34 under runsc/systrap):**

| Check | Result | Consequence |
|---|---|---|
| Server starts | **No, by default**: the startup checksum of its own executable fails (code 246), consistent with systrap patching syscall instructions in memory | Render `skip_binary_checksum_checks = 1` for sandboxed servers only |
| Memory limit detected | **No**: cgroup memory files are not visible; ClickHouse sees the VM's 15.7 GiB instead of the pod's 2 GiB | `max_server_memory_usage` is always rendered as explicit bytes (85% of the pod) |
| CPU limit detected | **No**: `max_threads` defaulted to the VM's 4 cores | `max_threads` (and later pool sizes) are explicit per size |
| Runaway query | `MEMORY_LIMIT_EXCEEDED` (code 241), no restart | OK with explicit limits |
| Full cluster (TLS, Keeper TLS, replication, gateway) | Works (M2 e2e) | OK |

**Performance** (1 vCPU / 2 GiB pods, `max_threads=1`, best of 3, seconds):

| | CPU (100M hashes) | insert 30M rows | OPTIMIZE FINAL | GROUP BY | point query |
|---|---|---|---|---|---|
| runc | 0.644 | 10.8 | 10.9 | 2.1 | 0.005 |
| gVisor | 4.9 | 27.2 | 84.4 | 10.7 | 0.02 |
| ratio | 7.6x | 2.5x | 7.7x | 5.0x | 4x |

A second run reproduced these numbers within 15%. Turning off ClickHouse's query profiler made no difference.

**Conclusion.** gVisor is functionally viable with the two settings above, but **in this environment it is far outside the acceptance thresholds** (p50 ≤ 1.3x, throughput ≥ 0.7x). The environment is unusually hostile: systrap inside a Firecracker microVM, with no KVM and nested containers. The numbers don't transfer to bare EC2 instances, and the decision isn't final. **M6 must repeat `test/spike/gvisor-bench.sh` on real cloud nodes** (systrap on Nitro, and Kata on `*.metal`) before the shared tier's SLO and pricing are set. If gVisor misses the thresholds there too, the fallback in Q5 (Kata on metal) needs your decision. M2 enforces gVisor for shared servers because isolation matters more than speed, and the enforcement rules are independent of the runtime's name: it's a parameter.
