# ADR-0003: Sandbox runtime for shared-tier server pods

Status: **Proposed, provisional**. The final choice comes from the M2/M6 benchmarks. Date: 2026-11-04

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
