# Profiling & Benchmark Toolchain

This project ships with a local-only profiling and benchmarking
toolchain so that future optimization work is evidence-driven. All
commands write artifacts to `.profile/` (gitignored).

## Prerequisites

- **Go 1.25+** — required for the `tool` directive in `go.mod`.
- One-time setup: `make profile-install` — runs `go install tool`,
  which materializes `benchstat` from the `go.mod` tool dep into
  `$(go env GOBIN)` (default `$GOPATH/bin`).

## Quick start

```bash
make profile-cpu    # bench + CPU profile → .profile/cpu.prof
make flamegraph     # open CPU flame graph in browser (pprof -http=:8080)
make benchstat      # diff current bench vs .profile/bench.base.txt
```

That's the 80% workflow. The other targets are for deeper dives.

## Targets

| Target | What it does | Artifact |
|---|---|---|
| `profile-install` | `go install tool` → materialize benchstat | `$GOBIN/benchstat` |
| `profile-cpu` | bench with `-cpuprofile` | `.profile/cpu.prof` |
| `profile-mem` | bench with `-memprofile -memprofilerate=1` (captures every alloc) | `.profile/mem.prof` |
| `profile-mutex` | bench with `-mutexprofile -mutexprofilefraction=1` (captures every contended lock) | `.profile/mutex.prof` |
| `profile-trace` | bench with `-trace` | `.profile/trace.out` |
| `flamegraph` | `go tool pprof -http=:8080 .profile/cpu.prof` | (browser tab) |
| `benchstat` | diff current bench vs `.profile/bench.base.txt` | `.profile/bench.new.txt` |

Override the bench for any profile target:

```bash
make profile-cpu PROFILE_BENCH=BenchmarkUltraLargeSubscribersSinglePublisher
make profile-mem PROFILE_BENCH=BenchmarkHighLoadParallel
```

## Workflow: investigating a hotspot

1. Run a profile on a representative bench:
   ```bash
   make profile-cpu PROFILE_BENCH=BenchmarkMultipleSubscribers
   ```
2. Open the flame graph:
   ```bash
   make flamegraph
   ```
3. In the browser, look at the **top** of the flame graph (the
   widest stack is the hottest path). Click a frame to focus on it;
   `Source` view shows the annotated source line.
4. Verify with `go tool pprof` directly when you want a quick check
   without the browser:
   ```bash
   go tool pprof -top -nodecount=10 .profile/cpu.prof
   go tool pprof -list 'createOrLoadSubscription' .profile/cpu.prof
   ```
5. After making a code change, re-run with `make benchstat` to
   confirm the change is statistically real (`p < 0.05`).

## Workflow: validating a fix (A/B comparison)

1. Save a baseline:
   ```bash
   make benchstat   # first run bootstraps .profile/bench.base.txt
   ```
2. Make your change.
3. Re-run:
   ```bash
   make benchstat   # second run diffs against the baseline
   ```
4. Read the `Δ` and `p=` columns:
   - `p < 0.05` → real change (improvement or regression).
   - `p ≥ 0.05` or `~` → noise; rerun to be sure.

For a stronger signal, use `-count=20` (default in the Makefile is
`-count=10`; benchstat convention is ≥ 10 for `p < 0.05` to be
meaningful).

## Reading flame graphs (3-line primer)

- **Width = time spent.** Wide frames are hot. Read top-down.
- **Color** in the default pprof `-http` UI is decorative; switch to
  the `Source` view to see annotated source.
- The call stack is **caller below, callee above**. A spike in a
  callee means work being done on behalf of its caller.

## Reading benchstat output (3-line primer)

```
name                  old time/op    new time/op    delta
PublishSingleSub-18    163ns ± 2%     160ns ± 2%   -1.84%  (p=0.041 n=10+10)
```

- `old` vs `new` — baseline vs current.
- `± X%` — noise band; overlap between the two intervals means
  indistinguishable.
- `delta` — percent change; `(p=0.041 n=10+10)` means the
  probability of seeing this delta by chance is 4.1%, on 10 paired
  samples. `p < 0.05` is the conventional "real change" threshold.
- A `~` in the delta column means "within noise" — rerun, or
  increase `-count`.

## Cost notes

- `-memprofilerate=1` (used by `profile-mem`) adds ~5–10% wall time
  because it records every allocation, not the 1% default sample.
- `-mutexprofilefraction=1` (used by `profile-mutex`) adds ~20%
  wall time and only captures contention when the bench actually
  contends — for low-contention benches the resulting `.prof` is
  empty, which is a valid signal.
- `benchstat -count=10` is 10× a normal `make bench` run; for
  tight iteration, use `-count=5` or save the baseline after a
  known-good change.

## What we do NOT profile

- **Network or syscall latency** — the library is in-process; no
  I/O. If you need to investigate a real workload's latency, you'll
  need application-level tracing, not pprof.
- **GC pressure beyond allocation count** — the `allocs/op` column
  in the bench output is the leading indicator. For GC pause
  detail, use `profile-trace` and look at the `GC` lane in the
  `go tool trace` UI.

## When to update this doc

- After adding a new bench: update the table in `Targets` if the
  bench is a likely profiling target, otherwise leave it.
- After changing the `profile-cpu` / `profile-mem` default bench:
  the `PROFILE_BENCH` variable in the `Makefile` is the source of
  truth.
- After upgrading Go: confirm the `tool` directive still parses
  (`go mod tidy` should be a no-op).
