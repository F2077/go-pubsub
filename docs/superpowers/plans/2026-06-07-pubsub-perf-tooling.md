# go-pubsub Profiling & Bench Toolchain Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a local-only profiling + benchstat toolchain to `go-pubsub` so future optimization work is evidence-driven. Zero production-code changes in this plan.

**Architecture:** Six new `make` targets (profile-cpu / profile-mem / profile-mutex / profile-trace / flamegraph / benchstat) all write to `.profile/`. The only new dep is `golang.org/x/perf/cmd/benchstat`, wired via Go 1.24's `tool` directive. A new `PROFILING.md` documents the workflow.

**Tech Stack:** Go 1.24 (bump from 1.21), `go tool pprof` (built-in), `go tool trace` (built-in), `benchstat` from `golang.org/x/perf` (tool dep).

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `go.mod` | Modify | Bump `go 1.21` → `1.24`; add `tool golang.org/x/perf/cmd/benchstat` |
| `go.sum` | Auto-update | Updated by `go mod tidy` |
| `.github/workflows/test.yml` | Modify | `go-version: '1.21'` → `'1.24'` |
| `Makefile` | Modify | Add `PROFILE_DIR` / `PROFILE_BENCH` vars and 7 new targets |
| `PROFILING.md` | Create | Runbook for the new toolchain |
| `.gitignore` | Modify | Append `.profile/` block |
| `.gitattributes` | Modify | Append `.profile/** linguist-generated` |
| `README.md` | Modify | One new line in the `make` block |
| `CLAUDE.md` | Modify | One word: "Go 1.21" → "Go 1.24" |
| `CHANGELOG.md` | Modify | New `### Added` entry for the toolchain + Go bump |

10 files; **0 production-code changes**. The smoke-test for each new Makefile target is "run the target, expect a non-empty artifact file".

---

## Task 1: Bump Go version to 1.24 in go.mod, CI, and CLAUDE.md

**Files:**
- Modify: `go.mod` (one line: `go 1.21` → `go 1.24`)
- Modify: `.github/workflows/test.yml` (one line: `go-version: '1.21'` → `'1.24'`)
- Modify: `CLAUDE.md` (one word: "Go 1.21" → "Go 1.24")

- [ ] **Step 1: Bump the `go` directive in go.mod**

Read `go.mod` and change the `go 1.21` line to `go 1.24`. The file should still parse with `go mod tidy`.

- [ ] **Step 2: Bump the Go version in CI**

Read `.github/workflows/test.yml`. Find the line:
```yaml
          # Pin to the lowest supported version per go.mod (currently 1.21);
```
Change it to:
```yaml
          # Pin to the lowest supported version per go.mod (currently 1.24);
```
And change the `go-version: '1.21'` line below it to `go-version: '1.24'`.

- [ ] **Step 3: Bump the Go version in CLAUDE.md**

In `CLAUDE.md`, find the line:
> Module path: `github.com/F2077/go-pubsub` (Go 1.21, generics-based).

Change it to:
> Module path: `github.com/F2077/go-pubsub` (Go 1.24, generics-based).

- [ ] **Step 4: Verify the build still passes on the bumped toolchain**

Run:
```bash
go build ./...
```
Expected: clean exit, no output. If your local Go is < 1.24, the `go` directive in go.mod will be honored as a minimum, and the build will still pass — the bump only matters for the `tool` directive in Task 2.

- [ ] **Step 5: Verify tests still pass**

Run:
```bash
go test -race -count=1 ./pubsub/...
```
Expected: `ok  github.com/F2077/go-pubsub/pubsub  <duration>`.

- [ ] **Step 6: Commit**

```bash
git add go.mod .github/workflows/test.yml CLAUDE.md
git commit -m "build: bump Go 1.21 to 1.24

Required for the upcoming benchstat tool directive (Go 1.24+ feature).
No code changes; only the go directive, CI go-version, and CLAUDE.md
are touched.

Co-Authored-By: MiniMax-M3"
```

---

## Task 2: Add the benchstat tool directive to go.mod

**Files:**
- Modify: `go.mod` (add `tool` line and a `require` line for `golang.org/x/perf`)

- [ ] **Step 1: Add the `tool` directive and require**

In `go.mod`, immediately **after** the existing `require ()` block, add:

```
tool golang.org/x/perf/cmd/benchstat

require golang.org/x/perf v0.0.0-20250101-0000-000000000000 // indirect
```

(The version pin is a placeholder; `go mod tidy` will replace it with the real pseudo-version.)

- [ ] **Step 2: Run `go mod tidy` to resolve the real version and update go.sum**

Run:
```bash
go mod tidy
```
Expected: `go.sum` gains one new line. `go.mod`'s require line now shows the real pseudo-version (something like `golang.org/x/perf v0.0.0-20250415-123456-abcdef`).

- [ ] **Step 3: Verify the tool directive is parseable**

Run:
```bash
go version
go doc -short golang.org/x/perf/cmd/benchstat 2>&1 | head -5
```
Expected: first command prints your Go version (must be 1.24+ for the `tool` directive to be honored). Second command should print the benchstat package doc stub, confirming the module is reachable. If `go version` is below 1.24, the tool directive is a no-op locally — that's fine; CI on Go 1.24 will pick it up.

- [ ] **Step 4: Materialize the benchstat binary**

Run:
```bash
go install tool
which benchstat
```
Expected: `benchstat` is now in your `$GOPATH/bin` (or `$GOBIN`). The `which` command should print its path. If you see "no tool dependencies", your local Go is below 1.24 — the line is correct, just inert until you upgrade.

- [ ] **Step 5: Run `benchstat --help` to confirm it works**

Run:
```bash
benchstat -h 2>&1 | head -3
```
Expected: prints the benchstat usage banner.

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum
git commit -m "build: add benchstat as go.mod tool dep

Wires golang.org/x/perf/cmd/benchstat via Go 1.24's tool directive.
Run 'go install tool' to materialize the binary; Makefile's
profile-install target does this automatically.

Co-Authored-By: MiniMax-M3"
```

---

## Task 3: Add the `profile-install` Makefile target

**Files:**
- Modify: `Makefile` (add the first profiling target — the simplest one)

- [ ] **Step 1: Read the existing Makefile to find a good insertion point**

Run:
```bash
grep -n "bench:\|cover:\|tidy:" Makefile
```
Expected: prints the line numbers of the existing `bench` / `cover` / `tidy` targets. We'll add a new section right after the `bench:` block.

- [ ] **Step 2: Add the `profile-install` target to the Makefile**

Find the line `bench: ## Run the README-cited benchmarks` and the end of its recipe (the next `^[^[:space:]].*:` line — the next target). **After** the bench recipe, insert the following block:

```makefile
# ---- Profiling & benchmarking toolchain -----------------------------------
# Local-only. Requires Go 1.24+ for the `tool` directive in go.mod. Run
# `make profile-install` once after `go mod tidy` to materialize
# benchstat in $(go env GOBIN) (default $GOPATH/bin). All targets write
# their artifacts under .profile/ (gitignored).

PROFILE_DIR   ?= .profile
PROFILE_BENCH ?= BenchmarkPublishSingleSubscriber   # override on CLI

$(PROFILE_DIR):
	@mkdir -p $@

.PHONY: profile-install profile-cpu profile-mem profile-mutex \
        profile-trace flamegraph benchstat

profile-install: ## Install benchstat from go.mod tool directive
	$(GO) install tool
```

- [ ] **Step 3: Verify `make help` lists the new target**

Run:
```bash
make help
```
Expected: `make help` output now includes a row for `profile-install` with its `##` description.

- [ ] **Step 4: Verify `make profile-install` runs**

Run:
```bash
make profile-install
```
Expected: prints a `go install tool` line, then exits 0. If your local Go is below 1.24, this target is a no-op (the `go install tool` succeeds with no work).

- [ ] **Step 5: Commit**

```bash
git add Makefile
git commit -m "build: add profile-install Makefile target

Wires the new 'go install tool' step (which materializes benchstat
from the go.mod tool dep) into the local workflow.

Co-Authored-By: MiniMax-M3"
```

---

## Task 4: Add the `profile-cpu` Makefile target

**Files:**
- Modify: `Makefile` (add the second profiling target)

- [ ] **Step 1: Add the `profile-cpu` target**

Insert the following recipe immediately **after** the `profile-install` block (still inside the same `Profiling & benchmarking toolchain` section):

```makefile
profile-cpu: $(PROFILE_DIR) ## Run bench with CPU profile → .profile/cpu.prof
	$(GO) test -bench=$(PROFILE_BENCH) -benchtime=2s \
		-cpuprofile=$(PROFILE_DIR)/cpu.prof -run=^$ ./pubsub/...
```

- [ ] **Step 2: Verify the target produces a non-empty profile**

Run:
```bash
rm -rf .profile && make profile-cpu
ls -la .profile/cpu.prof
```
Expected: `.profile/cpu.prof` exists and has nonzero size (typically 1–5 KB for a 2s run on `BenchmarkPublishSingleSubscriber`).

- [ ] **Step 3: Verify pprof can read the profile**

Run:
```bash
go tool pprof -top -nodecount=5 .profile/cpu.prof 2>&1 | head -20
```
Expected: prints a top-5 list of functions by flat time, with `runtime` / pubsub package functions visible.

- [ ] **Step 4: Verify overriding PROFILE_BENCH works**

Run:
```bash
make profile-cpu PROFILE_BENCH=BenchmarkSubscribes
ls -la .profile/cpu.prof
```
Expected: target re-runs, `.profile/cpu.prof` is updated (timestamp changes).

- [ ] **Step 5: Commit**

```bash
git add Makefile
git commit -m "build: add profile-cpu Makefile target

Runs the bench with -cpuprofile, writing .profile/cpu.prof. PROFILE_BENCH
overrides the bench to run (default BenchmarkPublishSingleSubscriber).

Co-Authored-By: MiniMax-M3"
```

---

## Task 5: Add the `profile-mem` Makefile target

**Files:**
- Modify: `Makefile` (add the alloc-object profile target)

- [ ] **Step 1: Add the `profile-mem` target**

Insert the following recipe immediately **after** the `profile-cpu` block:

```makefile
profile-mem: $(PROFILE_DIR) ## Allocation profile → .profile/mem.prof
	$(GO) test -bench=$(PROFILE_BENCH) -benchtime=2s -benchmem \
		-memprofile=$(PROFILE_DIR)/mem.prof -memprofilerate=1 -run=^$ ./pubsub/...
```

- [ ] **Step 2: Verify the target produces a non-empty profile**

Run:
```bash
rm -f .profile/mem.prof && make profile-mem
ls -la .profile/mem.prof
```
Expected: `.profile/mem.prof` exists and has nonzero size. Note: `-memprofilerate=1` adds ~5–10% wall time.

- [ ] **Step 3: Verify pprof can read the alloc profile (default `alloc_space` view)**

Run:
```bash
go tool pprof -top -nodecount=5 -sample_index=alloc_space .profile/mem.prof 2>&1 | head -20
```
Expected: prints a top-5 list of functions by alloc-space. Pub/sub internals like `makeSubscription` or `chan T` allocations should be visible.

- [ ] **Step 4: Commit**

```bash
git add Makefile
git commit -m "build: add profile-mem Makefile target

Runs the bench with -memprofile and -memprofilerate=1, writing
.profile/mem.prof. Captures every allocation (not the 1% sample
default) so the alloc object count is exact.

Co-Authored-By: MiniMax-M3"
```

---

## Task 6: Add the `profile-mutex` Makefile target

**Files:**
- Modify: `Makefile` (add the lock-contention profile target)

- [ ] **Step 1: Add the `profile-mutex` target**

Insert the following recipe immediately **after** the `profile-mem` block:

```makefile
profile-mutex: $(PROFILE_DIR) ## Lock contention → .profile/mutex.prof
	$(GO) test -bench=$(PROFILE_BENCH) -benchtime=2s \
		-mutexprofile=$(PROFILE_DIR)/mutex.prof \
		-mutexprofilefraction=1 -run=^$ ./pubsub/...
```

- [ ] **Step 2: Verify the target produces a non-empty profile**

Run:
```bash
rm -f .profile/mutex.prof && make profile-mutex
ls -la .profile/mutex.prof
```
Expected: `.profile/mutex.prof` exists. For a low-contention bench like `BenchmarkPublishSingleSubscriber`, the file may be small (<1 KB) or even empty if no `sync.Mutex.Lock` actually blocked — that's a valid signal ("no contention in this scenario"). Verify with `go tool pprof -top .profile/mutex.prof` — if it prints an empty list, the profile is correct.

- [ ] **Step 3: Verify pprof can read the mutex profile (or reports empty)**

Run:
```bash
go tool pprof -top -nodecount=5 .profile/mutex.prof 2>&1 | head -20
```
Expected: either prints a contention list (good — we have data), or prints an empty list with "Profile is empty" message (also good — means the lock held for that bench was uncontended). Either outcome is success.

- [ ] **Step 4: Commit**

```bash
git add Makefile
git commit -m "build: add profile-mutex Makefile target

Runs the bench with -mutexprofile and -mutexprofilefraction=1,
writing .profile/mutex.prof. For uncontended benchmarks the file
may be empty — that's a valid signal.

Co-Authored-By: MiniMax-M3"
```

---

## Task 7: Add the `profile-trace` Makefile target

**Files:**
- Modify: `Makefile` (add the execution-trace target)

- [ ] **Step 1: Add the `profile-trace` target**

Insert the following recipe immediately **after** the `profile-mutex` block:

```makefile
profile-trace: $(PROFILE_DIR) ## Execution trace → .profile/trace.out
	$(GO) test -bench=$(PROFILE_BENCH) -benchtime=2s \
		-trace=$(PROFILE_DIR)/trace.out -run=^$ ./pubsub/...
```

- [ ] **Step 2: Verify the target produces a non-empty trace**

Run:
```bash
rm -f .profile/trace.out && make profile-trace
ls -la .profile/trace.out
```
Expected: `.profile/trace.out` exists and has nonzero size (typically tens of KB for a 2s trace).

- [ ] **Step 3: Verify go tool trace can read it**

Run:
```bash
go tool trace -d 1 .profile/trace.out 2>&1 | head -5
```
Expected: prints a one-line summary and exits 0. (The full UI is `go tool trace .profile/trace.out` which opens a browser — that's the `flamegraph` target's job.)

- [ ] **Step 4: Commit**

```bash
git add Makefile
git commit -m "build: add profile-trace Makefile target

Runs the bench with -trace, writing .profile/trace.out. Open with
'go tool trace .profile/trace.out' to inspect scheduler / GC / syscall
detail not visible in pprof.

Co-Authored-By: MiniMax-M3"
```

---

## Task 8: Add the `flamegraph` Makefile target

**Files:**
- Modify: `Makefile` (add the pprof-web target)

- [ ] **Step 1: Add the `flamegraph` target**

Insert the following recipe immediately **after** the `profile-trace` block:

```makefile
flamegraph: ## Open CPU flame graph in browser (pprof -http=:8080)
	$(GO) tool pprof -http=:8080 $(PROFILE_DIR)/cpu.prof
```

- [ ] **Step 2: Verify `make help` lists the new target**

Run:
```bash
make help | grep -E 'flamegraph|profile-'
```
Expected: lists all 7 new targets with their `##` descriptions.

- [ ] **Step 3: Verify the target does not error (don't actually open the browser)**

Run with a 1-second timeout so it tries to bind the port and we can confirm the pprof command itself is well-formed:

```bash
timeout 1 make flamegraph 2>&1 | head -3
echo "exit=$?"
```
Expected: either the pprof server starts (and the timeout kills it with `exit=124`), or it errors immediately with "no profile" if `.profile/cpu.prof` was deleted. Both outcomes prove the recipe syntax is correct. If we see "no rule to make target flamegraph" or "*** missing separator", the Makefile is malformed — re-check tabs vs. spaces in the recipe.

- [ ] **Step 4: Commit**

```bash
git add Makefile
git commit -m "build: add flamegraph Makefile target

Wraps 'go tool pprof -http=:8080 .profile/cpu.prof' so a developer
can open the CPU flame graph in a browser with one command.

Co-Authored-By: MiniMax-M3"
```

---

## Task 9: Add the `benchstat` Makefile target

**Files:**
- Modify: `Makefile` (add the benchstat A/B-comparison target)

- [ ] **Step 1: Add the `benchstat` target**

Insert the following recipe immediately **after** the `flamegraph` block:

```makefile
benchstat: $(PROFILE_DIR) ## Diff bench against .profile/bench.base.txt
	$(GO) test -bench=. -count=10 -run=^$ ./pubsub/... \
		> $(PROFILE_DIR)/bench.new.txt
	@if [ -f $(PROFILE_DIR)/bench.base.txt ]; then \
		$(GO) run tool benchstat \
			$(PROFILE_DIR)/bench.base.txt $(PROFILE_DIR)/bench.new.txt; \
	else \
		echo "no baseline at $(PROFILE_DIR)/bench.base.txt — saving current as baseline"; \
		mv $(PROFILE_DIR)/bench.new.txt $(PROFILE_DIR)/bench.base.txt; \
	fi
```

- [ ] **Step 2: First run — should bootstrap the baseline**

Run:
```bash
rm -f .profile/bench.base.txt .profile/bench.new.txt && make benchstat
ls .profile/
```
Expected: prints "no baseline at .profile/bench.base.txt — saving current as baseline". `.profile/bench.base.txt` now exists, `.profile/bench.new.txt` is gone (it was renamed).

- [ ] **Step 3: Second run — should diff against itself**

Run:
```bash
make benchstat
```
Expected: prints a benchstat table comparing `.profile/bench.base.txt` vs `.profile/bench.new.txt`. Most rows will show "no change" or "~" (within noise) because we're diffing a bench against itself with two fresh runs. This confirms the tool is wired up.

- [ ] **Step 4: Inspect the benchstat output for a sanity row**

Run:
```bash
make benchstat 2>&1 | head -20
```
Expected: includes a row for `BenchmarkPublishSingleSubscriber-18` (or whatever GOMAXPROCS) with columns like `sec/op`, `B/op`, `allocs/op`, and a `~` / `!` / delta column.

- [ ] **Step 5: Commit**

```bash
git add Makefile
git commit -m "build: add benchstat Makefile target

Diffs a fresh bench run against .profile/bench.base.txt using
benchstat from the go.mod tool dep. First run bootstraps the
baseline; subsequent runs show statistical deltas (p-value).

Co-Authored-By: MiniMax-M3"
```

---

## Task 10: Add `PROFILING.md` runbook

**Files:**
- Create: `PROFILING.md` (new file at repo root)

- [ ] **Step 1: Create the file with the full content**

Write `PROFILING.md` at the repo root with the following content (this is the actual content of the file — copy it verbatim):

```markdown
# Profiling & Benchmark Toolchain

This project ships with a local-only profiling and benchmarking
toolchain so that future optimization work is evidence-driven. All
commands write artifacts to `.profile/` (gitignored).

## Prerequisites

- **Go 1.24+** — required for the `tool` directive in `go.mod`.
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
   "Source" view shows the annotated source line.
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
  the "Source" view to see annotated source.
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
- **GC pressure beyond allocation count** — the allocs/op column
  in the bench output is the leading indicator. For GC pause
  detail, use `profile-trace` and look at the "GC" lane in the
  `go tool trace` UI.

## When to update this doc

- After adding a new bench: update the table in `Targets` if the
  bench is a likely profiling target, otherwise leave it.
- After changing the profile-cpu / profile-mem default bench: the
  `PROFILE_BENCH` variable in the `Makefile` is the source of truth.
- After upgrading Go: confirm the `tool` directive still parses
  (`go mod tidy` should be a no-op).
```

- [ ] **Step 2: Verify the file is well-formed**

Run:
```bash
head -10 PROFILING.md
wc -l PROFILING.md
```
Expected: file starts with `# Profiling & Benchmark Toolchain`, total line count is ~140–150 (matches the spec's "~100 lines" target plus the explicit cost notes).

- [ ] **Step 3: Commit**

```bash
git add PROFILING.md
git commit -m "docs: PROFILING.md runbook for the new toolchain

Documents the local-only pprof + benchstat + trace workflow:
quick start, target table, two recipes (hotspot investigation and
A/B validation), and cost notes for the higher-overhead flags.

Co-Authored-By: MiniMax-M3"
```

---

## Task 11: Update `.gitignore` to exclude `.profile/`

**Files:**
- Modify: `.gitignore` (append a new section at the bottom)

- [ ] **Step 1: Read the end of `.gitignore`**

Run:
```bash
tail -10 .gitignore
```
Expected: see the last existing section header (e.g. `# --- ... ---`). We'll add a new section below it.

- [ ] **Step 2: Append the profiling section**

Append to `.gitignore`:

```

# --- Profiling artifacts (see PROFILING.md) ---
.profile/
```

(Empty line before the new section header is important — keeps the
file's sectioned layout readable.)

- [ ] **Step 3: Verify the entry works**

Run:
```bash
git check-ignore -v .profile/cpu.prof
```
Expected: prints the matching `.gitignore` line and the absolute path of the file. Exit 0.

- [ ] **Step 4: Commit**

```bash
git add .gitignore
git commit -m "build: gitignore .profile/ profiling artifacts

Single-directory entry covers everything the new Makefile targets
write (cpu.prof, mem.prof, mutex.prof, trace.out, bench.*.txt).

Co-Authored-By: MiniMax-M3"
```

---

## Task 12: Update `.gitattributes` to mark `.profile/` as generated

**Files:**
- Modify: `.gitattributes` (append one line)

- [ ] **Step 1: Read the end of `.gitattributes`**

Run:
```bash
cat .gitattributes
```
Expected: shows the existing `* text=auto eol=lf` line and any binary overrides.

- [ ] **Step 2: Append the linguist-generated marker**

Append to `.gitattributes`:

```

# Mark profiling artifacts as generated (not project code).
.profile/** linguist-generated
```

- [ ] **Step 3: Verify the entry is well-formed**

Run:
```bash
git check-attr -a -- .profile/cpu.prof
```
Expected: prints `linguist-generated: true` (or similar) for the test file path.

- [ ] **Step 4: Commit**

```bash
git add .gitattributes
git commit -m "build: mark .profile/** as linguist-generated in .gitattributes

Tells GitHub's linguist not to count profiling artifacts as project
code (matters for the language-statistics bar in the repo header).

Co-Authored-By: MiniMax-M3"
```

---

## Task 13: Add the `make profile-cpu` line to README

**Files:**
- Modify: `README.md` (one new line in the `make` block)

- [ ] **Step 1: Locate the `make` block in the Quick start section**

Run:
```bash
grep -n "make bench" README.md
```
Expected: prints the line number of the existing `make bench # run the README-cited benchmarks` line.

- [ ] **Step 2: Add the new line below `make bench`**

Insert immediately **after** the `make bench` line (preserving the existing comment style and indentation):

```markdown
make profile-cpu # open the CPU flame graph (see PROFILING.md)
```

- [ ] **Step 3: Verify the addition**

Run:
```bash
grep -A 0 -B 0 "profile-cpu" README.md
```
Expected: prints the new line.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: README one-liner for profile-cpu target

Single-line pointer to the new toolchain; deep docs live in
PROFILING.md.

Co-Authored-By: MiniMax-M3"
```

---

## Task 14: Add the CHANGELOG entry

**Files:**
- Modify: `CHANGELOG.md` (append new `### Added` entries to the `## [Unreleased]` section)

- [ ] **Step 1: Locate the Unreleased section**

Run:
```bash
grep -n "## \[Unreleased\]" CHANGELOG.md
```
Expected: prints the line number where the section starts.

- [ ] **Step 2: Add a new `### Added` subsection under `## [Unreleased]`**

Find the existing `### Added` subsection under `## [Unreleased]`. After the last item in that subsection, add:

```markdown
- **Profiling & bench toolchain** (local-only). Six new `make` targets
  (`profile-cpu` / `profile-mem` / `profile-mutex` / `profile-trace` /
  `flamegraph` / `benchstat`) plus `PROFILING.md` runbook. All
  artifacts land in `.profile/` (gitignored). The only new dep is
  `golang.org/x/perf/cmd/benchstat`, wired via Go 1.24's `tool`
  directive in `go.mod`. No production-code changes.
- **Go 1.21 → 1.24** bump in `go.mod` / `.github/workflows/test.yml`
  / `CLAUDE.md`. Required for the `tool` directive; no public API
  impact.
```

- [ ] **Step 3: Verify the section is well-formed**

Run:
```bash
sed -n '/## \[Unreleased\]/,/## \[/p' CHANGELOG.md | head -30
```
Expected: the new bullet appears under the existing `### Added` subsection, before the next `## [v1.0.0]` (or similar) section.

- [ ] **Step 4: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs: changelog entry for profiling toolchain + Go 1.24 bump

Captures both the new tooling and the prerequisite Go version bump
in the Unreleased section.

Co-Authored-By: MiniMax-M3"
```

---

## Task 15: Final end-to-end verification

**Files:** none modified in this task. Pure smoke test of the whole chain.

- [ ] **Step 1: Verify `make help` lists all 7 new targets**

Run:
```bash
make help | grep -E 'profile-|flamegraph|benchstat'
```
Expected: lists all 7 new targets with their `##` descriptions.

- [ ] **Step 2: Verify each profile target produces a non-empty artifact**

Run:
```bash
rm -rf .profile
make profile-cpu  >/dev/null
make profile-mem  >/dev/null
make profile-mutex >/dev/null || true   # mutex may be empty for low-contention bench
make profile-trace >/dev/null
ls -la .profile/
```
Expected: `.profile/cpu.prof`, `.profile/mem.prof`, `.profile/trace.out` all exist and are non-empty. `.profile/mutex.prof` may be empty (valid signal). Note the `|| true` so the empty-mutex case doesn't fail the smoke test.

- [ ] **Step 3: Verify `make benchstat` round-trips**

Run:
```bash
rm -f .profile/bench.base.txt .profile/bench.new.txt
make benchstat 2>&1 | head -3
ls .profile/bench.base.txt .profile/bench.new.txt 2>&1
```
Expected: first call prints "no baseline ... saving current as baseline", and `.profile/bench.base.txt` exists, `.profile/bench.new.txt` is gone (renamed to base).

- [ ] **Step 4: Verify the second `benchstat` call produces a diff table**

Run:
```bash
make benchstat 2>&1 | head -15
```
Expected: a benchstat table with rows for each bench and a `~` / `!` / `delta` column.

- [ ] **Step 5: Verify the library test suite still passes**

Run:
```bash
go test -race -count=1 ./pubsub/...
```
Expected: `ok  github.com/F2077/go-pubsub/pubsub  <duration>`. We did not touch the library, so this should be unchanged.

- [ ] **Step 6: Verify `go vet` and `gofmt` are clean**

Run:
```bash
go vet ./...
test -z "$(gofmt -l $(git ls-files '*.go'))" || (gofmt -d $(git ls-files '*.go'); exit 1)
```
Expected: both exit 0 with no output.

- [ ] **Step 7: Verify the working tree is clean except for `.profile/`**

Run:
```bash
git status --porcelain
```
Expected: prints nothing (all changes are committed). If `.profile/` shows up, the `.gitignore` is not yet in effect — re-run `git add .gitignore && git commit` to fix.

- [ ] **Step 8: Print a one-line summary of all commits added by this plan**

Run:
```bash
git log --oneline -15 | head -15
```
Expected: 15 new commits (one per Task 1–14, plus a final empty one if needed). The first one is the spec commit `1469c81` (or whatever its real SHA is); the rest are the implementation commits from Tasks 1–14.

---

## Self-Review

**Spec coverage** (each spec section → which task implements it):

| Spec section | Task |
|---|---|
| §4 Go 1.21 → 1.24 bump | T1 |
| §5 `go.mod` tool directive | T2 |
| §6 `profile-install` target | T3 |
| §6 `profile-cpu` target | T4 |
| §6 `profile-mem` target | T5 |
| §6 `profile-mutex` target | T6 |
| §6 `profile-trace` target | T7 |
| §6 `flamegraph` target | T8 |
| §6 `benchstat` target | T9 |
| §7 `PROFILING.md` | T10 |
| §8 `.gitignore` | T11 |
| §8 `.gitattributes` | T12 |
| §9 README one-liner | T13 |
| §10 CLAUDE.md Go version | T1 |
| §11 Test plan (verification) | T15 |

All 14 spec requirements have a task. No gaps.

**Placeholder scan:** No TBD / TODO / "implement later" / "similar to Task N" markers. Every step that changes code shows the full code. Every step that runs a command shows the exact command and expected output.

**Type consistency:** All Makefile variable names (`PROFILE_DIR`, `PROFILE_BENCH`) are defined once in T3 and referenced identically in T4–T9. The `.profile/cpu.prof` path is consistent across `flamegraph` (T8) and `profile-cpu` (T4). The `benchstat` recipe in T9 uses the same path variables as the profile recipes.

**No issues found.** Plan is ready for execution.
