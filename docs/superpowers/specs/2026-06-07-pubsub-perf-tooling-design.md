# go-pubsub — Profiling & Benchmark Toolchain Design

**Date:** 2026-06-07
**Status:** Approved (brainstorming)
**Scope:** Add a local-only profiling and benchmarking toolchain. **No production-code optimization in this change** — the goal is to put measurement in place so that future optimization work is evidence-driven.

## 1. Motivation

The `go-pubsub` library's per-publish hot path is already strong
(`BenchmarkPublishSingleSubscriber`: **163 ns/op, 96 B/op, 2 allocs/op** on
Linux / Intel Core Ultra 5 125H — see `README.md`'s "Benchmark Results"
section). Pushing the next 10–20% requires knowing **where** the time and
allocations go under load, and **whether** a candidate change actually
moves the needle.

Today the toolchain has a `make bench` target that prints raw numbers, but:

- No way to attribute time / allocations to specific functions
  (pprof / `go tool trace`).
- No way to say "is change X statistically faster than change Y"
  (`benchstat`).
- No way to see lock contention under multi-subscriber load
  (`-mutexprofile`).

This spec adds those capabilities **without** changing the library or
public API.

## 2. Non-goals

- **No production-code changes.** This spec is tooling only.
- **No CI integration.** All targets are local-interaction. (CI may pick
  this up later via `workflow_dispatch`; that's a separate design.)
- **No continuous flame-graph diffing or regression alerts.** A human
  runs `make profile-cpu` or `make benchstat` and reads the result.
- **No new benchmarks.** The existing 16 in `bench_test.go` are the
  target surface.

## 3. Toolchain choices

| Capability | Tool | Why |
|---|---|---|
| CPU profile (flame graph) | `go tool pprof` (built-in) | Zero extra dep; `-http=:8080` opens browser-based flame graph viewer — no `flamegraph.pl` / `go-torch` needed. |
| Allocation profile | `go tool pprof` with `-memprofile` + `-memprofilerate=1` | Captures every allocation so the per-call alloc count is exact. |
| Mutex / contention profile | `go test -mutexprofile` + `-mutexprofilefraction=1` | Native to Go 1.21+; `-fraction=1` captures every contended lock. |
| Execution trace | `go test -trace=trace.out` | Built-in; shows scheduler / GC / syscall detail not visible in pprof. |
| Statistical A/B comparison | `golang.org/x/perf/cmd/benchstat` | The standard tool for `p < 0.05` confidence on bench numbers. |

The only **new** dep is `benchstat`. Wired in via Go's `tool` directive
in `go.mod` (Go 1.24+).

## 4. Go version bump (prerequisite)

The `tool` directive in `go.mod` is a **Go 1.24 feature**. We bump:

- `go.mod`'s `go` directive: `1.21` → `1.24`
- `.github/workflows/test.yml`'s `go-version`: `'1.21'` → `'1.24'`

This is a one-line, two-line change. It does **not** change the public
API. CLAUDE.md currently says "Go 1.21"; that line will be updated to
"Go 1.24".

## 5. `go.mod` changes

```diff
 module github.com/F2077/go-pubsub

-go 1.21
+go 1.24

 require (
     github.com/google/uuid v1.6.0
     github.com/stretchr/testify v1.10.0
     go.uber.org/goleak v1.3.0
 )

+tool golang.org/x/perf/cmd/benchstat
+
+require golang.org/x/perf v0.0.0-20260101-0000-000000000000 // indirect
```

(Exact version pinned by `go mod tidy`; the placeholder above just
shows the shape.) `go install tool` then materializes the `benchstat`
binary into `$(go env GOBIN)` (default `$GOPATH/bin`).

## 6. Makefile targets

Six new targets, added to the existing `Makefile` next to `bench` /
`cover` / `lint`. All write artifacts under `.profile/` (gitignored).

```makefile
PROFILE_DIR   ?= .profile
PROFILE_BENCH ?= BenchmarkPublishSingleSubscriber   # override on CLI

$(PROFILE_DIR):
	@mkdir -p $@

.PHONY: profile-install profile-cpu profile-mem profile-mutex \
        profile-trace flamegraph benchstat

profile-install: ## Install benchstat from go.mod tool directive
	$(GO) install tool

profile-cpu: $(PROFILE_DIR) ## Run bench with CPU profile → .profile/cpu.prof
	$(GO) test -bench=$(PROFILE_BENCH) -benchtime=2s \
		-cpuprofile=$(PROFILE_DIR)/cpu.prof -run=^$ ./pubsub/...

profile-mem: $(PROFILE_DIR) ## Allocation profile → .profile/mem.prof
	$(GO) test -bench=$(PROFILE_BENCH) -benchtime=2s -benchmem \
		-memprofile=$(PROFILE_DIR)/mem.prof -memprofilerate=1 -run=^$ ./pubsub/...

profile-mutex: $(PROFILE_DIR) ## Lock contention → .profile/mutex.prof
	$(GO) test -bench=$(PROFILE_BENCH) -benchtime=2s \
		-mutexprofile=$(PROFILE_DIR)/mutex.prof \
		-mutexprofilefraction=1 -run=^$ ./pubsub/...

profile-trace: $(PROFILE_DIR) ## Execution trace → .profile/trace.out
	$(GO) test -bench=$(PROFILE_BENCH) -benchtime=2s \
		-trace=$(PROFILE_DIR)/trace.out -run=^$ ./pubsub/...

flamegraph: ## Open CPU flame graph in browser (pprof -http=:8080)
	$(GO) tool pprof -http=:8080 $(PROFILE_DIR)/cpu.prof

benchstat: $(PROFILE_DIR) ## Diff bench against .profile/bench.base.txt
	$(GO) test -bench=. -count=10 -run=^$ ./pubsub/... \
		> $(PROFILE_DIR)/bench.new.txt
	@if [ -f $(PROFILE_DIR)/bench.base.txt ]; then \
		$(GO) run tool benchstat \
			$(PROFILE_DIR)/bench.base.txt $(PROFILE_DIR)/bench.new.txt; \
	else \
		echo "no baseline at $(PROFILE_DIR)/bench.base.txt — \
saving current as baseline"; \
		mv $(PROFILE_DIR)/bench.new.txt $(PROFILE_DIR)/bench.base.txt; \
	fi
```

### Defaults and overrides

- `PROFILE_BENCH=BenchmarkPublishSingleSubscriber` — single bench for
  fast iteration (~1s). Override per-run:
  `make profile-cpu PROFILE_BENCH=BenchmarkUltraLargeSubscribersSinglePublisher`.
- `benchtime=2s` — long enough for a clear flame graph, short enough
  to iterate.
- `benchstat` uses `-count=10` (benchstat convention for statistical
  significance).
- All `.prof` and `.trace` files are gitignored.

## 7. New doc: `PROFILING.md`

~100 lines at repo root. Contents:

1. **Why this exists** — one paragraph: bench numbers are good; need
   tools to find the next 10% gain.
2. **Prerequisites** — Go 1.24+, one-time `make profile-install`.
3. **Quick start** — 3 commands: `make profile-cpu`, `make flamegraph`,
   `make benchstat`.
4. **Workflow: investigating a hotspot** —
   `make profile-cpu PROFILE_BENCH=…` → open flame graph → read top
   frames → fix → `make benchstat` to confirm.
5. **Workflow: validating a fix** — `make bench > .profile/bench.base.txt`
   → make change → `make benchstat` → read the `p=` column.
6. **Reading flame graphs** — 3-line primer on color (red = growth, but
   for a single run it's just legend decoration) / stack order (callee
   on top of caller).
7. **Reading benchstat output** — 3-line primer on the `Δ` / `p=`
   columns. `p < 0.05` means "statistically real"; `~` means noise.
8. **Cost notes** — `-memprofilerate=1` adds ~5–10% wall time;
   `-mutexprofilefraction=1` adds ~20%; `benchstat -count=10` is 10×
  the normal bench wall time.

## 8. `.gitignore` and `.gitattributes`

Append to `.gitignore`:

```
# --- Profiling artifacts (see PROFILING.md) ---
.profile/
```

(The Makefile puts everything in `.profile/`, so a single directory
entry covers it. `*.prof` and `*.trace` outside that tree are unusual;
if any appear in the future, add them then.)

Append to `.gitattributes`:

```
.profile/**   linguist-generated
```

(Lets GitHub's linguist not count profiling artifacts as project code.)

## 9. `README.md` change

One line added to the "Quick start" `make` block (around the existing
`make help` / `make all` / `make cover` / `make bench` lines):

```diff
 make cover     # write cover.out + cover.html
 make bench     # run the README-cited benchmarks
+make profile-cpu # open the CPU flame graph (see PROFILING.md)
```

That's the entire README delta. The deep doc lives in `PROFILING.md`.

## 10. CLAUDE.md update

The line:

> Module path: `github.com/F2077/go-pubsub` (Go 1.21, generics-based).

becomes:

> Module path: `github.com/F2077/go-pubsub` (Go 1.24, generics-based).

(One word changed. Bumped because of the `tool` directive in `go.mod`.)

## 11. Test plan

The tooling itself needs no new unit tests (it's a Makefile + a binary
that ships with Go). What needs verification:

1. **Smoke-test each target**:
   - `make profile-install` → `benchstat` appears in `$GOBIN`
   - `make profile-cpu` → `.profile/cpu.prof` exists, non-empty
   - `make flamegraph` → opens browser (manual check)
   - `make profile-mem` → `.profile/mem.prof` exists
   - `make profile-mutex` → `.profile/mutex.prof` exists
   - `make profile-trace` → `.profile/trace.out` exists
2. **End-to-end `make benchstat`**:
   - First run: prints "no baseline … saving current as baseline"
   - Second run: prints a benchstat diff (will likely show no
     significant change vs itself — confirms the tool is wired up)
3. **`make help`** lists every new target with its `##` description.
4. **`go test -race -count=1 ./...`** still green (we did not change
   the library).
5. **CI**: `.github/workflows/test.yml` continues to pass on Go 1.24.

## 12. Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| `tool` directive lock-in to Go 1.24+ alienates some users | Low (1.24 is 1+ year old as of 2026-06) | Document the version bump in CHANGELOG; reference the spec. |
| `-mutexprofilefraction=1` distorts real contention behavior | Medium | The Makefile comment calls it out; users who need production-fair contention numbers should drop the flag (the default 1% sample is what most projects use). |
| `.profile/` accidentally gets committed | Low | Directory is gitignored. `.gitattributes` marks it `linguist-generated`. CI fails on `git status --porcelain` dirty trees. |
| `benchstat @latest` pins to whatever the dev's network returns | Low | The Makefile installs from `go.mod`'s `tool` directive, which `go mod tidy` resolved. Reproducible. |

## 13. Out of scope (deferred)

- **No CI integration** — the `workflow_dispatch` job that runs bench
  + benchstat on demand is a separate spec.
- **No production-code optimization** — this spec adds measurement;
  the optimization that comes out of it is a separate brainstorm.
- **No flame-graph diffing between PRs** — that's a hosted service
  (e.g. https://www.speedscope.app/ upload) and a separate decision.
- **No tracing dashboard** — `go tool trace` UI is enough locally.

## 14. Files touched

| File | Change |
|---|---|
| `go.mod` | Bump `go 1.21` → `1.24`; add `tool golang.org/x/perf/cmd/benchstat` and its require. |
| `go.sum` | Updated by `go mod tidy`. |
| `.github/workflows/test.yml` | `go-version: '1.21'` → `'1.24'`. |
| `Makefile` | Add 6 new targets + `PROFILE_DIR` / `PROFILE_BENCH` vars. |
| `PROFILING.md` | New file, ~100 lines. |
| `.gitignore` | Append `.profile/` block. |
| `.gitattributes` | Append `.profile/** linguist-generated`. |
| `README.md` | One new line in the `make` block. |
| `CLAUDE.md` | One word: "Go 1.21" → "Go 1.24". |
| `CHANGELOG.md` | New `### Added` entry for the profiling toolchain + the Go bump. |

10 files; 0 production code changes.
