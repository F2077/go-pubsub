# Changelog

All notable changes to `go-pubsub` are documented in this file. The
format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed
- **Breaking**: rename `pubsub.SubscriptionCapacityExceed` →
  `pubsub.ErrSubscriptionCapacityExceeded` for Go error-naming
  convention (errors start with `Err`).
- **Breaking**: `WithLogger(nil)` and `WithId("")` now return the
  new sentinels `pubsub.ErrLoggerNil` and `pubsub.ErrBrokerIdEmpty`
  (previously bare `errors.New`); the original error messages are
  preserved.

### Added
- Package-level godoc on `pubsub` (renders on pkg.go.dev).
- Godoc comments on every exported identifier in `pubsub/`.
- New sentinels: `ErrLoggerNil`, `ErrBrokerIdEmpty`.
- `Makefile` (distilled from spf13/cobra + uber-go/zap conventions).
- `.github/workflows/test.yml` running `go test -race -coverprofile` on
  every push and PR.
- `.editorconfig`, `.golangci.yml`, `.gitattributes`.
- Governance files: `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`,
  `SECURITY.md`.
- `cmd/quickstart/README.md` documenting the runnable example.
- **Goroutine-leak guard** via `go.uber.org/goleak v1.3.0`; every test
  in the `pubsub` package now fails if a goroutine outlives its test.

### Fixed
- `gofmt -l` is clean across the tree (two test files were
  mis-formatted).
- Internal `slog` messages translated to English to match
  `CLAUDE.md`'s user-visible-string rule.
- `ErrSubscriptionCapacityExceeded` message string now reads
  `"subscription capacity exceeded"` (was the leftover
  `"subscription capacity exceed"` from the rename).
- `BenchmarkHighLoadParallel` deadlocked under the post-simplify
  broker: the original `b.RunParallel` worker drained `<-subs[0].Ch`
  synchronously, and the 100-buffer channel could not absorb the 180
  concurrent publishes from `SetParallelism(10) × GOMAXPROCS=18`, so
  the majority of workers blocked on receive forever. Redesigned with
  a dedicated drain goroutine that keeps all 10 000 subscriber
  channels empty while the parallel workers only `Publish`; the
  bench no longer hangs and `ns/op` rises ~13% (the real end-to-end
  cost, vs. the self-referential baseline that hid the contention).

### Added (profiling toolchain)
- **Profiling & bench toolchain** (local-only). Seven new `make`
  targets (`profile-install` / `profile-cpu` / `profile-mem` /
  `profile-mutex` / `profile-trace` / `flamegraph` / `benchstat`)
  plus `PROFILING.md` runbook. All artifacts land in `.profile/`
  (gitignored). The only new dep is
  `golang.org/x/perf/cmd/benchstat`, wired via Go 1.25's `tool`
  directive in `go.mod`. No production-code changes.
- **Go 1.21 → 1.25** bump in `go.mod` /
  `.github/workflows/test.yml` / `CLAUDE.md`. Required for the
  `tool` directive; no public API impact.

### Changed (hot-path alloc reductions)
Three independent commits knock out the allocs that were on every
publish. None of them change the public API; one changes a
documented contract on `Subscription.ErrCh` (see "API contract"
below).

- **Gated `slog.Debug` behind `Enabled()`** (`pubsub/broker.go`,
  `pubsub/subscriber.go`). Hot-path `logger.Debug(..., slog.Any(...))`
  calls — five in `createOrLoadSubscription`, two in `deliver` — now
  skip the variadic `[]any` slice when the level is disabled.
  `slog.Logger.Debug` always evaluates its variadic args at the call
  site, so even at `LevelError` the allocation happened. New test
  `TestDebugLevelLoggerExercisesHotPath` drives a Debug-level
  logger to keep the gated if-bodies honest.
- **Persistent `*time.Timer` + `Reset()`** for sliding timeouts
  (`pubsub/subscriber.go`). The previous design called
  `time.AfterFunc(timeout, closure)` once per topic in `Subscribe`
  and re-created it on every successful publish via `resetTimer`
  — a new `*runtime.timer` and a fresh closure per message, plus
  the closure's per-publish alloc. Replaced with one persistent
  `time.NewTimer(timeout)` per topic plus a single fire goroutine
  (`runTopicTimer`) that reads `t.C` and calls `handleTimeout`.
  `resetTimer` is now a `Stop+stop-drain+Reset` triple with zero
  allocation. The `Subscriber` struct picks up a new
  `timerDones map[string]chan struct{}` field for the fire
  goroutine's exit signal. New tests
  `TestReSubscribeSameTopicCleansUpOldTimer` and
  `TestResetTimerDrainsFiredTimer` (the latter is a synthetic test
  for the canonical `t.Stop+stop-drain+t.Reset` pattern, which is
  required for correctness — without the drain, a fire that races
  with a publish would deliver a spurious `ErrSubscriptionTimeout`).
- **Lazy `ErrCh`** (`pubsub/subscriber.go`). `Subscribe` no longer
  allocates a 1-slot error channel for topics that did not pass
  `WithTimeout`. Those subscriptions now have `ErrCh == nil`; a
  receive on a nil channel blocks forever, which is the correct
  "never errors" semantics for a subscription that cannot time
  out. `unsubscribe` already short-circuits when there is no
  `s.errChannels` entry to delete, so the Close path is unchanged.

#### API contract
`Subscription.ErrCh` is now documented as **nil** when `Subscribe`
was called without `WithTimeout`. Code that today does
`select { case err := <-sub.ErrCh: ... }` continues to compile and
behave correctly — the receive simply blocks forever. Callers that
want to distinguish "no timeout configured" from "timeout = ∞" can
compare `sub.ErrCh == nil` themselves. No `nil`-deref risk: `ErrCh`
is a receive-only field, so callers cannot accidentally `send` on it.

#### Bench deltas (baseline → this PR, `go test -bench=. -benchmem -benchtime=1s`)

| Benchmark                                          | Before         | After         | Δ allocs | Δ B/op |
|----------------------------------------------------|----------------|---------------|----------|--------|
| `PublishSingleSubscriber-18`                       | 179.6 ns/96 B/2 | 122.4 ns/0 B/0 | -2       | -96    |
| `MultipleSubscribers-18`                           | 6 576 ns/96 B/2 | 6 172 ns/0 B/0 | -2       | -96    |
| `MultiPublisherSingleSubscriber-18`                | 8 243 ns/784 B/22 | 2 833 ns/304 B/12 | -10  | -480   |
| `MultiPublisherMultipleSubscribers-18`             | 20 173 ns/784 B/22 | 16 982 ns/304 B/12 | -10 | -480 |
| `PublishWithTimeout-18`                            | 660.2 ns/504 B/7 | 477.5 ns/248 B/3 | -4   | -256   |
| `PublishAutoCreateTopic-18`                        | 847.5 ns/420 B/8 | 697.5 ns/208 B/4 | -4   | -212   |
| `Subscribes-18` (50 topics)                        | 105 403 ns/104 822 B/974 | 105 561 ns/82 315 B/562 | -412 | -22 507 |
| `HighLoadParallel-18` (10 000 sub, parallel)       | 115 895 ns/97 B/2  | 118 695 ns/3 B/0  | -2  | -94    |
| `BrokerTopics/*`                                   | unchanged           | unchanged         | 0   | 0      |

Statement coverage: 95.7 % → 96.2 % (the new Debug-level test
and the two new timer-lifecycle tests add a handful of fully
exercised statements).

### Changed (examples)
- `cmd/quickstart` rewritten from a 47-line single-subscription demo
  into a 13-phase ~260-line end-to-end walkthrough. The new program
  covers the full public surface in a single `go run`: two brokers
  (one production-shaped with `WithId`+`WithLogger`+`WithCapacity`,
  one tiny `cap=2` broker for the capacity-exceeded demo), the
  `BrokerOption` validation error path (`ErrLoggerNil` /
  `ErrBrokerIdEmpty`), three publishers and two subscribers,
  `Subscriber.Subscribes` (multi-topic) and `Subscriber.Subscribe`
  (single-topic, `Block` buffer + 400 ms sliding timeout), every
  exported `ChannelSize` constant's buffer-capacity contract, the
  lazy-`ErrCh` contract (nil for no-timeout subs), `OnClose` × 4
  (one per subscription, all fired), the natural firing of
  `ErrSubscriptionTimeout` on the drainer, `Subscriber.Close`
  idempotency + `ErrSubscriberClosed` on the second call and on
  post-close `Subscribe`, `ErrSubscriptionCapacityExceeded` via
  `errors.Is`, and a final `Broker.Topics()` snapshot that may
  briefly show non-empty topics after `subscriber.Close()` due to
  the documented asynchronous reaping. A generic
  `drainSubscription[T]` helper keeps the per-subscription consume
  logic in one place; `main` is a thin error wrapper around `run()`.
  No library-code changes; the existing unit tests in `pubsub/`
  remain the contract.

### Changed (examples split)
- `cmd/quickstart` split into two binaries so the README's
  **Quick Start** has a runnable counterpart.
  - `cmd/quickstart/main.go` (NEW) — a ~50-line minimum viable
    example that mirrors README's Quick Start verbatim:
    one broker, one publisher, one subscriber, one topic,
    one publish, one receive. `go run ./cmd/quickstart`
    prints `Received: CPU over 90%!`. Replaces the
    13-phase end-to-end script that previously lived in
    this directory.
  - `cmd/quickstart-e2e/main.go` (NEW) — the previous
    13-phase ~260-line end-to-end walk-through, unchanged in
    behavior. Renamed the trailing "ok" line from
    `quickstart: ok` → `quickstart-e2e: ok` to match the new
    binary name.
  - `cmd/quickstart/README.md` rewritten to describe the
    minimal Quick Start binary and point at
    `cmd/quickstart-e2e/README.md` for the deeper
    walk-through.
  - `cmd/quickstart-e2e/README.md` (NEW) — the previous
    contents of `cmd/quickstart/README.md`, with the
    internal `cmd/quickstart/main.go` references updated to
    `cmd/quickstart-e2e/main.go`.
  - `README.md` Quick Start rewritten to match the new
    minimal binary 1:1 (the old snippet had drifted from
    the actual library behaviour in three places: `panic`
    instead of `log.Fatal` was fine, but the IIFE-wrapped
    `defer sub.Close()` was dead ceremony, the
    `WithTimeout[string](5*time.Second)` was unreachable
    in the happy-path run, and the `// Output: "..."`
    comment is a godoc convention that doesn't apply to
    `cmd/` binaries). The new snippet uses synchronous
    `Publish` (so the 200 ms sliding timer demonstrably
    resets on the happy path) and points at
    `cmd/quickstart-e2e` for the long-form demo.

### Changed (test-side cleanup)
- Test files now share a single `testLogger()` / `benchLogger()` pair
  in `pubsub/helpers_test.go`; ~19 inline `slog.New(...)` duplicates
  collapsed. No behavior change.
- `BenchmarkBrokerTopics` no longer registers a `defer` per
  subscription in its setup loop — it now uses the same
  `b.Cleanup` pattern as the rest of `bench_test.go`.
- `TestBrokerOptionValidation` (table test) replaces
  `TestWithLoggerNil` + `TestWithIdEmpty`, and asserts
  `errors.Is(err, ErrLoggerNil / ErrBrokerIdEmpty)` so the
  exported sentinels are no longer dead.
- `TestSubscriberClosedPostConditions` replaces
  `TestSubscribeAfterClose` + `TestSubscriberCloseTwice` with
  a single test of subtests.
- README bench table refreshed from the post-simplify
  `make bench` run.

### Changed (simplify pass)
- `pubsub/subscriber.go`: drop `tt.t.Stop()` in the `runTopicTimer`
  done-branch. The goroutine is about to return and `tt.t.C` has no
  consumer; same reasoning as `744d7df` for `resetTimer` — the timer
  is unreachable once `unsubscribe` deletes the `topicTimer`, so GC
  handles cleanup. (commit `e5ec990`)
- `pubsub/subscriber.go`: drop `if s.closed { return ErrSubscriberClosed }`
  in `unsubscribe`. `Subscriber.Close` drains `s.subs` *before* setting
  `s.closed=true`, and `Subscription.Close` is only callable while
  `s.closed` is still false — the `s.subs[sub]` short-circuit is the
  real idempotency guard, the `closed`-flag check was redundant.
  (commit `e5ec990`)
- `pubsub/subscriber.go`: gate the two remaining unconditional
  `slog.Any` calls in `Subscribe` (timeout-config branch) and
  `handleTimeout` behind `Enabled()`. The other 7+ Debug sites in
  the file already hoist a `debug` bool; these two slipped through.
  (commit `e5ec990`)
- `pubsub/bench_test.go`: drop the duplicate `b.StopTimer()` in
  `BenchmarkHighLoadParallel` (lines 299 and 302 both stopped the
  timer back-to-back; second call is a no-op). (commit `e5ec990`)

Coverage drops 95.9% → 95.4% by design: the dropped `tt.t.Stop()`
branch was 100% exercised by the race tests, and the new if-gates
are never entered at `LevelError` (which is the whole point of
gating them). `BenchmarkPublishSingleSubscriber` stays at ~108 ns/op,
0 B/op, 0 allocs/op.

### Removed
- GPL-3.0 license. The project is now MIT.

## [v1.0.0] - 2024-01-01

### Added
- Initial public release.
- `Broker[T]`, `Publisher[T]`, `Subscriber[T]`, `Subscription[T]`
  with generics.
- Options: `WithLogger`, `WithId`, `WithCapacity` (broker);
  `WithChannelSize`, `WithTimeout` (subscription).
- Sentinels: `ErrSubscriberClosed`, `ErrSubscriptionTimeout`.
- Per-topic channel-size constants `Block`/`Single`/`Small`/`Medium`/
  `Large`/`Huge`.
- Race-detector-clean test suite (95.7% statement coverage on
  the `pubsub` package).
- README-cited benchmark suite (`bench_test.go`).
- `cmd/quickstart` runnable example.

[Unreleased]: https://github.com/F2077/go-pubsub/compare/v1.0.0...HEAD
[v1.0.0]: https://github.com/F2077/go-pubsub/releases/tag/v1.0.0
