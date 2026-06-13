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
- `.editorconfig`, `.golangci.yml`, `.gitattributes`, `codecov.yml`.
- Governance files: `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`,
  `SECURITY.md`.
- `cmd/quickstart/README.md` documenting the runnable example.
- **Goroutine-leak guard** via `go.uber.org/goleak v1.3.0`; every test
  in the `pubsub` package now fails if a goroutine outlives its test.
- **Codecov integration**: CI uploads `coverage.out` to Codecov with
  a 90% project / 80% patch target (opt-in until the repo is enabled
  on codecov.io — the upload step is non-fatal by default).

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
