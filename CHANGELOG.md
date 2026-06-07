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

### Fixed
- `gofmt -l` is clean across the tree (two test files were
  mis-formatted).
- Internal `slog` messages translated to English to match
  `CLAUDE.md`'s user-visible-string rule.

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
