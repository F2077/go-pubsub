# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`go-pubsub` is a lightweight, in-process Pub/Sub library for Go. It is pure fire-and-forget: zero persistence, no delivery guarantees, designed for transient, low-latency data flows (real-time streaming-media packets, gaming events, live signals). Full subscriber channels drop messages silently — there is no backpressure.

Module path: `github.com/F2077/go-pubsub` (Go 1.25, generics-based).
Direct dependencies: `github.com/google/uuid`, `github.com/stretchr/testify`.

## Common commands

```bash
# Build
go build ./...

# Run all unit tests in the pubsub package
go test ./pubsub/...

# Run a single test (matches -run regex against test names)
go test -run TestBasicPubSub ./pubsub/...
go test -run TestChannelOverflow ./pubsub/...

# Run benchmarks (the README's published numbers come from these)
go test -bench=. ./pubsub/...

# Run a specific benchmark, e.g.
go test -bench=BenchmarkPublishWithTimeout ./pubsub/...

# Race detector — strongly encouraged for concurrency changes
go test -race ./pubsub/...

# Vet / static check
go vet ./...

# Coverage report (one-line summary per package)
go test -cover ./pubsub/...

# Hunt for flaky tests (N iterations with race detector)
go test -count=10 -race ./pubsub/...

# Run the example program
go run ./cmd/quickstart
```

The toolchain story is `make` + CI, not just `go test`/`go vet`:
- `Makefile` — `make test` / `make bench` / `make lint` / `make cover` wrap the common commands.
- `.golangci.yml` — v2 schema; enables `govet`, `ineffassign`, `unused`, `revive`, `staticcheck`.
- `.github/workflows/test.yml` — on push/PR runs: `go mod tidy -diff`, a **gofmt gate** (`gofmt -l` must be empty — always `gofmt -w` before committing!), `go vet`, `go test -race -count=1 -coverprofile`, and `golangci-lint v2.12.2` (`only-new-issues: true`). So run `gofmt -w` + `go vet` + `go test -race` locally before pushing, or CI will go red.

The `infertypeargs` lint (e.g. `WithLogger[string]` when `T` is inferred)
fires across every file in the codebase. It is pre-existing stylistic
noise, not a correctness issue; it is explicitly excluded in
`.golangci.yml` (staticcheck "unnecessary type arguments"), so it will
not fail CI. It is out of scope for unrelated work.

## Code layout

```
pubsub/
  pubsub.go         # package marker only (single-line file)
  broker.go         # Broker[T], subscription[T], options, capacity mgmt
  publisher.go      # Publisher[T] (thin wrapper around broker.Publish)
  subscriber.go     # Subscriber[T], Subscription[T], SubscriptionOption
  coverage_test.go   # unit tests for the public surface (gap-fillers)
  concurrent_test.go # concurrency safety-net tests (run under -race)
  example_test.go    # godoc Example* tests; show up on pkg.go.dev
  perf_test.go       # zero-alloc assertions (//go:build !race)
  pubsub_test.go     # unit tests
  bench_test.go      # benchmarks cited in README
cmd/
  quickstart/main.go  # runnable "Quick Start" example
```

Everything user-facing lives in the `pubsub` package; the `cmd/quickstart` binary is the only consumer and doubles as documentation.

## Architecture

Three generic types (`T` = message payload) form the public surface:

- **`Broker[T]`** owns the topic→subscription map and the global capacity cap (default 8192 topics). It guards the map with `sync.RWMutex` and is the only place a new `subscription` is created or removed. All log output is emitted through an injected `*slog.Logger` (defaults to `slog.NewTextHandler(os.Stdout, nil)` — tests pass a stderr handler to keep bench/test output clean).
- **`Publisher[T]`** is a UUID-keyed handle on a broker. `Publish(topic, msg)` resolves the topic's subscription via `createOrLoadSubscription` and calls `subscription.deliver`.
- **`Subscriber[T]`** holds per-topic `chan T` in a `map[string]chan T` guarded by `subscriber.mutex`, plus a per-topic `*time.Timer` (created only when `WithTimeout` is set). The per-subscription `chan error` lives on each `*Subscription` (not shared, so one sub's `Close` can't close another's `ErrCh`). `Subscribe` returns a `*Subscription[T]` exposing the read-only `Ch` and `ErrCh` channels and a `Close` method.

The internal `subscription[T]` is the fan-out unit: it tracks all `Subscriber`s for one topic. On `deliver` it snapshots the subscriber set into a `sync.Pool`-recycled slice (zero allocation on the hot path — the pool stores `*[]*Subscriber[T]` so only a pointer boxes into `interface{}`), then for each subscriber it holds `subscriber.mutex` to read `channels[topic]` and non-blockingly `select`-send (`default: drop`). A successful send calls `subscriber.resetTimer` **outside** the lock — that is the sliding timeout.

### Locking pattern (important)

`subscription.removeSubscriber` deliberately fires `broker.tryRemoveSubscription` in a **separate goroutine**. The comment in `broker.go` explains: holding `subscription` write → `broker` write while another path can do `broker` → `subscription` would deadlock, so the removal is decoupled. If you touch the locking order, preserve this pattern; re-check for deadlocks with `go test -race`.

`deliver`'s fan-out holds `subscriber.mutex` while reading `channels[topic]` and sending. This serializes send against `unsubscribe`'s `close` of the same channel — the fix for a send-on-closed-channel race that used to panic the publisher goroutine (see Gotchas). `resetTimer` is called **outside** that lock because it too takes `subscriber.mutex`; calling it inside would re-enter and deadlock. The order is one-way: `deliver` releases `subscription.rwMutex` (snapshot) before taking `subscriber.mutex` (send), so there is no `subscription → subscriber` holding to conflict with `unsubscribe`'s `subscriber → broker → subscription` order.

### Capacity and lifecycle

- `Broker.WithCapacity` caps the number of **topics** (not subscribers). New topics above the cap return `SubscriptionCapacityExceed` (wrap with `%w` so callers can `errors.Is`).
- `Subscription.Close` removes the subscriber from the topic, closes both channels for that topic on the subscriber side, stops the timer, and lets the broker garbage-collect the subscription if it became empty.
- `Subscriber.Close` unsubscribes from every topic and marks the subscriber closed; subsequent `Subscribe` calls return `ErrSubscriberClosed`.

### Options pattern

Broker options (`WithLogger`, `WithId`, `WithCapacity`) return `error` because the first two validate input. Subscription options (`WithChannelSize`, `WithTimeout`) return nothing — they just mutate the local `subscriptionOptions` struct inside `Subscribe`. `ChannelSize` has named constants `Block(0)`, `Single(1)`, `Small(10)`, `Medium(100)`, `Large(1000)`, `Huge(10000)`; default is `Medium`. `DefaultTimeout` is `0` (disabled); only `> 0` enables the sliding timer that delivers `ErrSubscriptionTimeout` to `ErrCh`.

## Testing conventions

- Tests live next to the code in `pubsub/`. Use `testify/assert` only where it cleans things up; most cases use stdlib `t.Fatal` / `t.Errorf`.
- `pubsub_test` (external test package) for godoc `Example*` tests so the
  examples only use exported API; internal `pubsub` (same-package tests)
  for tests that need to touch unexported fields like `subscriber.topics`.
- Godoc example names must match real identifiers: use
  `ExampleSubscriber_Subscribes` to attach to a method, not
  `ExampleSubscribes` — the godoc build step verifies the identifier.
- Every test constructs its own broker and typically injects a `slog` handler at `LevelInfo` (or `LevelError` for benchmarks) to keep logs out of test output — follow that pattern instead of using the default stdout logger.
- Tests use real timing (`time.After`, `time.Sleep`) for timeout/overflow cases. Avoid asserting exact message counts where the channel size forces drops — `TestChannelOverflow` is the model: it asserts the count does not exceed capacity rather than expecting a specific number.
- Benchmarks (in `bench_test.go`) all use `b.ReportAllocs()`. When adding a new benchmark, follow the same `ResetTimer` / drain-channel-each-iter pattern to keep results comparable to those in the README.

## When NOT to use this

From the README's own guidance — and worth restating so future changes don't accidentally try to add them:

- Persistent queues, durable storage, message replay → use a real broker.
- Guaranteed/at-least-once delivery → this library drops on full channels and provides no acknowledgement path.
- Cross-process pub/sub → this is in-process only; no network protocol.

## Gotchas

- **Receive-without-`ok` counts closed-channel zero values as messages.**
  `case <-sub.Ch: received++` will count zero values forever on a closed
  channel and report success vacuously. Always write
  `case v, ok := <-sub.Ch: if !ok { break }; received++` in tests.
  The original `TestMultiPublisherMultipleSubscribers` was passing for the
  wrong reason until this guard was added.
- **Topic reaping is asynchronous.** `subscription.removeSubscriber` fires
  `broker.tryRemoveSubscription` in a separate goroutine to avoid
  `subscription → broker` lock-order deadlock (see `broker.go:285`).
  A freshly-emptied topic may still be visible to `Broker.Topics()`
  immediately after `Subscription.Close()` returns.
- **`Subscriber.Subscribes` leaks partial subscriptions on failure.** If
  the 2nd topic in a `Subscribes` call fails (e.g. broker at capacity),
  the topics that succeeded remain in the subscriber's set. Documented
  by `TestSubscribesPartialFailure`; if you change this, do it in its own
  commit and decide explicitly whether cleanup is the new contract.
- **`deliver` sends and `unsubscribe` closes under the same `subscriber.mutex`.**
  The fan-out reads `channels[topic]` and `select`-sends while holding
  `subscriber.mutex`; `unsubscribe` deletes the entry and `close`s the
  channel under the same lock. This mutual exclusion is what prevents a
  send-on-closed-channel panic. An earlier version did a lock-free
  `sync.Map.Load` + bare `select`-send, which raced `unsubscribe`'s close
  and crashed the publisher goroutine — `TestConcurrentPublishWithDynamicSubscribe`
  (4 publishers + 8 dynamic subscribe/close goroutines) is the regression
  test. If you change the send or close path, keep them behind the same
  lock and re-run that test under `-race`.
