# go-pubsub

<img src="logo.png" width="128px" alt="logo">

[![Go Reference](https://pkg.go.dev/badge/github.com/F2077/go-pubsub.svg)](https://pkg.go.dev/github.com/F2077/go-pubsub)
[![CI](https://github.com/F2077/go-pubsub/actions/workflows/test.yml/badge.svg)](https://github.com/F2077/go-pubsub/actions/workflows/test.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/F2077/go-pubsub)](https://goreportcard.com/report/github.com/F2077/go-pubsub)
[![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?logo=go)](https://go.dev/doc/devel/release#go1.25)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

Lightweight, in-process Pub/Sub for Go—perfect for transient data flows like real-time streaming-media packets. It’s pure fire-and-forget: zero persistence, no delivery guarantees, just ultra-fast, one-way messaging.

---

## Installation

```bash
go get github.com/F2077/go-pubsub
```

## Quick Start

The shortest path is a runnable four-step recipe. The same program
lives at `cmd/quickstart/main.go` — clone the repo and run:

```bash
go run ./cmd/quickstart
```

Expected output:

```
Received: CPU over 90%!
```

Source:

```go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/F2077/go-pubsub/pubsub"
)

func main() {
	// 1. Create a broker.
	broker, err := pubsub.NewBroker[string]()
	if err != nil {
		log.Fatal(err)
	}

	// 2. Create a publisher and a subscriber bound to that broker.
	publisher := pubsub.NewPublisher[string](broker)
	subscriber := pubsub.NewSubscriber[string](broker)

	// 3. Subscribe to a topic. WithChannelSize sets the per-topic
	// channel's buffer; WithTimeout arms a sliding 200ms timer that
	// resets on every successful publish and fires ErrSubscriptionTimeout
	// to ErrCh if no publish lands within the window.
	sub, err := subscriber.Subscribe("alerts",
		pubsub.WithChannelSize[string](pubsub.Medium),
		pubsub.WithTimeout[string](200*time.Millisecond),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	// 4. Publish synchronously. A successful delivery resets the
	// sliding timer in step 3, so the 200ms window is irrelevant
	// for a happy-path run.
	if err := publisher.Publish("alerts", "CPU over 90%!"); err != nil {
		log.Fatal(err)
	}

	// 5. Receive. With WithTimeout set, ErrCh is a buffered error
	// channel that receives ErrSubscriptionTimeout exactly once when
	// the timer fires; without WithTimeout it would be nil. In this
	// happy-path the timer never fires — Publish landed first.
	select {
	case msg := <-sub.Ch:
		fmt.Println("Received:", msg)
	case err := <-sub.ErrCh:
		log.Println("Timeout:", err)
	}
}
```

For a deeper walk-through that exercises every exported symbol
(`Subscribes`, `OnClose`, the lazy-`ErrCh` contract, the
capacity-exceeded path, sliding timeouts firing naturally, …) see
`cmd/quickstart-e2e`:

```bash
go run ./cmd/quickstart-e2e
```

## Key Features

- 🚀 **Zero Persistence**: Messages vanish if channels are full or subscribers time out.
- ⏱️ **Auto-Expiry**: Idle subscriptions close automatically (configurable timeout).
- 🔒 **Concurrency-Safe**: Efficient locking for high concurrency.
- 📦 **Capacity Control**: Set max subscriptions per broker (prevents memory leaks).
- 📡 **Topic-Based**: Simple publish/subscribe with string topics.

## Advanced Configuration

### Custom Broker

```go
broker, _ := pubsub.NewBroker[string](
	pubsub.WithCapacity[string](5000),  // Max 5000 topics
	pubsub.WithLogger[string](customLogger), // Inject your logger
	pubsub.WithId[string]("broker-1"),  // Custom broker ID
)
```

### Subscriber Options

```go
// Subscribe with custom settings
sub, _ := subscriber.Subscribe("metrics",
	pubsub.WithChannelSize[string](pubsub.Huge),  // 10000-message buffer
	pubsub.WithTimeout[string](10*time.Second),   // Timeout after 10s inactivity
)
```

## When to Use

- ✅ Real-time pub-sub
- ✅ Low-latency gaming/live events
- ❌ **Not for**: Persistent queues, guaranteed delivery.

## Performance Notes

- 🔥 **Fast fan-out**: Optimized for many subscribers per topic.
- ⚠️ **No backpressure**: Full channels drop messages silently.

## Development

A `Makefile` is provided; run `make help` to see the targets. The full
local quality gate is `make ci` (format check + `go vet` + race-enabled
tests + benchmarks). The same gate runs in CI on every push and PR via
`.github/workflows/test.yml`.

```bash
make help      # list every target
make all       # fmt + vet + test
make cover     # write cover.out + cover.html
make bench     # run the README-cited benchmarks
make profile-cpu # open the CPU flame graph (see PROFILING.md)
```

---

## Benchmark Results

_All benchmarks run with_ `go test -bench=. -benchmem -run=^$ ./pubsub/...` _via_ `make bench`. _Numbers below are the median of 3 runs on_ **goos: linux**, **goarch: amd64**, **pkg: github.com/F2077/go-pubsub**, **cpu: Intel(R) Core(TM) Ultra 5 125H**.

| Benchmark                                          | Iterations |             ns/op |   B/op | allocs/op |
|----------------------------------------------------|-----------:|------------------:|-------:|----------:|
| BenchmarkPublishSingleSubscriber-18                | 11 088 721 |       107.7 ns/op |     0 |         0 |
| BenchmarkMultipleSubscribers-18                    |    185 701 |       6 459 ns/op |     0 |         0 |
| BenchmarkMultiPublisherSingleSubscriber-18         |    472 838 |       2 507 ns/op |   304 |        12 |
| BenchmarkMultiPublisherMultipleSubscribers-18      |     75 205 |      15 996 ns/op |   304 |        12 |
| BenchmarkUltraLargeSubscribersSinglePublisher-18   |        606 |   2 017 115 ns/op |  1028 |         0 |
| BenchmarkPublishChannelSizes/Small-18              | 11 431 624 |       106.6 ns/op |     0 |         0 |
| BenchmarkPublishChannelSizes/Medium-18             | 10 783 516 |       105.2 ns/op |     0 |         0 |
| BenchmarkPublishChannelSizes/Large-18              | 11 358 231 |       104.4 ns/op |     0 |         0 |
| BenchmarkPublishWithTimeout-18                     |  3 303 381 |       375.2 ns/op |   248 |         3 |
| BenchmarkHighLoadParallel-18                       |     14 727 |      80 579 ns/op |  3795 |         0 |
| BenchmarkSubscribes-18                             |     19 392 |      61 532 ns/op | 71919 |       320 |
| BenchmarkBrokerTopics/10-18                        |  6 745 747 |       173.1 ns/op |   160 |         1 |
| BenchmarkBrokerTopics/100-18                       |    982 081 |       1 152 ns/op |  1792 |         1 |
| BenchmarkBrokerTopics/1000-18                      |    108 994 |      10 800 ns/op | 16384 |         1 |
| BenchmarkStructPayload-18                          | 10 919 252 |       107.9 ns/op |     0 |         0 |
| BenchmarkPublishAutoCreateTopic-18                 |    833 221 |       2 138 ns/op |  3083 |         8 |

_Note: `allocs/op = 0` on the fan-out benchmarks means the library's hot path
is zero-allocation (the snapshot slice is recycled via `sync.Pool`). The
non-zero `B/op` on `UltraLarge` / `HighLoadParallel` comes from the benchmark
harness's drain goroutines (spawned per run to keep subscriber channels
empty), not from library code._

---

## Contributing

Bug reports, feature ideas, and PRs are welcome. See
[`CONTRIBUTING.md`](./CONTRIBUTING.md) for the workflow and
[`CODE_OF_CONDUCT.md`](./CODE_OF_CONDUCT.md) for community norms.

## License

This project is licensed under the **MIT License** — see
[`LICENSE`](./LICENSE) for the full text. Copyright © 2024 F2077.
