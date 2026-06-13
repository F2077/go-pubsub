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

_All benchmarks run with_ `go test -bench=. -benchmem -run=^$ ./pubsub/...` _via_ `make bench`. _Numbers below are from the post-simplify pass on_ **goos: linux**, **goarch: amd64**, **pkg: github.com/F2077/go-pubsub**, **cpu: Intel(R) Core(TM) Ultra 5 125H**.

| Benchmark                                          | Iterations |             ns/op |   B/op | allocs/op |
|----------------------------------------------------|-----------:|------------------:|-------:|----------:|
| BenchmarkPublishSingleSubscriber-18                | 10 018 934 |       122.4 ns/op |     0 |         0 |
| BenchmarkMultipleSubscribers-18                    |    196 680 |       6 172 ns/op |     0 |         0 |
| BenchmarkMultiPublisherSingleSubscriber-18         |    397 332 |       2 833 ns/op |   304 |        12 |
| BenchmarkMultiPublisherMultipleSubscribers-18      |     73 789 |      16 982 ns/op |   304 |        12 |
| BenchmarkUltraLargeSubscribersSinglePublisher-18   |        402 |   3 631 646 ns/op |     0 |         0 |
| BenchmarkPublishChannelSizes/Small-18              | 10 672 664 |       114.4 ns/op |     0 |         0 |
| BenchmarkPublishChannelSizes/Medium-18             | 10 732 318 |       120.2 ns/op |     0 |         0 |
| BenchmarkPublishChannelSizes/Large-18              | 10 731 602 |       113.8 ns/op |     0 |         0 |
| BenchmarkPublishWithTimeout-18                     |  2 497 294 |       477.5 ns/op |   248 |         3 |
| BenchmarkHighLoadParallel-18                       |      9 981 |     118 695 ns/op |     3 |         0 |
| BenchmarkSubscribes-18                             |     12 158 |     105 561 ns/op | 82315 |       562 |
| BenchmarkBrokerTopics/10-18                        |  4 792 022 |       268.8 ns/op |   256 |         3 |
| BenchmarkBrokerTopics/100-18                       |    866 445 |       1 388 ns/op |  1888 |         3 |
| BenchmarkBrokerTopics/1000-18                      |     95 181 |      12 637 ns/op | 16480 |         3 |
| BenchmarkStructPayload-18                          | 10 343 504 |       117.5 ns/op |     0 |         0 |
| BenchmarkPublishAutoCreateTopic-18                 |  1 548 001 |       697.5 ns/op |   208 |         4 |

---

## Contributing

Bug reports, feature ideas, and PRs are welcome. See
[`CONTRIBUTING.md`](./CONTRIBUTING.md) for the workflow and
[`CODE_OF_CONDUCT.md`](./CODE_OF_CONDUCT.md) for community norms.

## License

This project is licensed under the **MIT License** — see
[`LICENSE`](./LICENSE) for the full text. Copyright © 2024 F2077.
