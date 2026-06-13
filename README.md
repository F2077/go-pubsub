# go-pubsub

<img src="logo.png" width="128px" alt="logo">

[![Go Reference](https://pkg.go.dev/badge/github.com/F2077/go-pubsub.svg)](https://pkg.go.dev/github.com/F2077/go-pubsub)
[![CI](https://github.com/F2077/go-pubsub/actions/workflows/test.yml/badge.svg)](https://github.com/F2077/go-pubsub/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/F2077/go-pubsub/graph/badge.svg)](https://codecov.io/gh/F2077/go-pubsub)
[![Go Report Card](https://goreportcard.com/badge/github.com/F2077/go-pubsub)](https://goreportcard.com/report/github.com/F2077/go-pubsub)
[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?logo=go)](https://go.dev/doc/devel/release#go1.21)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

Lightweight, in-process Pub/Sub for Go—perfect for transient data flows like real-time streaming-media packets. It’s pure fire-and-forget: zero persistence, no delivery guarantees, just ultra-fast, one-way messaging.

---

## Installation

```bash
go get github.com/F2077/go-pubsub
```

## Quick Start

```go
package main

import (
	"fmt"
	"github.com/F2077/go-pubsub/pubsub"
	"time"
)

func main() {
	// 1. Create a broker (supports generics)
	broker, err := pubsub.NewBroker[string]()
	if err != nil {
		panic(err)
	}

	// 2. Create a publisher
	publisher := pubsub.NewPublisher[string](broker)

	// 3. Create a subscriber
	subscriber := pubsub.NewSubscriber[string](broker)

	// 4. Subscribe to a topic with buffer size and timeout
	sub, err := subscriber.Subscribe("alerts",
		pubsub.WithChannelSize[string](pubsub.Medium), // Buffer 100 messages
		pubsub.WithTimeout[string](5*time.Second),     // Auto-close if idle
	)
	if err != nil {
		panic(err)
	}
	defer func(sub *pubsub.Subscription[string]) {
		_ = sub.Close()
	}(sub)

	// 5. Publish a message
	go func() {
		_ = publisher.Publish("alerts", "CPU over 90%!")
	}()

	// 6. Listen for messages or timeouts
	select {
	case msg := <-sub.Ch:
		fmt.Println("Received:", msg) // Output: "CPU over 90%!"
	case err := <-sub.ErrCh:
		fmt.Println("Error:", err)
	}
}

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

| Benchmark                                          | Iterations |             ns/op |  B/op | allocs/op |
|----------------------------------------------------|-----------:|------------------:|------:|----------:|
| BenchmarkPublishSingleSubscriber-18                |  6 781 626 |       179.6 ns/op |    96 |         2 |
| BenchmarkMultipleSubscribers-18                    |    183 459 |       6 576 ns/op |    96 |         2 |
| BenchmarkMultiPublisherSingleSubscriber-18         |    138 272 |       8 243 ns/op |   784 |        22 |
| BenchmarkMultiPublisherMultipleSubscribers-18      |     61 338 |      20 173 ns/op |   784 |        22 |
| BenchmarkUltraLargeSubscribersSinglePublisher-18   |        357 |   3 583 216 ns/op |    96 |         2 |
| BenchmarkPublishChannelSizes/Small-18              |  6 551 547 |       185.2 ns/op |    96 |         2 |
| BenchmarkPublishChannelSizes/Medium-18             |  6 627 157 |       181.5 ns/op |    96 |         2 |
| BenchmarkPublishChannelSizes/Large-18              |  6 533 964 |       181.8 ns/op |    96 |         2 |
| BenchmarkPublishWithTimeout-18                     |  1 809 643 |       660.2 ns/op |   504 |         7 |
| BenchmarkHighLoadParallel-18                       |      9 979 |     115 895 ns/op |    97 |         2 |
| BenchmarkSubscribes-18                             |     10 000 |     105 403 ns/op | 104822 |       974 |
| BenchmarkBrokerTopics/10-18                        |  4 648 761 |       255.0 ns/op |   256 |         3 |
| BenchmarkBrokerTopics/100-18                       |    823 272 |       1 373 ns/op |  1888 |         3 |
| BenchmarkBrokerTopics/1000-18                      |     94 299 |      12 836 ns/op | 16480 |         3 |
| BenchmarkStructPayload-18                          |  6 343 249 |       187.3 ns/op |    96 |         2 |
| BenchmarkPublishAutoCreateTopic-18                 |  1 205 358 |       847.5 ns/op |   420 |         8 |

---

## Contributing

Bug reports, feature ideas, and PRs are welcome. See
[`CONTRIBUTING.md`](./CONTRIBUTING.md) for the workflow and
[`CODE_OF_CONDUCT.md`](./CODE_OF_CONDUCT.md) for community norms.

## License

This project is licensed under the **MIT License** — see
[`LICENSE`](./LICENSE) for the full text. Copyright © 2024 F2077.
