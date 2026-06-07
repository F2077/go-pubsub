# go-pubsub

<img src="logo.png" width="128px" alt="logo">

[![Go Reference](https://pkg.go.dev/badge/github.com/F2077/go-pubsub.svg)](https://pkg.go.dev/github.com/F2077/go-pubsub)
[![CI](https://github.com/F2077/go-pubsub/actions/workflows/test.yml/badge.svg)](https://github.com/F2077/go-pubsub/actions/workflows/test.yml)
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
```

---

## Benchmark Results

_All benchmarks run on_ **goos: windows**, **goarch: amd64**, **pkg: github.com/F2077/go-pubsub**, **cpu: Intel(R) Core(TM) i7-10700F CPU @ 2.90GHz**

| Benchmark                                        | Iterations |           ns/op | B/op | allocs/op |
|--------------------------------------------------|-----------:|----------------:|-----:|----------:|
| BenchmarkPublishSingleSubscriber-16              |  5 188 107 |     233.0 ns/op |   96 |         2 |
| BenchmarkMultipleSubscribers-16                  |    143 594 |     8 089 ns/op |   96 |         2 |
| BenchmarkMultiPublisherSingleSubscriber-16       |    259 663 |     4 732 ns/op |  776 |        21 |
| BenchmarkMultiPublisherMultipleSubscribers-16    |     67 593 |    17 823 ns/op |  776 |        21 |
| BenchmarkUltraLargeSubscribersSinglePublisher-16 |        471 | 2 846 125 ns/op |   96 |         2 |
| BenchmarkPublishChannelSizes/Small-16            |  5 271 156 |     230.2 ns/op |   96 |         2 |
| BenchmarkPublishChannelSizes/Medium-16           |  5 134 640 |     229.5 ns/op |   96 |         2 |
| BenchmarkPublishChannelSizes/Large-16            |  5 238 266 |     231.8 ns/op |   96 |         2 |
| BenchmarkPublishWithTimeout-16                   |  1 345 124 |     861.1 ns/op |  507 |         7 |
| BenchmarkHighLoadParallel-16                     |     14 728 |    83 785 ns/op |  100 |         2 |

---

## Contributing

Bug reports, feature ideas, and PRs are welcome. See
[`CONTRIBUTING.md`](./CONTRIBUTING.md) for the workflow and
[`CODE_OF_CONDUCT.md`](./CODE_OF_CONDUCT.md) for community norms.

## License

This project is licensed under the **MIT License** — see
[`LICENSE`](./LICENSE) for the full text. Copyright © 2024 F2077.
