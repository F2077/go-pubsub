# go-pubsub

<img src="logo.png" width="128px" alt="logo">

A lightweight real-time Pub-Sub library for Go. Ideal for transient data scenarios like live dashboards, gaming events, or short-lived notifications. No persistence, no delivery guarantees—just fast
fire-and-forget communication.

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
	"github.com/F2077/go-pubsub"
	"time"
)

func main() {
	// 1. Create a broker (supports generics)
	broker, _ := pubsub.NewBroker[string]()

	// 2. Create a publisher
	publisher := pubsub.NewPublisher[string](broker)

	// 3. Create a subscriber
	subscriber := pubsub.NewSubscriber[string](broker)

	// 4. Subscribe to a topic with buffer size and timeout
	sub, _ := subscriber.Subscribe("alerts",
		pubsub.WithChannelSize[string](pubsub.Medium), // Buffer 100 messages
		pubsub.WithTimeout[string](5*time.Second),     // Auto-close if idle
	)
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
		fmt.Println("Error:", err) // e.g., timeout
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
pubsub.WithCapacity(5000), // Max 5000 topics
pubsub.WithLogger(customLogger), // Inject your logger
pubsub.WithId("broker-1"), // Custom broker ID
)
```

### Subscriber Options

```go
// Subscribe with custom settings
sub, _ := subscriber.Subscribe("metrics",
pubsub.WithChannelSize[string](pubsub.Huge), // 1000-message buffer
pubsub.WithTimeout[string](10 * time.Second), // Timeout after 10s inactivity
)
```

## When to Use

- ✅ Real-time pub-sub
- ✅ Low-latency gaming/live events
- ❌ **Not for**: Persistent queues, guaranteed delivery.

## Performance Notes

- 🔥 **Fast fan-out**: Optimized for many subscribers per topic.
- ⚠️ **No backpressure**: Full channels drop messages silently.

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

**Contributions welcome!** Report bugs or suggest features via issues.
