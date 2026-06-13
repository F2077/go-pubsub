// Command quickstart is the minimal go-pubsub example mirrored in
// README's "Quick Start" section.
//
// It demonstrates the four-step recipe a reader of pkg.go.dev would
// arrive at by themselves:
//
//  1. Create a broker.
//  2. Create a publisher and a subscriber bound to that broker.
//  3. Subscribe to a topic — WithChannelSize buffers deliveries,
//     WithTimeout arms a sliding timer that delivers
//     ErrSubscriptionTimeout to ErrCh after the configured idle period.
//  4. Publish a message and consume it on sub.Ch.
//
// Run: `go run ./cmd/quickstart`.
//
// For a longer walk-through that exercises every exported symbol
// (multiple brokers, the capacity-exceeded path, sliding timeouts
// firing naturally, OnClose, Subscribes, the lazy-ErrCh contract, etc.)
// see `cmd/quickstart-e2e`.
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

	// 4. Publish synchronously: Publish returns once the message has
	// been delivered to every subscriber's per-topic channel (or
	// dropped if the channel is full — fire-and-forget semantics).
	// A successful delivery also resets the sliding timer in step 3,
	// so the 200ms window is irrelevant for a happy-path run.
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
