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
