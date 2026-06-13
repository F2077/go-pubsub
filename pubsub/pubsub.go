// Package pubsub is a lightweight, in-process Pub/Sub library for Go.
//
// A Broker owns the topic→subscription map and a global topic capacity cap
// (see DefaultCapacity and WithCapacity). Publishers and Subscribers are
// thin generic handles on a Broker; each carries its own UUID so logs and
// introspection can attribute work to a specific handle.
//
// # Design constraints
//
// go-pubsub is intentionally fire-and-forget:
//
//   - Zero persistence. Messages exist only in the per-topic buffered channel
//     of a Subscriber; if the channel is full, the message is dropped silently.
//   - No delivery guarantees. There is no acknowledgement path and no
//     redelivery. Subscribers that fall behind lose messages.
//   - In-process only. There is no network protocol; do not use this for
//     cross-process pub/sub.
//
// # Typical usage
//
//	broker, _ := pubsub.NewBroker[string](
//	    pubsub.WithCapacity[string](1024),
//	)
//	publisher := pubsub.NewPublisher[string](broker)
//	subscriber := pubsub.NewSubscriber[string](broker)
//
//	sub, _ := subscriber.Subscribe("alerts",
//	    pubsub.WithChannelSize[string](pubsub.Medium),
//	    pubsub.WithTimeout[string](5*time.Second),
//	)
//	defer sub.Close()
//
//	go func() { _ = publisher.Publish("alerts", "CPU over 90%") }()
//
//	select {
//	case msg := <-sub.Ch:
//	    // handle msg
//	case err := <-sub.ErrCh:
//	    // handle ErrSubscriptionTimeout
//	}
//
// For persistent queues, durable storage, or at-least-once delivery, use a
// real message broker (NATS, Kafka, RabbitMQ, …) instead.
package pubsub
