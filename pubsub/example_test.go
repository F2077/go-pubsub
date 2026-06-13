package pubsub_test

import (
	"errors"
	"fmt"
	"time"

	"github.com/F2077/go-pubsub/pubsub"
)

// ExampleBroker demonstrates the most common usage: create a generic broker,
// inject options, build publishers and subscribers on top of it.
func ExampleBroker() {
	broker, err := pubsub.NewBroker[string](
		pubsub.WithId[string]("alerts-broker"),
		pubsub.WithCapacity[string](1024),
	)
	if err != nil {
		panic(err)
	}

	publisher := pubsub.NewPublisher[string](broker)
	subscriber := pubsub.NewSubscriber[string](broker)

	sub, err := subscriber.Subscribe("cpu")
	if err != nil {
		panic(err)
	}
	defer func(sub *pubsub.Subscription[string]) { _ = sub.Close() }(sub)

	if err := publisher.Publish("cpu", "over 90%"); err != nil {
		panic(err)
	}

	fmt.Println(<-sub.Ch)
	// Output: over 90%
}

// ExampleWithChannelSize shows how to set the per-subscriber buffered channel.
// Larger buffers reduce drops when the consumer is slower than the producer.
func ExampleWithChannelSize() {
	broker, _ := pubsub.NewBroker[int]()
	subscriber := pubsub.NewSubscriber[int](broker)

	// Block keeps Publishes blocking on send, which lets us see the buffer
	// depth in action: we publish first, then read.
	sub, _ := subscriber.Subscribe("metrics", pubsub.WithChannelSize[int](pubsub.Single))
	defer func(sub *pubsub.Subscription[int]) { _ = sub.Close() }(sub)

	publisher := pubsub.NewPublisher[int](broker)
	_ = publisher.Publish("metrics", 42)
	fmt.Println(<-sub.Ch)
	// Output: 42
}

// ExampleWithTimeout shows the sliding-timeout subscription: the subscription
// auto-closes with ErrSubscriptionTimeout after the configured idle period.
// A successful Publish before the deadline resets the timer.
func ExampleWithTimeout() {
	broker, _ := pubsub.NewBroker[string]()
	subscriber := pubsub.NewSubscriber[string](broker)

	sub, _ := subscriber.Subscribe("heartbeat", pubsub.WithTimeout[string](50*time.Millisecond))
	defer func(sub *pubsub.Subscription[string]) { _ = sub.Close() }(sub)

	select {
	case err := <-sub.ErrCh:
		if errors.Is(err, pubsub.ErrSubscriptionTimeout) {
			fmt.Println("timed out")
		}
	case <-time.After(time.Second):
		fmt.Println("no timeout in 1s")
	}
	// Output: timed out
}

// ExamplePublisher demonstrates that publishers and subscribers are decoupled:
// one publisher can feed any number of subscribers on the same broker.
func ExamplePublisher() {
	broker, _ := pubsub.NewBroker[string]()
	p := pubsub.NewPublisher[string](broker)

	subA, _ := pubsub.NewSubscriber[string](broker).Subscribe("events")
	subB, _ := pubsub.NewSubscriber[string](broker).Subscribe("events")
	defer func() { _ = subA.Close() }()
	defer func() { _ = subB.Close() }()

	_ = p.Publish("events", "hello")

	fmt.Println(<-subA.Ch)
	fmt.Println(<-subB.Ch)
	// Output:
	// hello
	// hello
}

// ExampleSubscriber_Subscribes is the multi-topic variant: one call subscribes
// to many topics and returns a *Subscription per topic.
func ExampleSubscriber_Subscribes() {
	broker, _ := pubsub.NewBroker[string]()
	subscriber := pubsub.NewSubscriber[string](broker)

	subs, err := subscriber.Subscribes([]string{"a", "b", "c"})
	if err != nil {
		panic(err)
	}
	defer func() {
		for _, s := range subs {
			_ = s.Close()
		}
	}()

	fmt.Println(len(subs))
	// Output: 3
}

// ExampleSubscriber_Close shows that calling Close removes the subscriber from
// every topic it was on and that subsequent Subscribe calls return
// ErrSubscriberClosed.
func ExampleSubscriber_Close() {
	broker, _ := pubsub.NewBroker[string]()
	subscriber := pubsub.NewSubscriber[string](broker)

	_ = subscriber.Close()

	_, err := subscriber.Subscribe("anything")
	fmt.Println(errors.Is(err, pubsub.ErrSubscriberClosed))
	// Output: true
}

// ExampleBroker_Topics shows the introspection helper: a snapshot of the
// topics currently held by the broker. Note that when the last subscriber for
// a topic unsubscribes, the topic is reaped asynchronously — observing a
// freshly-empty broker right after Close may still see the topic.
func ExampleBroker_Topics() {
	broker, _ := pubsub.NewBroker[string]()
	sub := pubsub.NewSubscriber[string](broker)

	a, _ := sub.Subscribe("alpha")
	b, _ := sub.Subscribe("beta")
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()

	topics := broker.Topics()
	fmt.Println(len(topics) >= 1)
	// Output: true
}
