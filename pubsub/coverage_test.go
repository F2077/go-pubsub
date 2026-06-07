package pubsub

import (
	"errors"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testLogger constructs a slog.Logger that writes to stderr at Info level.
// Mirrors the convention in pubsub_test.go: keep log noise out of test output
// without silencing information useful for diagnosis.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

// --- Broker options: validation error paths -------------------------------

// TestWithLoggerNil verifies the WithLogger option rejects a nil logger
// rather than crashing the broker on first log call.
func TestWithLoggerNil(t *testing.T) {
	_, err := NewBroker[string](WithLogger[string](nil))
	if err == nil {
		t.Fatal("expected error for nil logger, got nil")
	}
}

// TestWithIdEmpty verifies WithId rejects an empty id.
func TestWithIdEmpty(t *testing.T) {
	_, err := NewBroker[string](WithId[string](""))
	if err == nil {
		t.Fatal("expected error for empty id, got nil")
	}
}

// TestWithIdAndWithLogger verifies happy-path construction preserves both.
func TestWithIdAndWithLogger(t *testing.T) {
	broker, err := NewBroker[string](
		WithId[string]("test-broker"),
		WithLogger[string](testLogger()),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if broker.Id() != "test-broker" {
		t.Fatalf("expected id=test-broker, got %q", broker.Id())
	}
}

// --- Broker accessors -----------------------------------------------------

// TestBrokerAccessors covers Id(), Capacity(), and String() format.
func TestBrokerAccessors(t *testing.T) {
	broker, err := NewBroker[int](WithCapacity[int](42))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if broker.Id() == "" {
		t.Fatal("broker Id() should not be empty when not explicitly set")
	}
	if broker.Capacity() != 42 {
		t.Fatalf("expected capacity 42, got %d", broker.Capacity())
	}
	// String() format is part of the public contract (used in logs).
	if got := broker.String(); got == "" {
		t.Fatal("broker String() should not be empty")
	}
}

// TestBrokerTopics_ReflectsSubscribe verifies Topics() returns topics with
// active subscribers. Topics with no subscribers are reaped asynchronously,
// so we only assert on the live ones here.
func TestBrokerTopics_ReflectsSubscribe(t *testing.T) {
	broker, _ := NewBroker[string]()
	sub := NewSubscriber[string](broker)

	a, err := sub.Subscribe("alpha")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()

	b, err := sub.Subscribe("beta")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()

	topics := broker.Topics()
	if len(topics) != 2 {
		t.Fatalf("expected 2 topics, got %d (%v)", len(topics), topics)
	}
	// Topics() returns an unordered slice; check both names are present.
	seen := make(map[string]bool, len(topics))
	for _, topic := range topics {
		seen[topic] = true
	}
	if !seen["alpha"] || !seen["beta"] {
		t.Fatalf("expected alpha and beta in topics, got %v", topics)
	}
}

// --- Publisher accessors -------------------------------------------------

// TestPublisherAccessors covers Publisher.Id() and Publisher.String().
func TestPublisherAccessors(t *testing.T) {
	broker, _ := NewBroker[string]()
	pub := NewPublisher[string](broker)
	if pub.Id() == "" {
		t.Fatal("publisher Id() should not be empty")
	}
	if got := pub.String(); got == "" {
		t.Fatal("publisher String() should not be empty")
	}
	// Two publishers on the same broker must have distinct ids.
	pub2 := NewPublisher[string](broker)
	if pub.Id() == pub2.Id() {
		t.Fatalf("expected distinct publisher ids, both %q", pub.Id())
	}
}

// --- Subscriber accessors ------------------------------------------------

// TestSubscriberAccessors covers Subscriber.Id() and Subscriber.String().
func TestSubscriberAccessors(t *testing.T) {
	broker, _ := NewBroker[string]()
	sub := NewSubscriber[string](broker)
	if sub.Id() == "" {
		t.Fatal("subscriber Id() should not be empty")
	}
	if got := sub.String(); got == "" {
		t.Fatal("subscriber String() should not be empty")
	}
}

// --- Subscriber lifecycle -------------------------------------------------

// TestSubscribeAfterClose verifies that subscribing on a closed subscriber
// returns ErrSubscriberClosed. This is the documented contract.
func TestSubscribeAfterClose(t *testing.T) {
	broker, _ := NewBroker[string]()
	sub := NewSubscriber[string](broker)
	if err := sub.Close(); err != nil {
		t.Fatalf("first Close() should succeed, got %v", err)
	}
	_, err := sub.Subscribe("anything")
	if !errors.Is(err, ErrSubscriberClosed) {
		t.Fatalf("expected ErrSubscriberClosed, got %v", err)
	}
}

// TestSubscriberCloseTwice verifies a second Close() returns ErrSubscriberClosed.
func TestSubscriberCloseTwice(t *testing.T) {
	broker, _ := NewBroker[string]()
	sub := NewSubscriber[string](broker)
	if err := sub.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := sub.Close(); !errors.Is(err, ErrSubscriberClosed) {
		t.Fatalf("second Close: expected ErrSubscriberClosed, got %v", err)
	}
}

// TestSubscriberCloseUnsubscribesAll verifies that Close() on a multi-topic
// subscriber closes every per-topic channel and ErrCh.
func TestSubscriberCloseUnsubscribesAll(t *testing.T) {
	broker, _ := NewBroker[string]()
	sub := NewSubscriber[string](broker)

	topics := []string{"a", "b", "c"}
	subs, err := sub.Subscribes(topics)
	if err != nil {
		t.Fatal(err)
	}
	if err := sub.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	for i, s := range subs {
		select {
		case _, ok := <-s.Ch:
			if ok {
				t.Fatalf("subscription %d: Ch should be closed", i)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("subscription %d: Ch did not close", i)
		}
		select {
		case _, ok := <-s.ErrCh:
			if ok {
				t.Fatalf("subscription %d: ErrCh should be closed", i)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("subscription %d: ErrCh did not close", i)
		}
	}
}

// --- Sliding timeout -----------------------------------------------------

// TestSlidingTimeoutResetsOnActivity verifies that successful Publishes
// reset the per-topic timer, keeping the subscription alive past the initial
// deadline. After activity stops, the timer should fire and deliver
// ErrSubscriptionTimeout to ErrCh.
func TestSlidingTimeoutResetsOnActivity(t *testing.T) {
	broker, _ := NewBroker[string]()
	sub := NewSubscriber[string](broker)

	const idleTimeout = 100 * time.Millisecond
	s, err := sub.Subscribe("heartbeat", WithTimeout[string](idleTimeout))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	pub := NewPublisher[string](broker)

	// Publish every 30 ms — well within the 100 ms timeout — for 250 ms total.
	// That should comfortably exceed one idleTimeout without ever firing it.
	tick := time.NewTicker(30 * time.Millisecond)
	defer tick.Stop()
	deadline := time.After(250 * time.Millisecond)
publish:
	for {
		select {
		case <-tick.C:
			if err := pub.Publish("heartbeat", "tick"); err != nil {
				t.Fatalf("publish: %v", err)
			}
			// drain the message so the channel never fills
			select {
			case <-s.Ch:
			case <-time.After(50 * time.Millisecond):
				t.Fatal("did not receive published message in time")
			}
		case <-deadline:
			break publish
		}
	}

	// After the activity burst, the subscription must NOT have timed out yet
	// (we just published, so the timer was reset).
	select {
	case err := <-s.ErrCh:
		t.Fatalf("unexpected timeout during activity burst: %v", err)
	default:
	}

	// Now stop publishing and wait for the timeout to fire.
	select {
	case err := <-s.ErrCh:
		if !errors.Is(err, ErrSubscriptionTimeout) {
			t.Fatalf("expected ErrSubscriptionTimeout, got %v", err)
		}
	case <-time.After(2 * idleTimeout):
		t.Fatal("expected timeout to fire after activity stopped")
	}
}

// --- Publish behavior ----------------------------------------------------

// TestPublishAutoCreatesSubscription verifies that Publishing to a topic
// with no subscribers succeeds: the subscription is created on demand.
// With no subscribers, the deliver step is a no-op, so the call should
// return nil error and create the entry in the broker.
func TestPublishAutoCreatesSubscription(t *testing.T) {
	broker, _ := NewBroker[string]()
	pub := NewPublisher[string](broker)

	if err := pub.Publish("new-topic", "hello"); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	// Wait briefly for the async reaper to remove the empty topic.
	// (Reaping is fire-and-forget — we don't strictly need to assert it.)
	time.Sleep(50 * time.Millisecond)
	_ = broker.Topics() // touch the API to confirm it does not panic on empty
}

// TestPublishCapacityExceeded verifies that creating a topic beyond the
// broker's capacity returns a wrapped SubscriptionCapacityExceed.
func TestPublishCapacityExceeded(t *testing.T) {
	broker, _ := NewBroker[string](WithCapacity[string](1))
	pub := NewPublisher[string](broker)

	if err := pub.Publish("first", "ok"); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	err := pub.Publish("second", "should fail")
	if !errors.Is(err, SubscriptionCapacityExceed) {
		t.Fatalf("expected SubscriptionCapacityExceed, got %v", err)
	}
}

// --- Generic payload type ------------------------------------------------

// packet is a small struct used to confirm the generic T parameter works
// with non-primitive payload types (an obvious requirement, but currently
// no test exercises it).
type packet struct {
	seq  int
	body string
}

// TestStructPayload verifies a struct payload round-trips intact through
// publish → deliver → receive.
func TestStructPayload(t *testing.T) {
	broker, _ := NewBroker[packet]()
	pub := NewPublisher[packet](broker)
	sub := NewSubscriber[packet](broker)

	s, err := sub.Subscribe("pkt")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	want := packet{seq: 7, body: "alpha"}
	if err := pub.Publish("pkt", want); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-s.Ch:
		if got != want {
			t.Fatalf("expected %+v, got %+v", want, got)
		}
	case <-time.After(time.Second):
		t.Fatal("did not receive struct payload")
	}
}

// --- Subscribes partial failure -----------------------------------------

// TestSubscribesPartialFailure documents the current behavior of Subscribes:
// if the 2nd Subscribe call fails (e.g. capacity exceeded), the first
// subscription is left in place and NOT closed. This is an observable
// characteristic that future changes to the API should preserve or
// intentionally break with a separate commit.
//
// If this test is ever re-evaluated, the contract should be made explicit
// (clean up partial subs on error, or document the leak).
func TestSubscribesPartialFailure(t *testing.T) {
	broker, _ := NewBroker[string](WithCapacity[string](1))
	sub := NewSubscriber[string](broker)

	// "first" succeeds, "second" should fail because the broker can only
	// hold one topic and "first" already used it.
	_, err := sub.Subscribes([]string{"first", "second"})
	if !errors.Is(err, SubscriptionCapacityExceed) {
		t.Fatalf("expected SubscriptionCapacityExceed, got %v", err)
	}

	// "first" was successfully created and remains on the subscriber.
	// (Documented current behavior — see comment above.)
	sub.mutex.Lock()
	_, firstTracked := sub.topics["first"]
	_, secondTracked := sub.topics["second"]
	sub.mutex.Unlock()
	if !firstTracked {
		t.Fatal("'first' should still be tracked on subscriber after partial failure")
	}
	if secondTracked {
		t.Fatal("'second' should not be tracked on subscriber")
	}
}

// --- Concurrent stress ---------------------------------------------------

// TestConcurrentPublishAllDelivered spins up many concurrent publishers and
// asserts the subscriber receives exactly the expected total. This guards
// against lost messages, double-counts, or races in the deliver path.
func TestConcurrentPublishAllDelivered(t *testing.T) {
	const (
		numPublishers     = 16
		msgsPerPublisher  = 200
		expectedTotal     = numPublishers * msgsPerPublisher
	)

	broker, _ := NewBroker[int]()
	pub := NewPublisher[int](broker)
	sub := NewSubscriber[int](broker)

	s, err := sub.Subscribe("hot", WithChannelSize[int](Huge))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	var wg sync.WaitGroup
	var sent atomic.Int64
	for p := 0; p < numPublishers; p++ {
		wg.Add(1)
		go func(pubID int) {
			defer wg.Done()
			for i := 0; i < msgsPerPublisher; i++ {
				if err := pub.Publish("hot", pubID*1000+i); err != nil {
					t.Errorf("publish: %v", err)
					return
				}
				sent.Add(1)
			}
		}(p)
	}

	// Reader runs concurrently.
	readerDone := make(chan struct{})
	var received atomic.Int64
	go func() {
		for range s.Ch {
			if received.Add(1) >= int64(expectedTotal) {
				close(readerDone)
				return
			}
		}
		close(readerDone)
	}()

	wg.Wait()

	// Give the reader up to 5s to drain.
	select {
	case <-readerDone:
	case <-time.After(5 * time.Second):
		got := received.Load()
		t.Fatalf("expected %d messages, received %d (sent=%d)",
			expectedTotal, got, sent.Load())
	}

	if got := received.Load(); got != int64(expectedTotal) {
		t.Fatalf("expected %d messages, got %d", expectedTotal, got)
	}
}
