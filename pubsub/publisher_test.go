package pubsub

import (
	"errors"
	"testing"
)

// --- Publisher accessors --------------------------------------------------

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

// --- Publish behavior -----------------------------------------------------

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
}

// TestPublishCapacityExceeded verifies that creating a topic beyond the
// broker's capacity returns a wrapped ErrSubscriptionCapacityExceeded.
func TestPublishCapacityExceeded(t *testing.T) {
	broker, _ := NewBroker[string](WithCapacity[string](1))
	pub := NewPublisher[string](broker)

	if err := pub.Publish("first", "ok"); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	err := pub.Publish("second", "should fail")
	if !errors.Is(err, ErrSubscriptionCapacityExceeded) {
		t.Fatalf("expected ErrSubscriptionCapacityExceeded, got %v", err)
	}
}
