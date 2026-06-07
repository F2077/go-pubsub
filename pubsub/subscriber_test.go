package pubsub

import (
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// --- Subscriber accessors -------------------------------------------------

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

// TestUnsubscribe verifies that closing a subscription before publishing
// drops the message: the channel is closed, not silently filled.
func TestUnsubscribe(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	broker, err := NewBroker[string](WithLogger[string](logger))
	if err != nil {
		t.Fatal(err)
	}
	publisher := NewPublisher(broker)
	subscriber := NewSubscriber[string](broker)

	sub, err := subscriber.Subscribe("topic_unsub")
	if err != nil {
		t.Fatal(err)
	}

	// 立即反订阅
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}

	// 发布消息后，由于已反订阅，通道应已关闭
	if err := publisher.Publish("topic_unsub", "message"); err != nil {
		t.Fatal(err)
	}

	// 稍作等待
	time.Sleep(100 * time.Millisecond)

	select {
	case _, ok := <-sub.Ch:
		if ok {
			t.Fatal("expected message channel to be closed after unsubscribe")
		}
	default:
		// 如果通道被关闭，则 select 会立即走 default 分支
	}

	select {
	case _, ok := <-sub.ErrCh:
		if ok {
			t.Fatal("expected error channel to be closed after unsubscribe")
		}
	default:
	}
}

// --- Sliding timeout -----------------------------------------------------

// TestSubscriptionTimeout verifies that a subscription with WithTimeout
// delivers ErrSubscriptionTimeout to ErrCh after the configured idle period
// when no Publish happens.
func TestSubscriptionTimeout(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	broker, err := NewBroker[string](WithLogger[string](logger))
	if err != nil {
		t.Fatal(err)
	}
	subscriber := NewSubscriber[string](broker)

	// 设置超时为 1 秒
	sub, err := subscriber.Subscribe("timeout_topic", WithTimeout[string](1*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer func(sub *Subscription[string]) {
		_ = sub.Close()
	}(sub)

	select {
	case err := <-sub.ErrCh:
		if !errors.Is(err, ErrSubscriptionTimeout) {
			t.Fatalf("expected ErrSubscriptionTimeout, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected timeout error not received")
	}
}

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

// --- Channel-buffer overflow ---------------------------------------------

// TestChannelOverflow verifies that when more messages are published than
// the per-topic channel can hold, the surplus is dropped (fire-and-forget),
// not blocking the publisher and not exceeding the channel capacity.
func TestChannelOverflow(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	broker, err := NewBroker[int](WithLogger[int](logger))
	if err != nil {
		t.Fatal(err)
	}
	publisher := NewPublisher(broker)
	subscriber := NewSubscriber[int](broker)

	// 使用较小的通道容量 Small
	sub, err := subscriber.Subscribe("topic_overflow", WithChannelSize[int](Small), WithTimeout[int](5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer func(sub *Subscription[int]) {
		_ = sub.Close()
	}(sub)

	totalMessages := 50
	for i := 0; i < totalMessages; i++ {
		if err := publisher.Publish("topic_overflow", i); err != nil {
			t.Fatal(err)
		}
	}

	// 尝试从通道中读取所有消息
	count := 0
	done := make(chan struct{})
	go func() {
		for range sub.Ch {
			count++
		}
		close(done)
	}()
	// 等待一小段时间，然后关闭订阅让 goroutine 结束
	time.Sleep(100 * time.Millisecond)
	_ = sub.Close()
	<-done

	if count > int(Small) {
		t.Fatalf("received %d messages, which exceeds channel capacity %d", count, Small)
	} else {
		t.Logf("received %d messages with channel capacity %d", count, Small)
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
	if !errors.Is(err, ErrSubscriptionCapacityExceeded) {
		t.Fatalf("expected ErrSubscriptionCapacityExceeded, got %v", err)
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

// TestSubscriber_Subscribes verifies the multi-topic Subscribe call: one
// call attaches to many topics and returns a *Subscription per topic, in
// the same order. Verifies capacity-exceeded error semantics.
func TestSubscriber_Subscribes(t *testing.T) {
	broker, _ := NewBroker[string]()
	subscriber := NewSubscriber[string](broker)
	defer func(subscriber *Subscriber[string]) {
		_ = subscriber.Close()
	}(subscriber)

	// 测试正常多主题订阅
	topics := []string{"topic1", "topic2", "topic3"}
	subs, err := subscriber.Subscribes(topics)
	assert.NoError(t, err)
	assert.Equal(t, len(topics), len(subs))

	// 验证订阅状态
	subscriber.mutex.Lock()
	assert.Equal(t, len(topics), len(subscriber.topics))
	for _, topic := range topics {
		assert.Contains(t, subscriber.topics, topic)
		value, _ := subscriber.channels.Load(topic)
		assert.NotNil(t, value)
	}
	subscriber.mutex.Unlock()

	// 测试错误场景（超过 Broker 容量）
	smallBroker, _ := NewBroker[string](WithCapacity[string](1))
	subscriber2 := NewSubscriber[string](smallBroker)
	defer func(subscriber2 *Subscriber[string]) {
		_ = subscriber2.Close()
	}(subscriber2)

	_, err = subscriber2.Subscribes([]string{"topicA", "topicB"})
	assert.ErrorIs(t, err, ErrSubscriptionCapacityExceeded)
}
