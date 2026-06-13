package pubsub

import (
	"errors"
	"sync"
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

// TestSubscriberClosedPostConditions verifies the documented behavior of
// every method on a subscriber that has already been Closed.
func TestSubscriberClosedPostConditions(t *testing.T) {
	broker, _ := NewBroker[string]()
	sub := NewSubscriber[string](broker)
	if err := sub.Close(); err != nil {
		t.Fatalf("first Close() should succeed, got %v", err)
	}

	t.Run("Subscribe returns ErrSubscriberClosed", func(t *testing.T) {
		_, err := sub.Subscribe("anything")
		if !errors.Is(err, ErrSubscriberClosed) {
			t.Fatalf("expected ErrSubscriberClosed, got %v", err)
		}
	})

	t.Run("Close returns ErrSubscriberClosed", func(t *testing.T) {
		if err := sub.Close(); !errors.Is(err, ErrSubscriberClosed) {
			t.Fatalf("second Close: expected ErrSubscriberClosed, got %v", err)
		}
	})
}

// TestSubscriberCloseUnsubscribesAll verifies that Close() on a multi-topic
// subscriber closes every per-topic Ch. ErrCh is closed only on topics
// that subscribed with WithTimeout; topics without WithTimeout have
// ErrCh == nil (a receive on a nil channel blocks forever, which is
// the correct "never errors" semantics). The "ErrCh is allocated and
// closed after Close" path is covered by TestSubscribeWithTimeoutErrChIsAllocated
// plus the normal Close() cleanup.
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
		if s.ErrCh == nil {
			continue // no-timeout 订阅：ErrCh 始终 nil，receive 永远阻塞
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
	broker, err := NewBroker[string](WithLogger[string](testLogger()))
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
	broker, err := NewBroker[string](WithLogger[string](testLogger()))
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
	broker, err := NewBroker[int](WithLogger[int](testLogger()))
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

// --- ErrCh allocation ----------------------------------------------------

// TestSubscribeWithoutTimeoutErrChIsNil verifies that a Subscribe call
// without WithTimeout produces a *Subscription whose ErrCh is nil.
// This is the contract: a subscription that cannot time out has no
// error channel, and a receive on a nil channel blocks forever —
// which is the intended "never errors" semantics.
func TestSubscribeWithoutTimeoutErrChIsNil(t *testing.T) {
	broker, _ := NewBroker[string]()
	sub, err := NewSubscriber[string](broker).Subscribe("no_timeout_topic")
	if err != nil {
		t.Fatal(err)
	}
	defer func(sub *Subscription[string]) { _ = sub.Close() }(sub)

	if sub.ErrCh != nil {
		t.Errorf("ErrCh should be nil for Subscribe without WithTimeout, got non-nil")
	}

	// Sanity: receiving from nil ErrCh must block forever (use a short
	// timeout to verify the blocking behavior without hanging the test).
	select {
	case err := <-sub.ErrCh:
		t.Errorf("nil ErrCh should not deliver; got %v", err)
	case <-time.After(50 * time.Millisecond):
		// expected: receive blocks because the channel is nil
	}
}

// TestSubscribeWithTimeoutErrChIsAllocated is the inverse: when
// WithTimeout is passed, ErrCh must be a real channel and remain open
// until Close. Verifies we did not accidentally make ErrCh nil in
// both branches.
func TestSubscribeWithTimeoutErrChIsAllocated(t *testing.T) {
	broker, _ := NewBroker[string]()
	sub, err := NewSubscriber[string](broker).Subscribe(
		"with_timeout_topic", WithTimeout[string](1*time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func(sub *Subscription[string]) { _ = sub.Close() }(sub)

	if sub.ErrCh == nil {
		t.Fatal("ErrCh should be non-nil for Subscribe with WithTimeout")
	}
}

// --- Timer lifecycle -----------------------------------------------------

// TestReSubscribeSameTopicCleansUpOldTimer verifies that calling Subscribe
// a second time for the same topic with WithTimeout replaces the prior
// timer and its fire goroutine without leaking either. The old fire
// goroutine must observe the closed done channel and exit; the new one
// must own the fresh *time.Timer. goleak.VerifyTestMain in TestMain is
// the safety net that fails the test binary if either goroutine leaks.
func TestReSubscribeSameTopicCleansUpOldTimer(t *testing.T) {
	broker, err := NewBroker[string](WithLogger[string](testLogger()))
	if err != nil {
		t.Fatal(err)
	}
	sub := NewSubscriber[string](broker)

	first, err := sub.Subscribe("dup_topic", WithTimeout[string](1*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close() }()

	// Re-Subscribe same topic with a different timeout.
	second, err := sub.Subscribe("dup_topic", WithTimeout[string](2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("re-Subscribe should return a fresh *Subscription, not the cached one")
	}

	// Verify the subscriber's internal map points at exactly one timer
	// (the new one) — old timer entry should have been replaced.
	sub.mutex.Lock()
	timerCount := len(sub.timers)
	doneCount := len(sub.timerDones)
	sub.mutex.Unlock()
	if timerCount != 1 {
		t.Errorf("expected 1 timer entry after re-Subscribe, got %d", timerCount)
	}
	if doneCount != 1 {
		t.Errorf("expected 1 timerDones entry after re-Subscribe, got %d", doneCount)
	}
}

// TestResetTimerDrainsFiredTimer is a synthetic test for the standard
// time.Timer.Reset pattern: when the timer has already fired, t.Stop()
// returns false and the channel may carry a stale value. The drain
// `select { case <-t.C: default: }` prevents the fire goroutine from
// consuming that stale value and calling handleTimeout spuriously,
// which would deliver a false ErrSubscriptionTimeout to the user.
//
// Without the drain, a publish that races a natural timeout (timer
// fires, then publish's resetTimer runs before the fire goroutine
// reads) would produce a ghost timeout. We simulate that race by
// synthesizing a Subscriber state where the timer is already expired
// and the fire goroutine is parked on t.C.
func TestResetTimerDrainsFiredTimer(t *testing.T) {
	broker, err := NewBroker[string](WithLogger[string](testLogger()))
	if err != nil {
		t.Fatal(err)
	}
	s := NewSubscriber[string](broker)
	// 关键：让 handleTimeout 在 topics 检查时不会 short-circuit
	s.topics["x"] = struct{}{}

	// 构造一个必然已 fire 的 timer
	t0 := time.NewTimer(5 * time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	s.timers["x"] = t0
	s.timeouts["x"] = 1 * time.Hour

	errCh := make(chan error, 10)
	done := make(chan struct{})
	s.timerDones["x"] = done

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.runTopicTimer("x", t0, errCh, done)
	}()

	// 触发 resetTimer — t0 已 fire，t.Stop() 应返回 false，走 drain
	s.resetTimer("x")

	// 给 fire goroutine 时间；如果 drain 失败它会读 stale value 并 send 到 errCh
	time.Sleep(50 * time.Millisecond)

	select {
	case err := <-errCh:
		t.Fatalf("spurious timeout from undrained stale channel value: %v", err)
	default:
	}

	// 清理
	close(done)
	wg.Wait()
}

// TestHandleTimeoutDoesNotDeadlockWhenConsumerIsNotDraining 验证：errCh 是
// cap=1 的缓冲通道，如果调用方不读、timer 又反复 fire，handleTimeout 的
// 阻塞 send 会把整个 subscriber 锁住（因为它持着 s.mutex）。修复后用
// non-blocking send 直接 drop，subscriber 仍然可被其他路径访问。
//
// 复现路径：
//  1. Subscribe("t", WithTimeout(50ms)) — 起一个 fire goroutine
//  2. 故意不读 ErrCh，让 errCh 在第一次 fire 后被填满
//  3. Publish 一次（触发 resetTimer 重置 timer）
//  4. 等到第二次 fire，pre-fix 路径下 send 会卡死 + 整个 mutex 卡死；
//     post-fix 路径下 send 走 default，subscriber.Close() 立刻返回。
func TestHandleTimeoutDoesNotDeadlockWhenConsumerIsNotDraining(t *testing.T) {
	broker, _ := NewBroker[string]()
	sub := NewSubscriber[string](broker)
	pub := NewPublisher[string](broker)

	s, err := sub.Subscribe("t", WithTimeout[string](50*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	_ = s // 不读 ErrCh、不主动 Close——让 Close 由后面 goroutine 来收
	// 故意不 drain ErrCh；这里想证 "调用方忘了读" 不会卡死系统。

	// 等到第一次 fire 填满 errCh。
	time.Sleep(80 * time.Millisecond)

	// Publish 一次触发 resetTimer，重新武装 50ms 后的第二次 fire。
	if err := pub.Publish("t", "x"); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// 再等 80ms 让第二次 fire 发生——pre-fix 这次 send 会卡在 s.mutex 上。
	time.Sleep(80 * time.Millisecond)

	// 用 timeout 包裹 subscriber.Close()。如果 handleTimeout 把 s.mutex
	// 锁死了，Close 会等不到锁；2s 超时即视为死锁。
	closed := make(chan error, 1)
	go func() {
		closed <- sub.Close()
	}()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("subscriber.Close returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber.Close deadlocked — handleTimeout held s.mutex on full errCh")
	}
}
