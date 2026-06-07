package pubsub

import (
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- Broker options: validation error paths -------------------------------

// TestBrokerOptionValidation exercises every validating BrokerOption's
// rejection path through errors.Is, so the sentinel errors are not
// dead code.
func TestBrokerOptionValidation(t *testing.T) {
	cases := []struct {
		name    string
		opt     BrokerOption[string]
		wantErr error
	}{
		{"nil logger", WithLogger[string](nil), ErrLoggerNil},
		{"empty id", WithId[string](""), ErrBrokerIdEmpty},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewBroker[string](tc.opt)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected %v, got %v", tc.wantErr, err)
			}
		})
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
	// Topics() returns an unordered slice; sort it before comparing.
	sort.Strings(topics)
	if want := []string{"alpha", "beta"}; len(topics) == 2 && (topics[0] != want[0] || topics[1] != want[1]) {
		t.Fatalf("expected %v, got %v", want, topics)
	}
}

// --- End-to-end integration through the broker ---------------------------

// TestBasicPubSub tests the canonical happy path: create a broker, attach
// a publisher and a subscriber, publish one message, read it back.
func TestBasicPubSub(t *testing.T) {
	broker, err := NewBroker[string](WithLogger[string](testLogger()))
	if err != nil {
		t.Fatal(err)
	}
	publisher := NewPublisher(broker)
	subscriber := NewSubscriber[string](broker)

	sub, err := subscriber.Subscribe("topic1")
	if err != nil {
		t.Fatal(err)
	}
	defer func(sub *Subscription[string]) {
		_ = sub.Close()
	}(sub)

	msgToSend := "hello world"
	if err := publisher.Publish("topic1", msgToSend); err != nil {
		t.Fatal(err)
	}

	select {
	case msg, ok := <-sub.Ch:
		if !ok {
			t.Fatal("message channel closed unexpectedly")
		}
		if msg != msgToSend {
			t.Fatalf("expected message %q, got %q", msgToSend, msg)
		}
	case err, ok := <-sub.ErrCh:
		if ok {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

// TestMultipleSubscribers verifies fan-out: a single Publish reaches every
// Subscriber attached to the topic.
func TestMultipleSubscribers(t *testing.T) {
	broker, err := NewBroker[string](WithLogger[string](testLogger()))
	if err != nil {
		t.Fatal(err)
	}
	publisher := NewPublisher(broker)
	subscriber1 := NewSubscriber[string](broker)
	subscriber2 := NewSubscriber[string](broker)

	sub1, err := subscriber1.Subscribe("topic_multi")
	if err != nil {
		t.Fatal(err)
	}
	defer func(sub1 *Subscription[string]) {
		_ = sub1.Close()
	}(sub1)
	sub2, err := subscriber2.Subscribe("topic_multi")
	if err != nil {
		t.Fatal(err)
	}
	defer func(sub2 *Subscription[string]) {
		_ = sub2.Close()
	}(sub2)

	msg := "broadcast message"
	if err := publisher.Publish("topic_multi", msg); err != nil {
		t.Fatal(err)
	}

	for i, sub := range []*Subscription[string]{sub1, sub2} {
		select {
		case received, ok := <-sub.Ch:
			if !ok {
				t.Fatalf("subscriber %d message channel closed unexpectedly", i+1)
			}
			if received != msg {
				t.Fatalf("subscriber %d expected %q, got %q", i+1, msg, received)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("timeout waiting for subscriber %d message", i+1)
		}
	}
}

// TestMultiPublisherSingleSubscriber verifies fan-in: many Publishers
// feeding a single Subscriber all arrive (within channel capacity).
func TestMultiPublisherSingleSubscriber(t *testing.T) {
	broker, err := NewBroker[int](WithLogger[int](testLogger()))
	if err != nil {
		t.Fatal(err)
	}
	// 创建多个发布者
	numPublishers := 3
	var publishers []*Publisher[int]
	for i := 0; i < numPublishers; i++ {
		publishers = append(publishers, NewPublisher(broker))
	}
	// 单个订阅者
	subscriber := NewSubscriber[int](broker)
	sub, err := subscriber.Subscribe("multi_pub_single_sub", WithChannelSize[int](Medium))
	if err != nil {
		t.Fatal(err)
	}
	defer func(sub *Subscription[int]) {
		_ = sub.Close()
	}(sub)

	// 每个发布者各发送 10 条消息
	numMessagesPerPublisher := 10
	totalMessages := numPublishers * numMessagesPerPublisher

	var wg sync.WaitGroup
	for _, p := range publishers {
		wg.Add(1)
		go func(pub *Publisher[int]) {
			defer wg.Done()
			for i := 0; i < numMessagesPerPublisher; i++ {
				if err := pub.Publish("multi_pub_single_sub", i); err != nil {
					t.Error(err)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}(p)
	}
	wg.Wait()

	// 读取所有消息
	received := 0
	timeout := time.After(2 * time.Second)
Loop:
	for {
		select {
		case <-sub.Ch:
			received++
			if received >= totalMessages {
				break Loop
			}
		case <-timeout:
			break Loop
		}
	}
	if received != totalMessages {
		t.Fatalf("expected %d messages, received %d", totalMessages, received)
	}
}

// TestMultiPublisherMultipleSubscribers verifies the full mesh: many
// Publishers, many Subscribers, one topic.
func TestMultiPublisherMultipleSubscribers(t *testing.T) {
	broker, err := NewBroker[int](WithLogger[int](testLogger()))
	if err != nil {
		t.Fatal(err)
	}
	// 创建多个发布者
	numPublishers := 3
	var publishers []*Publisher[int]
	for i := 0; i < numPublishers; i++ {
		publishers = append(publishers, NewPublisher(broker))
	}

	// 创建多个订阅者（不立即关闭——每个订阅者必须保持订阅才能收到消息）
	numSubscribers := 3
	subs := make([]*Subscription[int], 0, numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		subscriber := NewSubscriber[int](broker)
		sub, err := subscriber.Subscribe("multi_pub_multi_sub", WithChannelSize[int](Medium))
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
	}
	t.Cleanup(func() {
		for _, sub := range subs {
			_ = sub.Close()
		}
	})

	// 每个发布者各发送 5 条消息，预期每个订阅者收到 numPublishers * 5 条消息
	numMessagesPerPublisher := 5
	totalMessages := numPublishers * numMessagesPerPublisher

	var wg sync.WaitGroup
	for _, p := range publishers {
		wg.Add(1)
		go func(pub *Publisher[int]) {
			defer wg.Done()
			for i := 0; i < numMessagesPerPublisher; i++ {
				if err := pub.Publish("multi_pub_multi_sub", i); err != nil {
					t.Error(err)
				}
				time.Sleep(5 * time.Millisecond)
			}
		}(p)
	}
	wg.Wait()

	// 检查每个订阅者收到的消息数量
	// 必须检查 ok 标志：关闭的通道会立即返回 (0, false)，如果不检查就会被误算为消息
	for i, sub := range subs {
		received := 0
		timeout := time.After(2 * time.Second)
	Loop:
		for {
			select {
			case _, ok := <-sub.Ch:
				if !ok {
					break Loop
				}
				received++
				if received >= totalMessages {
					break Loop
				}
			case <-timeout:
				break Loop
			}
		}
		if received != totalMessages {
			t.Fatalf("subscriber %d: expected %d messages, received %d", i+1, totalMessages, received)
		}
	}
}

// TestStructPayload verifies a struct payload round-trips intact through
// publish → deliver → receive. Reuses benchPacket from bench_test.go to
// avoid two structurally identical test fixtures.
func TestStructPayload(t *testing.T) {
	broker, _ := NewBroker[benchPacket]()
	pub := NewPublisher[benchPacket](broker)
	sub := NewSubscriber[benchPacket](broker)

	s, err := sub.Subscribe("pkt")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	want := benchPacket{seq: 7, body: "alpha"}
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

// TestConcurrentPublishAllDelivered spins up many concurrent publishers and
// asserts the subscriber receives exactly the expected total. This guards
// against lost messages, double-counts, or races in the deliver path.
func TestConcurrentPublishAllDelivered(t *testing.T) {
	const (
		numPublishers    = 16
		msgsPerPublisher = 200
		expectedTotal    = numPublishers * msgsPerPublisher
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
