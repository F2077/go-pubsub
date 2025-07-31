package pubsub

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	"testing"
	"time"

	"log/slog"
)

// TestBasicPubSub 测试基本的发布订阅场景
func TestBasicPubSub(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	broker, err := NewBroker[string](WithLogger[string](logger))
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

// TestSubscriptionTimeout 测试订阅超时场景
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

// TestUnsubscribe 测试反订阅场景，订阅后反订阅，发布消息时不应再收到消息
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

// TestMultipleSubscribers 测试同一主题多个订阅者同时能收到发布的消息
func TestMultipleSubscribers(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	broker, err := NewBroker[string](WithLogger[string](logger))
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

// TestChannelOverflow 测试通道溢出情况，保证通道内消息数不会超过设定容量（多余消息会被丢弃）
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

// TestMultiPublisherSingleSubscriber 测试多个发布者对单个订阅者的消息分发
func TestMultiPublisherSingleSubscriber(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	broker, err := NewBroker[int](WithLogger[int](logger))
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

// TestMultiPublisherMultipleSubscribers 测试多个发布者和多个订阅者场景
func TestMultiPublisherMultipleSubscribers(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	broker, err := NewBroker[int](WithLogger[int](logger))
	if err != nil {
		t.Fatal(err)
	}
	// 创建多个发布者
	numPublishers := 3
	var publishers []*Publisher[int]
	for i := 0; i < numPublishers; i++ {
		publishers = append(publishers, NewPublisher(broker))
	}

	// 创建多个订阅者
	numSubscribers := 3
	var subs []*Subscription[int]
	for i := 0; i < numSubscribers; i++ {
		subscriber := NewSubscriber[int](broker)
		sub, err := subscriber.Subscribe("multi_pub_multi_sub", WithChannelSize[int](Medium))
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
		_ = sub.Close()
	}

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
	for i, sub := range subs {
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
			t.Fatalf("subscriber %d: expected %d messages, received %d", i+1, totalMessages, received)
		}
	}
}

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
	assert.ErrorIs(t, err, SubscriptionCapacityExceed)
}
