package pubsub

import (
	"log/slog"
	"os"
	"sync"
	"testing"
)

// / BenchmarkPublishSingleSubscriber 基准测试：单个订阅者下的发布性能
func BenchmarkPublishSingleSubscriber(b *testing.B) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	broker, _ := NewBroker[int](WithLogger[int](logger))
	publisher := NewPublisher(broker)
	subscriber := NewSubscriber[int](broker)

	sub, err := subscriber.Subscribe("benchmark_topic")
	if err != nil {
		b.Fatal(err)
	}
	defer func(sub *Subscription[int]) {
		_ = sub.Close()
	}(sub)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := publisher.Publish("benchmark_topic", i); err != nil {
			b.Fatal(err)
		}
		// 清空通道，避免消息积压
		select {
		case <-sub.Ch:
		default:
		}
	}
}

// BenchmarkMultipleSubscribers 基准测试：多个订阅者下的发布性能
func BenchmarkMultipleSubscribers(b *testing.B) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	broker, _ := NewBroker[int](WithLogger[int](logger))
	publisher := NewPublisher(broker)
	numSubscribers := 100

	subs := make([]*Subscription[int], 0, numSubscribers)

	// 创建多个订阅者
	for i := 0; i < numSubscribers; i++ {
		s := NewSubscriber[int](broker)
		sub, err := s.Subscribe("multi_bench")
		if err != nil {
			b.Fatal(err)
		}
		subs = append(subs, sub)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := publisher.Publish("multi_bench", i); err != nil {
			b.Fatal(err)
		}
		// 尝试从每个订阅中读取消息
		for _, sub := range subs {
			select {
			case <-sub.Ch:
			default:
			}
		}
	}
	b.StopTimer()
	// 清理所有订阅
	var wg sync.WaitGroup
	for _, sub := range subs {
		wg.Add(1)
		go func(s *Subscription[int]) {
			defer wg.Done()
			_ = s.Close()
		}(sub)
	}
	wg.Wait()
}

// BenchmarkMultiPublisherSingleSubscriber 基准测试：多个发布者下单个订阅者的发布性能
func BenchmarkMultiPublisherSingleSubscriber(b *testing.B) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	broker, _ := NewBroker[int](WithLogger[int](logger))
	numPublishers := 5
	var publishers []*Publisher[int]
	for i := 0; i < numPublishers; i++ {
		publishers = append(publishers, NewPublisher(broker))
	}
	subscriber := NewSubscriber[int](broker)
	sub, err := subscriber.Subscribe("bench_multi_pub_single_sub", WithChannelSize[int](Medium))
	if err != nil {
		b.Fatal(err)
	}
	defer func(sub *Subscription[int]) {
		_ = sub.Close()
	}(sub)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for _, pub := range publishers {
			wg.Add(1)
			go func(p *Publisher[int]) {
				defer wg.Done()
				if err := p.Publish("bench_multi_pub_single_sub", i); err != nil {
					b.Error(err)
					return
				}
			}(pub)
		}
		wg.Wait()
		// 清空订阅通道
		select {
		case <-sub.Ch:
		default:
		}
	}
}

// BenchmarkMultiPublisherMultipleSubscribers 基准测试：多个发布者下多个订阅者的发布性能
func BenchmarkMultiPublisherMultipleSubscribers(b *testing.B) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	broker, _ := NewBroker[int](WithLogger[int](logger))
	numPublishers := 5
	var publishers []*Publisher[int]
	for i := 0; i < numPublishers; i++ {
		publishers = append(publishers, NewPublisher(broker))
	}

	numSubscribers := 50
	subs := make([]*Subscription[int], 0, numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		s := NewSubscriber[int](broker)
		sub, err := s.Subscribe("bench_multi_pub_multi_sub", WithChannelSize[int](Medium))
		if err != nil {
			b.Fatal(err)
		}
		subs = append(subs, sub)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for _, pub := range publishers {
			wg.Add(1)
			go func(p *Publisher[int]) {
				defer wg.Done()
				if err := p.Publish("bench_multi_pub_multi_sub", i); err != nil {
					b.Error(err)
					return
				}
			}(pub)
		}
		wg.Wait()
		// 从所有订阅者中尽量清空消息
		for _, sub := range subs {
			select {
			case <-sub.Ch:
			default:
			}
		}
	}
	b.StopTimer()
	var wg sync.WaitGroup
	for _, sub := range subs {
		wg.Add(1)
		go func(s *Subscription[int]) {
			defer wg.Done()
			_ = s.Close()
		}(sub)
	}
	wg.Wait()
}

func BenchmarkUltraLargeSubscribersSinglePublisher(b *testing.B) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	broker, err := NewBroker[int](WithLogger[int](logger))
	if err != nil {
		b.Fatal(err)
	}
	publisher := NewPublisher(broker)

	// 设置超大规模订阅者数量，例如 10,000 个
	numSubscribers := 10000
	subscribers := make([]*Subscription[int], 0, numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		// 使用默认或自定义的通道容量（例如 Medium）
		sub, err := NewSubscriber[int](broker).Subscribe("ultra_topic", WithChannelSize[int](Medium))
		if err != nil {
			b.Fatal(err)
		}
		subscribers = append(subscribers, sub)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := publisher.Publish("ultra_topic", i); err != nil {
			b.Fatal(err)
		}
		// 清空所有订阅者的消息通道，防止消息堆积影响基准测试
		for _, sub := range subscribers {
			select {
			case <-sub.Ch:
			default:
			}
		}
	}
	b.StopTimer()
	// 关闭所有订阅
	for _, sub := range subscribers {
		_ = sub.Close()
	}
}
