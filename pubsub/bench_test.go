package pubsub

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

// BenchmarkPublishSingleSubscriber 基准测试：单个订阅者下的发布性能
func BenchmarkPublishSingleSubscriber(b *testing.B) {
	broker, _ := NewBroker[int](WithLogger[int](benchLogger()))
	pub := NewPublisher(broker)
	sub, _ := NewSubscriber[int](broker).Subscribe("benchmark_topic")
	defer func(sub *Subscription[int]) {
		_ = sub.Close()
	}(sub)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := pub.Publish("benchmark_topic", i); err != nil {
			b.Fatal(err)
		}
		// 读取上一次消息，防止堆积
		select {
		case <-sub.Ch:
		default:
		}
	}
	b.ReportAllocs()
}

// BenchmarkMultipleSubscribers 基准测试：多个订阅者下的发布性能
func BenchmarkMultipleSubscribers(b *testing.B) {
	broker, _ := NewBroker[int](WithLogger[int](benchLogger()))
	pub := NewPublisher(broker)
	const numSubs = 100

	// 准备多个订阅者
	subs := make([]*Subscription[int], 0, numSubs)
	for i := 0; i < numSubs; i++ {
		sub, _ := NewSubscriber[int](broker).Subscribe("multi_bench")
		subs = append(subs, sub)
	}
	// 测试结束后再统一关闭
	b.Cleanup(func() {
		for _, s := range subs {
			_ = s.Close()
		}
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := pub.Publish("multi_bench", i); err != nil {
			b.Fatal(err)
		}
		// 从每个订阅者通道读取一次
		for _, s := range subs {
			select {
			case <-s.Ch:
			default:
			}
		}
	}
	b.ReportAllocs()
}

// BenchmarkMultiPublisherSingleSubscriber 基准测试：多个发布者对单个订阅者的发布性能
func BenchmarkMultiPublisherSingleSubscriber(b *testing.B) {
	broker, _ := NewBroker[int](WithLogger[int](benchLogger()))
	const numPubs = 5

	// 多个发布者
	pubs := make([]*Publisher[int], 0, numPubs)
	for i := 0; i < numPubs; i++ {
		pubs = append(pubs, NewPublisher(broker))
	}
	sub, _ := NewSubscriber[int](broker).
		Subscribe("bench_multi_pub_single_sub", WithChannelSize[int](Medium))
	defer func(sub *Subscription[int]) {
		_ = sub.Close()
	}(sub)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for _, p := range pubs {
			wg.Add(1)
			go func(pub *Publisher[int]) {
				defer wg.Done()
				if err := pub.Publish("bench_multi_pub_single_sub", i); err != nil {
					b.Error(err)
				}
			}(p)
		}
		wg.Wait()
		// 清空订阅通道
		select {
		case <-sub.Ch:
		default:
		}
	}
	b.ReportAllocs()
}

// BenchmarkMultiPublisherMultipleSubscribers 基准测试：多个发布者和多个订阅者
func BenchmarkMultiPublisherMultipleSubscribers(b *testing.B) {
	broker, _ := NewBroker[int](WithLogger[int](benchLogger()))
	const (
		numPubs = 5
		numSubs = 50
	)

	// 发布者列表
	pubs := make([]*Publisher[int], 0, numPubs)
	for i := 0; i < numPubs; i++ {
		pubs = append(pubs, NewPublisher(broker))
	}

	// 订阅者列表
	subs := make([]*Subscription[int], 0, numSubs)
	for i := 0; i < numSubs; i++ {
		sub, _ := NewSubscriber[int](broker).
			Subscribe("bench_multi_pub_multi_sub", WithChannelSize[int](Medium))
		subs = append(subs, sub)
	}
	b.Cleanup(func() {
		for _, s := range subs {
			_ = s.Close()
		}
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for _, p := range pubs {
			wg.Add(1)
			go func(pub *Publisher[int]) {
				defer wg.Done()
				if err := pub.Publish("bench_multi_pub_multi_sub", i); err != nil {
					b.Error(err)
				}
			}(p)
		}
		wg.Wait()
		// 清空每个订阅者的通道
		for _, s := range subs {
			select {
			case <-s.Ch:
			default:
			}
		}
	}
	b.ReportAllocs()
}

// BenchmarkUltraLargeSubscribersSinglePublisher 基准测试：超大规模订阅者场景
func BenchmarkUltraLargeSubscribersSinglePublisher(b *testing.B) {
	broker, _ := NewBroker[int](WithLogger[int](benchLogger()))
	pub := NewPublisher(broker)

	const numSubs = 10000
	subs := make([]*Subscription[int], 0, numSubs)
	for i := 0; i < numSubs; i++ {
		sub, _ := NewSubscriber[int](broker).
			Subscribe("ultra_topic", WithChannelSize[int](Medium))
		subs = append(subs, sub)
	}
	b.Cleanup(func() {
		for _, s := range subs {
			_ = s.Close()
		}
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := pub.Publish("ultra_topic", i); err != nil {
			b.Fatal(err)
		}
		for _, s := range subs {
			select {
			case <-s.Ch:
			default:
			}
		}
	}
	b.ReportAllocs()
}

// BenchmarkPublishChannelSizes 表驱动基准：不同通道容量对性能影响
func BenchmarkPublishChannelSizes(b *testing.B) {
	cases := []struct {
		name string
		size ChannelSize
	}{
		{"Small", Small},
		{"Medium", Medium},
		{"Large", Large},
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			broker, _ := NewBroker[int](WithLogger[int](benchLogger()))
			pub := NewPublisher(broker)
			sub, _ := NewSubscriber[int](broker).
				Subscribe("size_bench", WithChannelSize[int](c.size))
			defer func(sub *Subscription[int]) {
				_ = sub.Close()
			}(sub)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := pub.Publish("size_bench", i); err != nil {
					b.Fatal(err)
				}
				<-sub.Ch
			}
			b.ReportAllocs()
		})
	}
}

// BenchmarkPublishWithTimeout 测试 Publishing + Timeout 场景
func BenchmarkPublishWithTimeout(b *testing.B) {
	broker, _ := NewBroker[string](WithLogger[string](benchLogger()))
	pub := NewPublisher(broker)
	sub, _ := NewSubscriber[string](broker).
		Subscribe("timeout_bench", WithTimeout[string](50*time.Millisecond))
	defer func(sub *Subscription[string]) {
		_ = sub.Close()
	}(sub)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := pub.Publish("timeout_bench", "msg"); err != nil {
			b.Fatal(err)
		}
		select {
		case <-sub.Ch:
		case <-time.After(10 * time.Millisecond):
		}
	}
	b.ReportAllocs()
}

// BenchmarkHighLoadParallel 并行高负载测试：大量并发发布
func BenchmarkHighLoadParallel(b *testing.B) {
	broker, _ := NewBroker[int](WithLogger[int](benchLogger()))
	publisher := NewPublisher(broker)

	// 创建超大数量订阅者，确保每个发布都能被消费
	const numSubs = 10000
	subs := make([]*Subscription[int], 0, numSubs)
	for i := 0; i < numSubs; i++ {
		sub, _ := NewSubscriber[int](broker).
			Subscribe("highload_topic", WithChannelSize[int](Medium))
		subs = append(subs, sub)
	}
	b.Cleanup(func() {
		for _, s := range subs {
			_ = s.Close()
		}
	})

	// 设置并行度：每个 P 启动 10 个 goroutine
	parallelFactor := 10
	procs := runtime.GOMAXPROCS(0)
	b.SetParallelism(parallelFactor)
	b.ReportAllocs()

	b.ResetTimer()

	// 独立 drain goroutine 持续抽干 10000 个订阅者 channel,让 publish
	// 路径不会因为 channel 满而大量丢消息;drain 速率远高于 publish 速率,
	// 稳态下 buffer 几乎总是空的(满了立即被 drain 抽走),bench 测的是
	// "10000 订阅者下,publish 路径"的吞吐。
	drainDone := make(chan struct{})
	go func() {
		for {
			for _, s := range subs {
				select {
				case <-s.Ch:
				case <-drainDone:
					return
				}
			}
		}
	}()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := publisher.Publish("highload_topic", 123); err != nil {
				b.Error(err)
			}
		}
	})

	b.StopTimer()
	close(drainDone)

	b.StopTimer()
	b.Logf("GOMAXPROCS=%d, Parallelism=%d, Subscribers=%d",
		procs, procs*parallelFactor, len(subs))
}

// BenchmarkSubscribes measures the cost of subscribing one subscriber to
// many topics in a single Subscribes call.
func BenchmarkSubscribes(b *testing.B) {
	broker, _ := NewBroker[int](WithLogger[int](benchLogger()))
	sub := NewSubscriber[int](broker)

	const topicCount = 50
	topics := make([]string, topicCount)
	for i := range topics {
		topics[i] = fmt.Sprintf("topic_%d", i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		subs, err := sub.Subscribes(topics)
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		for _, s := range subs {
			_ = s.Close()
		}
		b.StartTimer()
	}
	b.ReportAllocs()
}

// BenchmarkBrokerTopics measures the cost of the Topics() introspection
// helper as the number of live subscriptions grows.
func BenchmarkBrokerTopics(b *testing.B) {
	cases := []struct {
		name      string
		numTopics int
	}{
		{"10", 10},
		{"100", 100},
		{"1000", 1000},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			broker, _ := NewBroker[int](WithLogger[int](benchLogger()))
			sub2 := NewSubscriber[int](broker)
			subs := make([]*Subscription[int], 0, c.numTopics)
			for j := 0; j < c.numTopics; j++ {
				s, _ := sub2.Subscribe(fmt.Sprintf("u_%d", j))
				subs = append(subs, s)
			}
			b.Cleanup(func() {
				for _, s := range subs {
					_ = s.Close()
				}
			})

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = broker.Topics()
			}
			b.ReportAllocs()
		})
	}
}

// benchPacket is a small struct used to confirm the generic T parameter
// is exercised with a non-primitive payload in benchmarks.
type benchPacket struct {
	seq  int
	body string
}

// BenchmarkStructPayload measures single-publisher / single-subscriber
// throughput with a struct payload (vs. the int payloads used elsewhere).
func BenchmarkStructPayload(b *testing.B) {
	broker, _ := NewBroker[benchPacket](WithLogger[benchPacket](benchLogger()))
	pub := NewPublisher[benchPacket](broker)
	sub, _ := NewSubscriber[benchPacket](broker).Subscribe("struct_topic")
	defer func(sub *Subscription[benchPacket]) { _ = sub.Close() }(sub)

	pkt := benchPacket{seq: 1, body: "payload"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := pub.Publish("struct_topic", pkt); err != nil {
			b.Fatal(err)
		}
		select {
		case <-sub.Ch:
		default:
		}
	}
	b.ReportAllocs()
}

// BenchmarkPublishAutoCreateTopic measures the cost of publishing to a
// topic that does not yet exist on the broker. This exercises the
// createOrLoadSubscription fast path (read-lock lookup, no write).
func BenchmarkPublishAutoCreateTopic(b *testing.B) {
	broker, _ := NewBroker[int](WithCapacity[int](uint32(b.N)+16), WithLogger[int](benchLogger()))
	pub := NewPublisher[int](broker)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topic := fmt.Sprintf("auto_%d", i)
		if err := pub.Publish(topic, i); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
}
