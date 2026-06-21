//go:build !race

package pubsub

import "testing"

// TestDeliverZeroAlloc 断言多订阅者场景下稳态 Publish 是零分配的。
//
// Finding: subscription.deliver 原本每次 Publish 都 make 一个 snapshot
// slice（N 个订阅者时分配 N×8 字节）；改用 sync.Pool 复用后热路径应零
// 分配。用 testing.AllocsPerRun 在 warmup 之后测稳态——它会先把 func 当
// warmup 跑一次，再跑 100 次取平均，正好隔离掉首次 createOrLoadSubscription
// 建表的一次性分配。
//
// 排空（range + select 接收）本身零分配，不影响测量。
//
// 本文件带 //go:build !race 约束：race detector 给每次内存读写插桩，会
// 给 sync.Pool / channel 操作引入额外分配，零分配断言在 -race 下不成立。
// deliver 的并发正确性由其它并发测试（如 TestMultiPublisher*）在 -race
// 下覆盖。
func TestDeliverZeroAlloc(t *testing.T) {
	broker, err := NewBroker[int](WithLogger[int](benchLogger()))
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	pub := NewPublisher(broker)

	const numSubs = 100
	subs := make([]*Subscription[int], 0, numSubs)
	for i := 0; i < numSubs; i++ {
		s, err := NewSubscriber[int](broker).Subscribe("zero_alloc_topic")
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		subs = append(subs, s)
	}
	t.Cleanup(func() {
		for _, s := range subs {
			_ = s.Close()
		}
	})

	// warmup：首次 Publish 会 createOrLoadSubscription（建 topic + 每个
	// subscriber 的 channel），那一次有分配；稳态测的是后续 Publish。
	if err := pub.Publish("zero_alloc_topic", 1); err != nil {
		t.Fatalf("warmup Publish: %v", err)
	}

	allocs := testing.AllocsPerRun(100, func() {
		_ = pub.Publish("zero_alloc_topic", 1)
		// 排空每个订阅者的 per-topic channel，避免下一轮因 buffer 满（Medium=100）
		// 走 drop 分支；drop 本身不分配，但保持稳态干净。
		for _, s := range subs {
			select {
			case <-s.Ch:
			default:
			}
		}
	})

	if allocs != 0 {
		t.Fatalf("expected 0 allocs/op on multi-subscriber Publish, got %v", allocs)
	}
}
