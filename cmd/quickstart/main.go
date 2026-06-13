// Command quickstart is the runnable end-to-end example for the
// go-pubsub public surface. It wires up a broker with custom id and
// capacity, validates the option error paths, fans three topics across
// two subscribers, demonstrates the sliding-timeout contract, exercises
// every exported sentinel, and introspects the broker on exit.
//
// Run from the repo root:
//
//	go run ./cmd/quickstart
package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/F2077/go-pubsub/pubsub"
)

func main() {
	// 顶层把 run() 包起来，让所有错误都走 stderr + 非零退出码；
	// 不让 panic 混在正常 stdout 文本里。
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "quickstart:", err)
		os.Exit(1)
	}
}

// run 把整个端到端走完拆成 13 个 phase。phase 之间通过共享变量串联，
// 但每个 phase 内部都是自包含的，可以单独阅读。
func run() error {
	// 把 broker 的锁跟踪日志压到 Warn 级别，避免一次 happy-path
	// 跑下来刷一堆读/写锁 acquired/released 噪声。
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))

	// === Phase 1: 构造两个 broker =====================================
	// 主流程用 mainBroker，cap 走默认 + 显式 WithId。
	// tinyBroker (cap=2) 仅用于 phase 8 触发容量错误，不参与消息流。
	mainBroker, err := pubsub.NewBroker[string](
		pubsub.WithLogger[string](logger),
		pubsub.WithId[string]("alerts-router"),
		pubsub.WithCapacity[string](pubsub.DefaultCapacity),
	)
	if err != nil {
		return fmt.Errorf("new main broker: %w", err)
	}
	tinyBroker, err := pubsub.NewBroker[string](pubsub.WithCapacity[string](2))
	if err != nil {
		return fmt.Errorf("new tiny broker: %w", err)
	}

	fmt.Println("=== 1. Brokers ===")
	fmt.Println("main :", mainBroker) // 走 String()
	fmt.Println("tiny :", tinyBroker)
	fmt.Println("main.Capacity()      :", mainBroker.Capacity())
	fmt.Println("pubsub.DefaultCapacity :", pubsub.DefaultCapacity)

	// === Phase 2: BrokerOption 校验错误路径 =============================
	// WithLogger / WithId 在非法输入下返回 sentinel；用 errors.Is 区分。
	fmt.Println("\n=== 2. BrokerOption validation (errors.Is) ===")
	if _, err := pubsub.NewBroker[string](pubsub.WithLogger[string](nil)); !errors.Is(err, pubsub.ErrLoggerNil) {
		return fmt.Errorf("WithLogger(nil): expected ErrLoggerNil, got %v", err)
	}
	fmt.Println("  WithLogger(nil)  ->", pubsub.ErrLoggerNil)

	if _, err := pubsub.NewBroker[string](pubsub.WithId[string]("")); !errors.Is(err, pubsub.ErrBrokerIdEmpty) {
		return fmt.Errorf("WithId(\"\"): expected ErrBrokerIdEmpty, got %v", err)
	}
	fmt.Println("  WithId(\"\")      ->", pubsub.ErrBrokerIdEmpty)

	// === Phase 3: 构造 publisher / subscriber handle ====================
	// 两个 publisher 共用 mainBroker，两个 subscriber 同样共用。
	pubA := pubsub.NewPublisher[string](mainBroker)
	pubB := pubsub.NewPublisher[string](mainBroker)
	subA := pubsub.NewSubscriber[string](mainBroker)
	subB := pubsub.NewSubscriber[string](mainBroker)

	fmt.Println("\n=== 3. Publisher / Subscriber handles ===")
	fmt.Println("  pubA :", pubA)
	fmt.Println("  pubB :", pubB, " id=", pubB.Id())
	fmt.Println("  subA :", subA)
	fmt.Println("  subB :", subB, " id=", subB.Id())

	// === Phase 4: subA 用 Subscribes 一次订 3 个 topic =================
	// WithChannelSize(Medium) 给所有 3 个 sub 用同一个 100 槽缓冲；
	// 没传 WithTimeout，ErrCh 应该是 nil（lazy alloc 合约）。
	subATopics := []string{"metrics", "audit", "alerts"}
	subsA, err := subA.Subscribes(
		subATopics,
		pubsub.WithChannelSize[string](pubsub.Medium),
	)
	if err != nil {
		return fmt.Errorf("subA.Subscribes: %w", err)
	}

	fmt.Println("\n=== 4. subA.Subscribes(metrics, audit, alerts) ===")
	for i, s := range subsA {
		// OnClose 是 quickstart 拿到 topic 名字的唯一路径（Subscription
		// 没暴露 Topic()），所以同时充当 "topic 名称回显" + 关闭回调。
		s.OnClose = func(topic string) {
			fmt.Printf("  [OnClose] subA sub#%d topic=%q\n", i, topic)
		}
		fmt.Printf("  sub#%d topic=%-8s Ch cap=%d ErrCh==nil? %v\n",
			i, subATopics[i], cap(s.Ch), s.ErrCh == nil)
	}

	// === Phase 5: subB 单独订 "alerts" + Block + 滑动超时 ==============
	// 400ms 的短超时让我们能在一个 run() 内就等到 timeout 真的 fire，
	// 不至于让 quickstart 拖到分钟级。
	subBsub, err := subB.Subscribe(
		"alerts",
		pubsub.WithChannelSize[string](pubsub.Block),
		pubsub.WithTimeout[string](400*time.Millisecond),
	)
	if err != nil {
		return fmt.Errorf("subB.Subscribe: %w", err)
	}
	subBsub.OnClose = func(topic string) {
		fmt.Printf("  [OnClose] subBsub closing topic=%q\n", topic)
	}

	fmt.Println("\n=== 5. subB.Subscribe(alerts, Block, 400ms timeout) ===")
	fmt.Printf("  subBsub.Ch cap=%d (Block=0 means unbuffered)\n", cap(subBsub.Ch))
	fmt.Printf("  subBsub.ErrCh==nil? %v (timeout set => non-nil)\n", subBsub.ErrCh == nil)

	// === Phase 6: 起 4 个 drain goroutine 各自消费一条 sub ============
	// 收尾时通过 done channel 等齐。subA 三个 drain 等 Ch 关闭；
	// subBsub 的 drain 同时监听 Ch + ErrCh（任一边关闭即退出）。
	fmt.Println("\n=== 6. Drain goroutines (4× spawned) ===")
	dones := []<-chan struct{}{
		drainSubscription("subA:metrics", subsA[0], nil),
		drainSubscription("subA:audit", subsA[1], nil),
		drainSubscription("subA:alerts", subsA[2], nil),
		drainSubscription("subB:alerts", subBsub, subBsub.ErrCh),
	}
	fmt.Println("  4 drainers running; will be joined in Phase 12")

	// === Phase 7: Publish 突发流量 ====================================
	fmt.Println("\n=== 7. Publish burst ===")
	for i := 0; i < 5; i++ {
		_ = pubA.Publish("metrics", fmt.Sprintf("m=%d", i))
		_ = pubA.Publish("audit", fmt.Sprintf("a=%d", i))
		_ = pubB.Publish("alerts", fmt.Sprintf("alert=%d", i))
	}
	fmt.Println("  15 messages dispatched (5 per topic × 3 topics)")

	// === Phase 8: 容量错误（tiny broker，cap=2）=========================
	fmt.Println("\n=== 8. ErrSubscriptionCapacityExceeded (tiny broker, cap=2) ===")
	pubT := pubsub.NewPublisher[string](tinyBroker)
	subT := pubsub.NewSubscriber[string](tinyBroker)
	if err := pubT.Publish("t1", "x"); err != nil {
		return fmt.Errorf("Publish t1 should succeed: %w", err)
	}
	fmt.Println("  Publish(t1)    -> ok (1/2)")
	if err := pubT.Publish("t2", "x"); err != nil {
		return fmt.Errorf("Publish t2 should succeed: %w", err)
	}
	fmt.Println("  Publish(t2)    -> ok (2/2)")
	_, err = subT.Subscribe("t3")
	if !errors.Is(err, pubsub.ErrSubscriptionCapacityExceeded) {
		return fmt.Errorf("Subscribe t3: expected ErrSubscriptionCapacityExceeded, got %v", err)
	}
	fmt.Println("  Subscribe(t3)  ->", err)

	// === Phase 9: 等 subBsub 的滑动 timeout 自然 fire ===================
	// 上面最后一次 publish 之后 400ms，timer 就会 fire。
	// drain goroutine 看到 ErrSubscriptionTimeout 就会打印并退出。
	fmt.Println("\n=== 9. Sliding timeout (subBsub, 400ms) ===")
	time.Sleep(500 * time.Millisecond)

	// === Phase 10: ErrSubscriberClosed 路径 =============================
	// 这里分两条路：
	//  1) subBsub.Close() -> 走 OnClose + unsubscribe（topic=alerts 退出）
	//  2) subB.Close() 第一次 -> 所有 topic 都已经退订，no-op，nil
	//  3) subB.Close() 第二次 -> 返回 ErrSubscriberClosed
	//  4) subB.Subscribe(...) -> 返回 ErrSubscriberClosed
	fmt.Println("\n=== 10. ErrSubscriberClosed ===")
	if err := subBsub.Close(); err != nil {
		return fmt.Errorf("subBsub.Close: %w", err)
	}
	if err := subB.Close(); err != nil {
		return fmt.Errorf("first subB.Close should be a no-op, got %v", err)
	}
	if err := subB.Close(); !errors.Is(err, pubsub.ErrSubscriberClosed) {
		return fmt.Errorf("second subB.Close: expected ErrSubscriberClosed, got %v", err)
	}
	fmt.Println("  subBsub.Close()  -> OnClose fired above")
	fmt.Println("  subB.Close() 1st -> nil (no topics left)")
	fmt.Println("  subB.Close() 2nd ->", pubsub.ErrSubscriberClosed)
	if _, err := subB.Subscribe("after-close"); !errors.Is(err, pubsub.ErrSubscriberClosed) {
		return fmt.Errorf("Subscribe after Close: expected ErrSubscriberClosed, got %v", err)
	}
	fmt.Println("  subB.Subscribe()  ->", pubsub.ErrSubscriberClosed)

	// === Phase 11: 逐个关 subA 的 sub，触发 OnClose × 3 ================
	// Subscriber.Close 不会触发 OnClose（设计上 OnClose 只挂在
	// Subscription.Close 上），所以要拿到 3 个 OnClose 回调只能
	// 显式 sub.Close() 三个。
	fmt.Println("\n=== 11. OnClose × 3 (subA's subscriptions) ===")
	for _, s := range subsA {
		if err := s.Close(); err != nil {
			return fmt.Errorf("subA sub.Close: %w", err)
		}
	}
	// 三个 topic 都已退订，subA.Close() 这里是 no-op。再次调一次
	// 演示 Subscriber.Close 也走 ErrSubscriberClosed 这条路。
	if err := subA.Close(); err != nil {
		return fmt.Errorf("subA.Close (after per-sub close) should be no-op, got %v", err)
	}
	if err := subA.Close(); !errors.Is(err, pubsub.ErrSubscriberClosed) {
		return fmt.Errorf("second subA.Close: expected ErrSubscriberClosed, got %v", err)
	}

	// === Phase 12: 等所有 drain goroutine 退出 =========================
	fmt.Println("\n=== 12. Drain summary ===")
	for _, done := range dones {
		<-done
	}
	fmt.Println("  all drainers exited")

	// === Phase 13: 退出前 introspect broker ============================
	// 异步 topic reaping：topic 最后一个订阅者退订后，broker 在独立
	// goroutine 里才回收，所以 Topics() 看到的可能是 0~3 的中间态。
	// 最坏情况：3 个 topic 都还在（reaping 还没跑）；最好情况：0
	// （reaping 跑得比 Close 完还快）。实测通常落在 0~1。
	fmt.Println("\n=== 13. Broker.Topics() at exit ===")
	topics := mainBroker.Topics()
	fmt.Printf("  %d topic(s) still tracked: %v\n", len(topics), topics)

	fmt.Println("\nquickstart: ok")
	return nil
}

// drainSubscription 消费一条 *Subscription 直到 Ch 关闭，或者
// errCh 给出错误（subBsub 走 timeout 路径会触发此分支）。
// 返回的 channel 在 drain 退出时关闭，让调用方能 join。
func drainSubscription[T any](label string, sub *pubsub.Subscription[T], errCh <-chan error) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		if errCh == nil {
			// 没传 ErrCh：单纯排空 Ch。subA 的 3 个 sub 走这条。
			for range sub.Ch {
				// 静默排空；输出让 phase 7 那种 burst 自证
			}
			return
		}
		// 同时监听 Ch + ErrCh，任一边关闭 / 给出错误即退出。
		for {
			select {
			case _, ok := <-sub.Ch:
				if !ok {
					return
				}
			case err, ok := <-errCh:
				if !ok {
					return
				}
				if errors.Is(err, pubsub.ErrSubscriptionTimeout) {
					fmt.Printf("  [timeout] %s: %v\n", label, err)
					return
				}
				fmt.Printf("  [error]   %s: %v\n", label, err)
				return
			}
		}
	}()
	return done
}
