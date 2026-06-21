package pubsub

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentPublishWithDynamicSubscribe 在持续 Publish 同一 topic 的同时，
// 多个 goroutine 反复 Subscribe 再 Close 该 topic。验证 subscription.subscribers
// map 的并发增删与 deliver 的持锁 send / unsubscribe 的持锁 close 互斥——
// 不死锁、不 panic、不竞态。
//
// 这个测试是为 send-on-closed-channel race 修复立的安全网：在 race detector 下
// 必须稳定绿。
//
// 无显式断言：测试通过 = (1) wg.Wait() 归零（无死锁）+ (2) 无 goroutine panic
// + (3) race 检测无报告。这三者覆盖了 fan-out send 与 close 的交错窗口。
func TestConcurrentPublishWithDynamicSubscribe(t *testing.T) {
	broker, _ := NewBroker[int](WithLogger[int](testLogger()))
	pub := NewPublisher(broker)
	const topic = "dynamic_topic"

	// 常驻订阅者：让 topic 始终存活，publisher 不会在并发中途反复走创建路径。
	persistent, err := NewSubscriber[int](broker).Subscribe(topic)
	if err != nil {
		t.Fatalf("persistent Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = persistent.Close() })

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var subErrs atomic.Int32 // 记 Subscribe 失败：订阅已存在的 topic 不该失败，失败即 bug

	// 持续发布者
	const numPub = 4
	for p := 0; p < numPub; p++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			v := seed
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = pub.Publish(topic, v)
				v++
			}
		}(p)
	}

	// 动态订阅者：反复 Subscribe → 收几条 → Close，给 deliver 制造并发增删
	const numDynamic = 8
	for d := 0; d < numDynamic; d++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 40; j++ {
				sub, err := NewSubscriber[int](broker).Subscribe(topic)
				if err != nil {
					// 订阅已存在的 topic 不该失败；记下来，别静默早退让测试假绿
					subErrs.Add(1)
					continue
				}
				// 短暂收一点，制造 snapshot 与 send 的重叠窗口
				deadline := time.After(2 * time.Millisecond)
			drain:
				for {
					select {
					case _, ok := <-sub.Ch:
						if !ok {
							break drain
						}
					case <-deadline:
						break drain
					}
				}
				_ = sub.Close()
				runtime.Gosched()
			}
		}()
	}

	// 常驻订阅者排空，避免其 channel 长期满（Medium=100）造成持续 drop 噪音
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-persistent.Ch:
			}
		}
	}()

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()

	if n := subErrs.Load(); n > 0 {
		t.Errorf("unexpected Subscribe failures: %d (subscribing to an existing topic should not fail)", n)
	}
}
