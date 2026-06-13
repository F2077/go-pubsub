package pubsub

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ErrSubscriberClosed is returned by Subscribe, Subscribes, and Close on a
// Subscriber that has already been Closed.
var ErrSubscriberClosed = errors.New("subscriber is closed")

// ErrSubscriptionTimeout is delivered to Subscription.ErrCh when a Subscribe
// call with WithTimeout receives no Publish within the configured idle
// period. A successful Publish resets the sliding timer.
var ErrSubscriptionTimeout = errors.New("subscription timeout")

// ChannelSize is the per-topic buffer depth for a subscription. The
// pre-defined Block / Single / Small / Medium / Large / Huge constants
// cover the common cases; any non-negative uint16 is accepted.
type ChannelSize uint16

// Pre-defined ChannelSize values. The Medium default applies when
// Subscribe is called without WithChannelSize.
var (
	Block  ChannelSize = 0 // Block makes the per-topic channel unbuffered: Publishes block until the subscriber receives.
	Single ChannelSize = 1 // Single makes the per-topic channel buffered with capacity 1.
	Small              = Single * 10
	Medium             = Small * 10 // Medium is the default channel size (100).
	Large              = Medium * 10
	Huge               = Large * 10
)

// Defaults applied when Subscribe is called without the corresponding
// SubscriptionOption. Use Medium by default; timeouts are disabled unless
// the caller passes WithTimeout with a positive duration.
var (
	DefaultChannelSize = Medium
	DefaultTimeout     = 0 * time.Second
)

// Subscriber receives messages on topics it has subscribed to. Each
// Subscriber has its own UUID and is bound to exactly one Broker.
//
// The zero value is not usable; construct with NewSubscriber.
//
// topicTimer 是一条订阅的滑动超时相关状态的封装：常驻 *time.Timer、
// 该 topic 的 timeout 长度、fire goroutine 的退出信号、用于停掉 fire
// goroutine 的 done 通道。原本 4 个 map 按 topic 平铺，Subscribe /
// unsubscribe 各要查 4 次；合成 1 个 struct 之后只查 1 次。
type topicTimer struct {
	t       *time.Timer
	timeout time.Duration
	done    chan struct{} // 关闭此通道让 fire goroutine 从 select 醒来
	exit    chan struct{} // fire goroutine 退出时 close，unsubscribe 等它
}

// Subscriber is safe for concurrent use by multiple goroutines.
type Subscriber[T any] struct {
	mutex sync.Mutex

	id       string
	broker   *Broker[T]
	topics   map[string]struct{}
	subs     map[*Subscription[T]]struct{} // 所有存活的 *Subscription；Close 时按此迭代
	channels sync.Map
	closed   bool

	timers map[string]*topicTimer
}

// NewSubscriber creates a Subscriber bound to the given broker, with its
// own UUID and an empty topic set. Use Subscribe to attach to topics.
func NewSubscriber[T any](broker *Broker[T]) *Subscriber[T] {
	return &Subscriber[T]{
		id:      uuid.New().String(),
		broker:  broker,
		topics:  map[string]struct{}{},
		subs:    map[*Subscription[T]]struct{}{},
		timers:  make(map[string]*topicTimer),
	}
}

// String returns the subscriber's debug form: SUBSCRIBER#<id>@<broker>.
// Implements fmt.Stringer.
func (s *Subscriber[T]) String() string {
	return fmt.Sprintf("SUBSCRIBER#%s@%s", s.id, s.broker)
}

// Id returns the subscriber's UUID.
func (s *Subscriber[T]) Id() string {
	return s.id
}

// Subscribe attaches to topic and returns a *Subscription whose Ch yields
// every message published to that topic. Delivery is non-blocking: a
// message is dropped for a subscriber whose per-topic channel is full.
//
// Returns ErrSubscriberClosed if the subscriber is already closed, or a
// wrapped ErrSubscriptionCapacityExceeded if subscribing would push the
// broker over its capacity. Note that even on capacity error, any topics
// that were successfully subscribed earlier in the call remain attached.
func (s *Subscriber[T]) Subscribe(topic string, opts ...SubscriptionOption[T]) (*Subscription[T], error) {
	options := &subscriptionOptions[T]{
		size:    DefaultChannelSize,
		timeout: DefaultTimeout,
	}
	for _, opt := range opts {
		opt(options)
	}

	s.mutex.Lock()
	if s.broker.logger.Enabled(context.TODO(), slog.LevelDebug) {
		s.broker.logger.Debug("Subscriber.Subscribe acquired lock", slog.Any("subscriber", s))
	}
	defer func() {
		s.mutex.Unlock()
		if s.broker.logger.Enabled(context.TODO(), slog.LevelDebug) {
			s.broker.logger.Debug("Subscriber.Subscribe released lock", slog.Any("subscriber", s))
		}
	}()

	if s.closed {
		return nil, ErrSubscriberClosed
	}

	// 获取或创建主题对应的订阅
	sub, err := s.broker.createOrLoadSubscription(topic)
	if err != nil {
		return nil, err
	}

	// 创建对应主题的消息通道
	ch, _ := s.channels.LoadOrStore(topic, make(chan T, options.size))

	// 每条 *Subscription 拥有自己的 errCh。无 timeout 时为 nil——
	// receive-only nil channel 永久阻塞，对不可能超时的订阅者是正确行为。
	// 注意：errCh 不再共享给同 topic 的其他 *Subscription，避免一个 sub
	// 的 Close 误关另一条 sub 的 ErrCh。
	var errCh chan error
	if options.timeout > 0 {
		errCh = make(chan error, 1)
	}

	// 将当前订阅者添加到订阅
	sub.addSubscriber(s)
	// 将主题添加到订阅者主题集合
	s.topics[topic] = struct{}{}

	// 把将要返回的 *Subscription 注册到 s.subs，让 Subscriber.Close 找得到。
	// 注意在 defer Unlock 之前必须先构造好 ret，否则 s.subs 拿不到 ret 的指针。
	ret := &Subscription[T]{
		topic:      topic,
		subscriber: s,
		Ch:         ch.(chan T),
		ErrCh:      errCh,
		errCh:      errCh,
	}
	s.subs[ret] = struct{}{}

	// 设置超时逻辑（仅当 timeout > 0 时）
	if options.timeout > 0 {
		// 停掉旧的 timer + 旧 fire goroutine（re-Subscribe 同 topic 的情况）。
		// close(tt.done) 让旧 fire goroutine 从 select 醒来并退出。<-tt.exit
		// 等它真的退完，避免后续 unsubscribe 阶段在它还活着时关 errCh。
		if oldTT, ok := s.timers[topic]; ok {
			close(oldTT.done)
			<-oldTT.exit
			oldTT.t.Stop()
			delete(s.timers, topic)
		}

		// 创建常驻 timer + 专用的 fire goroutine
		if s.broker.logger.Enabled(context.TODO(), slog.LevelDebug) {
			s.broker.logger.Debug("subscription timeout configured",
				slog.Any("topic", topic),
				slog.Any("timeout", options.timeout),
			)
		}
		s.timers[topic] = &topicTimer{
			t:       time.NewTimer(options.timeout),
			timeout: options.timeout,
			done:    make(chan struct{}),
			exit:    make(chan struct{}),
		}
		tt := s.timers[topic]
		go s.runTopicTimer(topic, tt, ret.errCh)
	}

	return ret, nil
}

// Subscribes attaches to every topic in topics in one call and returns a
// *Subscription per topic, in the same order. If any Subscribe call
// fails, the error is returned and any subscriptions that succeeded
// earlier in the iteration remain attached (partial-failure semantics).
func (s *Subscriber[T]) Subscribes(topics []string, opts ...SubscriptionOption[T]) ([]*Subscription[T], error) {
	if s.closed {
		return nil, ErrSubscriberClosed
	}

	subs := make([]*Subscription[T], len(topics))
	for i, topic := range topics {
		sub, err := s.Subscribe(topic, opts...)
		if err != nil {
			return nil, err
		}
		subs[i] = sub
	}
	return subs, nil
}

// Close unsubscribes from every topic the subscriber is on and marks it
// closed. Further Subscribe / Subscribes calls return ErrSubscriberClosed.
// Calling Close twice returns ErrSubscriberClosed on the second call.
func (s *Subscriber[T]) Close() error {
	if s.closed {
		return ErrSubscriberClosed
	}

	// 关闭意味着取消对于所有 topic 的订阅
	for sub := range s.subs {
		if err := s.unsubscribe(sub); err != nil {
			return err
		}
	}
	s.closed = true
	return nil
}

func (s *Subscriber[T]) unsubscribe(sub *Subscription[T]) error {
	s.mutex.Lock()
	if s.broker.logger.Enabled(context.TODO(), slog.LevelDebug) {
		s.broker.logger.Debug("Subscriber.unsubscribe acquired lock", slog.Any("subscriber", s))
	}
	defer func() {
		s.mutex.Unlock()
		if s.broker.logger.Enabled(context.TODO(), slog.LevelDebug) {
			s.broker.logger.Debug("Subscriber.unsubscribe released lock", slog.Any("subscriber", s))
		}
	}()

	// 幂等：这是本 unsubscribe 的唯一短路径。
	// - Subscriber.Close() 在循环结束之后才置 s.closed=true，所以循环期间
	//   s.closed 必为 false；
	// - 重复 Close 同一 sub 时，s.subs 里已经被前一次 unsubscribe 删掉了。
	// 老设计靠 s.errChannels.LoadAndDelete 的 ok==false 自然幂等；
	// 现在 errCh 挂在 sub 上，重复 close 会 panic，所以这里 short-circuit。
	if _, ok := s.subs[sub]; !ok {
		return nil
	}

	topic := sub.topic

	// 将自身从订阅中移除
	// 获取 Broker 的读锁以安全访问 subscriptions
	s.broker.rwMutex.RLock()
	bSub, ok := s.broker.subscriptions[topic]
	s.broker.rwMutex.RUnlock()

	if ok {
		bSub.removeSubscriber(s)
	}

	// 从 s.subs 注销本 sub；从 s.topics 注销 topic
	delete(s.subs, sub)
	delete(s.topics, topic)

	// 关闭对应主题的消息通道
	if ch, ok := s.channels.LoadAndDelete(topic); ok {
		close(ch.(chan T))
	}

	// 先停掉 fire goroutine：close(tt.done) 让它从 select 醒来。<-tt.exit
	// 确保它已经回到外层、不会再调 handleTimeout / 写 errCh——然后才能
	// 安全关闭本 sub 自己的 errCh（向已关闭的 channel 写入会 panic）。
	if tt, ok := s.timers[topic]; ok {
		close(tt.done)
		<-tt.exit
		delete(s.timers, topic)
	}

	// 关闭本 sub 自己的 errCh（cap 1，给 timeout 用的）。
	// 此时 fire goroutine 已退出，无人在写。
	if sub.errCh != nil {
		close(sub.errCh)
	}

	return nil
}

// runTopicTimer 在独立 goroutine 里循环读 t.C，每次 fire 调 handleTimeout。
// 收到 done 信号就退出，并通过 exit 通知 unsubscribe 自己已经退出。
// unsubscribe 会 <-exit 等这个信号，确保没人还在写 errCh，再 close(errCh)。
// 该 goroutine 的生命周期 == 该 topic 的订阅生命周期。
// 注意：<--done 之后不需要 tt.t.Stop()，因为 t.C 已无消费者、*topicTimer
// 在 unsubscribe 删掉 s.timers[topic] 之后整体不可达，会被 GC 回收。
func (s *Subscriber[T]) runTopicTimer(topic string, tt *topicTimer, errCh chan error) {
	defer close(tt.exit)
	for {
		select {
		case <-tt.t.C:
			s.handleTimeout(topic, errCh)
		case <-tt.done:
			return
		}
	}
}

// 重置定时器。Go 1.23+ 文档明确：Reset 之后的 receive 不可能再拿到旧
// duration 对应的时间值，所以无需先 Stop 再 drain。少一次 mutex 内的
// channel select，每次 Publish 也就少一次 hot-path 负担。
func (s *Subscriber[T]) resetTimer(topic string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if tt, ok := s.timers[topic]; ok && tt.timeout > 0 {
		tt.t.Reset(tt.timeout)
	}
}

func (s *Subscriber[T]) handleTimeout(topic string, errCh chan<- error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.broker.logger.Enabled(context.TODO(), slog.LevelDebug) {
		s.broker.logger.Debug("subscription timeout fired", slog.Any("topic", topic))
	}
	// 检查是否仍订阅该主题
	if _, ok := s.topics[topic]; !ok {
		return
	}

	// 用 non-blocking send：errCh 是 cap=1 的缓冲通道，如果调用方没在
	// 读而 timer 又多次 fire，阻塞 send 会把本 goroutine 卡在 s.mutex
	// 上，进而把后面任何想拿锁的 goroutine（deliver → resetTimer 也走
	// 这条路）一起拖死。直接 drop 这次 timeout 信号，调用方依然可以从
	// Ch 关闭 / OnClose 收到订阅结束的信号。
	select {
	case errCh <- ErrSubscriptionTimeout:
	default:
	}
}

// Subscription is the read-only handle returned by Subscribe. Ch receives
// messages published to the topic; ErrCh receives ErrSubscriptionTimeout
// when a Subscribe with WithTimeout receives no message within the
// configured idle period. If Subscribe was called without WithTimeout,
// ErrCh is nil — a receive from a nil channel blocks forever, which is
// the correct behavior for a subscription that cannot time out.
// Close releases the topic.
//
// Subscription is single-topic; create one Subscription per (subscriber,
// topic) pair.
type Subscription[T any] struct {
	topic      string
	subscriber *Subscriber[T]
	Ch         <-chan T
	ErrCh      <-chan error
	errCh      chan error // 写端，Close 时由 unsubscribe 关闭
	// OnClose, if non-nil, is invoked synchronously by Close before the
	// underlying unsubscribe runs. The hook receives the topic name.
	OnClose func(topic string)
}

// Close releases the subscription: removes the subscriber from the topic,
// closes the per-topic Ch and ErrCh, stops the timer, and runs OnClose if
// set. The broker may then garbage-collect the subscription if no
// subscribers remain.
func (sub *Subscription[T]) Close() error {
	if sub.OnClose != nil {
		sub.OnClose(sub.topic)
	}
	return sub.subscriber.unsubscribe(sub)
}

// SubscriptionOption configures a Subscribe call. SubscriptionOptions
// never fail; they just mutate a per-call options struct.
type SubscriptionOption[T any] func(*subscriptionOptions[T])

type subscriptionOptions[T any] struct {
	size    ChannelSize
	timeout time.Duration
}

// WithChannelSize sets the per-topic channel size for this subscription.
// See the Block / Single / Small / Medium / Large / Huge constants.
func WithChannelSize[T any](size ChannelSize) SubscriptionOption[T] {
	return func(opts *subscriptionOptions[T]) {
		opts.size = size
	}
}

// WithTimeout sets a sliding idle-timeout. A successful Publish resets
// the timer; if it elapses, ErrSubscriptionTimeout is delivered to ErrCh
// exactly once. A zero or negative duration disables the timer.
func WithTimeout[T any](timeout time.Duration) SubscriptionOption[T] {
	return func(opts *subscriptionOptions[T]) {
		opts.timeout = timeout
	}
}
