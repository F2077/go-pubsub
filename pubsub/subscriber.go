package pubsub

import (
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
// Subscriber is safe for concurrent use by multiple goroutines.
type Subscriber[T any] struct {
	mutex sync.Mutex

	id          string
	broker      *Broker[T]
	topics      map[string]struct{}
	channels    sync.Map
	errChannels sync.Map
	closed      bool

	timers     map[string]*time.Timer
	timeouts   map[string]time.Duration
	timerDones map[string]chan struct{}
}

// NewSubscriber creates a Subscriber bound to the given broker, with its
// own UUID and an empty topic set. Use Subscribe to attach to topics.
func NewSubscriber[T any](broker *Broker[T]) *Subscriber[T] {
	return &Subscriber[T]{
		id:         uuid.New().String(),
		broker:     broker,
		topics:     map[string]struct{}{},
		timers:     make(map[string]*time.Timer),
		timeouts:   make(map[string]time.Duration),
		timerDones: make(map[string]chan struct{}),
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
	s.broker.logger.Debug("Subscriber.Subscribe acquired lock", slog.Any("subscriber", s))
	defer func() {
		s.mutex.Unlock()
		s.broker.logger.Debug("Subscriber.Subscribe released lock", slog.Any("subscriber", s))
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

	// 创建对应主题的错误通道（仅在启用 timeout 时才分配）。
	// 无 timeout 时 ErrCh 留 nil——receive-only nil channel 永久阻塞，
	// 对不可能超时的订阅者是正确行为。
	errCh := func() chan error {
		if options.timeout <= 0 {
			return nil
		}
		raw, _ := s.errChannels.LoadOrStore(topic, make(chan error, 1))
		return raw.(chan error)
	}()

	// 将当前订阅者添加到订阅
	sub.addSubscriber(s)
	// 将主题添加到订阅者主题集合
	s.topics[topic] = struct{}{}

	// 设置超时逻辑（仅当 timeout > 0 时）
	if options.timeout > 0 {
		// 停掉旧的 timer + 旧 fire goroutine（re-Subscribe 同 topic 的情况）
		if oldDone, ok := s.timerDones[topic]; ok {
			close(oldDone)
			delete(s.timerDones, topic)
		}
		if oldTimer, ok := s.timers[topic]; ok {
			oldTimer.Stop()
			delete(s.timers, topic)
		}

		// 创建常驻 timer + 专用的 fire goroutine
		s.timeouts[topic] = options.timeout
		s.broker.logger.Debug("subscription timeout configured",
			slog.Any("topic", topic),
			slog.Any("timeout", options.timeout),
		)
		t := time.NewTimer(options.timeout)
		s.timers[topic] = t

		done := make(chan struct{})
		s.timerDones[topic] = done
		go s.runTopicTimer(topic, t, errCh, done)
	}

	return &Subscription[T]{
		topic:      topic,
		subscriber: s,
		Ch:         ch.(chan T),
		ErrCh:      errCh,
	}, nil
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

	// 关闭意味着取消对于所有主题的订阅
	for topic := range s.topics {
		err := s.unsubscribe(topic)
		if err != nil {
			return err
		}
	}
	s.closed = true
	return nil
}

func (s *Subscriber[T]) unsubscribe(topic string) error {
	s.mutex.Lock()
	s.broker.logger.Debug("Subscriber.unsubscribe acquired lock", slog.Any("subscriber", s))
	defer func() {
		s.mutex.Unlock()
		s.broker.logger.Debug("Subscriber.unsubscribe released lock", slog.Any("subscriber", s))
	}()

	if s.closed {
		return ErrSubscriberClosed
	}

	// 将自身从订阅中移除
	// 获取 Broker 的读锁以安全访问 subscriptions
	s.broker.rwMutex.RLock()
	sub, ok := s.broker.subscriptions[topic]
	s.broker.rwMutex.RUnlock()

	if ok {
		sub.removeSubscriber(s)
	}

	// 关闭对应主题的消息通道
	if ch, ok := s.channels.LoadAndDelete(topic); ok {
		close(ch.(chan T))
	}

	// 关闭对应主题的错误通道
	if errCh, ok := s.errChannels.LoadAndDelete(topic); ok {
		close(errCh.(chan error))
	}

	// 从主题集合中删除主题
	delete(s.topics, topic)

	// 停止并删除定时器
	if t, ok := s.timers[topic]; ok {
		t.Stop()
		delete(s.timers, topic)
		delete(s.timeouts, topic)
	}
	if done, ok := s.timerDones[topic]; ok {
		close(done)
		delete(s.timerDones, topic)
	}

	return nil
}

// runTopicTimer 在独立 goroutine 里循环读 t.C，每次 fire 调 handleTimeout。
// 收到 done 信号就停掉 timer 并退出。该 goroutine 的生命周期 == 该 topic
// 的订阅生命周期（Subscribe 创建，unsubscribe 通过 close(done) 终止）。
func (s *Subscriber[T]) runTopicTimer(topic string, t *time.Timer, errCh chan error, done <-chan struct{}) {
	for {
		select {
		case <-t.C:
			s.handleTimeout(topic, errCh)
		case <-done:
			t.Stop()
			return
		}
	}
}

// 重置定时器
func (s *Subscriber[T]) resetTimer(topic string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if timeout, ok := s.timeouts[topic]; ok && timeout > 0 {
		if t, ok := s.timers[topic]; ok {
			// 官方推荐模式：Stop + drain 防止 stale channel value 被 fire
			// goroutine 误读走，触发虚假的 timeout。
			if !t.Stop() {
				select {
				case <-t.C:
				default:
				}
			}
			t.Reset(timeout)
		}
	}
}

func (s *Subscriber[T]) handleTimeout(topic string, errCh chan<- error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.broker.logger.Debug("subscription timeout fired", slog.Any("topic", topic))
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
	return sub.subscriber.unsubscribe(sub.topic)
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
