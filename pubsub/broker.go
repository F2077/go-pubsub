package pubsub

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/google/uuid"
)

// DefaultCapacity is the default maximum number of topics a Broker will hold
// before Publish / Subscribe returns ErrSubscriptionCapacityExceeded.
const DefaultCapacity = uint32(8192)

// ErrSubscriptionCapacityExceeded is returned by Publish and Subscribe when
// the broker already holds DefaultCapacity (or the value passed to
// WithCapacity) topics. The cap value is included in the message text
// returned by fmt.Errorf at the call site; the error itself does not
// currently expose the cap programmatically — use errors.Is to detect.
var ErrSubscriptionCapacityExceeded = errors.New("subscription capacity exceeded")

// ErrLoggerNil is returned by WithLogger when a nil *slog.Logger is passed.
var ErrLoggerNil = errors.New("logger cannot be nil")

// ErrBrokerIdEmpty is returned by WithId when an empty string is passed.
var ErrBrokerIdEmpty = errors.New("id cannot be empty")

// Broker owns the topic→subscription map and the global topic capacity cap.
// A single Broker is the only place a topic is registered, and all
// Publishers and Subscribers must share one.
//
// The zero value is not usable; construct with NewBroker.
//
// Broker is safe for concurrent use by multiple goroutines.
type Broker[T any] struct {
	logger  *slog.Logger
	rwMutex sync.RWMutex

	id            string
	subscriptions map[string]*subscription[T]
	capacity      uint32
}

// NewBroker creates a Broker with a random UUID id and DefaultCapacity,
// logging to os.Stdout at the default level. Use BrokerOptions to override
// the id, capacity, or logger.
func NewBroker[T any](options ...BrokerOption[T]) (*Broker[T], error) {
	b := &Broker[T]{
		logger:        slog.New(slog.NewTextHandler(os.Stdout, nil)),
		id:            uuid.New().String(),
		subscriptions: make(map[string]*subscription[T]),
		capacity:      DefaultCapacity,
	}
	for _, option := range options {
		if option == nil {
			continue
		}
		err := option(b)
		if err != nil {
			return nil, err
		}
	}
	return b, nil
}

// BrokerOption configures a Broker. BrokerOptions return an error so they
// can validate input (see WithLogger and WithId).
type BrokerOption[T any] func(*Broker[T]) error

// WithLogger sets the *slog.Logger the broker uses for internal lock-trace
// and lifecycle messages. Passing nil returns ErrLoggerNil and the broker
// is left unchanged.
func WithLogger[T any](logger *slog.Logger) BrokerOption[T] {
	return func(b *Broker[T]) error {
		if logger == nil {
			return ErrLoggerNil
		}
		b.logger = logger
		return nil
	}
}

// WithId overrides the random UUID assigned by NewBroker. Useful when the
// broker id is propagated through a wider system (tracing, log filtering).
// Passing an empty string returns ErrBrokerIdEmpty.
func WithId[T any](id string) BrokerOption[T] {
	return func(b *Broker[T]) error {
		if id == "" {
			return ErrBrokerIdEmpty
		}
		b.id = id
		return nil
	}
}

// WithCapacity sets the maximum number of topics the broker will hold.
// New topics above this cap cause Publish / Subscribe to return a wrapped
// ErrSubscriptionCapacityExceeded. The default is DefaultCapacity.
func WithCapacity[T any](capacity uint32) BrokerOption[T] {
	return func(b *Broker[T]) error {
		b.capacity = capacity
		return nil
	}
}

// Id returns the broker's identifier (a random UUID by default, or
// whatever was passed to WithId).
func (b *Broker[T]) Id() string {
	return b.id
}

// Capacity returns the maximum number of topics this broker will hold.
func (b *Broker[T]) Capacity() uint32 {
	return b.capacity
}

// Topics returns a snapshot slice of the topic names currently held by
// the broker. The returned slice is a copy; callers may mutate it freely.
//
// Note that topic reaping is asynchronous: when the last subscriber for a
// topic unsubscribes, the topic is removed in a separate goroutine to
// avoid a subscription→broker lock-order deadlock. A freshly-emptied topic
// may still appear in the returned slice for a short window.
func (b *Broker[T]) Topics() []string {
	if b.logger.Enabled(context.TODO(), slog.LevelDebug) {
		b.logger.Debug("Broker.Topics acquired read lock", slog.Any("broker", b))
	}
	b.rwMutex.RLock()
	defer func() {
		b.rwMutex.RUnlock()
		if b.logger.Enabled(context.TODO(), slog.LevelDebug) {
			b.logger.Debug("Broker.Topics released read lock", slog.Any("broker", b))
		}
	}()

	results := make([]string, 0, len(b.subscriptions))
	for k := range b.subscriptions {
		results = append(results, k)
	}
	return results
}

// String returns a stable human-readable form of the broker, in the format
// BROKER#<id>(cap: <n>). Implements fmt.Stringer.
func (b *Broker[T]) String() string {
	return fmt.Sprintf("BROKER#%s(cap: %d)", b.id, b.capacity)
}

func (b *Broker[T]) createOrLoadSubscription(topic string) (*subscription[T], error) {
	// 把 5 次重复的 logger.Enabled 折成一次。context.TODO() 在 logger
	// 实现里被直接丢弃（broker 自带的 TextHandler 不读 ctx），但走 slog
	// 文档要求传一个，所以留着。
	debug := b.logger.Enabled(context.TODO(), slog.LevelDebug)
	if debug {
		b.logger.Debug("Broker.createOrLoadSubscription acquired read lock", slog.Any("broker", b))
	}
	b.rwMutex.RLock()
	if sub, ok := b.subscriptions[topic]; ok {
		b.rwMutex.RUnlock()
		if debug {
			b.logger.Debug("Broker.createOrLoadSubscription released read lock", slog.Any("broker", b))
		}
		return sub, nil
	}
	b.rwMutex.RUnlock()
	if debug {
		b.logger.Debug("Broker.createOrLoadSubscription released read lock", slog.Any("broker", b))
	}

	b.rwMutex.Lock()
	if debug {
		b.logger.Debug("Broker.createOrLoadSubscription acquired write lock", slog.Any("broker", b))
	}
	defer func() {
		b.rwMutex.Unlock()
		if debug {
			b.logger.Debug("Broker.createOrLoadSubscription released write lock", slog.Any("broker", b))
		}
	}()

	// 再次检查防止竞态(也就是可能在上边的加读锁的检查topic对应订阅的时候其他协程创建了订阅)
	if sub, ok := b.subscriptions[topic]; ok {
		return sub, nil
	}

	// 仅在创建新主题时检查容量
	if len(b.subscriptions) >= int(b.capacity) {
		return nil, fmt.Errorf("%w (cap=%d)", ErrSubscriptionCapacityExceeded, b.capacity)
	}

	sub := newSubscription[T](b.logger, topic, b)
	b.subscriptions[topic] = sub
	return b.subscriptions[topic], nil
}

func (b *Broker[T]) tryRemoveSubscription(topic string) {
	if b.logger.Enabled(context.TODO(), slog.LevelDebug) {
		b.logger.Debug("Broker.tryRemoveSubscription acquired write lock", slog.Any("broker", b))
	}
	b.rwMutex.Lock()
	defer func() {
		b.rwMutex.Unlock()
		if b.logger.Enabled(context.TODO(), slog.LevelDebug) {
			b.logger.Debug("Broker.tryRemoveSubscription released write lock", slog.Any("broker", b))
		}
	}()

	// 当订阅中没有任何订阅者的时候就可以删除订阅了
	if sub, ok := b.subscriptions[topic]; ok && sub.isEmpty() {
		delete(b.subscriptions, topic)
	}
}

type subscription[T any] struct {
	logger  *slog.Logger
	rwMutex sync.RWMutex

	topic       string
	broker      *Broker[T]
	subscribers map[string]*Subscriber[T]
}

func newSubscription[T any](logger *slog.Logger, topic string, broker *Broker[T]) *subscription[T] {
	return &subscription[T]{
		logger:      logger,
		topic:       topic,
		broker:      broker,
		subscribers: make(map[string]*Subscriber[T]),
	}
}

func (s *subscription[T]) isEmpty() bool {
	if s.logger.Enabled(context.TODO(), slog.LevelDebug) {
		s.logger.Debug("subscription.isEmpty acquired read lock")
	}
	s.rwMutex.RLock()
	defer func() {
		s.rwMutex.RUnlock()
		if s.logger.Enabled(context.TODO(), slog.LevelDebug) {
			s.logger.Debug("subscription.isEmpty released read lock")
		}
	}()
	return len(s.subscribers) == 0
}

func (s *subscription[T]) addSubscriber(subscriber *Subscriber[T]) {
	if s.logger.Enabled(context.TODO(), slog.LevelDebug) {
		s.logger.Debug("subscription.addSubscriber acquired write lock")
	}
	s.rwMutex.Lock()
	defer func() {
		s.rwMutex.Unlock()
		if s.logger.Enabled(context.TODO(), slog.LevelDebug) {
			s.logger.Debug("subscription.addSubscriber released write lock")
		}
	}()

	s.subscribers[subscriber.id] = subscriber
}

func (s *subscription[T]) removeSubscriber(subscriber *Subscriber[T]) {
	if s.logger.Enabled(context.TODO(), slog.LevelDebug) {
		s.logger.Debug("subscription.removeSubscriber acquired write lock")
	}
	s.rwMutex.Lock()
	defer func() {
		s.rwMutex.Unlock()
		if s.logger.Enabled(context.TODO(), slog.LevelDebug) {
			s.logger.Debug("subscription.removeSubscriber released write lock")
		}
	}()

	delete(s.subscribers, subscriber.id)
	// 通知 Broker 检查并(如果可以的话)清理订阅(也就是当前订阅有可能删除的是最后一个订阅者，这种情况下中间人就应当删除此订阅了)
	// 注意这里使用了独立的协程执行
	// 解决了： 避免锁嵌套死锁(协程解耦：将 broker.tryRemoveSubscription 放到新协程中，脱离当前锁的作用域，打破锁顺序依赖)
	//    若当前协程直接调用 tryRemoveSubscription 则当前路径持有锁的顺序为 subscription 锁（写）→ broker 锁（写），如果其他路径持有锁的顺序为 broker 锁 → subscription 锁，则会导致死锁。
	// 引发了潜在问题：
	//    异步调用可能导致 Broker 检查时订阅者列表已变更。
	// 如何解决的：
	//    tryRemoveSubscription 内部会检查订阅中订阅者是否为空
	go s.broker.tryRemoveSubscription(s.topic)
}

func (s *subscription[T]) deliver(message T) {
	// 把 lock 范围缩到只 snapshot subscribers 这一步：map iteration 在
	// Go 1.21+ 是 stable 的，但 release 锁之后可以避免长尾 N=10000 时
	// 的 Subscribe/unsubscribe 阻塞。每个 subscriber 的 channels 是
	// sync.Map（lock-free），resetTimer 拿的是 subscriber.mutex 跟
	// 订阅锁无关——所以后续 fan-out 不需要订阅锁。
	debug := s.logger.Enabled(context.TODO(), slog.LevelDebug)
	if debug {
		s.logger.Debug("subscription.deliver acquired read lock")
	}
	s.rwMutex.RLock()
	snapshot := make([]*Subscriber[T], 0, len(s.subscribers))
	for _, sub := range s.subscribers {
		snapshot = append(snapshot, sub)
	}
	s.rwMutex.RUnlock()
	if debug {
		s.logger.Debug("subscription.deliver released read lock")
	}

	for _, subscriber := range snapshot {
		// 持 subscriber.mutex 读 channel 并 send：与 unsubscribe 的持锁 close 互斥，
		// race-safe 地消除 send-on-closed 竞态。锁内只做 non-blocking send（default
		// drop），不阻塞、不长持锁；resetTimer 在锁外调用，避免与 resetTimer 自身
		// 拿 subscriber.mutex 形成重入死锁。channels[topic] 不存在即订阅者已
		// unsubscribe，跳过即可。
		delivered := false
		subscriber.mutex.Lock()
		if ch, ok := subscriber.channels[s.topic]; ok {
			select {
			case ch <- message:
				delivered = true
			default:
			}
		}
		subscriber.mutex.Unlock()
		if delivered {
			subscriber.resetTimer(s.topic)
		}
	}
}
