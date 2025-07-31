package pubsub

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"log/slog"
	"os"
	"sync"
)

const (
	DefaultCapacity = uint32(8192)
)

var (
	SubscriptionCapacityExceed = errors.New("subscription capacity exceed")
)

type Broker[T any] struct {
	logger  *slog.Logger
	rwMutex sync.RWMutex

	id            string
	subscriptions map[string]*subscription[T]
	capacity      uint32
}

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

type BrokerOption[T any] func(*Broker[T]) error

func WithLogger[T any](logger *slog.Logger) BrokerOption[T] {
	return func(b *Broker[T]) error {
		if logger == nil {
			return errors.New("logger cannot be nil")
		}
		b.logger = logger
		return nil
	}
}

func WithId[T any](id string) BrokerOption[T] {
	return func(b *Broker[T]) error {
		if id == "" {
			return errors.New("id cannot be empty")
		}
		b.id = id
		return nil
	}
}

func WithCapacity[T any](capacity uint32) BrokerOption[T] {
	return func(b *Broker[T]) error {
		b.capacity = capacity
		return nil
	}
}

func (b *Broker[T]) Id() string {
	return b.id
}

func (b *Broker[T]) Capacity() uint32 {
	return b.capacity
}

func (b *Broker[T]) Topics() []string {
	b.rwMutex.RLock()
	b.logger.Debug("Broker.Topics 获得了📖🔒", slog.Any("Broker", b))
	defer func() {
		b.rwMutex.RUnlock()
		b.logger.Debug("Broker.Topics 释放了📖🔒", slog.Any("Broker", b))
	}()

	results := make([]string, 0)
	for k := range b.subscriptions {
		results = append(results, k)
	}
	return results
}

func (b *Broker[T]) String() string {
	return fmt.Sprintf("BROKER#%s(cap: %d)", b.id, b.capacity)
}

func (b *Broker[T]) createOrLoadSubscription(topic string) (*subscription[T], error) {
	b.rwMutex.RLock()
	b.logger.Debug("Broker.createOrLoadSubscription 获得了📖🔒", slog.Any("Broker", b))
	if sub, ok := b.subscriptions[topic]; ok {
		b.rwMutex.RUnlock()
		b.logger.Debug("Broker.createOrLoadSubscription 释放了📖🔒", slog.Any("Broker", b))
		return sub, nil
	}
	b.rwMutex.RUnlock()
	b.logger.Debug("Broker.createOrLoadSubscription 释放了📖🔒", slog.Any("Broker", b))

	b.rwMutex.Lock()
	b.logger.Debug("Broker.createOrLoadSubscription 获得了✍️🔒", slog.Any("Broker", b))
	defer func() {
		b.rwMutex.Unlock()
		b.logger.Debug("Broker.createOrLoadSubscription 释放了✍️🔒", slog.Any("Broker", b))
	}()

	// 再次检查防止竞态(也就是可能在上边的加读锁的检查topic对应订阅的时候其他协程创建了订阅)
	if sub, ok := b.subscriptions[topic]; ok {
		return sub, nil
	}

	// 仅在创建新主题时检查容量
	if len(b.subscriptions) >= int(b.capacity) {
		return nil, fmt.Errorf("%w subscription capacity exceeds %d", SubscriptionCapacityExceed, b.capacity)
	}

	sub := newSubscription[T](b.logger, topic, b)
	b.subscriptions[topic] = sub
	return b.subscriptions[topic], nil
}

func (b *Broker[T]) tryRemoveSubscription(topic string) {
	b.rwMutex.Lock()
	b.logger.Debug("Broker.tryRemoveSubscription 获得了✍️🔒", slog.Any("Broker", b))
	defer func() {
		b.rwMutex.Unlock()
		b.logger.Debug("Broker.tryRemoveSubscription 释放了✍️🔒", slog.Any("Broker", b))
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
	s.rwMutex.RLock()
	s.logger.Debug("subscription.isEmpty 获得了📖🔒")
	defer func() {
		s.rwMutex.RUnlock()
		s.logger.Debug("subscription.isEmpty 释放了📖🔒")
	}()
	return len(s.subscribers) == 0
}

func (s *subscription[T]) addSubscriber(subscriber *Subscriber[T]) {
	s.rwMutex.Lock()
	s.logger.Debug("subscription.addSubscriber 获得了✍️🔒")
	defer func() {
		s.rwMutex.Unlock()
		s.logger.Debug("subscription.addSubscriber 释放了✍️🔒")
	}()

	s.subscribers[subscriber.id] = subscriber
}

func (s *subscription[T]) removeSubscriber(subscriber *Subscriber[T]) {
	s.rwMutex.Lock()
	s.logger.Debug("subscription.removeSubscriber 获得了✍️🔒")
	defer func() {
		s.rwMutex.Unlock()
		s.logger.Debug("subscription.removeSubscriber 释放了✍️🔒")
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
	s.rwMutex.RLock()
	s.logger.Debug("subscription.deliver 获得了📖🔒")
	defer func() {
		s.rwMutex.RUnlock()
		s.logger.Debug("subscription.deliver 释放了📖🔒")
	}()

	for _, subscriber := range s.subscribers {
		// 仅发送到对应主题的 Channel
		if ch, ok := subscriber.channels.Load(s.topic); ok {
			select {
			case ch.(chan T) <- message:
				// 消息成功送达，重置定时器
				subscriber.resetTimer(s.topic)
			default:
				// Drop message if channel full
			}
		}
	}
}
