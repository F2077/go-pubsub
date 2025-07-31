package pubsub

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"log/slog"
	"sync"
	"time"
)

var (
	ErrSubscriberClosed    = errors.New("subscriber is closed")
	ErrSubscriptionTimeout = errors.New("subscription timeout")
)

type ChannelSize uint16

var (
	Block  ChannelSize = 0
	Single ChannelSize = 1
	Small              = Single * 10
	Medium             = Small * 10
	Large              = Medium * 10
	Huge               = Large * 10
)

var (
	DefaultChannelSize = Medium
	DefaultTimeout     = 0 * time.Second
)

type Subscriber[T any] struct {
	mutex sync.Mutex

	id          string
	broker      *Broker[T]
	topics      map[string]struct{}
	channels    sync.Map
	errChannels sync.Map
	closed      bool

	timers   map[string]*time.Timer
	timeouts map[string]time.Duration
}

func NewSubscriber[T any](broker *Broker[T]) *Subscriber[T] {
	return &Subscriber[T]{
		id:       uuid.New().String(),
		broker:   broker,
		topics:   map[string]struct{}{},
		timers:   make(map[string]*time.Timer),
		timeouts: make(map[string]time.Duration),
	}
}

func (s *Subscriber[T]) String() string {
	return fmt.Sprintf("SUBSCRIBER#%s@%s", s.id, s.broker)
}

func (s *Subscriber[T]) Id() string {
	return s.id
}

func (s *Subscriber[T]) Subscribe(topic string, opts ...SubscriptionOption[T]) (*Subscription[T], error) {
	options := &subscriptionOptions[T]{
		size:    DefaultChannelSize,
		timeout: DefaultTimeout,
	}
	for _, opt := range opts {
		opt(options)
	}

	s.mutex.Lock()
	s.broker.logger.Debug("Subscriber.Subscribe 获得了🔒", slog.Any("Subscriber", s))
	defer func() {
		s.mutex.Unlock()
		s.broker.logger.Debug("Subscriber.Subscribe 释放了🔒", slog.Any("Subscriber", s))
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

	// 创建对应主题的错误通道
	errCh, _ := s.errChannels.LoadOrStore(topic, make(chan error, 1))

	// 将当前订阅者添加到订阅
	sub.addSubscriber(s)
	// 将主题添加到订阅者主题集合
	s.topics[topic] = struct{}{}

	// 设置超时逻辑（仅当 timeout > 0 时）
	if options.timeout > 0 {
		// 停止旧的定时器（如果存在）
		if oldTimer, ok := s.timers[topic]; ok {
			oldTimer.Stop()
		}

		// 创建新定时器
		s.timeouts[topic] = options.timeout
		s.broker.logger.Debug("订阅超时时间", slog.Any("topic", topic), slog.Any("timeout", options.timeout))
		timer := time.AfterFunc(options.timeout, func() {
			s.handleTimeout(topic, errCh.(chan error))
		})
		s.timers[topic] = timer
	}

	return &Subscription[T]{
		topic:      topic,
		subscriber: s,
		Ch:         ch.(chan T),
		ErrCh:      errCh.(chan error),
	}, nil
}

func (s *Subscriber[T]) Subscribes(topics []string, opts ...SubscriptionOption[T]) ([]*Subscription[T], error) {
	if s.closed {
		return nil, ErrSubscriberClosed
	}

	subs := make([]*Subscription[T], len(topics), len(topics))
	for i, topic := range topics {
		sub, err := s.Subscribe(topic, opts...)
		if err != nil {
			return nil, err
		}
		subs[i] = sub
	}
	return subs, nil
}

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
	s.broker.logger.Debug("Subscriber.unsubscribe 获得了🔒", slog.Any("Subscriber", s))
	defer func() {
		s.mutex.Unlock()
		s.broker.logger.Debug("Subscriber.unsubscribe 释放了🔒", slog.Any("Subscriber", s))
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
	if timer, ok := s.timers[topic]; ok {
		timer.Stop()
		delete(s.timers, topic)
		delete(s.timeouts, topic)
	}

	return nil
}

// 重置定时器
func (s *Subscriber[T]) resetTimer(topic string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if timeout, ok := s.timeouts[topic]; ok && timeout > 0 {
		if timer, ok := s.timers[topic]; ok {
			timer.Stop()
			newTimer := time.AfterFunc(timeout, func() {
				if errCh, ok := s.errChannels.Load(topic); ok {
					s.handleTimeout(topic, errCh.(chan error))
				}
			})
			s.timers[topic] = newTimer
		}
	}
}

func (s *Subscriber[T]) handleTimeout(topic string, errCh chan<- error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.broker.logger.Debug("订阅超时处理", slog.Any("topic", topic))
	// 检查是否仍订阅该主题
	if _, ok := s.topics[topic]; !ok {
		return
	}

	// 发送超时错误
	errCh <- ErrSubscriptionTimeout
}

type Subscription[T any] struct {
	topic      string
	subscriber *Subscriber[T]
	Ch         <-chan T
	ErrCh      <-chan error
	OnClose    func(topic string)
}

func (sub *Subscription[T]) Close() error {
	if sub.OnClose != nil {
		sub.OnClose(sub.topic)
	}
	return sub.subscriber.unsubscribe(sub.topic)
}

type SubscriptionOption[T any] func(*subscriptionOptions[T])

type subscriptionOptions[T any] struct {
	size    ChannelSize
	timeout time.Duration
}

// WithChannelSize 设置通道大小
func WithChannelSize[T any](size ChannelSize) SubscriptionOption[T] {
	return func(opts *subscriptionOptions[T]) {
		opts.size = size
	}
}

// WithTimeout 设置超时时间（大于 0 表示启用超时）
func WithTimeout[T any](timeout time.Duration) SubscriptionOption[T] {
	return func(opts *subscriptionOptions[T]) {
		opts.timeout = timeout
	}
}
