package pubsub

import (
	"fmt"
	"github.com/google/uuid"
)

type Publisher[T any] struct {
	id     string
	broker *Broker[T]
}

func NewPublisher[T any](broker *Broker[T]) *Publisher[T] {
	return &Publisher[T]{
		id:     uuid.New().String(),
		broker: broker,
	}
}

func (p *Publisher[T]) String() string {
	return fmt.Sprintf("PUBLISHER#%s@%s", p.id, p.broker)
}

func (p *Publisher[T]) Id() string {
	return p.id
}

func (p *Publisher[T]) Publish(topic string, message T) error {
	// 获取或创建主题的订阅
	sub, err := p.broker.createOrLoadSubscription(topic)
	if err != nil {
		return err
	}
	// 执行订阅的消息递送
	sub.deliver(message)
	return nil
}
