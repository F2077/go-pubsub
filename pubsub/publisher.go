package pubsub

import (
	"fmt"

	"github.com/google/uuid"
)

// Publisher publishes messages to topics on a Broker. Each Publisher has
// its own UUID so logs and introspection can attribute publishes to a
// specific handle.
//
// The zero value is not usable; construct with NewPublisher.
type Publisher[T any] struct {
	id     string
	broker *Broker[T]
}

// NewPublisher creates a Publisher bound to the given broker. The publisher
// is identified by a random UUID; this id appears in String and any log
// message that includes the publisher.
func NewPublisher[T any](broker *Broker[T]) *Publisher[T] {
	return &Publisher[T]{
		id:     uuid.New().String(),
		broker: broker,
	}
}

// String returns the publisher's debug form: PUBLISHER#<id>@<broker>.
// Implements fmt.Stringer.
func (p *Publisher[T]) String() string {
	return fmt.Sprintf("PUBLISHER#%s@%s", p.id, p.broker)
}

// Id returns the publisher's UUID.
func (p *Publisher[T]) Id() string {
	return p.id
}

// Publish resolves (or creates) the subscription for topic and delivers
// message to every subscriber on that topic non-blockingly. Returns a
// wrapped ErrSubscriptionCapacityExceeded if creating a new topic would
// push the broker over its capacity.
//
// Publish never blocks: if a subscriber's per-topic channel is full, the
// message is dropped for that subscriber. There is no acknowledgement
// path; successful return only means the broker accepted the message.
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
