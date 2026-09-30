package middleware

import (
	"errors"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
)

type WorkQueueMiddleware struct {
	QueueName   string
	Connection  *amqp.Connection
	Channel     *amqp.Channel
	isConsuming atomic.Bool
}

func NewWorkQueueMiddleware(queueName string, conn *amqp.Connection, channel *amqp.Channel) (*WorkQueueMiddleware, error) {
	_, err := queueDeclare(channel, queueName, WORK_QUEUE_DURABILITY, WORK_QUEUE_EXCLUSIVITY)
	if err != nil {
		closeErr := closeResources(conn, channel)
		return nil, errors.Join(err, closeErr)
	}
	return &WorkQueueMiddleware{
		QueueName:  queueName,
		Connection: conn,
		Channel:    channel,
	}, nil
}

func (q *WorkQueueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	err := checkResources(q.isConsuming.Load(), q.Connection, q.Channel)
	if err != nil {
		return err
	}
	err = q.Channel.Qos(
		PREFETCH_COUNT,
		PREFETCH_SIZE,
		GLOBAL,
	)
	if err != nil {
		if q.Channel.IsClosed() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}
	q.isConsuming.Store(true)
	if err := consumeMessages(q.QueueName, q.Channel, q.QueueName, callbackFunc); err != nil {
		q.isConsuming.Store(false)
		return err
	}
	if q.isConsuming.Load() {
		return ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (q *WorkQueueMiddleware) StopConsuming() error {
	if q.Connection.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	if !q.isConsuming.Load() {
		return nil
	}
	q.isConsuming.Store(false)
	return cancelChannel(q.QueueName, q.Channel)
}

func (q *WorkQueueMiddleware) Send(msg Message) error {
	if q.Channel.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	if q.Connection.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	return publish(q.Channel, DEFAULT_EXCHANGE, q.QueueName, msg.Body)
}

func (q *WorkQueueMiddleware) Close() error {
	if err := closeResources(q.Connection, q.Channel); err != nil {
		return ErrMessageMiddlewareClose
	}
	return nil
}
