package middleware

import (
	"errors"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct {
	exchange    string
	bindings    []string
	Connection  *amqp.Connection
	Channel     *amqp.Channel
	QueueName   string
	isConsuming atomic.Bool
}

func NewExchangeMiddleware(exchange string, keys []string, conn *amqp.Connection, channel *amqp.Channel) (*ExchangeMiddleware, error) {
	err := channel.ExchangeDeclare(exchange, DIRECT_EXCHANGE, true, false, false, false, nil)
	if err != nil {
		closeErr := closeResources(conn, channel)
		return nil, errors.Join(err, closeErr)
	}

	return &ExchangeMiddleware{
		exchange:   exchange,
		bindings:   keys,
		Connection: conn,
		Channel:    channel,
	}, nil
}

func (e *ExchangeMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	err := checkResources(e.isConsuming.Load(), e.Connection, e.Channel)
	if err != nil {
		return err
	}
	queueName, err := queueDeclare(e.Channel, DEFAULT_QUEUE_NAME, EXCHANGE_QUEUE_DURABILITY, EXCHANGE_QUEUE_EXCLUSIVITY)
	if err != nil {
		if e.Channel.IsClosed() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}
	e.QueueName = queueName
	for _, key := range e.bindings {
		err = e.Channel.QueueBind(
			queueName,
			key,
			e.exchange,
			false,
			nil)
		if err != nil {
			if e.Channel.IsClosed() {
				return ErrMessageMiddlewareDisconnected
			}
			return ErrMessageMiddlewareMessage
		}
	}
	e.isConsuming.Store(true)
	if err := consumeMessages(e.QueueName, e.Channel, e.QueueName, callbackFunc); err != nil {
		e.isConsuming.Store(false)
		return err
	}
	if e.isConsuming.Load() {
		return ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (e *ExchangeMiddleware) StopConsuming() error {
	if e.Connection.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	if !e.isConsuming.Load() {
		return nil
	}
	e.isConsuming.Store(false)
	return cancelChannel(e.QueueName, e.Channel)
}

func (e *ExchangeMiddleware) Send(msg Message) error {
	if e.Channel.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	if e.Connection.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}

	for _, key := range e.bindings {
		if err := publish(e.Channel, e.exchange, key, msg.Body); err != nil {
			return err
		}
	}
	return nil
}

func (e *ExchangeMiddleware) SendTo(msg Message, routingKey string) error {
	if e.Channel.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	if e.Connection.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}

	if err := publish(e.Channel, e.exchange, routingKey, msg.Body); err != nil {
		return err
	}
	return nil
}

func (e *ExchangeMiddleware) Close() error {
	if err := closeResources(e.Connection, e.Channel); err != nil {
		return ErrMessageMiddlewareClose
	}
	return nil
}
