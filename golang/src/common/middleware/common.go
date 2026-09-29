package middleware

import (
	"errors"

	amqp "github.com/rabbitmq/amqp091-go"
)

func closeResources(conn *amqp.Connection, channel *amqp.Channel) error {
	connErr := conn.Close()
	chErr := channel.Close()
	return errors.Join(connErr, chErr)
}

func consumeMessages(queueName string, channel *amqp.Channel, consumerTag string, callbackFunc func(msg Message, ack func(), nack func())) error {
	msgs, err := channel.Consume(
		queueName,
		consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return ErrMessageMiddlewareMessage
	}
	for d := range msgs {
		msg := Message{Body: string(d.Body)}
		ack := func() {
			err := d.Ack(false)
			if err != nil {
				return
			}
		}
		nack := func() {
			err := d.Nack(false, false)
			if err != nil {
				return
			}
		}
		callbackFunc(msg, ack, nack)
	}
	return nil
}

func cancelChannel(queueName string, channel *amqp.Channel) error {
	if err := channel.Cancel(queueName, false); err != nil {
		if channel.IsClosed() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}
	return nil
}

func publish(channel *amqp.Channel, exchange string, routingKey, body string) error {
	err := channel.Publish(
		exchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(body),
		})
	if err != nil {
		return ErrMessageMiddlewareMessage
	}
	return nil
}

func queueDeclare(channel *amqp.Channel, name string, durable bool, exclusive bool) (string, error) {
	queue, err := channel.QueueDeclare(name, durable, false, exclusive, false, nil)
	if err != nil {
		return "", err
	}
	return queue.Name, err
}

func checkResources(isConsuming bool, connection *amqp.Connection, channel *amqp.Channel) error {
	if isConsuming {
		return ErrMessageMiddlewareMessage
	}
	if connection.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	if channel.IsClosed() {
		return ErrMessageMiddlewareMessage
	}
	return nil
}
