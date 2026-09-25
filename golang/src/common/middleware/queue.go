package middleware

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	conn        *amqp.Connection
	sendChannel *amqp.Channel
	recvChannel *amqp.Channel
	queueName   string
	consumerTag string
}

func NewQueueMiddleware(conn *amqp.Connection, sendChannel *amqp.Channel, recvChannel *amqp.Channel, queueName string) *QueueMiddleware {
	consumerTag := GenerateNewConsumerTag(queueName)
	return &QueueMiddleware{
		conn:        conn,
		sendChannel: sendChannel,
		recvChannel: recvChannel,
		queueName:   queueName,
		consumerTag: consumerTag,
	}
}

func (qm *QueueMiddleware) Send(msg Message) error {
	body := []byte(msg.Body)

	err := qm.sendChannel.Publish(
		"",
		qm.queueName,
		false, // mandatory
		false, // inmediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	if err != nil {
		if qm.sendChannel.IsClosed() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}
	return nil
}

func (qm *QueueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	msgs, err := qm.recvChannel.Consume(
		qm.queueName,
		qm.consumerTag, // consumerTag
		false,          // autoack
		false,          // exclusive
		false,          // nolocal
		false,          // nowait
		nil,            // args
	)
	if err != nil {
		if qm.recvChannel.IsClosed() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}

	for d := range msgs {
		ProcessDelivery(d, callbackFunc)
	}
	return nil
}

func (qm *QueueMiddleware) StopConsuming() error {
	err := qm.recvChannel.Cancel(qm.consumerTag, false)
	if err != nil {
		return ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (qm *QueueMiddleware) Close() error {
	return CloseAllResources(qm.conn, qm.sendChannel, qm.recvChannel)
}
