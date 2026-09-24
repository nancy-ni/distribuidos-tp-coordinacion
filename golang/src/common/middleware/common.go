package middleware

import (
	"fmt"
	"math/rand/v2"

	amqp "github.com/rabbitmq/amqp091-go"
)

func ProcessDelivery(d amqp.Delivery, callbackFunc func(msg Message, ack func(), nack func())) {
	msg := Message{Body: string(d.Body)}

	ack := func() { d.Ack(false) }
	nack := func() { d.Nack(false, true) }

	callbackFunc(msg, ack, nack)
}

func CloseAllResources(conn *amqp.Connection, sendChannel *amqp.Channel, recvChannel *amqp.Channel) error {
	err := sendChannel.Close()
	if err != nil {
		return ErrMessageMiddlewareClose
	}
	err = recvChannel.Close()
	if err != nil {
		return ErrMessageMiddlewareClose
	}
	err = conn.Close()
	if err != nil {
		return ErrMessageMiddlewareClose
	}
	return nil
}

func GenerateNewConsumerTag(name string) string {
	return fmt.Sprintf("consumer-tag-%s-%d", name, rand.Uint64())
}
