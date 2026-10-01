package middleware

import (
	"strconv"

	amqp "github.com/rabbitmq/amqp091-go"
)

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	conn, err := amqp.Dial("amqp://" + connectionSettings.Hostname + ":" + strconv.Itoa(connectionSettings.Port))
	if err != nil {
		return nil, err
	}
	sendChannel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}
	recvChannel, err := conn.Channel()
	if err != nil {
		sendChannel.Close()
		conn.Close()
		return nil, err
	}

	_, err = sendChannel.QueueDeclare(
		queueName,
		true,  // durable
		false, // autodelete
		false, // exclusive
		false, // nowait
		nil,   // args
	)
	if err != nil {
		sendChannel.Close()
		recvChannel.Close()
		conn.Close()
		return nil, err
	}

	return NewQueueMiddleware(conn, sendChannel, recvChannel, queueName), nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	conn, err := amqp.Dial("amqp://" + connectionSettings.Hostname + ":" + strconv.Itoa(connectionSettings.Port))
	if err != nil {
		return nil, err
	}
	sendChannel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}
	recvChannel, err := conn.Channel()
	if err != nil {
		sendChannel.Close()
		conn.Close()
		return nil, err
	}

	err = sendChannel.ExchangeDeclare(
		exchange,
		"topic",
		true,  // durable
		false, // autodelete
		false, // internal
		false, // nowait
		nil,   // args
	)
	if err != nil {
		recvChannel.Close()
		sendChannel.Close()
		conn.Close()
		return nil, err
	}

	q, err := recvChannel.QueueDeclare(
		"",    // name
		false, // durable
		true,  // autodelete
		true,  // exclusive
		false, // nowait
		nil,   // args
	)
	if err != nil {
		recvChannel.Close()
		sendChannel.Close()
		conn.Close()
		return nil, err
	}

	for _, routingKey := range keys {
		err := recvChannel.QueueBind(
			q.Name,
			routingKey,
			exchange,
			false, // nowait
			nil,   // args
		)
		if err != nil {
			recvChannel.Close()
			sendChannel.Close()
			conn.Close()
			return nil, err
		}
	}
	return NewExchangeMiddleware(conn, sendChannel, recvChannel, exchange, q.Name, keys), nil
}
